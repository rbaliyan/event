package poison

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()

	t.Run("IncrementFailure increments count", func(t *testing.T) {
		store := NewMemoryStore()

		count, err := store.IncrementFailure(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IncrementFailure failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %d", count)
		}

		count, err = store.IncrementFailure(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IncrementFailure failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}
	})

	t.Run("GetFailureCount returns current count", func(t *testing.T) {
		store := NewMemoryStore()

		count, err := store.GetFailureCount(ctx, "msg-1")
		if err != nil {
			t.Fatalf("GetFailureCount failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected count 0, got %d", count)
		}

		store.IncrementFailure(ctx, "msg-1")
		store.IncrementFailure(ctx, "msg-1")

		count, err = store.GetFailureCount(ctx, "msg-1")
		if err != nil {
			t.Fatalf("GetFailureCount failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}
	})

	t.Run("MarkPoison and IsPoison", func(t *testing.T) {
		store := NewMemoryStore()

		// Not poisoned initially
		isPoisoned, err := store.IsPoison(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IsPoison failed: %v", err)
		}
		if isPoisoned {
			t.Error("expected not poisoned initially")
		}

		// Mark as poison
		err = store.MarkPoison(ctx, "msg-1", time.Hour)
		if err != nil {
			t.Fatalf("MarkPoison failed: %v", err)
		}

		// Now poisoned
		isPoisoned, err = store.IsPoison(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IsPoison failed: %v", err)
		}
		if !isPoisoned {
			t.Error("expected poisoned after MarkPoison")
		}
	})

	t.Run("IsPoison returns false after TTL expires", func(t *testing.T) {
		store := NewMemoryStore()

		store.MarkPoison(ctx, "msg-1", 10*time.Millisecond)

		// Poisoned immediately
		isPoisoned, _ := store.IsPoison(ctx, "msg-1")
		if !isPoisoned {
			t.Error("expected poisoned before expiry")
		}

		// Wait for expiry
		time.Sleep(20 * time.Millisecond)

		// Not poisoned after expiry
		isPoisoned, _ = store.IsPoison(ctx, "msg-1")
		if isPoisoned {
			t.Error("expected not poisoned after expiry")
		}
	})

	t.Run("ClearPoison removes quarantine", func(t *testing.T) {
		store := NewMemoryStore()

		store.MarkPoison(ctx, "msg-1", time.Hour)

		err := store.ClearPoison(ctx, "msg-1")
		if err != nil {
			t.Fatalf("ClearPoison failed: %v", err)
		}

		isPoisoned, _ := store.IsPoison(ctx, "msg-1")
		if isPoisoned {
			t.Error("expected not poisoned after ClearPoison")
		}
	})

	t.Run("ClearFailures resets count", func(t *testing.T) {
		store := NewMemoryStore()

		store.IncrementFailure(ctx, "msg-1")
		store.IncrementFailure(ctx, "msg-1")

		err := store.ClearFailures(ctx, "msg-1")
		if err != nil {
			t.Fatalf("ClearFailures failed: %v", err)
		}

		count, _ := store.GetFailureCount(ctx, "msg-1")
		if count != 0 {
			t.Errorf("expected count 0 after ClearFailures, got %d", count)
		}
	})

	t.Run("Cleanup removes expired quarantine entries", func(t *testing.T) {
		store := NewMemoryStore()

		store.MarkPoison(ctx, "msg-1", 10*time.Millisecond)
		store.MarkPoison(ctx, "msg-2", time.Hour)

		time.Sleep(20 * time.Millisecond)

		store.Cleanup()

		// msg-1 should be cleaned up
		isPoisoned1, _ := store.IsPoison(ctx, "msg-1")
		if isPoisoned1 {
			t.Error("expected msg-1 to be cleaned up")
		}

		// msg-2 should still be quarantined
		isPoisoned2, _ := store.IsPoison(ctx, "msg-2")
		if !isPoisoned2 {
			t.Error("expected msg-2 to still be quarantined")
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		store := NewMemoryStore()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(4)

			go func() {
				defer wg.Done()
				store.IncrementFailure(ctx, "msg-concurrent")
			}()

			go func() {
				defer wg.Done()
				store.GetFailureCount(ctx, "msg-concurrent")
			}()

			go func() {
				defer wg.Done()
				store.IsPoison(ctx, "msg-concurrent")
			}()

			go func() {
				defer wg.Done()
				store.MarkPoison(ctx, "msg-concurrent", time.Minute)
			}()
		}

		wg.Wait()
	})
}

func TestDetector(t *testing.T) {
	ctx := context.Background()

	t.Run("NewDetector with default options", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store)

		if detector.Threshold() != 5 {
			t.Errorf("expected threshold 5, got %d", detector.Threshold())
		}
		if detector.QuarantineTime() != time.Hour {
			t.Errorf("expected quarantine time 1h, got %v", detector.QuarantineTime())
		}
	})

	t.Run("NewDetector with custom options", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store,
			WithThreshold(3),
			WithQuarantineTime(30*time.Minute),
		)

		if detector.Threshold() != 3 {
			t.Errorf("expected threshold 3, got %d", detector.Threshold())
		}
		if detector.QuarantineTime() != 30*time.Minute {
			t.Errorf("expected quarantine time 30m, got %v", detector.QuarantineTime())
		}
	})

	t.Run("Check returns false for new message", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store)

		isPoisoned, err := detector.Check(ctx, "msg-1")
		if err != nil {
			t.Fatalf("Check failed: %v", err)
		}
		if isPoisoned {
			t.Error("expected not poisoned for new message")
		}
	})

	t.Run("RecordFailure returns false below threshold", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithThreshold(3))

		// First two failures should not trigger quarantine
		for i := 0; i < 2; i++ {
			quarantined, err := detector.RecordFailure(ctx, "msg-1")
			if err != nil {
				t.Fatalf("RecordFailure failed: %v", err)
			}
			if quarantined {
				t.Errorf("expected not quarantined after %d failures", i+1)
			}
		}
	})

	t.Run("RecordFailure returns true at threshold", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithThreshold(3))

		// First two failures
		detector.RecordFailure(ctx, "msg-1")
		detector.RecordFailure(ctx, "msg-1")

		// Third failure should trigger quarantine
		quarantined, err := detector.RecordFailure(ctx, "msg-1")
		if err != nil {
			t.Fatalf("RecordFailure failed: %v", err)
		}
		if !quarantined {
			t.Error("expected quarantined at threshold")
		}

		// Check should now return true
		isPoisoned, _ := detector.Check(ctx, "msg-1")
		if !isPoisoned {
			t.Error("expected Check to return true after quarantine")
		}
	})

	t.Run("RecordSuccess clears failure count", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithThreshold(5))

		detector.RecordFailure(ctx, "msg-1")
		detector.RecordFailure(ctx, "msg-1")

		count, _ := detector.GetFailureCount(ctx, "msg-1")
		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}

		err := detector.RecordSuccess(ctx, "msg-1")
		if err != nil {
			t.Fatalf("RecordSuccess failed: %v", err)
		}

		count, _ = detector.GetFailureCount(ctx, "msg-1")
		if count != 0 {
			t.Errorf("expected count 0 after success, got %d", count)
		}
	})

	t.Run("Release clears quarantine and failures", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithThreshold(2))

		// Quarantine message
		detector.RecordFailure(ctx, "msg-1")
		detector.RecordFailure(ctx, "msg-1")

		isPoisoned, _ := detector.Check(ctx, "msg-1")
		if !isPoisoned {
			t.Error("expected quarantined")
		}

		// Release
		err := detector.Release(ctx, "msg-1")
		if err != nil {
			t.Fatalf("Release failed: %v", err)
		}

		// Should no longer be poisoned
		isPoisoned, _ = detector.Check(ctx, "msg-1")
		if isPoisoned {
			t.Error("expected not poisoned after Release")
		}

		// Failure count should be reset
		count, _ := detector.GetFailureCount(ctx, "msg-1")
		if count != 0 {
			t.Errorf("expected count 0 after Release, got %d", count)
		}
	})

	t.Run("GetFailureCount returns current count", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store)

		detector.RecordFailure(ctx, "msg-1")
		detector.RecordFailure(ctx, "msg-1")
		detector.RecordFailure(ctx, "msg-1")

		count, err := detector.GetFailureCount(ctx, "msg-1")
		if err != nil {
			t.Fatalf("GetFailureCount failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected count 3, got %d", count)
		}
	})

	t.Run("WithThreshold ignores non-positive values", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithThreshold(0), WithThreshold(-1))

		if detector.Threshold() != 5 {
			t.Errorf("expected default threshold 5, got %d", detector.Threshold())
		}
	})

	t.Run("WithQuarantineTime ignores non-positive values", func(t *testing.T) {
		store := NewMemoryStore()
		detector := NewDetector(store, WithQuarantineTime(0), WithQuarantineTime(-time.Hour))

		if detector.QuarantineTime() != time.Hour {
			t.Errorf("expected default quarantine time 1h, got %v", detector.QuarantineTime())
		}
	})
}

func TestError(t *testing.T) {
	t.Run("NewError creates error with fields", func(t *testing.T) {
		err := NewError("msg-123", "exceeded threshold")

		if err.MessageID != "msg-123" {
			t.Errorf("expected msg-123, got %s", err.MessageID)
		}
		if err.Reason != "exceeded threshold" {
			t.Errorf("expected exceeded threshold, got %s", err.Reason)
		}
	})

	t.Run("Error message format", func(t *testing.T) {
		err := NewError("msg-123", "exceeded threshold")

		expected := "poison message msg-123: exceeded threshold"
		if err.Error() != expected {
			t.Errorf("expected %s, got %s", expected, err.Error())
		}
	})

	t.Run("IsPoisonError returns true for poison error", func(t *testing.T) {
		err := NewError("msg-123", "test")

		if !IsPoisonError(err) {
			t.Error("expected IsPoisonError to return true")
		}
	})

	t.Run("IsPoisonError returns false for other errors", func(t *testing.T) {
		err := errors.New("some other error")

		if IsPoisonError(err) {
			t.Error("expected IsPoisonError to return false for non-poison error")
		}
	})

	t.Run("Error Is method works with errors.Is", func(t *testing.T) {
		err := NewError("msg-123", "test")

		if !errors.Is(err, &Error{}) {
			t.Error("expected errors.Is to match poison.Error")
		}
	})

	t.Run("Error Is returns false for other types", func(t *testing.T) {
		err := NewError("msg-123", "test")

		if err.Is(errors.New("other")) {
			t.Error("expected Is to return false for non-poison error")
		}
	})
}

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.Threshold != 5 {
		t.Errorf("expected threshold 5, got %d", opts.Threshold)
	}
	if opts.QuarantineTime != time.Hour {
		t.Errorf("expected quarantine time 1h, got %v", opts.QuarantineTime)
	}
}

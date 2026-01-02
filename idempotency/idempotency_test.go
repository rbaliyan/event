package idempotency

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()

	t.Run("IsDuplicate returns false for new message", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		isDuplicate, err := store.IsDuplicate(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected false for new message")
		}
	})

	t.Run("IsDuplicate returns true for processed message", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		store.MarkProcessed(ctx, "msg-1")

		isDuplicate, err := store.IsDuplicate(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected true for processed message")
		}
	})

	t.Run("MarkProcessed stores message", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		err := store.MarkProcessed(ctx, "msg-1")
		if err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		if store.Len() != 1 {
			t.Errorf("expected 1 entry, got %d", store.Len())
		}
	})

	t.Run("MarkProcessedWithTTL stores message", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		err := store.MarkProcessedWithTTL(ctx, "msg-1", 30*time.Minute)
		if err != nil {
			t.Fatalf("MarkProcessedWithTTL failed: %v", err)
		}

		isDuplicate, _ := store.IsDuplicate(ctx, "msg-1")
		if !isDuplicate {
			t.Error("expected message to be marked as processed")
		}
	})

	t.Run("Remove removes message", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		store.MarkProcessed(ctx, "msg-1")

		err := store.Remove(ctx, "msg-1")
		if err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		isDuplicate, _ := store.IsDuplicate(ctx, "msg-1")
		if isDuplicate {
			t.Error("expected message to be removed")
		}
	})

	t.Run("Remove non-existent message does not error", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		err := store.Remove(ctx, "never-existed")
		if err != nil {
			t.Errorf("Remove non-existent should not error: %v", err)
		}
	})

	t.Run("Len returns correct count", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		if store.Len() != 0 {
			t.Errorf("expected 0, got %d", store.Len())
		}

		store.MarkProcessed(ctx, "msg-1")
		store.MarkProcessed(ctx, "msg-2")
		store.MarkProcessed(ctx, "msg-3")

		if store.Len() != 3 {
			t.Errorf("expected 3, got %d", store.Len())
		}
	})

	t.Run("expired entries return false", func(t *testing.T) {
		store := NewMemoryStore(10 * time.Millisecond)
		defer store.Close()

		store.MarkProcessed(ctx, "msg-1")

		// Should be duplicate immediately
		isDuplicate, _ := store.IsDuplicate(ctx, "msg-1")
		if !isDuplicate {
			t.Error("expected duplicate before expiry")
		}

		// Wait for expiry
		time.Sleep(20 * time.Millisecond)

		// Should not be duplicate after expiry
		isDuplicate, _ = store.IsDuplicate(ctx, "msg-1")
		if isDuplicate {
			t.Error("expected not duplicate after expiry")
		}
	})

	t.Run("Close can be called multiple times", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)

		// Should not panic
		store.Close()
		store.Close()
		store.Close()
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(3)

			// Concurrent writes
			go func(id int) {
				defer wg.Done()
				store.MarkProcessed(ctx, "msg-concurrent")
			}(i)

			// Concurrent reads
			go func(id int) {
				defer wg.Done()
				store.IsDuplicate(ctx, "msg-concurrent")
			}(i)

			// Concurrent removes
			go func(id int) {
				defer wg.Done()
				store.Remove(ctx, "msg-concurrent")
			}(i)
		}

		wg.Wait()
	})

	t.Run("different messages are tracked independently", func(t *testing.T) {
		store := NewMemoryStore(time.Hour)
		defer store.Close()

		store.MarkProcessed(ctx, "msg-1")

		// msg-1 is duplicate
		isDuplicate1, _ := store.IsDuplicate(ctx, "msg-1")
		if !isDuplicate1 {
			t.Error("msg-1 should be duplicate")
		}

		// msg-2 is not duplicate
		isDuplicate2, _ := store.IsDuplicate(ctx, "msg-2")
		if isDuplicate2 {
			t.Error("msg-2 should not be duplicate")
		}
	})

	t.Run("overwrite existing entry updates expiry", func(t *testing.T) {
		store := NewMemoryStore(50 * time.Millisecond)
		defer store.Close()

		store.MarkProcessed(ctx, "msg-1")

		// Wait a bit
		time.Sleep(30 * time.Millisecond)

		// Mark again with longer TTL
		store.MarkProcessedWithTTL(ctx, "msg-1", 100*time.Millisecond)

		// Wait past original expiry
		time.Sleep(30 * time.Millisecond)

		// Should still be duplicate due to updated TTL
		isDuplicate, _ := store.IsDuplicate(ctx, "msg-1")
		if !isDuplicate {
			t.Error("expected duplicate after TTL update")
		}
	})
}

func TestErrAlreadyProcessed(t *testing.T) {
	if ErrAlreadyProcessed.Error() != "message already processed" {
		t.Errorf("unexpected error message: %s", ErrAlreadyProcessed.Error())
	}
}

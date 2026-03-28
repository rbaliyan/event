package transport

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_DisabledByDefault(t *testing.T) {
	// nil circuit breaker should be a no-op
	var cb *CircuitBreaker
	if err := cb.Allow(); err != nil {
		t.Fatalf("nil cb.Allow() = %v, want nil", err)
	}
	cb.RecordSuccess()
	cb.RecordFailure()
	if cb.IsEnabled() {
		t.Fatal("nil cb.IsEnabled() = true, want false")
	}
	if s := cb.State(); s != "closed" {
		t.Fatalf("nil cb.State() = %q, want %q", s, "closed")
	}

	// threshold <= 0 returns nil
	cb = NewCircuitBreaker(0, time.Second)
	if cb != nil {
		t.Fatal("NewCircuitBreaker(0, ...) should return nil")
	}
	cb = NewCircuitBreaker(-1, time.Second)
	if cb != nil {
		t.Fatal("NewCircuitBreaker(-1, ...) should return nil")
	}
}

func TestCircuitBreaker_StaysClosedOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Second)

	for i := 0; i < 100; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("Allow() failed on call %d: %v", i, err)
		}
		cb.RecordSuccess()
	}

	if s := cb.State(); s != "closed" {
		t.Fatalf("State() = %q, want %q", s, "closed")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Second)

	// 3 consecutive failures should trip the breaker
	for i := 0; i < 3; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("Allow() failed on failure %d: %v", i, err)
		}
		cb.RecordFailure()
	}

	if s := cb.State(); s != "open" {
		t.Fatalf("State() = %q, want %q", s, "open")
	}

	// Subsequent calls should be rejected
	err := cb.Allow()
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow() = %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Second)

	// 2 failures, then a success
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()

	// 2 more failures — should NOT trip (count was reset)
	cb.RecordFailure()
	cb.RecordFailure()

	if s := cb.State(); s != "closed" {
		t.Fatalf("State() = %q, want %q after reset", s, "closed")
	}
}

func TestCircuitBreaker_HalfOpenAfterCooldown(t *testing.T) {
	cb := NewCircuitBreaker(1, 20*time.Millisecond)

	// Trip the breaker
	cb.Allow()
	cb.RecordFailure()
	if s := cb.State(); s != "open" {
		t.Fatalf("State() = %q, want %q", s, "open")
	}

	// Before cooldown — rejected
	err := cb.Allow()
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow() before cooldown = %v, want ErrCircuitOpen", err)
	}

	// Wait for cooldown
	time.Sleep(30 * time.Millisecond)

	// First call should be allowed (probe)
	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() after cooldown = %v, want nil (probe)", err)
	}
	if s := cb.State(); s != "half-open" {
		t.Fatalf("State() = %q, want %q", s, "half-open")
	}

	// Concurrent calls during probe should be rejected
	err = cb.Allow()
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow() during probe = %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreaker_ProbeSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)

	// Trip and wait for cooldown
	cb.Allow()
	cb.RecordFailure()
	time.Sleep(15 * time.Millisecond)

	// Probe
	if err := cb.Allow(); err != nil {
		t.Fatalf("probe Allow() = %v, want nil", err)
	}
	cb.RecordSuccess()

	if s := cb.State(); s != "closed" {
		t.Fatalf("State() after probe success = %q, want %q", s, "closed")
	}

	// All calls should be allowed now
	if err := cb.Allow(); err != nil {
		t.Fatalf("Allow() after close = %v, want nil", err)
	}
}

func TestCircuitBreaker_ProbeFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(1, 10*time.Millisecond)

	// Trip and wait for cooldown
	cb.Allow()
	cb.RecordFailure()
	time.Sleep(15 * time.Millisecond)

	// Probe
	if err := cb.Allow(); err != nil {
		t.Fatalf("probe Allow() = %v, want nil", err)
	}
	cb.RecordFailure()

	if s := cb.State(); s != "open" {
		t.Fatalf("State() after probe failure = %q, want %q", s, "open")
	}

	// Should be rejected again (cooldown restarted)
	err := cb.Allow()
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Allow() after reopen = %v, want ErrCircuitOpen", err)
	}
}

func TestCircuitBreaker_ConcurrentSafety(t *testing.T) {
	cb := NewCircuitBreaker(5, 10*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				err := cb.Allow()
				if err == nil {
					if j%3 == 0 {
						cb.RecordFailure()
					} else {
						cb.RecordSuccess()
					}
				}
				_ = cb.State()
				_ = cb.IsEnabled()
			}
		}()
	}
	wg.Wait()

	// Just verify it didn't panic or deadlock; state is non-deterministic
	s := cb.State()
	if s != "closed" && s != "open" && s != "half-open" {
		t.Fatalf("unexpected state: %q", s)
	}
}

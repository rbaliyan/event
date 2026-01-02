package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucket(t *testing.T) {
	t.Run("NewTokenBucket creates limiter", func(t *testing.T) {
		limiter := NewTokenBucket(100, 10)

		if limiter.Limit() != 100 {
			t.Errorf("expected limit 100, got %f", limiter.Limit())
		}
		if limiter.Burst() != 10 {
			t.Errorf("expected burst 10, got %d", limiter.Burst())
		}
	})

	t.Run("Allow consumes token", func(t *testing.T) {
		limiter := NewTokenBucket(100, 5)
		ctx := context.Background()

		// Should allow burst number of requests
		for i := 0; i < 5; i++ {
			if !limiter.Allow(ctx) {
				t.Errorf("expected Allow to return true at iteration %d", i)
			}
		}
	})

	t.Run("Allow returns false when exhausted", func(t *testing.T) {
		limiter := NewTokenBucket(1, 1) // Very low rate
		ctx := context.Background()

		// First request should succeed
		if !limiter.Allow(ctx) {
			t.Error("expected first Allow to succeed")
		}

		// Second request should fail (no tokens)
		if limiter.Allow(ctx) {
			t.Error("expected second Allow to fail")
		}
	})

	t.Run("Wait blocks until token available", func(t *testing.T) {
		limiter := NewTokenBucket(100, 1)
		ctx := context.Background()

		// Consume the token
		limiter.Allow(ctx)

		// Wait should complete (tokens replenish at 100/sec = 10ms per token)
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err := limiter.Wait(ctxWithTimeout)
		if err != nil {
			t.Errorf("Wait failed: %v", err)
		}
	})

	t.Run("Wait respects context cancellation", func(t *testing.T) {
		limiter := NewTokenBucket(0.001, 1) // Very slow rate
		ctx := context.Background()

		// Consume the token
		limiter.Allow(ctx)

		// Wait with short timeout should fail
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()

		err := limiter.Wait(ctxWithTimeout)
		if err == nil {
			t.Error("expected Wait to fail with context deadline")
		}
	})

	t.Run("Reserve returns valid reservation", func(t *testing.T) {
		limiter := NewTokenBucket(100, 5)
		ctx := context.Background()

		reservation := limiter.Reserve(ctx)

		if !reservation.OK() {
			t.Error("expected reservation to be OK")
		}
	})

	t.Run("SetLimit updates limit", func(t *testing.T) {
		limiter := NewTokenBucket(100, 10)

		limiter.SetLimit(200)

		if limiter.Limit() != 200 {
			t.Errorf("expected limit 200, got %f", limiter.Limit())
		}
	})

	t.Run("SetBurst updates burst", func(t *testing.T) {
		limiter := NewTokenBucket(100, 10)

		limiter.SetBurst(20)

		if limiter.Burst() != 20 {
			t.Errorf("expected burst 20, got %d", limiter.Burst())
		}
	})
}

func TestTokenBucketReservation(t *testing.T) {
	t.Run("OK returns true for valid reservation", func(t *testing.T) {
		limiter := NewTokenBucket(100, 5)
		ctx := context.Background()

		reservation := limiter.Reserve(ctx)

		if !reservation.OK() {
			t.Error("expected OK to return true")
		}
	})

	t.Run("Delay returns wait time", func(t *testing.T) {
		limiter := NewTokenBucket(100, 5)
		ctx := context.Background()

		reservation := limiter.Reserve(ctx)
		delay := reservation.Delay()

		// First reservation should have no delay
		if delay != 0 {
			t.Errorf("expected no delay, got %v", delay)
		}
	})

	t.Run("Cancel does not panic", func(t *testing.T) {
		limiter := NewTokenBucket(100, 5)
		ctx := context.Background()

		reservation := limiter.Reserve(ctx)

		// Should not panic
		reservation.Cancel()
	})
}

func TestLimiterInterface(t *testing.T) {
	// Verify TokenBucket implements Limiter interface
	var _ Limiter = (*TokenBucket)(nil)
}

func BenchmarkTokenBucketAllow(b *testing.B) {
	limiter := NewTokenBucket(1000000, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(ctx)
	}
}

func BenchmarkTokenBucketWait(b *testing.B) {
	limiter := NewTokenBucket(1000000, 1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Wait(ctx)
	}
}

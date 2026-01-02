// Package ratelimit provides rate limiting for message processing.
//
// Rate limiting prevents resource exhaustion by controlling the rate at which
// messages are processed. This package provides:
//   - Token bucket rate limiting for local rate limiting
//   - Redis-based distributed rate limiting for multi-instance deployments
//   - Sliding window rate limiting for more accurate distributed limiting
//   - Configurable rates and burst sizes
//
// # Overview
//
// The package provides three rate limiter implementations:
//   - TokenBucket: Local, in-memory token bucket (golang.org/x/time/rate)
//   - RedisLimiter: Distributed fixed-window using Redis counters
//   - SlidingWindowLimiter: Distributed sliding window using Redis sorted sets
//
// # When to Use Each Limiter
//
// TokenBucket:
//   - Single instance deployments
//   - Local per-instance rate limiting
//   - Low latency requirements (no network calls)
//
// RedisLimiter:
//   - Multi-instance deployments
//   - Global rate limiting across instances
//   - Simple fixed-window behavior acceptable
//
// SlidingWindowLimiter:
//   - Multi-instance deployments
//   - More accurate rate limiting needed
//   - Avoiding burst at window boundaries
//
// # Basic Usage
//
//	// Local rate limiter: 100 requests/second with burst of 10
//	limiter := ratelimit.NewTokenBucket(100, 10)
//
//	// Distributed rate limiter using Redis
//	limiter := ratelimit.NewRedisLimiter(redisClient, "my-service", 100, time.Second)
//
//	// Use in handler
//	if err := limiter.Wait(ctx); err != nil {
//	    return err // Context cancelled
//	}
//	// Process message
//
// # Non-Blocking Check
//
//	if limiter.Allow(ctx) {
//	    // Process immediately
//	} else {
//	    // Rate limited - skip or queue
//	}
//
// # Best Practices
//
//   - Use TokenBucket for single-instance or per-instance limits
//   - Use RedisLimiter or SlidingWindowLimiter for global limits
//   - Set appropriate burst sizes to handle traffic spikes
//   - Monitor rate limit rejections for capacity planning
package ratelimit

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// Limiter is the interface for rate limiters.
//
// All implementations must be safe for concurrent use.
//
// Implementations:
//   - TokenBucket: Local, in-memory token bucket
//   - RedisLimiter: Distributed fixed-window using Redis
//   - SlidingWindowLimiter: Distributed sliding window using Redis
type Limiter interface {
	// Allow returns true if an event can happen right now.
	// This is a non-blocking check.
	Allow(ctx context.Context) bool

	// Wait blocks until an event is allowed or context is cancelled.
	// Returns context.Canceled or context.DeadlineExceeded if cancelled.
	Wait(ctx context.Context) error

	// Reserve returns a reservation for a future event.
	// The caller can check Delay() to know when the event can happen.
	Reserve(ctx context.Context) Reservation
}

// Reservation represents a rate limit reservation.
//
// A Reservation allows the caller to know when an event can happen
// and optionally cancel the reservation if no longer needed.
type Reservation interface {
	// OK returns whether the reservation was successful.
	// If false, the rate limit is exhausted.
	OK() bool

	// Delay returns how long to wait before the event can happen.
	// Returns 0 if the event can happen immediately.
	Delay() time.Duration

	// Cancel cancels the reservation, allowing tokens to be used
	// by other requests. Should be called if the event won't happen.
	Cancel()
}

// TokenBucket implements a local token bucket rate limiter.
//
// TokenBucket uses the golang.org/x/time/rate package for efficient
// in-memory rate limiting. It's suitable for single-instance deployments
// or per-instance rate limiting.
//
// The token bucket algorithm:
//   - Tokens are added at the specified rate (rps)
//   - A maximum of 'burst' tokens can accumulate
//   - Each event consumes one token
//   - If no tokens available, the event is delayed or rejected
//
// Example:
//
//	// 100 requests per second with burst of 10
//	limiter := ratelimit.NewTokenBucket(100, 10)
//
//	// Blocking wait
//	if err := limiter.Wait(ctx); err != nil {
//	    return err
//	}
//
//	// Non-blocking check
//	if limiter.Allow(ctx) {
//	    process()
//	}
type TokenBucket struct {
	limiter *rate.Limiter
}

// NewTokenBucket creates a new token bucket rate limiter.
//
// Parameters:
//   - rps: Events per second (rate at which tokens are added)
//   - burst: Maximum burst size (maximum tokens that can accumulate)
//
// Example:
//
//	// Allow 100 requests/second, with up to 10 burst
//	limiter := ratelimit.NewTokenBucket(100, 10)
func NewTokenBucket(rps float64, burst int) *TokenBucket {
	return &TokenBucket{
		limiter: rate.NewLimiter(rate.Limit(rps), burst),
	}
}

// Allow returns true if an event can happen right now.
// Consumes one token if available.
func (t *TokenBucket) Allow(ctx context.Context) bool {
	return t.limiter.Allow()
}

// Wait blocks until an event is allowed or context is cancelled.
// Returns nil when an event can proceed, or context error if cancelled.
func (t *TokenBucket) Wait(ctx context.Context) error {
	return t.limiter.Wait(ctx)
}

// Reserve returns a reservation for a future event.
// The caller should check OK() and Delay() to determine when to proceed.
func (t *TokenBucket) Reserve(ctx context.Context) Reservation {
	return &tokenBucketReservation{r: t.limiter.Reserve()}
}

// SetLimit updates the rate limit dynamically.
//
// Can be used to adjust rates based on backpressure or configuration changes.
func (t *TokenBucket) SetLimit(rps float64) {
	t.limiter.SetLimit(rate.Limit(rps))
}

// SetBurst updates the burst size dynamically.
func (t *TokenBucket) SetBurst(burst int) {
	t.limiter.SetBurst(burst)
}

// Limit returns the current rate limit (events per second).
func (t *TokenBucket) Limit() float64 {
	return float64(t.limiter.Limit())
}

// Burst returns the current burst size.
func (t *TokenBucket) Burst() int {
	return t.limiter.Burst()
}

// tokenBucketReservation wraps rate.Reservation.
type tokenBucketReservation struct {
	r *rate.Reservation
}

func (r *tokenBucketReservation) OK() bool {
	return r.r.OK()
}

func (r *tokenBucketReservation) Delay() time.Duration {
	return r.r.Delay()
}

func (r *tokenBucketReservation) Cancel() {
	r.r.Cancel()
}

// Compile-time check
var _ Limiter = (*TokenBucket)(nil)

package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLimiter implements a distributed rate limiter using Redis.
//
// RedisLimiter uses a fixed-window algorithm with Redis counters.
// It tracks the number of events in each time window using INCR with EXPIRE.
//
// Fixed-window behavior:
//   - Each window is a fixed time period (e.g., 1 second)
//   - Counter resets at window boundaries
//   - May allow 2x limit at window boundaries (burst at edges)
//
// For more accurate limiting, use SlidingWindowLimiter.
//
// Redis Commands Used:
//   - INCR: Increment counter
//   - EXPIRE: Set window expiration
//   - GET: Check current count
//   - DEL: Reset limiter
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	limiter := ratelimit.NewRedisLimiter(rdb, "api-service", 100, time.Second)
//
//	// Check before processing
//	if limiter.Allow(ctx) {
//	    processRequest()
//	} else {
//	    return errors.New("rate limited")
//	}
type RedisLimiter struct {
	client redis.Cmdable
	key    string
	limit  int           // Maximum events per window
	window time.Duration // Window duration
}

// NewRedisLimiter creates a new Redis-based rate limiter.
//
// Parameters:
//   - client: A connected Redis client
//   - key: Unique identifier for this limiter (e.g., "api-service", "user:123")
//   - limit: Maximum events allowed per window
//   - window: Duration of each window
//
// Example:
//
//	// 100 requests per second
//	limiter := ratelimit.NewRedisLimiter(rdb, "my-service", 100, time.Second)
//
//	// 1000 requests per minute
//	limiter := ratelimit.NewRedisLimiter(rdb, "my-service", 1000, time.Minute)
func NewRedisLimiter(client redis.Cmdable, key string, limit int, window time.Duration) *RedisLimiter {
	return &RedisLimiter{
		client: client,
		key:    "ratelimit:" + key,
		limit:  limit,
		window: window,
	}
}

// Allow returns true if an event can happen right now.
//
// Uses a Lua script for atomic increment and check. On Redis error,
// returns true (fail open) to prevent blocking all requests.
func (r *RedisLimiter) Allow(ctx context.Context) bool {
	// Lua script for atomic increment and check
	script := redis.NewScript(`
		local key = KEYS[1]
		local limit = tonumber(ARGV[1])
		local window = tonumber(ARGV[2])

		local current = redis.call('INCR', key)
		if current == 1 then
			redis.call('EXPIRE', key, window)
		end

		if current > limit then
			return 0
		end
		return 1
	`)

	result, err := script.Run(ctx, r.client, []string{r.key}, r.limit, int(r.window.Seconds())).Int()
	if err != nil {
		// On error, allow the request (fail open)
		return true
	}

	return result == 1
}

// Wait blocks until an event is allowed or context is cancelled.
//
// Polls Allow() with a delay based on the rate limit. Returns nil
// when allowed, or context error if the context expires.
func (r *RedisLimiter) Wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if r.Allow(ctx) {
				return nil
			}
			// Wait before retrying
			time.Sleep(r.window / time.Duration(r.limit))
		}
	}
}

// Reserve returns a reservation for a future event.
//
// For Redis limiter, this is equivalent to calling Allow() and returning
// a reservation with the appropriate delay.
func (r *RedisLimiter) Reserve(ctx context.Context) Reservation {
	allowed := r.Allow(ctx)
	delay := time.Duration(0)
	if !allowed {
		delay = r.window / time.Duration(r.limit)
	}
	return &redisReservation{
		ok:    allowed,
		delay: delay,
	}
}

// Remaining returns the number of remaining events in the current window.
//
// Returns limit if no events have been recorded in this window.
func (r *RedisLimiter) Remaining(ctx context.Context) (int, error) {
	val, err := r.client.Get(ctx, r.key).Int()
	if err == redis.Nil {
		return r.limit, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redis get: %w", err)
	}

	remaining := r.limit - val
	if remaining < 0 {
		remaining = 0
	}

	return remaining, nil
}

// Reset resets the rate limiter by deleting the Redis key.
//
// Use for testing or administrative purposes.
func (r *RedisLimiter) Reset(ctx context.Context) error {
	return r.client.Del(ctx, r.key).Err()
}

// redisReservation implements Reservation for Redis limiter.
type redisReservation struct {
	ok    bool
	delay time.Duration
}

func (r *redisReservation) OK() bool {
	return r.ok
}

func (r *redisReservation) Delay() time.Duration {
	return r.delay
}

func (r *redisReservation) Cancel() {
	// No-op for Redis limiter
}

// Compile-time check
var _ Limiter = (*RedisLimiter)(nil)

package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// SlidingWindowLimiter implements a sliding window rate limiter using Redis.
//
// Unlike RedisLimiter (fixed-window), SlidingWindowLimiter tracks individual
// events in a Redis sorted set, providing more accurate rate limiting without
// the burst-at-boundary issue of fixed windows.
//
// Sliding-window behavior:
//   - Each event is stored with its timestamp as score
//   - Window slides with current time (always looks back 'window' duration)
//   - Events older than the window are automatically removed
//   - Provides consistent rate limiting regardless of when requests arrive
//
// Trade-offs vs Fixed Window:
//   - More accurate: no 2x burst at boundaries
//   - Higher memory: stores each event timestamp
//   - Slightly more CPU: sorted set operations
//
// Redis Commands Used:
//   - ZADD: Add event with timestamp
//   - ZREMRANGEBYSCORE: Remove old events
//   - ZCARD: Count events in window
//   - DEL: Reset limiter
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	limiter := ratelimit.NewSlidingWindowLimiter(rdb, "api-service", 100, time.Second)
//
//	if limiter.Allow(ctx) {
//	    processRequest()
//	}
type SlidingWindowLimiter struct {
	client redis.Cmdable
	key    string
	limit  int           // Maximum events per window
	window time.Duration // Window duration
}

// NewSlidingWindowLimiter creates a new sliding window rate limiter.
//
// Parameters:
//   - client: A connected Redis client
//   - key: Unique identifier for this limiter
//   - limit: Maximum events allowed per window
//   - window: Duration of the sliding window
//
// Example:
//
//	// 100 requests per second with sliding window
//	limiter := ratelimit.NewSlidingWindowLimiter(rdb, "my-service", 100, time.Second)
func NewSlidingWindowLimiter(client redis.Cmdable, key string, limit int, window time.Duration) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		client: client,
		key:    "ratelimit:sliding:" + key,
		limit:  limit,
		window: window,
	}
}

// Allow returns true if an event can happen right now.
//
// Uses a Lua script for atomic window cleanup, count check, and event recording.
// On Redis error, returns true (fail open) to prevent blocking all requests.
func (s *SlidingWindowLimiter) Allow(ctx context.Context) bool {
	now := time.Now()
	windowStart := now.Add(-s.window).UnixMicro()
	member := fmt.Sprintf("%d", now.UnixMicro())

	// Lua script for atomic sliding window check
	script := redis.NewScript(`
		local key = KEYS[1]
		local windowStart = tonumber(ARGV[1])
		local limit = tonumber(ARGV[2])
		local now = ARGV[3]
		local windowMs = tonumber(ARGV[4])

		-- Remove old entries
		redis.call('ZREMRANGEBYSCORE', key, '-inf', windowStart)

		-- Count current entries
		local count = redis.call('ZCARD', key)

		if count >= limit then
			return 0
		end

		-- Add new entry
		redis.call('ZADD', key, now, now)
		redis.call('PEXPIRE', key, windowMs)

		return 1
	`)

	result, err := script.Run(ctx, s.client, []string{s.key},
		windowStart,
		s.limit,
		member,
		s.window.Milliseconds(),
	).Int()

	if err != nil {
		// On error, allow the request (fail open)
		return true
	}

	return result == 1
}

// Wait blocks until an event is allowed or context is cancelled.
//
// Uses exponential backoff between retries, capped at window/limit.
// Returns nil when allowed, or context error if the context expires.
func (s *SlidingWindowLimiter) Wait(ctx context.Context) error {
	backoff := time.Millisecond * 10

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if s.Allow(ctx) {
				return nil
			}
			// Exponential backoff with max
			time.Sleep(backoff)
			backoff *= 2
			if backoff > s.window/time.Duration(s.limit) {
				backoff = s.window / time.Duration(s.limit)
			}
		}
	}
}

// Reserve returns a reservation for a future event.
//
// For sliding window limiter, this is equivalent to calling Allow() and
// returning a reservation with the appropriate delay.
func (s *SlidingWindowLimiter) Reserve(ctx context.Context) Reservation {
	allowed := s.Allow(ctx)
	delay := time.Duration(0)
	if !allowed {
		delay = s.window / time.Duration(s.limit)
	}
	return &redisReservation{
		ok:    allowed,
		delay: delay,
	}
}

// Count returns the number of events in the current sliding window.
//
// Removes old entries first to get an accurate count.
func (s *SlidingWindowLimiter) Count(ctx context.Context) (int64, error) {
	windowStart := time.Now().Add(-s.window).UnixMicro()

	// Remove old entries first
	s.client.ZRemRangeByScore(ctx, s.key, "-inf", fmt.Sprintf("%d", windowStart))

	return s.client.ZCard(ctx, s.key).Result()
}

// Remaining returns the number of remaining events in the current window.
//
// Returns limit - current count.
func (s *SlidingWindowLimiter) Remaining(ctx context.Context) (int, error) {
	count, err := s.Count(ctx)
	if err != nil {
		return 0, err
	}

	remaining := s.limit - int(count)
	if remaining < 0 {
		remaining = 0
	}

	return remaining, nil
}

// Reset resets the rate limiter by deleting the Redis sorted set.
//
// Use for testing or administrative purposes.
func (s *SlidingWindowLimiter) Reset(ctx context.Context) error {
	return s.client.Del(ctx, s.key).Err()
}

// Compile-time check
var _ Limiter = (*SlidingWindowLimiter)(nil)

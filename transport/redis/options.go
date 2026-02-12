package redis

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
)

// Option configures the Redis transport
type Option func(*Transport)

// WithCodec sets the codec for message serialization
func WithCodec(c codec.Codec) Option {
	return func(t *Transport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithConsumerGroup sets the base consumer group ID
func WithConsumerGroup(groupID string) Option {
	return func(t *Transport) {
		if groupID != "" {
			t.groupID = groupID
		}
	}
}

// WithMaxLen sets the max length for streams (MAXLEN)
func WithMaxLen(n int64) Option {
	return func(t *Transport) {
		if n > 0 {
			t.maxLen = n
		}
	}
}

// WithMaxAge sets the max age for messages in streams (MINID-based trimming).
// Messages older than this duration will be automatically trimmed on each publish.
//
// This ensures messages don't stay in Redis forever when no consumers are registered.
// Redis Streams use timestamp-based IDs, so this calculates MINID from (now - maxAge).
//
// Set to 0 (default) for unlimited retention (messages stay forever).
// Recommended: Set this to a reasonable value (e.g., 24*time.Hour) for production.
func WithMaxAge(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.maxAge = d
		}
	}
}

// WithBlockTime sets the block time for XREADGROUP
func WithBlockTime(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.blockTime = d
		}
	}
}

// WithLogger sets the logger
func WithLogger(l *slog.Logger) Option {
	return func(t *Transport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithErrorHandler sets the error handler callback
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithSendTimeout sets the timeout for sending messages to subscriber channels.
// This provides backpressure control when handlers are slow.
//
// Behavior on timeout:
//   - Message is NOT dropped - it stays in the Redis Pending Entries List (PEL)
//   - The consumer continues processing other messages
//   - The timed-out message will be redelivered on consumer restart or via XCLAIM
//
// Set to 0 (default) to block indefinitely until the handler is ready.
// Use a non-zero timeout to prevent slow handlers from blocking the consumer.
func WithSendTimeout(d time.Duration) Option {
	return func(t *Transport) {
		t.sendTimeout = d
	}
}

// WithClaimInterval enables automatic claiming of orphaned messages.
// When a consumer dies without acknowledging messages, those messages remain
// in the Pending Entries List (PEL) forever. This option starts a background
// goroutine that periodically claims and reprocesses orphaned messages.
//
// Parameters:
//   - interval: How often to check for orphaned messages (e.g., 30*time.Second)
//   - minIdle: Minimum time a message must be idle before claiming (e.g., 60*time.Second)
//
// Set interval to 0 to disable (default).
func WithClaimInterval(interval, minIdle time.Duration) Option {
	return func(t *Transport) {
		t.claimInterval = interval
		t.claimMinIdle = minIdle
	}
}


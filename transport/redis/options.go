package redis

import (
	"context"
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

// =============================================================================
// Reliability Store Options
// =============================================================================
// Redis Streams does NOT provide native deduplication, DLQ, or poison detection.
// These features must be implemented via external stores. Inject stores here
// to enable these reliability features.
//
// Compare with NATS JetStream which provides these features natively.

// IdempotencyStore checks for duplicate messages.
type IdempotencyStore interface {
	IsDuplicate(ctx context.Context, messageID string) (bool, error)
	MarkProcessed(ctx context.Context, messageID string) error
}

// DLQHandler is called when a message fails processing after max retries.
type DLQHandler func(ctx context.Context, eventName string, msgID string, payload []byte, err error) error

// PoisonDetector tracks and quarantines repeatedly failing messages.
type PoisonDetector interface {
	Check(ctx context.Context, messageID string) (bool, error)
	RecordFailure(ctx context.Context, messageID string) (bool, error)
	RecordSuccess(ctx context.Context, messageID string) error
}

// WithIdempotencyStore sets a store for deduplication.
//
// When set, the transport checks each incoming message against the store
// and skips duplicates. This provides exactly-once processing semantics.
//
// Redis Streams does NOT deduplicate natively - this store is required
// for exactly-once semantics.
//
// Example:
//
//	store := idempotency.NewRedisStore(redisClient, time.Hour)
//	transport := redis.New(client,
//	    redis.WithIdempotencyStore(store),
//	)
func WithIdempotencyStore(store IdempotencyStore) Option {
	return func(t *Transport) {
		t.idempotencyStore = store
	}
}

// WithDLQHandler sets a handler for messages that fail after max retries.
//
// When set, messages that exceed MaxRetries are passed to this handler
// for dead letter queue storage instead of being dropped.
//
// Redis Streams does NOT have native DLQ support - this handler is required
// to capture failed messages.
//
// Example:
//
//	handler := func(ctx context.Context, event, msgID string, payload []byte, err error) error {
//	    return dlqStore.Store(ctx, event, msgID, payload, nil, err, 0, "redis")
//	}
//	transport := redis.New(client,
//	    redis.WithDLQHandler(handler),
//	    redis.WithMaxRetries(3),
//	)
func WithDLQHandler(handler DLQHandler) Option {
	return func(t *Transport) {
		t.dlqHandler = handler
	}
}

// WithPoisonDetector sets a detector for poison messages.
//
// When set, the transport checks if messages are quarantined before
// delivery and tracks failures for poison detection.
//
// Redis Streams does NOT track delivery attempts natively - this detector
// is required for poison message handling.
//
// Example:
//
//	detector := poison.NewDetector(store, poison.WithThreshold(5))
//	transport := redis.New(client,
//	    redis.WithPoisonDetector(detector),
//	)
func WithPoisonDetector(detector PoisonDetector) Option {
	return func(t *Transport) {
		t.poisonDetector = detector
	}
}

// WithMaxRetries sets maximum delivery attempts before sending to DLQ.
//
// When a message handler returns an error, the message stays in the
// Pending Entries List (PEL) for redelivery. After this many attempts,
// the message is acknowledged and sent to the DLQ handler (if configured).
//
// Default: 0 (unlimited retries)
//
// Example:
//
//	transport := redis.New(client,
//	    redis.WithMaxRetries(3),
//	    redis.WithDLQHandler(handler),
//	)
func WithMaxRetries(n int) Option {
	return func(t *Transport) {
		t.maxRetries = n
	}
}

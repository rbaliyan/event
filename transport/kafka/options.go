package kafka

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
)

// Option configures the Kafka transport
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

// WithPartitions sets the number of partitions for new topics
func WithPartitions(n int32) Option {
	return func(t *Transport) {
		if n > 0 {
			t.partitions = n
		}
	}
}

// WithReplication sets the replication factor for new topics
func WithReplication(n int16) Option {
	return func(t *Transport) {
		if n > 0 {
			t.replication = n
		}
	}
}

// WithRetention sets the message retention time for topics.
// Messages older than this duration will be deleted by Kafka.
//
// This ensures messages don't stay in Kafka forever when no consumers are registered.
// Maps to Kafka topic config "retention.ms".
//
// Set to 0 (default) to use broker's default retention (usually 7 days).
// Recommended: Set this to a reasonable value (e.g., 24*time.Hour) for production.
func WithRetention(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.retention = d
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
// This provides backpressure detection when handlers are slow.
//
// Behavior on timeout:
//   - Message is NOT dropped - it is retried with 100ms backoff until delivered
//   - Unlike Redis/NATS, Kafka uses offset-based commit, so messages CANNOT be skipped
//   - Processing blocks until the handler channel has capacity
//
// Set to 0 (default) to block indefinitely until the handler is ready.
// Use a non-zero timeout to detect slow handlers (logged as warnings) while
// still guaranteeing message delivery.
//
// Note: This timeout does NOT skip messages - it only controls logging frequency.
// For actual backpressure, consider increasing the channel buffer size.
func WithSendTimeout(d time.Duration) Option {
	return func(t *Transport) {
		t.sendTimeout = d
	}
}


package nats

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
)

// =============================================================================
// JetStream Transport Options
// =============================================================================

// WithCodec sets the codec for message serialization
func WithCodec(c codec.Codec) JSOption {
	return func(t *JetStreamTransport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithReplicas sets the number of replicas for streams
func WithReplicas(n int) JSOption {
	return func(t *JetStreamTransport) {
		if n > 0 {
			t.replicas = n
		}
	}
}

// WithMaxAge sets the max age for messages in streams
func WithMaxAge(d time.Duration) JSOption {
	return func(t *JetStreamTransport) {
		if d > 0 {
			t.maxAge = d
		}
	}
}

// WithLogger sets the logger
func WithLogger(l *slog.Logger) JSOption {
	return func(t *JetStreamTransport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithErrorHandler sets the error handler callback
func WithErrorHandler(fn func(error)) JSOption {
	return func(t *JetStreamTransport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithSendTimeout sets the timeout for sending messages to subscriber channels.
// This provides backpressure control when handlers are slow.
//
// Behavior on timeout:
//   - Message is NOT dropped - it is Nak'd for immediate redelivery by NATS
//   - The consumer continues processing other messages
//   - NATS will redeliver the message to this or another consumer
//
// Set to 0 (default) to block indefinitely until the handler is ready.
// Use a non-zero timeout to prevent slow handlers from blocking the consumer.
func WithSendTimeout(d time.Duration) JSOption {
	return func(t *JetStreamTransport) {
		t.sendTimeout = d
	}
}

// =============================================================================
// Native JetStream Feature Options
// =============================================================================
// These options enable JetStream native features. When enabled, the broker
// handles these features directly - no external stores needed.

// WithDeduplication enables JetStream native message deduplication.
//
// When enabled, the transport automatically sets the Nats-Msg-Id header
// using the message ID on publish. JetStream rejects duplicate messages
// within the deduplication window.
//
// This eliminates the need for a separate idempotency store.
//
// The duration parameter sets the deduplication window. Messages with the
// same ID within this window are considered duplicates.
//
// Example:
//
//	transport := nats.NewJetStream(conn,
//	    nats.WithDeduplication(2 * time.Minute),
//	)
func WithDeduplication(window time.Duration) JSOption {
	return func(t *JetStreamTransport) {
		t.dedupEnabled = true
		if window > 0 {
			t.dedupWindow = window
		}
	}
}

// WithMaxDeliver sets the maximum delivery attempts before giving up.
//
// When a message exceeds this limit, JetStream stops redelivering it
// and publishes an advisory to $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.
//
// This provides native poison message detection - repeatedly failing
// messages are automatically removed from the delivery queue.
//
// Set to 0 for unlimited deliveries (default JetStream behavior).
//
// Example:
//
//	transport := nats.NewJetStream(conn,
//	    nats.WithMaxDeliver(5), // Stop after 5 failed attempts
//	)
func WithMaxDeliver(n int) JSOption {
	return func(t *JetStreamTransport) {
		t.maxDeliver = n
	}
}

// WithAckWait sets how long JetStream waits for acknowledgment.
//
// If a consumer doesn't acknowledge a message within this duration,
// JetStream redelivers it to another consumer (or the same one).
//
// Default: 30 seconds
//
// Example:
//
//	transport := nats.NewJetStream(conn,
//	    nats.WithAckWait(time.Minute), // Allow 1 minute for processing
//	)
func WithAckWait(d time.Duration) JSOption {
	return func(t *JetStreamTransport) {
		if d > 0 {
			t.ackWait = d
		}
	}
}

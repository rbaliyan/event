package bridge

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// DedupKeyFn extracts a deduplication key from a source message. Two
// messages with the same key are treated as the same logical event by
// [Dedup].
//
// Return an empty string to bypass dedup for that message — it is
// forwarded unconditionally. Useful for messages that carry no natural
// identity (control signals, heartbeats).
type DedupKeyFn func(transport.Message) string

// DefaultDedupKey uses the message ID as the dedup key. Correct when
// the source guarantees globally unique IDs per logical event (most
// CDC sources do: MongoDB resume token data, Kafka offset+partition).
// Override when the message ID changes across redelivery but the
// logical event does not.
func DefaultDedupKey(msg transport.Message) string {
	if msg == nil {
		return ""
	}
	return msg.ID()
}

// DedupOptions configures the [Dedup] middleware.
type DedupOptions struct {
	// FailOpen controls the bridge's behaviour when the coordinator
	// returns an error.
	//
	//   - false (default): treat the coordinator error as a handler
	//     error. The source redelivers; consistency preserved at the
	//     cost of liveness while the coordinator is down.
	//   - true: forward the message anyway. Liveness preserved at the
	//     cost of possible duplicates.
	FailOpen bool

	// OnSkip fires when the coordinator reports the key is already
	// claimed and the message is dropped. Intended for observability.
	// MUST NOT block.
	OnSkip func(event string, msg transport.Message)
}

// Dedup returns middleware that suppresses duplicate publishes across
// bridge replicas using a [Coordinator]. For each incoming message,
// the middleware asks the coordinator to claim the key derived from
// keyFn(msg) for the duration ttl. If the claim succeeds the message
// continues through the pipeline; otherwise it is dropped (acked on
// the source).
//
// Guarantee:
//
//	Exactly one replica per claim window publishes each logical
//	event to the sink, assuming the coordinator is healthy and
//	claim TTL exceeds the source's redelivery window.
//
// Failure modes:
//
//   - Coordinator unavailable: see [DedupOptions.FailOpen].
//   - Replica crashes after claim before sink publish: the claim is
//     held until TTL expiry; subsequent redeliveries are dropped as
//     duplicates → the event is LOST in the sink. Operators who cannot
//     tolerate this should pair [Dedup] with [DLQ] or use a coordinator
//     that releases claims on explicit failure.
//
// ttl MUST be > 0. Panics otherwise (programmer error at wiring time).
func Dedup(coord Coordinator, keyFn DedupKeyFn, ttl time.Duration, opts ...func(*DedupOptions)) Middleware {
	if coord == nil {
		panic("bridge: Dedup requires a non-nil Coordinator")
	}
	if keyFn == nil {
		keyFn = DefaultDedupKey
	}
	if ttl <= 0 {
		panic("bridge: Dedup requires ttl > 0")
	}
	o := DedupOptions{}
	for _, opt := range opts {
		opt(&o)
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			key := keyFn(msg)
			if key == "" {
				return next(ctx, event, msg)
			}
			ok, err := coord.Claim(ctx, key, ttl)
			if err != nil {
				if o.FailOpen {
					return next(ctx, event, msg)
				}
				return err
			}
			if !ok {
				if o.OnSkip != nil {
					o.OnSkip(event, msg)
				}
				return nil
			}
			return next(ctx, event, msg)
		}
	}
}

// WithDedupFailOpen configures [Dedup] to forward messages when the
// coordinator returns an error. Default is fail-closed.
func WithDedupFailOpen(v bool) func(*DedupOptions) {
	return func(o *DedupOptions) { o.FailOpen = v }
}

// WithDedupOnSkip registers a callback that fires when the coordinator
// reports the key is already claimed. Use for metrics and debugging.
func WithDedupOnSkip(fn func(event string, msg transport.Message)) func(*DedupOptions) {
	return func(o *DedupOptions) { o.OnSkip = fn }
}

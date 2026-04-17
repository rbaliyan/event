package bridge

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/distributed"
	"github.com/rbaliyan/event/v3/transport"
)

// DistributedDedup returns a [Middleware] that uses a
// [distributed.Coordinator] for cross-replica deduplication at the
// bridge level.
//
// For each source message the middleware:
//  1. Extracts a dedup key via keyFn.
//  2. Calls [distributed.Coordinator.Acquire] to atomically claim
//     the key for this replica.
//  3. If acquired, forwards the message to the next handler (typically
//     the sink publish). On success it calls MarkProcessed so replayed
//     source messages are skipped. On failure it calls Reset so another
//     replica can retry.
//  4. If NOT acquired, the message was already claimed — it is dropped
//     (acked on the source).
//
// This replaces the handler-side [distributed.WorkerPoolMiddleware]
// when bridge mode is active: the dedup decision moves from "per
// handler invocation on every pod" to "once per event at the bridge",
// which is a single Acquire round-trip total.
//
// ttl should be longer than the longest expected sink publish latency.
// Use the same value as the WorkerPool state TTL.
//
// metrics is optional — pass nil to disable skip counting.
//
// Example:
//
//	bridge.WithMiddleware(
//	    bridge.DistributedDedup(
//	        stateManager,
//	        mongodb.DedupKeyFromChangeStream(),
//	        5*time.Minute,
//	        bridgeMetrics,
//	    ),
//	)
func DistributedDedup(coord distributed.Coordinator, keyFn DedupKeyFn, ttl time.Duration, metrics *Metrics) Middleware {
	if coord == nil {
		panic("bridge: DistributedDedup requires a non-nil Coordinator")
	}
	if keyFn == nil {
		keyFn = DefaultDedupKey
	}
	if ttl <= 0 {
		panic("bridge: DistributedDedup requires ttl > 0")
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			key := keyFn(msg)
			if key == "" {
				return next(ctx, event, msg)
			}

			// Scope the dedup key by event name so pumps for different
			// registered events on the same source do not race for the
			// same slot. Without this, all pumps on a shared-source bus
			// (e.g. mongodb CS with Created/Updated/Deleted on the same
			// collection) compete for the same key and only one publishes
			// — typically to the wrong sink stream for 2/3 of messages.
			// Prefixing with the event name gives each pump its own
			// dedup namespace while cross-replica dedup remains correct:
			// all replicas use the same event-prefixed key for each pump.
			scopedKey := event + ":" + key

			acquired, err := coord.Acquire(ctx, scopedKey, ttl)
			if err != nil {
				return err // fail-closed: source redelivers
			}
			if !acquired {
				if metrics != nil {
					metrics.RecordSkip(ctx, event)
				}
				return nil // another replica won — skip
			}

			if err := next(ctx, event, msg); err != nil {
				// Publish to sink failed — release so another replica
				// can retry on the next source redelivery.
				_ = coord.Reset(ctx, scopedKey)
				return err
			}

			// Successfully forwarded — mark processed so replayed
			// source messages (from resume token replay, crash recovery)
			// are not re-published.
			_ = coord.MarkProcessed(ctx, scopedKey)
			return nil
		}
	}
}

package distributed

import (
	"context"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3"
)

// PoolOption configures WorkerPoolMiddleware behavior.
type PoolOption func(*poolOptions)

type poolOptions struct {
	storePayload   *bool // nil = auto-detect from transport, non-nil = explicit
	maxPayloadSize int   // 0 = no limit
}

// WithPayloadRecovery explicitly enables payload storage for recovery.
//
// When enabled, the middleware stores message payload alongside state so that
// the RecoveryRunner can re-publish events if the worker crashes. This is
// required for transports that don't support re-delivery (e.g., MongoDB
// Change Streams).
//
// By default, payload storage is auto-detected from the transport's
// SupportsRedelivery() capability. Use this option to override the detection
// or to make the behavior deterministic at construction time.
func WithPayloadRecovery() PoolOption {
	return func(o *poolOptions) {
		t := true
		o.storePayload = &t
	}
}

// WithMaxPayloadSize sets the maximum payload size (in bytes) that will be
// stored for recovery. If a message payload exceeds this limit, the middleware
// falls back to regular Acquire without payload storage and logs a warning.
//
// Default: 0 (no limit). MongoDB's document limit is 16MB which provides
// a natural ceiling for MongoDB-backed state managers.
func WithMaxPayloadSize(size int) PoolOption {
	return func(o *poolOptions) {
		if size > 0 {
			o.maxPayloadSize = size
		}
	}
}

// WorkerPoolMiddleware creates middleware that emulates WorkerPool mode
// on Broadcast-only transports using atomic database state transitions.
//
// This middleware enables load-balanced message processing across multiple
// subscribers even when the underlying transport only supports Broadcast.
// Each message is processed by exactly one worker (the one that successfully
// acquires the state), while other workers skip the message silently.
//
// How it works:
//  1. Message arrives (delivered to all subscribers via Broadcast)
//  2. Each subscriber calls Acquire() to attempt acquiring the message state
//  3. Only the subscriber that succeeds processes the message
//  4. On success: MarkProcessed() is called to mark as done
//  5. On failure: Reset() is called to allow immediate retry by another worker
//
// Use cases:
//   - MongoDB Change Streams (Broadcast-only) with load-balanced processing
//   - Any transport where native WorkerPool isn't available
//   - Adding worker semantics to existing Broadcast subscriptions
//
// Parameters:
//   - coord: A Coordinator implementation (RedisStateManager for distributed)
//   - stateTTL: How long to hold the state (should exceed handler timeout)
//
// Example:
//
//	// Redis state manager for distributed deployments
//	sm := distributed.NewRedisStateManager(redisClient)
//
//	// Subscribe with worker emulation
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, 5*time.Minute),
//	    ),
//	)
//
// State TTL Guidelines:
//
//	Handler Timeout  |  Recommended State TTL
//	-----------------|-----------------------
//	30s              |  1-2 minutes
//	1 minute         |  3-5 minutes
//	5 minutes        |  10-15 minutes
//	No timeout       |  Set based on max expected processing time + buffer
//
// Error Handling:
//
// If the state manager returns an error during Acquire, the middleware logs the
// error and proceeds with processing. This "fail open" behavior ensures
// messages aren't lost due to state manager issues, at the cost of potential
// duplicates. Handlers should be idempotent.
//
// Worker Groups:
//
// To emulate worker groups with different state managers per group:
//
//	// Group A: order processors
//	smA := distributed.NewRedisStateManager(redis,
//	    distributed.WithPrefix("order-processors:"))
//
//	// Group B: analytics collectors
//	smB := distributed.NewRedisStateManager(redis,
//	    distributed.WithPrefix("analytics:"))
//
//	orderEvent.Subscribe(ctx, processOrder,
//	    event.WithMiddleware(distributed.WorkerPoolMiddleware[Order](smA, ttl)))
//
//	orderEvent.Subscribe(ctx, collectAnalytics,
//	    event.WithMiddleware(distributed.WorkerPoolMiddleware[Order](smB, ttl)))
func WorkerPoolMiddleware[T any](coord Coordinator, stateTTL time.Duration, opts ...PoolOption) event.Middleware[T] {
	o := &poolOptions{}
	for _, opt := range opts {
		opt(o)
	}

	// Check if coordinator also implements PayloadStore
	ps, hasPayloadStore := coord.(PayloadStore)

	return func(next event.Handler[T]) event.Handler[T] {
		// Track whether the transport supports redelivery
		var (
			detectOnce   sync.Once
			storePayload bool // true when transport lacks redelivery
		)

		// If explicitly configured, set immediately (no auto-detection needed)
		if o.storePayload != nil {
			storePayload = *o.storePayload
		}

		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Get message ID from context
			messageID := event.ContextEventID(ctx)
			if messageID == "" {
				// No message ID available, can't acquire state - proceed with processing
				// This shouldn't happen in normal operation but fail open for safety
				return next(ctx, ev, data)
			}

			// Auto-detect transport capability once per subscriber (skipped if explicit)
			if o.storePayload == nil {
				detectOnce.Do(func() {
					bus := event.ContextBus(ctx)
					if bus != nil && !bus.SupportsRedelivery() {
						storePayload = true
					}
				})
			}

			// Attempt to acquire the message state
			acquired, err := coord.Acquire(ctx, messageID, stateTTL)
			if err != nil {
				// State manager error - log and proceed with processing (fail open)
				// This prevents message loss at the cost of potential duplicates
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Warn("state acquisition error, proceeding with handler",
						"message_id", messageID,
						"error", err)
				}
				// Signal acquired on error (fail open = treat as acquired)
				if sig := event.ContextAcquisitionSignal(ctx); sig != nil {
					sig.Set(event.AcquisitionAcquired)
				}
				return next(ctx, ev, data)
			}

			if !acquired {
				// Another worker already acquired this message - skip silently
				if sig := event.ContextAcquisitionSignal(ctx); sig != nil {
					sig.Set(event.AcquisitionSkipped)
				}
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Debug("message acquired by another worker, skipping",
						"message_id", messageID)
				}
				return nil
			}

			// Signal successful acquisition
			if sig := event.ContextAcquisitionSignal(ctx); sig != nil {
				sig.Set(event.AcquisitionAcquired)
			}

			// Store payload for recovery if needed (after successful acquire)
			payloadStored := false
			if storePayload && hasPayloadStore {
				payload := event.ContextRawPayload(ctx)

				if o.maxPayloadSize > 0 && len(payload) > o.maxPayloadSize {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("payload exceeds max size, skipping payload storage",
							"message_id", messageID,
							"payload_size", len(payload),
							"max_size", o.maxPayloadSize)
					}
				} else if len(payload) > 0 {
					msgData := &MessageData{
						Payload:   payload,
						Metadata:  event.ContextMetadata(ctx),
						EventName: event.ContextName(ctx),
					}
					if storeErr := ps.StorePayload(ctx, messageID, msgData); storeErr != nil {
						logger := event.ContextLogger(ctx)
						if logger != nil {
							logger.Warn("failed to store payload for recovery",
								"message_id", messageID,
								"error", storeErr)
						}
					} else {
						payloadStored = true
					}
				}
			}

			// We acquired the message state - process it
			handlerErr := next(ctx, ev, data)

			// Record result
			if handlerErr == nil {
				// Success - mark as processed and clear payload if stored
				if payloadStored {
					_ = ps.ClearPayload(ctx, messageID)
				}
				if markErr := coord.MarkProcessed(ctx, messageID); markErr != nil {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("failed to mark message as processed",
							"message_id", messageID,
							"error", markErr)
					}
				}
			} else {
				// Failure — decide whether to reset or leave for recovery.
				// When payload was stored, the transport cannot redeliver the
				// original message. Calling Reset would delete the stored payload,
				// permanently losing the event. Leave the state in "processing"
				// so RecoveryRunner can find it via LoadStalePayloads and
				// re-publish with the stored payload.
				if payloadStored {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Info("handler failed, leaving state for recovery re-publish",
							"message_id", messageID,
							"error", handlerErr)
					}
				} else {
					// No stored payload — safe to reset for immediate retry
					if resetErr := coord.Reset(ctx, messageID); resetErr != nil {
						logger := event.ContextLogger(ctx)
						if logger != nil {
							logger.Warn("failed to reset message state",
								"message_id", messageID,
								"error", resetErr)
						}
					}
				}
			}

			return handlerErr
		}
	}
}

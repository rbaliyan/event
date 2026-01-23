package distributed

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3"
)

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
//   - sm: A StateManager implementation (RedisStateManager for distributed)
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
func WorkerPoolMiddleware[T any](sm StateManager, stateTTL time.Duration) event.Middleware[T] {
	return func(next event.Handler[T]) event.Handler[T] {
		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Get message ID from context
			messageID := event.ContextEventID(ctx)
			if messageID == "" {
				// No message ID available, can't acquire state - proceed with processing
				// This shouldn't happen in normal operation but fail open for safety
				return next(ctx, ev, data)
			}

			// Attempt to acquire the message state
			acquired, err := sm.Acquire(ctx, messageID, stateTTL)
			if err != nil {
				// State manager error - log and proceed with processing (fail open)
				// This prevents message loss at the cost of potential duplicates
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Warn("state acquisition error, proceeding with handler",
						"message_id", messageID,
						"error", err)
				}
				return next(ctx, ev, data)
			}

			if !acquired {
				// Another worker already acquired this message - skip silently
				// Return nil to acknowledge the message without processing
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Debug("message acquired by another worker, skipping",
						"message_id", messageID)
				}
				return nil
			}

			// We acquired the message state - process it
			handlerErr := next(ctx, ev, data)

			// Record result
			if handlerErr == nil {
				// Success - mark as processed
				if markErr := sm.MarkProcessed(ctx, messageID); markErr != nil {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("failed to mark message as processed",
							"message_id", messageID,
							"error", markErr)
					}
				}
			} else {
				// Failure - reset state for another worker to retry
				if resetErr := sm.Reset(ctx, messageID); resetErr != nil {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("failed to reset message state",
							"message_id", messageID,
							"error", resetErr)
					}
				}
			}

			return handlerErr
		}
	}
}

package distributed

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3"
)

// DistributedWorkerMiddleware creates middleware that emulates WorkerPool mode
// on Broadcast-only transports using distributed message claiming.
//
// This middleware enables load-balanced message processing across multiple
// subscribers even when the underlying transport only supports Broadcast.
// Each message is processed by exactly one worker (the one that successfully
// claims it), while other workers skip the message silently.
//
// How it works:
//  1. Message arrives (delivered to all subscribers via Broadcast)
//  2. Each subscriber calls TryClaim() to attempt claiming the message
//  3. Only the subscriber that succeeds processes the message
//  4. On success: Complete() is called to mark as done
//  5. On failure: Release() is called to allow immediate retry by another worker
//
// Use cases:
//   - MongoDB Change Streams (Broadcast-only) with load-balanced processing
//   - Any transport where native WorkerPool isn't available
//   - Adding worker semantics to existing Broadcast subscriptions
//
// Parameters:
//   - claimer: A MessageClaimer implementation (RedisClaimer for distributed)
//   - claimTTL: How long to hold the claim (should exceed handler timeout)
//
// Example:
//
//	// Redis claimer for distributed deployments
//	claimer := distributed.NewRedisClaimer(redisClient)
//
//	// Subscribe with worker emulation
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
//	    ),
//	)
//
// Claim TTL Guidelines:
//
//	Handler Timeout  |  Recommended Claim TTL
//	-----------------|-----------------------
//	30s              |  1-2 minutes
//	1 minute         |  3-5 minutes
//	5 minutes        |  10-15 minutes
//	No timeout       |  Set based on max expected processing time + buffer
//
// Error Handling:
//
// If the claimer returns an error during TryClaim, the middleware logs the
// error and proceeds with processing. This "fail open" behavior ensures
// messages aren't lost due to claimer issues, at the cost of potential
// duplicates. Handlers should be idempotent.
//
// Worker Groups:
//
// To emulate worker groups with different claimers per group:
//
//	// Group A: order processors
//	claimerA := distributed.NewRedisClaimer(redis,
//	    distributed.WithClaimerPrefix("order-processors:"))
//
//	// Group B: analytics collectors
//	claimerB := distributed.NewRedisClaimer(redis,
//	    distributed.WithClaimerPrefix("analytics:"))
//
//	orderEvent.Subscribe(ctx, processOrder,
//	    event.WithMiddleware(distributed.DistributedWorkerMiddleware[Order](claimerA, ttl)))
//
//	orderEvent.Subscribe(ctx, collectAnalytics,
//	    event.WithMiddleware(distributed.DistributedWorkerMiddleware[Order](claimerB, ttl)))
func DistributedWorkerMiddleware[T any](claimer MessageClaimer, claimTTL time.Duration) event.Middleware[T] {
	return func(next event.Handler[T]) event.Handler[T] {
		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Get message ID from context
			messageID := event.ContextEventID(ctx)
			if messageID == "" {
				// No message ID available, can't claim - proceed with processing
				// This shouldn't happen in normal operation but fail open for safety
				return next(ctx, ev, data)
			}

			// Attempt to claim the message
			claimed, err := claimer.TryClaim(ctx, messageID, claimTTL)
			if err != nil {
				// Claimer error - log and proceed with processing (fail open)
				// This prevents message loss at the cost of potential duplicates
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Warn("distributed worker claim error, proceeding with handler",
						"message_id", messageID,
						"error", err)
				}
				return next(ctx, ev, data)
			}

			if !claimed {
				// Another worker already claimed this message - skip silently
				// Return nil to acknowledge the message without processing
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Debug("message claimed by another worker, skipping",
						"message_id", messageID)
				}
				return nil
			}

			// We claimed the message - process it
			handlerErr := next(ctx, ev, data)

			// Record result
			if handlerErr == nil {
				// Success - mark as completed
				if completeErr := claimer.Complete(ctx, messageID); completeErr != nil {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("failed to complete claim",
							"message_id", messageID,
							"error", completeErr)
					}
				}
			} else {
				// Failure - release claim for another worker to retry
				if releaseErr := claimer.Release(ctx, messageID); releaseErr != nil {
					logger := event.ContextLogger(ctx)
					if logger != nil {
						logger.Warn("failed to release claim",
							"message_id", messageID,
							"error", releaseErr)
					}
				}
			}

			return handlerErr
		}
	}
}

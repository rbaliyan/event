// Package distributed provides utilities to emulate WorkerPool delivery mode
// on transports that only support Broadcast.
//
// Some transports (like MongoDB Change Streams) natively only support Broadcast mode
// where all subscribers receive every message. This package enables WorkerPool
// semantics by using distributed locking to ensure only one worker processes
// each message.
//
// Key components:
//
//   - MessageClaimer: Interface for distributed message claiming (locking)
//   - DistributedWorkerMiddleware: Middleware that uses a claimer to emulate WorkerPool
//   - RedisClaimer: Redis-based implementation using SETNX with TTL
//   - MemoryClaimer: In-memory implementation for single-instance or testing
//
// Architecture:
//
// In a Broadcast transport, all subscribers receive every message. With the
// DistributedWorkerMiddleware, each subscriber attempts to "claim" the message
// using a distributed lock. Only the subscriber that successfully claims the
// message processes it; others skip silently.
//
//	Message arrives (Broadcast to all)
//	    ├── Subscriber 1: TryClaim() → success → process → Complete()
//	    ├── Subscriber 2: TryClaim() → failed → skip (another worker claimed)
//	    └── Subscriber 3: TryClaim() → failed → skip (another worker claimed)
//
// Example usage with MongoDB transport:
//
//	// Create claimer (Redis for distributed deployments)
//	claimer := distributed.NewRedisClaimer(redisClient, distributed.WithClaimerTTL(5*time.Minute))
//
//	// Subscribe with middleware to emulate WorkerPool
//	mongoEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
//	    ),
//	)
//
// Claim TTL:
//
// The claim TTL should be longer than your handler's maximum execution time.
// If a worker crashes or hangs, the claim expires after TTL and another
// worker can claim and process the message. Choose TTL based on:
//   - Handler timeout + buffer
//   - Recovery needs (shorter = faster failover, longer = less duplicate risk)
//
// Worker Groups:
//
// For worker groups with MongoDB, create separate claimers with different prefixes:
//
//	// Group A workers
//	claimerA := distributed.NewRedisClaimer(redis, distributed.WithClaimerPrefix("group-a:"))
//	eventA.Subscribe(ctx, handlerA, event.WithMiddleware(
//	    distributed.DistributedWorkerMiddleware[T](claimerA, ttl),
//	))
//
//	// Group B workers
//	claimerB := distributed.NewRedisClaimer(redis, distributed.WithClaimerPrefix("group-b:"))
//	eventB.Subscribe(ctx, handlerB, event.WithMiddleware(
//	    distributed.DistributedWorkerMiddleware[T](claimerB, ttl),
//	))
package distributed

import (
	"context"
	"time"
)

// MessageClaimer provides distributed claiming (locking) for WorkerPool emulation.
//
// In Broadcast transports where all subscribers receive every message, a claimer
// ensures only one subscriber processes each message by using distributed locks.
//
// Implementations must be:
//   - Atomic: TryClaim must atomically check-and-set to prevent race conditions
//   - Distributed: State must be shared across all application instances
//   - TTL-aware: Claims must expire to handle worker crashes/hangs
//
// Implementations:
//   - RedisClaimer: Uses Redis SETNX with TTL for distributed deployments
//   - MemoryClaimer: In-memory for single-instance or testing
//
// Example implementing a custom claimer:
//
//	type PostgresClaimer struct {
//	    db  *sql.DB
//	    ttl time.Duration
//	}
//
//	func (c *PostgresClaimer) TryClaim(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
//	    result, err := c.db.ExecContext(ctx, `
//	        INSERT INTO message_claims (message_id, expires_at)
//	        VALUES ($1, $2)
//	        ON CONFLICT (message_id) DO NOTHING
//	    `, messageID, time.Now().Add(ttl))
//	    if err != nil {
//	        return false, err
//	    }
//	    rows, _ := result.RowsAffected()
//	    return rows > 0, nil
//	}
type MessageClaimer interface {
	// TryClaim attempts to claim a message for processing.
	//
	// If the claim succeeds, returns (true, nil) and the caller should process
	// the message, then call Complete() on success or Release() on failure.
	//
	// If the claim fails (another worker claimed it), returns (false, nil)
	// and the caller should skip processing (return nil to ack).
	//
	// The ttl parameter specifies how long the claim is valid. After TTL expires,
	// another worker can claim the message (useful for crash recovery).
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: Unique identifier for the message (from event.ContextEventID)
	//   - ttl: How long to hold the claim before it expires
	//
	// Returns:
	//   - (true, nil): Claim successful, process the message
	//   - (false, nil): Already claimed by another worker, skip
	//   - (false, error): Claimer error (log and decide how to handle)
	TryClaim(ctx context.Context, messageID string, ttl time.Duration) (claimed bool, err error)

	// Complete marks a message as successfully processed.
	//
	// After successful processing, call Complete to update the claim state.
	// This typically either:
	//   - Extends the claim TTL to prevent reprocessing during TTL window
	//   - Marks the message as "completed" in a persistent store
	//
	// If Complete fails, the message may be reprocessed when the claim expires.
	// Handlers should be idempotent to handle this safely.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: The message ID that was successfully processed
	//
	// Returns error if the completion record fails (log but don't fail the handler).
	Complete(ctx context.Context, messageID string) error

	// Release releases a claim without completing the message.
	//
	// Call this when processing fails and you want another worker to retry
	// immediately instead of waiting for the claim to expire. This is optional;
	// letting the claim expire naturally is also valid behavior.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: The message ID to release
	//
	// Returns error if the release fails (log but don't affect error propagation).
	Release(ctx context.Context, messageID string) error
}

// ClaimerOption configures a claimer implementation.
type ClaimerOption func(*claimerOptions)

// claimerOptions holds common configuration for claimer implementations.
type claimerOptions struct {
	prefix         string
	ttl            time.Duration
	completionTTL  time.Duration
	cleanupEnabled bool
	cleanupPeriod  time.Duration
}

// defaultClaimerOptions returns sensible defaults for claimer configuration.
func defaultClaimerOptions() *claimerOptions {
	return &claimerOptions{
		prefix:         "claim:",
		ttl:            5 * time.Minute,
		completionTTL:  24 * time.Hour,
		cleanupEnabled: true,
		cleanupPeriod:  time.Hour,
	}
}

// WithClaimerPrefix sets the key prefix for claim entries.
//
// Use different prefixes to create isolated claim namespaces:
//   - Per-service: "order-service:claim:"
//   - Per-environment: "prod:claim:"
//   - Per-worker-group: "processors:claim:"
//
// Default: "claim:"
func WithClaimerPrefix(prefix string) ClaimerOption {
	return func(o *claimerOptions) {
		o.prefix = prefix
	}
}

// WithClaimerTTL sets the default TTL for claims.
//
// The TTL should be longer than your handler's maximum execution time.
// If a worker crashes, other workers can claim the message after TTL expires.
//
// Default: 5 minutes
func WithClaimerTTL(ttl time.Duration) ClaimerOption {
	return func(o *claimerOptions) {
		if ttl > 0 {
			o.ttl = ttl
		}
	}
}

// WithCompletionTTL sets how long to remember completed messages.
//
// After a message is completed, its ID is remembered for this duration
// to prevent reprocessing if the same message is delivered again.
//
// Default: 24 hours
func WithCompletionTTL(ttl time.Duration) ClaimerOption {
	return func(o *claimerOptions) {
		if ttl > 0 {
			o.completionTTL = ttl
		}
	}
}

// WithCleanup enables or disables automatic cleanup of expired entries.
//
// Default: enabled with 1 hour period
func WithCleanup(enabled bool, period time.Duration) ClaimerOption {
	return func(o *claimerOptions) {
		o.cleanupEnabled = enabled
		if period > 0 {
			o.cleanupPeriod = period
		}
	}
}

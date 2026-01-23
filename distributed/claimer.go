// Package distributed provides utilities to emulate WorkerPool delivery mode
// on transports that only support Broadcast.
//
// Some transports (like MongoDB Change Streams) natively only support Broadcast mode
// where all subscribers receive every message. This package enables WorkerPool
// semantics using database atomic state transitions to ensure only one worker
// processes each message.
//
// Design Philosophy:
//
// This package uses database atomic operations, NOT distributed locks.
// Coordination is achieved through atomic state transitions in a shared database:
//
//   - All state managers store state in a shared database (Redis/MongoDB)
//   - State transitions are atomic database operations (SETNX, findOneAndUpdate)
//   - Multiple workers coordinate by atomically transitioning message state
//   - Subscribers MUST share the same database to coordinate
//   - Different databases = no coordination (by design)
//
// Key components:
//
//   - StateManager: Interface for atomic message state transitions
//   - WorkerPoolMiddleware: Middleware that uses a StateManager to emulate WorkerPool
//   - RedisStateManager: Redis-based implementation using atomic SETNX
//   - MongoStateManager: MongoDB-based implementation using findOneAndUpdate
//   - MemoryStateManager: In-memory implementation for single-instance or testing
//
// Architecture:
//
// In a Broadcast transport, all subscribers receive every message. With the
// WorkerPoolMiddleware, each subscriber attempts to atomically acquire the
// message state. Only the subscriber that succeeds processes it; others skip.
//
//	Message arrives (Broadcast to all)
//	    ├── Subscriber 1: Acquire() → success → process → MarkProcessed()
//	    ├── Subscriber 2: Acquire() → failed → skip (another worker acquired)
//	    └── Subscriber 3: Acquire() → failed → skip (another worker acquired)
//
// State Lifecycle:
//
//	(none) → Acquire() → "processing" → MarkProcessed() → "completed"
//	                         │
//	                         └── Reset() → (none) [for retry]
//
// Example usage with MongoDB transport:
//
//	// Create state manager (Redis for distributed deployments)
//	sm := distributed.NewRedisStateManager(redisClient, distributed.WithStateTTL(5*time.Minute))
//
//	// Subscribe with middleware to emulate WorkerPool
//	mongoEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, 5*time.Minute),
//	    ),
//	)
//
// State TTL:
//
// The state TTL should be longer than your handler's maximum execution time.
// If a worker crashes or hangs, the state expires after TTL and another
// worker can acquire and process the message. Choose TTL based on:
//   - Handler timeout + buffer
//   - Recovery needs (shorter = faster failover, longer = less duplicate risk)
//
// Worker Groups:
//
// For worker groups, create separate state managers with different prefixes:
//
//	// Group A workers
//	smA := distributed.NewRedisStateManager(redis, distributed.WithPrefix("group-a:"))
//	eventA.Subscribe(ctx, handlerA, event.WithMiddleware(
//	    distributed.WorkerPoolMiddleware[T](smA, ttl),
//	))
//
//	// Group B workers
//	smB := distributed.NewRedisStateManager(redis, distributed.WithPrefix("group-b:"))
//	eventB.Subscribe(ctx, handlerB, event.WithMiddleware(
//	    distributed.WorkerPoolMiddleware[T](smB, ttl),
//	))
package distributed

import (
	"context"
	"time"
)

// StateManager provides atomic message state transitions for WorkerPool emulation.
//
// In Broadcast transports where all subscribers receive every message, a StateManager
// ensures only one subscriber processes each message using database atomic operations.
//
// Implementations must be:
//   - Atomic: Acquire must atomically check-and-set to prevent race conditions
//   - Shared: State must be stored in a database shared by all coordinating workers
//   - TTL-aware: States must expire to handle worker crashes/hangs
//
// Implementations:
//   - RedisStateManager: Uses Redis atomic SETNX for distributed deployments
//   - MongoStateManager: Uses MongoDB atomic findOneAndUpdate
//   - MemoryStateManager: In-memory for single-instance or testing
//
// Example implementing a custom state manager:
//
//	type PostgresStateManager struct {
//	    db  *sql.DB
//	    ttl time.Duration
//	}
//
//	func (s *PostgresStateManager) Acquire(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
//	    // Atomic INSERT - only succeeds if row doesn't exist
//	    result, err := s.db.ExecContext(ctx, `
//	        INSERT INTO message_state (message_id, status, expires_at)
//	        VALUES ($1, 'processing', $2)
//	        ON CONFLICT (message_id) DO NOTHING
//	    `, messageID, time.Now().Add(ttl))
//	    if err != nil {
//	        return false, err
//	    }
//	    rows, _ := result.RowsAffected()
//	    return rows > 0, nil
//	}
type StateManager interface {
	// Acquire atomically transitions a message to "processing" state.
	//
	// If the transition succeeds, returns (true, nil) and the caller should
	// process the message, then call MarkProcessed() on success or Reset() on failure.
	//
	// If the transition fails (another worker acquired it), returns (false, nil)
	// and the caller should skip processing (return nil to ack).
	//
	// The ttl parameter specifies how long the state is valid. After TTL expires,
	// another worker can acquire the message (useful for crash recovery).
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: Unique identifier for the message (from event.ContextEventID)
	//   - ttl: How long to hold the state before it expires
	//
	// Returns:
	//   - (true, nil): Acquisition successful, process the message
	//   - (false, nil): Already acquired by another worker, skip
	//   - (false, error): Database error (log and decide how to handle)
	Acquire(ctx context.Context, messageID string, ttl time.Duration) (acquired bool, err error)

	// MarkProcessed transitions a message to "completed" state.
	//
	// After successful processing, call MarkProcessed to update the state.
	// This typically either:
	//   - Extends the state TTL to prevent reprocessing during TTL window
	//   - Marks the message as "completed" in a persistent store
	//
	// If MarkProcessed fails, the message may be reprocessed when the state expires.
	// Handlers should be idempotent to handle this safely.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: The message ID that was successfully processed
	//
	// Returns error if the state update fails (log but don't fail the handler).
	MarkProcessed(ctx context.Context, messageID string) error

	// Reset removes the message state to allow immediate reprocessing.
	//
	// Call this when processing fails and you want another worker to retry
	// immediately instead of waiting for the state to expire. This is optional;
	// letting the state expire naturally is also valid behavior.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - messageID: The message ID to reset
	//
	// Returns error if the reset fails (log but don't affect error propagation).
	Reset(ctx context.Context, messageID string) error

	// ListStale returns message IDs of states that appear stale.
	//
	// A stale state is one that has been in "processing" state for longer than
	// the specified staleTimeout. This typically indicates that the worker
	// processing the message has crashed or become unresponsive.
	//
	// This method enables active recovery: instead of waiting for states
	// to expire via TTL (passive), a background goroutine can periodically
	// check for stale states and reset them for faster failover.
	//
	// Parameters:
	//   - ctx: Context for cancellation and timeouts
	//   - staleTimeout: Duration after which a processing state is considered stale.
	//     Should be longer than expected handler execution time but shorter than state TTL.
	//   - limit: Maximum number of stale states to return (0 = no limit)
	//
	// Returns:
	//   - List of message IDs that are stale
	//   - error if the query fails
	//
	// Example staleTimeout values:
	//   - Handler timeout 30s → staleTimeout 1-2 minutes
	//   - Handler timeout 1m → staleTimeout 2-3 minutes
	//   - No timeout → staleTimeout based on expected max processing time
	ListStale(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error)
}

// Option configures a state manager implementation.
type Option func(*stateOptions)

// stateOptions holds common configuration for state manager implementations.
type stateOptions struct {
	prefix         string
	ttl            time.Duration
	completionTTL  time.Duration
	cleanupEnabled bool
	cleanupPeriod  time.Duration
}

// defaultStateOptions returns sensible defaults for state manager configuration.
func defaultStateOptions() *stateOptions {
	return &stateOptions{
		prefix:         "state:",
		ttl:            5 * time.Minute,
		completionTTL:  24 * time.Hour,
		cleanupEnabled: true,
		cleanupPeriod:  time.Hour,
	}
}

// WithPrefix sets the key prefix for state entries.
//
// Use different prefixes to create isolated state namespaces:
//   - Per-service: "order-service:state:"
//   - Per-environment: "prod:state:"
//   - Per-worker-group: "processors:state:"
//
// Default: "state:"
func WithPrefix(prefix string) Option {
	return func(o *stateOptions) {
		o.prefix = prefix
	}
}

// WithStateTTL sets the default TTL for processing states.
//
// The TTL should be longer than your handler's maximum execution time.
// If a worker crashes, other workers can acquire the message after TTL expires.
//
// Default: 5 minutes
func WithStateTTL(ttl time.Duration) Option {
	return func(o *stateOptions) {
		if ttl > 0 {
			o.ttl = ttl
		}
	}
}

// WithCompletedTTL sets how long to remember completed messages.
//
// After a message is completed, its ID is remembered for this duration
// to prevent reprocessing if the same message is delivered again.
//
// Default: 24 hours
func WithCompletedTTL(ttl time.Duration) Option {
	return func(o *stateOptions) {
		if ttl > 0 {
			o.completionTTL = ttl
		}
	}
}

// WithCleanup enables or disables automatic cleanup of expired entries.
//
// Default: enabled with 1 hour period
func WithCleanup(enabled bool, period time.Duration) Option {
	return func(o *stateOptions) {
		o.cleanupEnabled = enabled
		if period > 0 {
			o.cleanupPeriod = period
		}
	}
}

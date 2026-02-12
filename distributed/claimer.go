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
//   - Coordinator: Interface for atomic message state transitions
//   - PayloadStore: Optional interface for persisting message payload for recovery
//   - WorkerPoolMiddleware: Middleware that uses a Coordinator to emulate WorkerPool
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
//	sm := distributed.NewRedisStateManager(redisClient, distributed.WithCompletedTTL(48*time.Hour))
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

	event "github.com/rbaliyan/event/v3"
)

// Coordinator handles atomic state transitions for WorkerPool emulation.
//
// In Broadcast transports where all subscribers receive every message, a Coordinator
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
type Coordinator interface {
	// Acquire atomically transitions a message to "processing" state.
	//
	// Returns:
	//   - (true, nil): Acquisition successful, process the message
	//   - (false, nil): Already acquired by another worker, skip
	//   - (false, error): Database error
	Acquire(ctx context.Context, messageID string, ttl time.Duration) (acquired bool, err error)

	// MarkProcessed transitions a message to "completed" state.
	MarkProcessed(ctx context.Context, messageID string) error

	// Reset removes the message state to allow immediate reprocessing.
	Reset(ctx context.Context, messageID string) error

	// ListStale returns message IDs of states that have been in "processing"
	// state for longer than staleTimeout.
	ListStale(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error)
}

// PayloadStore persists message payload for recovery when the transport
// cannot redeliver lost messages (e.g., MongoDB Change Streams).
//
// This is an optional interface that Coordinator implementations can also
// implement. When a Coordinator also implements PayloadStore:
//   - WorkerPoolMiddleware stores payload after acquiring state
//   - RecoveryRunner re-publishes stale events via the injected Publisher
//
// Implementations:
//   - MongoStateManager: Stores payload in the same document (atomic with state)
//   - RedisStateManager: Stores payload in a companion key
//   - MemoryStateManager: Stores payload in memory (for testing)
type PayloadStore interface {
	// StorePayload persists payload alongside a message ID.
	// Called after a successful Acquire when the transport doesn't support
	// re-delivery. Not required to be atomic with Acquire.
	StorePayload(ctx context.Context, messageID string, data *MessageData) error

	// LoadStalePayloads returns stale messages that have stored payload.
	// Used by RecoveryRunner to re-publish lost events.
	LoadStalePayloads(ctx context.Context, staleTimeout time.Duration, limit int) ([]*StaleMessage, error)

	// ClearPayload removes stored payload for a message.
	// Called after successful processing or recovery re-publish.
	ClearPayload(ctx context.Context, messageID string) error
}

// Publisher sends events for recovery re-publishing.
// This is an alias for event.Sender.
type Publisher = event.Sender

// MessageData holds message payload for recovery re-publishing.
// Fields are populated from event context in WorkerPoolMiddleware:
//   - Payload:   event.ContextRawPayload(ctx) — raw message bytes
//   - Metadata:  event.ContextMetadata(ctx) — message metadata map
//   - EventName: event.ContextName(ctx) — event routing name
type MessageData struct {
	Payload   []byte
	Metadata  map[string]string
	EventName string
}

// StaleMessage is a stale state entry with its stored payload.
type StaleMessage struct {
	MessageID string
	Data      MessageData
	CreatedAt time.Time
}

// HasPayload returns true if the stale message has stored payload data.
func (s *StaleMessage) HasPayload() bool {
	return len(s.Data.Payload) > 0
}

// Option configures a state manager implementation.
type Option func(*stateOptions)

// StaleResetter is an optional interface that Coordinator implementations
// can provide for efficient batch stale state reset.
type StaleResetter interface {
	// ResetStale resets stale states in batch.
	// Returns the number of states reset.
	ResetStale(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error)
}

// stateOptions holds common configuration for state manager implementations.
type stateOptions struct {
	prefix         string
	completionTTL  time.Duration
	cleanupEnabled bool
	cleanupPeriod  time.Duration
	// MongoDB-specific options (only used by NewMongoStateManager)
	collectionName string
	capped         bool
	cappedSize     int64
	cappedMaxDocs  int64
}

// defaultStateOptions returns sensible defaults for state manager configuration.
func defaultStateOptions() *stateOptions {
	return &stateOptions{
		prefix:         "state:",
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

// WithCollection sets the MongoDB collection name for state storage.
//
// This option is only used by NewMongoStateManager and is ignored by other
// state manager implementations.
//
// Default: "_message_state"
func WithCollection(name string) Option {
	return func(o *stateOptions) {
		if name != "" {
			o.collectionName = name
		}
	}
}

// WithCapped enables MongoDB capped collection mode for high-throughput scenarios.
//
// This option is only used by NewMongoStateManager and is ignored by other
// state manager implementations.
//
// Capped collections are fixed-size collections that automatically remove
// the oldest documents when the size limit is reached.
//
// Parameters:
//   - sizeBytes: Maximum collection size in bytes (required, minimum 4096)
//   - maxDocs: Maximum number of documents (0 = unlimited, size-based only)
//
// IMPORTANT LIMITATIONS:
//   - Reset() becomes a no-op (MongoDB doesn't allow deletes in capped collections)
//   - No TTL index support
//   - Failed states wait for size-based removal
func WithCapped(sizeBytes int64, maxDocs int64) Option {
	return func(o *stateOptions) {
		o.capped = true
		o.cappedSize = sizeBytes
		o.cappedMaxDocs = maxDocs
	}
}

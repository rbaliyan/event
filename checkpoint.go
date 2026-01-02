package event

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// CheckpointStore persists subscriber positions for resumable subscriptions.
// Implementations should be safe for concurrent use.
//
// Use this to enable "start from latest, resume on reconnect" semantics:
//   - First connection: starts from latest messages (no checkpoint exists)
//   - Reconnection: resumes from last saved checkpoint
//
// Example implementations:
//   - Redis: store checkpoint as a hash field
//   - PostgreSQL: store in a checkpoints table
//   - File: store as JSON for development
type CheckpointStore interface {
	// Save persists the checkpoint position for a subscriber.
	// The position is typically the message timestamp or sequence number.
	Save(ctx context.Context, subscriberID string, position time.Time) error

	// Load retrieves the last saved checkpoint for a subscriber.
	// Returns zero time and nil error if no checkpoint exists (first run).
	Load(ctx context.Context, subscriberID string) (time.Time, error)

	// Delete removes a checkpoint (optional, for cleanup).
	Delete(ctx context.Context, subscriberID string) error
}

// CheckpointMiddleware creates middleware that automatically saves checkpoints
// after successful message processing.
//
// The middleware:
//   - Calls the next handler
//   - On success (nil error), saves the current time as checkpoint
//   - On failure, does NOT save (message will be reprocessed on restart)
//
// Example:
//
//	store := checkpoint.NewRedisStore(redisClient)
//	ev.Subscribe(ctx, handler,
//	    event.WithMiddleware(event.CheckpointMiddleware[Order](store, "order-processor")),
//	    event.WithCheckpointResume[Order](store, "order-processor"),
//	)
func CheckpointMiddleware[T any](store CheckpointStore, subscriberID string) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			// Process the message first
			err := next(ctx, ev, data)
			if err != nil {
				// Don't save checkpoint on failure - message should be reprocessed
				return err
			}

			// Save checkpoint on success
			// Use message timestamp from context if available, otherwise use current time
			checkpointTime := time.Now()
			if msgTime := ContextMessageTime(ctx); !msgTime.IsZero() {
				checkpointTime = msgTime
			}

			if saveErr := store.Save(ctx, subscriberID, checkpointTime); saveErr != nil {
				// Log but don't fail the handler - message was processed successfully
				if logger := ContextLogger(ctx); logger != nil {
					logger.Warn("failed to save checkpoint",
						"subscriber_id", subscriberID,
						"error", saveErr)
				}
			}

			return nil
		}
	}
}

// WithCheckpointResume configures the subscription to resume from the last
// saved checkpoint. If no checkpoint exists, starts from latest messages.
//
// This is typically used with CheckpointMiddleware for complete checkpoint support:
//
//	store := checkpoint.NewRedisStore(redisClient)
//	subscriberID := "order-processor-1"
//
//	ev.Subscribe(ctx, handler,
//	    event.WithCheckpointResume[Order](store, subscriberID),
//	    event.WithMiddleware(event.CheckpointMiddleware[Order](store, subscriberID)),
//	)
//
// On first run: receives only new messages (no historical backlog)
// On restart: resumes from last successfully processed message
func WithCheckpointResume[T any](store CheckpointStore, subscriberID string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		// Load checkpoint - this happens at subscribe time
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		lastCheckpoint, err := store.Load(ctx, subscriberID)
		if err != nil {
			// If we can't load checkpoint, start from latest (safe default)
			o.startFrom = transport.StartFromLatest
			return
		}

		if lastCheckpoint.IsZero() {
			// No checkpoint exists - first run, start from latest
			o.startFrom = transport.StartFromLatest
		} else {
			// Resume from checkpoint
			o.startFrom = transport.StartFromTimestamp
			o.startTime = lastCheckpoint
		}
	}
}

// WithCheckpoint is a convenience function that combines WithCheckpointResume
// and CheckpointMiddleware for complete checkpoint support.
//
// Example:
//
//	store := checkpoint.NewRedisStore(redisClient)
//	ev.Subscribe(ctx, handler,
//	    event.WithCheckpoint[Order](store, "order-processor"),
//	)
func WithCheckpoint[T any](store CheckpointStore, subscriberID string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		// Set up resume from checkpoint
		WithCheckpointResume[T](store, subscriberID)(o)

		// Add checkpoint middleware
		o.middleware = append(o.middleware, CheckpointMiddleware[T](store, subscriberID))
	}
}

// =============================================================================
// In-Memory Checkpoint Store (for testing and development)
// =============================================================================

// MemoryCheckpointStore is an in-memory checkpoint store for testing.
// Not suitable for production as checkpoints are lost on restart.
type MemoryCheckpointStore struct {
	mu          sync.RWMutex
	checkpoints map[string]time.Time
}

// NewMemoryCheckpointStore creates a new in-memory checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		checkpoints: make(map[string]time.Time),
	}
}

func (s *MemoryCheckpointStore) Save(ctx context.Context, subscriberID string, position time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[subscriberID] = position
	return nil
}

func (s *MemoryCheckpointStore) Load(ctx context.Context, subscriberID string) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpoints[subscriberID], nil
}

func (s *MemoryCheckpointStore) Delete(ctx context.Context, subscriberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.checkpoints, subscriberID)
	return nil
}

// =============================================================================
// Checkpoint Store Errors
// =============================================================================

var (
	// ErrCheckpointNotFound is returned when no checkpoint exists for a subscriber
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

// Compile-time check
var _ CheckpointStore = (*MemoryCheckpointStore)(nil)

// Package checkpoint provides checkpoint stores for persisting subscriber positions.
//
// Checkpoint stores enable "start from latest, resume on reconnect" semantics:
//   - First connection: starts from latest messages (no checkpoint exists)
//   - Reconnection: resumes from last saved checkpoint
//
// Available implementations:
//   - MemoryCheckpointStore: in-memory store for testing
//   - RedisCheckpointStore: Redis-backed store for production
//   - MongoCheckpointStore: MongoDB-backed store for production
package checkpoint

import (
	"context"
	"errors"
	"time"
)

// CheckpointStore persists subscriber positions for resumable subscriptions.
// Implementations should be safe for concurrent use.
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

// Checkpoint Store Errors
var (
	// ErrCheckpointNotFound is returned when no checkpoint exists for a subscriber
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

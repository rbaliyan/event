package checkpoint

import (
	"context"
	"sync"
	"time"
)

// MemoryCheckpointStore is an in-memory checkpoint store for testing.
//
// This store is not suitable for production as data is lost on restart.
// Use RedisStore or MongoStore for production workloads.
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

// Save persists the checkpoint position for a subscriber.
func (s *MemoryCheckpointStore) Save(ctx context.Context, subscriberID string, position time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.checkpoints[subscriberID] = position
	return nil
}

// Load retrieves the last saved checkpoint for a subscriber.
// Returns zero time and nil error if no checkpoint exists.
func (s *MemoryCheckpointStore) Load(ctx context.Context, subscriberID string) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.checkpoints[subscriberID], nil
}

// Delete removes a checkpoint for a subscriber.
func (s *MemoryCheckpointStore) Delete(ctx context.Context, subscriberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.checkpoints, subscriberID)
	return nil
}

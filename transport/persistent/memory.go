package persistent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryStore implements Store using in-memory storage.
// This is useful for testing or single-process deployments where
// persistence across restarts is not required.
//
// Note: Messages are lost on process restart. For true persistence,
// use a PostgreSQL store or the MongoDB persistent store from the
// event-mongodb module (https://github.com/rbaliyan/event-mongodb).
type MemoryStore struct {
	mu       sync.RWMutex
	events   map[string]*memoryEventStore
	sequence int64
}

type memoryEventStore struct {
	messages []*storedEntry
	acked    map[string]bool // Acked message IDs
	inflight map[string]bool // Messages currently being processed
}

type storedEntry struct {
	SequenceID string
	Data       []byte
	Timestamp  time.Time
	RetryCount int
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events: make(map[string]*memoryEventStore),
	}
}

func (s *MemoryStore) getOrCreate(eventName string) *memoryEventStore {
	s.mu.Lock()
	defer s.mu.Unlock()

	if es, ok := s.events[eventName]; ok {
		return es
	}

	es := &memoryEventStore{
		acked:    make(map[string]bool),
		inflight: make(map[string]bool),
	}
	s.events[eventName] = es
	return es
}

// Append adds a message to the store.
func (s *MemoryStore) Append(ctx context.Context, eventName string, data []byte) (string, error) {
	es := s.getOrCreate(eventName)

	seqID := fmt.Sprintf("%d", atomic.AddInt64(&s.sequence, 1))

	s.mu.Lock()
	defer s.mu.Unlock()

	es.messages = append(es.messages, &storedEntry{
		SequenceID: seqID,
		Data:       data,
		Timestamp:  time.Now(),
		RetryCount: 0,
	})

	return seqID, nil
}

// Fetch retrieves the next unprocessed message after the checkpoint.
// Messages are marked as in-flight until Ack or Nack is called.
func (s *MemoryStore) Fetch(ctx context.Context, eventName string, checkpoint string) (*StoredMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	es, ok := s.events[eventName]
	if !ok {
		return nil, nil
	}

	// Find messages after checkpoint that haven't been acked or are in-flight
	foundCheckpoint := checkpoint == ""
	for _, entry := range es.messages {
		if !foundCheckpoint {
			if entry.SequenceID == checkpoint {
				foundCheckpoint = true
			}
			continue
		}

		// Skip acked messages
		if es.acked[entry.SequenceID] {
			continue
		}

		// Skip in-flight messages (already being processed)
		if es.inflight[entry.SequenceID] {
			continue
		}

		// Mark as in-flight
		es.inflight[entry.SequenceID] = true

		return &StoredMessage{
			SequenceID: entry.SequenceID,
			Data:       entry.Data,
			Timestamp:  entry.Timestamp,
			RetryCount: entry.RetryCount,
		}, nil
	}

	return nil, nil
}

// Ack marks a message as acknowledged.
func (s *MemoryStore) Ack(ctx context.Context, eventName string, sequenceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	es, ok := s.events[eventName]
	if !ok {
		return nil
	}

	es.acked[sequenceID] = true
	delete(es.inflight, sequenceID)
	return nil
}

// Nack marks a message for redelivery.
func (s *MemoryStore) Nack(ctx context.Context, eventName string, sequenceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	es, ok := s.events[eventName]
	if !ok {
		return nil
	}

	// Find the entry and increment retry count
	for _, entry := range es.messages {
		if entry.SequenceID == sequenceID {
			entry.RetryCount++
			break
		}
	}

	// Remove from acked and inflight to allow redelivery
	delete(es.acked, sequenceID)
	delete(es.inflight, sequenceID)
	return nil
}

// MemoryCheckpointStore implements CheckpointStore using in-memory storage.
type MemoryCheckpointStore struct {
	mu          sync.RWMutex
	checkpoints map[string]string // key: eventName:consumerID
}

// NewMemoryCheckpointStore creates a new in-memory checkpoint store.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		checkpoints: make(map[string]string),
	}
}

func (s *MemoryCheckpointStore) key(eventName, consumerID string) string {
	return eventName + ":" + consumerID
}

// Load retrieves a checkpoint.
func (s *MemoryCheckpointStore) Load(ctx context.Context, eventName, consumerID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpoints[s.key(eventName, consumerID)], nil
}

// Save persists a checkpoint.
func (s *MemoryCheckpointStore) Save(ctx context.Context, eventName, consumerID string, checkpoint string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[s.key(eventName, consumerID)] = checkpoint
	return nil
}

// Compile-time checks
var (
	_ Store           = (*MemoryStore)(nil)
	_ CheckpointStore = (*MemoryCheckpointStore)(nil)
)

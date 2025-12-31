package dlq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// MemoryStore is an in-memory DLQ store for testing
type MemoryStore struct {
	mu       sync.RWMutex
	messages map[string]*Message
}

// NewMemoryStore creates a new in-memory DLQ store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		messages: make(map[string]*Message),
	}
}

// Store adds a message to the DLQ
func (s *MemoryStore) Store(ctx context.Context, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Make a copy
	stored := *msg
	if msg.Metadata != nil {
		stored.Metadata = make(map[string]string)
		for k, v := range msg.Metadata {
			stored.Metadata[k] = v
		}
	}

	s.messages[msg.ID] = &stored
	return nil
}

// Get retrieves a single message by ID
func (s *MemoryStore) Get(ctx context.Context, id string) (*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.messages[id]
	if !ok {
		return nil, fmt.Errorf("message not found: %s", id)
	}

	// Return a copy
	result := *msg
	return &result, nil
}

// List returns messages matching the filter
func (s *MemoryStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var messages []*Message

	for _, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			result := *msg
			messages = append(messages, &result)
		}
	}

	// Apply pagination
	start := filter.Offset
	if start >= len(messages) {
		return nil, nil
	}

	end := len(messages)
	if filter.Limit > 0 && start+filter.Limit < end {
		end = start + filter.Limit
	}

	return messages[start:end], nil
}

// Count returns the number of messages matching the filter
func (s *MemoryStore) Count(ctx context.Context, filter Filter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int64
	for _, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			count++
		}
	}

	return count, nil
}

// matchesFilter checks if a message matches the filter criteria
func (s *MemoryStore) matchesFilter(msg *Message, filter Filter) bool {
	if filter.EventName != "" && msg.EventName != filter.EventName {
		return false
	}

	if !filter.StartTime.IsZero() && msg.CreatedAt.Before(filter.StartTime) {
		return false
	}

	if !filter.EndTime.IsZero() && msg.CreatedAt.After(filter.EndTime) {
		return false
	}

	if filter.Error != "" && !strings.Contains(msg.Error, filter.Error) {
		return false
	}

	if filter.MaxRetries > 0 && msg.RetryCount > filter.MaxRetries {
		return false
	}

	if filter.Source != "" && msg.Source != filter.Source {
		return false
	}

	if filter.ExcludeRetried && msg.RetriedAt != nil {
		return false
	}

	return true
}

// MarkRetried marks a message as replayed
func (s *MemoryStore) MarkRetried(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[id]
	if !ok {
		return fmt.Errorf("message not found: %s", id)
	}

	now := time.Now()
	msg.RetriedAt = &now
	return nil
}

// Delete removes a message from the DLQ
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.messages[id]; !ok {
		return fmt.Errorf("message not found: %s", id)
	}

	delete(s.messages, id)
	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *MemoryStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-age)
	var deleted int64

	for id, msg := range s.messages {
		if msg.CreatedAt.Before(cutoff) {
			delete(s.messages, id)
			deleted++
		}
	}

	return deleted, nil
}

// DeleteByFilter removes messages matching the filter
func (s *MemoryStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64

	for id, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			delete(s.messages, id)
			deleted++
		}
	}

	return deleted, nil
}

// Stats returns DLQ statistics
func (s *MemoryStore) Stats(ctx context.Context) (*Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	var oldest, newest *time.Time

	for _, msg := range s.messages {
		stats.TotalMessages++

		if msg.RetriedAt != nil {
			stats.RetriedMessages++
		} else {
			stats.PendingMessages++
		}

		stats.MessagesByEvent[msg.EventName]++

		// Track error types (simplified - just use first word)
		errorType := msg.Error
		if idx := strings.Index(errorType, ":"); idx > 0 {
			errorType = errorType[:idx]
		}
		stats.MessagesByError[errorType]++

		if oldest == nil || msg.CreatedAt.Before(*oldest) {
			t := msg.CreatedAt
			oldest = &t
		}
		if newest == nil || msg.CreatedAt.After(*newest) {
			t := msg.CreatedAt
			newest = &t
		}
	}

	stats.OldestMessage = oldest
	stats.NewestMessage = newest

	return stats, nil
}

// Compile-time checks
var _ Store = (*MemoryStore)(nil)
var _ StatsProvider = (*MemoryStore)(nil)

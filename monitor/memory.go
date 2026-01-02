package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore implements Store using in-memory storage.
//
// MemoryStore is primarily intended for testing and development.
// It is not suitable for production use as data is lost on restart.
//
// Example:
//
//	store := monitor.NewMemoryStore()
//	defer store.Close()
//
//	middleware := monitor.Middleware[Order](store)
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]*Entry // key: eventID or eventID:subscriptionID
	closed  bool
}

// NewMemoryStore creates a new in-memory monitor store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		entries: make(map[string]*Entry),
	}
}

// makeKey creates the storage key based on delivery mode.
func makeKey(eventID, subscriptionID string, mode DeliveryMode) string {
	if mode == WorkerPool {
		return eventID
	}
	return eventID + ":" + subscriptionID
}

// Record creates or updates a monitor entry.
func (s *MemoryStore) Record(ctx context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store is closed")
	}

	key := makeKey(entry.EventID, entry.SubscriptionID, entry.DeliveryMode)

	// Create a copy to avoid mutation
	entryCopy := *entry
	if entry.Metadata != nil {
		entryCopy.Metadata = make(map[string]string, len(entry.Metadata))
		for k, v := range entry.Metadata {
			entryCopy.Metadata[k] = v
		}
	}

	s.entries[key] = &entryCopy
	return nil
}

// Get retrieves a monitor entry by its composite key.
func (s *MemoryStore) Get(ctx context.Context, eventID, subscriptionID string) (*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("store is closed")
	}

	// Try WorkerPool key first (eventID only)
	if entry, ok := s.entries[eventID]; ok {
		return entry, nil
	}

	// Try Broadcast key (eventID:subscriptionID)
	key := eventID + ":" + subscriptionID
	if entry, ok := s.entries[key]; ok {
		return entry, nil
	}

	return nil, nil
}

// GetByEventID returns all entries for an event ID.
func (s *MemoryStore) GetByEventID(ctx context.Context, eventID string) ([]*Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("store is closed")
	}

	var entries []*Entry

	for key, entry := range s.entries {
		if entry.EventID == eventID {
			entries = append(entries, entry)
		}
		// Also check for WorkerPool entries where key == eventID
		if key == eventID {
			// Already captured above, but ensure we don't miss it
			continue
		}
	}

	// Sort by started_at
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].StartedAt.Before(entries[j].StartedAt)
	})

	return entries, nil
}

// cursor represents the pagination cursor state.
type cursor struct {
	StartedAt time.Time `json:"s"`
	Key       string    `json:"k"`
}

// encodeCursor encodes a cursor to a string.
func encodeCursor(c cursor) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

// decodeCursor decodes a cursor from a string.
func decodeCursor(s string) (cursor, error) {
	var c cursor
	if s == "" {
		return c, nil
	}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// List returns a page of entries matching the filter.
func (s *MemoryStore) List(ctx context.Context, filter Filter) (*Page, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("store is closed")
	}

	// Collect matching entries
	var matches []*Entry
	for _, entry := range s.entries {
		if s.matchesFilter(entry, filter) {
			matches = append(matches, entry)
		}
	}

	// Sort by started_at
	if filter.OrderDesc {
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].StartedAt.After(matches[j].StartedAt)
		})
	} else {
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].StartedAt.Before(matches[j].StartedAt)
		})
	}

	// Apply cursor
	if filter.Cursor != "" {
		cur, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		// Find the position after the cursor
		idx := 0
		for i, entry := range matches {
			key := makeKey(entry.EventID, entry.SubscriptionID, entry.DeliveryMode)
			if filter.OrderDesc {
				if entry.StartedAt.Before(cur.StartedAt) || (entry.StartedAt.Equal(cur.StartedAt) && key <= cur.Key) {
					idx = i + 1
					break
				}
			} else {
				if entry.StartedAt.After(cur.StartedAt) || (entry.StartedAt.Equal(cur.StartedAt) && key > cur.Key) {
					idx = i
					break
				}
			}
		}
		if idx > 0 {
			matches = matches[idx:]
		}
	}

	// Apply limit
	limit := filter.EffectiveLimit()
	hasMore := len(matches) > limit
	if hasMore {
		matches = matches[:limit]
	}

	// Create next cursor
	var nextCursor string
	if hasMore && len(matches) > 0 {
		lastEntry := matches[len(matches)-1]
		nextCursor = encodeCursor(cursor{
			StartedAt: lastEntry.StartedAt,
			Key:       makeKey(lastEntry.EventID, lastEntry.SubscriptionID, lastEntry.DeliveryMode),
		})
	}

	return &Page{
		Entries:    matches,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// matchesFilter checks if an entry matches the filter criteria.
func (s *MemoryStore) matchesFilter(entry *Entry, filter Filter) bool {
	if filter.EventID != "" && entry.EventID != filter.EventID {
		return false
	}
	if filter.SubscriptionID != "" && entry.SubscriptionID != filter.SubscriptionID {
		return false
	}
	if filter.EventName != "" && entry.EventName != filter.EventName {
		return false
	}
	if filter.BusID != "" && entry.BusID != filter.BusID {
		return false
	}
	if filter.DeliveryMode != nil && entry.DeliveryMode != *filter.DeliveryMode {
		return false
	}
	if len(filter.Status) > 0 {
		found := false
		for _, s := range filter.Status {
			if entry.Status == s {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if filter.HasError != nil {
		hasError := entry.Error != ""
		if *filter.HasError != hasError {
			return false
		}
	}
	if !filter.StartTime.IsZero() && entry.StartedAt.Before(filter.StartTime) {
		return false
	}
	if !filter.EndTime.IsZero() && !entry.StartedAt.Before(filter.EndTime) {
		return false
	}
	if filter.MinDuration > 0 && entry.Duration < filter.MinDuration {
		return false
	}
	if filter.MinRetries > 0 && entry.RetryCount < filter.MinRetries {
		return false
	}
	return true
}

// Count returns the number of entries matching the filter.
func (s *MemoryStore) Count(ctx context.Context, filter Filter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, fmt.Errorf("store is closed")
	}

	var count int64
	for _, entry := range s.entries {
		if s.matchesFilter(entry, filter) {
			count++
		}
	}
	return count, nil
}

// UpdateStatus updates the status and related fields of an existing entry.
func (s *MemoryStore) UpdateStatus(ctx context.Context, eventID, subscriptionID string, status Status, err error, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store is closed")
	}

	// Try WorkerPool key first
	entry, ok := s.entries[eventID]
	if !ok {
		// Try Broadcast key
		key := eventID + ":" + subscriptionID
		entry, ok = s.entries[key]
		if !ok {
			return fmt.Errorf("entry not found: %s", eventID)
		}
	}

	entry.Status = status
	if err != nil {
		entry.Error = err.Error()
	}
	entry.Duration = duration
	now := time.Now()
	entry.CompletedAt = &now

	return nil
}

// DeleteOlderThan removes entries older than the specified age.
func (s *MemoryStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, fmt.Errorf("store is closed")
	}

	cutoff := time.Now().Add(-age)
	var deleted int64

	for key, entry := range s.entries {
		if entry.StartedAt.Before(cutoff) {
			delete(s.entries, key)
			deleted++
		}
	}

	return deleted, nil
}

// Close closes the store.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.entries = nil
	return nil
}

// Len returns the number of entries in the store (for testing).
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// RecordStart records when event processing begins.
// Implements event.MonitorStore interface.
func (s *MemoryStore) RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string,
	workerPool bool, metadata map[string]string, traceID, spanID string) error {

	mode := Broadcast
	if workerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:        eventID,
		SubscriptionID: subscriptionID,
		EventName:      eventName,
		BusID:          busID,
		DeliveryMode:   mode,
		Metadata:       metadata,
		Status:         StatusPending,
		StartedAt:      time.Now(),
		TraceID:        traceID,
		SpanID:         spanID,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *MemoryStore) RecordComplete(ctx context.Context, eventID, subscriptionID, status string,
	handlerErr error, duration time.Duration) error {

	return s.UpdateStatus(ctx, eventID, subscriptionID, Status(status), handlerErr, duration)
}

// Compile-time check that MemoryStore implements Store.
var _ Store = (*MemoryStore)(nil)

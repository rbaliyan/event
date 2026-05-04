package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	event "github.com/rbaliyan/event/v3"
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
//
// In WorkerPool mode, if an entry already exists it is not overwritten.
// This prevents losing workers from replacing the winning worker's data.
func (s *MemoryStore) Record(ctx context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("store is closed")
	}

	key := makeKey(entry.EventID, entry.SubscriptionID, entry.DeliveryMode)

	// In WorkerPool mode, don't overwrite an existing entry
	if entry.DeliveryMode == WorkerPool {
		if _, exists := s.entries[key]; exists {
			return nil
		}
	}

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
	if filter.SubscriberName != "" && entry.SubscriberName != filter.SubscriberName {
		return false
	}
	if filter.WorkerGroup != "" && entry.WorkerGroup != filter.WorkerGroup {
		return false
	}
	if filter.EventName != "" && entry.EventName != filter.EventName {
		return false
	}
	if filter.BusID != "" && entry.BusID != filter.BusID {
		return false
	}
	if filter.InstanceID != "" && entry.InstanceID != filter.InstanceID {
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
func (s *MemoryStore) RecordStart(ctx context.Context, params event.RecordStartParams) error {
	mode := Broadcast
	if params.WorkerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:               params.EventID,
		SubscriptionID:        params.SubscriptionID,
		SubscriberName:        params.SubscriberName,
		SubscriberDescription: params.SubscriberDescription,
		EventName:             params.EventName,
		BusID:                 params.BusID,
		DeliveryMode:          mode,
		Metadata:              params.Metadata,
		Status:                StatusPending,
		StartedAt:             time.Now(),
		TraceID:               params.TraceID,
		SpanID:                params.SpanID,
		WorkerGroup:           params.WorkerGroup,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *MemoryStore) RecordComplete(ctx context.Context, params event.RecordCompleteParams) error {
	return s.UpdateStatus(ctx, params.EventID, params.SubscriptionID, Status(params.Status), params.Error, params.Duration)
}

// RecordPublish records a producer-side publish milestone.
// Implements event.PublishAuditStore by writing an Entry with Status == StatusPublished
// and SubscriptionID == PublishMarker, so it shares the existing storage, indexes,
// pagination, filters, and HTTP/gRPC query surface used by handler entries.
//
// The entry is keyed under (EventID, PublishMarker), which cannot collide with
// real subscription IDs (random base32 strings) or WorkerPool entries (whose
// key is just EventID). Publishing the same event twice silently overwrites —
// at-least-once semantics from outbox relays make this safe.
func (s *MemoryStore) RecordPublish(ctx context.Context, params event.RecordPublishParams) error {
	now := time.Now()
	entry := &Entry{
		EventID:        params.EventID,
		SubscriptionID: PublishMarker,
		EventName:      params.EventName,
		BusID:          params.BusID,
		InstanceID:     params.BusName,
		DeliveryMode:   Broadcast,
		Metadata:       params.Metadata,
		Status:         StatusPublished,
		StartedAt:      now,
		CompletedAt:    &now,
		TraceID:        params.TraceID,
		SpanID:         params.SpanID,
	}
	return s.Record(ctx, entry)
}

// Summary returns aggregated statistics for entries matching the filter.
func (s *MemoryStore) Summary(ctx context.Context, filter Filter) (*Summary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("store is closed")
	}

	summary := &Summary{
		ByStatus:    make(map[Status]int64),
		ByEventName: make(map[string]*EventStats),
		ByInstance:  make(map[string]int64),
	}

	type durationAcc struct {
		totalMs int64
		count   int64
	}
	var globalDur durationAcc
	eventDurations := make(map[string]*durationAcc)

	for _, entry := range s.entries {
		if !s.matchesFilter(entry, filter) {
			continue
		}

		summary.TotalEntries++
		summary.ByStatus[entry.Status]++

		// Per-event stats
		es, ok := summary.ByEventName[entry.EventName]
		if !ok {
			es = &EventStats{}
			summary.ByEventName[entry.EventName] = es
			eventDurations[entry.EventName] = &durationAcc{}
		}
		es.Total++
		switch entry.Status {
		case StatusCompleted:
			es.Completed++
		case StatusFailed:
			es.Failed++
		case StatusRetrying:
			es.Retrying++
		case StatusPending:
			es.Pending++
		}

		if entry.Duration > 0 {
			ms := entry.Duration.Milliseconds()
			globalDur.totalMs += ms
			globalDur.count++
			ed := eventDurations[entry.EventName]
			ed.totalMs += ms
			ed.count++
		}

		if entry.InstanceID != "" {
			summary.ByInstance[entry.InstanceID]++
		}

		t := entry.StartedAt
		if summary.TimeRange.Oldest == nil || t.Before(*summary.TimeRange.Oldest) {
			cp := t
			summary.TimeRange.Oldest = &cp
		}
		if summary.TimeRange.Newest == nil || t.After(*summary.TimeRange.Newest) {
			cp := t
			summary.TimeRange.Newest = &cp
		}
	}

	// Compute global averages and rates
	if globalDur.count > 0 {
		summary.AvgDurationMs = globalDur.totalMs / globalDur.count
	}
	if summary.TotalEntries > 0 {
		summary.ErrorRate = float64(summary.ByStatus[StatusFailed]) / float64(summary.TotalEntries)
	}

	// Compute per-event averages and error rates
	for name, es := range summary.ByEventName {
		if ed := eventDurations[name]; ed.count > 0 {
			es.AvgDurationMs = ed.totalMs / ed.count
		}
		if es.Total > 0 {
			es.ErrorRate = float64(es.Failed) / float64(es.Total)
		}
	}

	if len(summary.ByInstance) == 0 {
		summary.ByInstance = nil
	}

	return summary, nil
}

// Compile-time check that MemoryStore implements Store.
var _ Store = (*MemoryStore)(nil)
var _ SummaryProvider = (*MemoryStore)(nil)

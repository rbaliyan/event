package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// TimestampedItem extends Identifier with a creation timestamp.
// Items stored in MemoryStore should implement this for proper
// time-based filtering and pagination.
type TimestampedItem interface {
	Identifier
	// GetCreatedAt returns when the item was created.
	GetCreatedAt() time.Time
}

// MemoryStore provides a thread-safe in-memory implementation of CoreStore.
// It supports all core operations including time-based filtering and pagination.
//
// This implementation is useful for testing and development.
// For production use, prefer MongoDB, PostgreSQL, or Redis stores.
type MemoryStore[T TimestampedItem] struct {
	mu    sync.RWMutex
	items map[string]T

	// defaultLimit is used when Filter.Limit is zero.
	defaultLimit int
}

// MemoryStoreOption configures a MemoryStore.
type MemoryStoreOption[T TimestampedItem] func(*MemoryStore[T])

// WithDefaultLimit sets the default page size.
func WithDefaultLimit[T TimestampedItem](limit int) MemoryStoreOption[T] {
	return func(s *MemoryStore[T]) {
		if limit > 0 {
			s.defaultLimit = limit
		}
	}
}

// NewMemoryStore creates a new in-memory store.
func NewMemoryStore[T TimestampedItem](opts ...MemoryStoreOption[T]) *MemoryStore[T] {
	s := &MemoryStore[T]{
		items:        make(map[string]T),
		defaultLimit: 100,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Create stores a new item.
func (s *MemoryStore[T]) Create(ctx context.Context, item T) error {
	id := item.GetID()
	if id == "" {
		return ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[id]; exists {
		return ErrAlreadyExists
	}
	s.items[id] = item
	return nil
}

// Get retrieves an item by ID.
func (s *MemoryStore[T]) Get(ctx context.Context, id string) (T, error) {
	var zero T
	if id == "" {
		return zero, ErrInvalidID
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.items[id]
	if !exists {
		return zero, ErrNotFound
	}
	return item, nil
}

// Update modifies an existing item.
func (s *MemoryStore[T]) Update(ctx context.Context, item T) error {
	id := item.GetID()
	if id == "" {
		return ErrInvalidID
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[id]; !exists {
		return ErrNotFound
	}
	s.items[id] = item
	return nil
}

// Delete removes an item by ID.
func (s *MemoryStore[T]) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, id)
	return nil
}

// List retrieves items matching the filter with pagination.
func (s *MemoryStore[T]) List(ctx context.Context, filter Filter) (*Page[T], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect and filter items
	var filtered []T
	for _, item := range s.items {
		if s.matchesFilter(item, filter) {
			filtered = append(filtered, item)
		}
	}

	// Sort by timestamp (and ID for stability)
	sort.Slice(filtered, func(i, j int) bool {
		ti := filtered[i].GetCreatedAt()
		tj := filtered[j].GetCreatedAt()
		if ti.Equal(tj) {
			if filter.OrderDesc {
				return filtered[i].GetID() > filtered[j].GetID()
			}
			return filtered[i].GetID() < filtered[j].GetID()
		}
		if filter.OrderDesc {
			return ti.After(tj)
		}
		return ti.Before(tj)
	})

	// Apply cursor
	cursor := DecodeCursor(filter.Cursor)
	startIdx := 0
	if !cursor.IsZero() {
		for i, item := range filtered {
			ts := item.GetCreatedAt()
			id := item.GetID()
			if filter.OrderDesc {
				if ts.Before(cursor.Timestamp) || (ts.Equal(cursor.Timestamp) && id < cursor.ID) {
					startIdx = i
					break
				}
			} else {
				if ts.After(cursor.Timestamp) || (ts.Equal(cursor.Timestamp) && id > cursor.ID) {
					startIdx = i
					break
				}
			}
		}
	}

	// Apply limit
	limit := filter.Limit
	if limit <= 0 {
		limit = s.defaultLimit
	}

	endIdx := startIdx + limit
	if endIdx > len(filtered) {
		endIdx = len(filtered)
	}

	page := &Page[T]{
		Items: filtered[startIdx:endIdx],
		Total: int64(len(filtered)),
	}

	// Set next cursor if there are more items
	if endIdx < len(filtered) {
		lastItem := page.Items[len(page.Items)-1]
		page.NextCursor = EncodeCursor(NewCursor(lastItem.GetCreatedAt(), lastItem.GetID()))
	}

	return page, nil
}

// Count returns the number of items matching the filter.
func (s *MemoryStore[T]) Count(ctx context.Context, filter Filter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int64
	for _, item := range s.items {
		if s.matchesFilter(item, filter) {
			count++
		}
	}
	return count, nil
}

// DeleteOlderThan removes items older than the given age.
func (s *MemoryStore[T]) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)

	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64
	for id, item := range s.items {
		if item.GetCreatedAt().Before(cutoff) {
			delete(s.items, id)
			deleted++
		}
	}
	return deleted, nil
}

// matchesFilter checks if an item matches the filter criteria.
func (s *MemoryStore[T]) matchesFilter(item T, filter Filter) bool {
	ts := item.GetCreatedAt()

	// Time range filter
	if !filter.StartTime.IsZero() && ts.Before(filter.StartTime) {
		return false
	}
	if !filter.EndTime.IsZero() && ts.After(filter.EndTime) {
		return false
	}

	return true
}

// Len returns the total number of items in the store.
func (s *MemoryStore[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.items)
}

// Clear removes all items from the store.
func (s *MemoryStore[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[string]T)
}

// Compile-time interface checks.
var _ CoreStore[TimestampedItem] = (*MemoryStore[TimestampedItem])(nil)
var _ CleanableStore[TimestampedItem] = (*MemoryStore[TimestampedItem])(nil)

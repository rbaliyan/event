package saga

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryStore is an in-memory saga store for testing
type MemoryStore struct {
	mu    sync.RWMutex
	sagas map[string]*State
}

// NewMemoryStore creates a new in-memory saga store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sagas: make(map[string]*State),
	}
}

// Create creates a new saga instance
func (s *MemoryStore) Create(ctx context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.sagas[state.ID]; exists {
		return fmt.Errorf("saga already exists: %s", state.ID)
	}

	// Make a copy
	stored := *state
	s.sagas[state.ID] = &stored

	return nil
}

// Get retrieves saga state by ID
func (s *MemoryStore) Get(ctx context.Context, id string) (*State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.sagas[id]
	if !ok {
		return nil, fmt.Errorf("saga not found: %s", id)
	}

	// Return a copy
	result := *state
	return &result, nil
}

// Update updates saga state
func (s *MemoryStore) Update(ctx context.Context, state *State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.sagas[state.ID]; !exists {
		return fmt.Errorf("saga not found: %s", state.ID)
	}

	// Make a copy
	stored := *state
	s.sagas[state.ID] = &stored

	return nil
}

// List lists sagas matching the filter
func (s *MemoryStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*State

	for _, state := range s.sagas {
		if filter.Name != "" && state.Name != filter.Name {
			continue
		}

		if len(filter.Status) > 0 {
			matched := false
			for _, status := range filter.Status {
				if state.Status == status {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		result := *state
		results = append(results, &result)

		if filter.Limit > 0 && len(results) >= filter.Limit {
			break
		}
	}

	return results, nil
}

// Cleanup removes completed sagas older than the specified age
func (s *MemoryStore) Cleanup(age time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-age)
	deleted := 0

	for id, state := range s.sagas {
		if state.CompletedAt != nil && state.CompletedAt.Before(cutoff) {
			delete(s.sagas, id)
			deleted++
		}
	}

	return deleted
}

// Compile-time check
var _ Store = (*MemoryStore)(nil)

package distributed

import (
	"context"
	"sync"
	"time"
)

// stateValue represents the state of a message.
type stateValue int

const (
	stateProcessing stateValue = iota
	stateCompleted
)

// stateEntry stores message state information.
type stateEntry struct {
	state     stateValue
	expiresAt time.Time
	updatedAt time.Time
	payload   *MessageData // optional payload for recovery re-publishing
}

// MemoryStateManager implements Coordinator and PayloadStore using in-memory storage.
//
// MemoryStateManager is suitable for:
//   - Single-instance deployments where only one process handles messages
//   - Development and testing environments
//   - Local workloads where distributed coordination isn't needed
//
// IMPORTANT: MemoryStateManager does NOT provide distributed coordination.
// In multi-instance deployments, each instance has its own independent state,
// so the same message could be processed by multiple instances. Use
// RedisStateManager or a MongoDB-backed state manager (available in the
// event-mongodb module: https://github.com/rbaliyan/event-mongodb) for
// production distributed deployments.
//
// Features:
//   - Thread-safe using mutex synchronization
//   - Automatic cleanup of expired entries (background goroutine)
//   - Configurable TTL and cleanup period
//   - No external dependencies
//
// Example:
//
//	// Create memory state manager
//	sm := distributed.NewMemoryStateManager()
//
//	// With custom options
//	sm := distributed.NewMemoryStateManager(
//	    distributed.WithCompletedTTL(24*time.Hour),
//	)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, 5*time.Minute),
//	    ),
//	)
type MemoryStateManager struct {
	mu            sync.RWMutex
	states        map[string]*stateEntry
	completionTTL time.Duration
	stopCleanup   chan struct{}
	cleanupDone   chan struct{}
}

// NewMemoryStateManager creates a new in-memory state manager.
//
// The state manager automatically starts a background goroutine for cleanup.
// Call Close() when done to stop the cleanup goroutine.
//
// Parameters:
//   - opts: Optional configuration (TTLs, cleanup settings)
//
// Returns a configured MemoryStateManager ready for use.
//
// Example:
//
//	sm := distributed.NewMemoryStateManager()
//	defer sm.Close()
//
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, ttl),
//	    ),
//	)
func NewMemoryStateManager(opts ...Option) *MemoryStateManager {
	o := defaultStateOptions()
	for _, opt := range opts {
		opt(o)
	}

	s := &MemoryStateManager{
		states:        make(map[string]*stateEntry),
		completionTTL: o.completionTTL,
		stopCleanup:   make(chan struct{}),
		cleanupDone:   make(chan struct{}),
	}

	if o.cleanupEnabled {
		go s.cleanup(o.cleanupPeriod)
	} else {
		close(s.cleanupDone)
	}

	return s
}

// Acquire atomically transitions a message to "processing" state.
//
// If the message is not in any state or the existing state has expired,
// creates a new processing state and returns true. Otherwise returns false.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message to acquire
//   - ttl: How long to hold the state
//
// Returns:
//   - (true, nil): Acquisition succeeded
//   - (false, nil): Already acquired or completed
func (s *MemoryStateManager) Acquire(_ context.Context, messageID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	// Check existing state
	if entry, exists := s.states[messageID]; exists {
		// Check if expired
		if now.Before(entry.expiresAt) {
			// Active state exists (processing or completed) - cannot acquire
			return false, nil
		}
		// Expired - can reacquire
	}

	// Create new processing state
	s.states[messageID] = &stateEntry{
		state:     stateProcessing,
		expiresAt: now.Add(ttl),
		updatedAt: now,
	}

	return true, nil
}

// MarkProcessed transitions a message to "completed" state.
//
// Updates the state to completed and extends the expiry time
// to prevent reprocessing within the completion window.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message that was successfully processed
//
// Returns nil (always succeeds for memory store).
func (s *MemoryStateManager) MarkProcessed(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if entry, exists := s.states[messageID]; exists {
		entry.state = stateCompleted
		entry.expiresAt = now.Add(s.completionTTL)
		entry.updatedAt = now
	}

	return nil
}

// Reset removes the message state to allow immediate reacquisition.
//
// Deletes the state entry so another Acquire can succeed immediately.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message to reset
//
// Returns nil (always succeeds for memory store).
func (s *MemoryStateManager) Reset(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.states, messageID)
	return nil
}

// ListStale returns message IDs of states that have been processing
// for longer than staleTimeout.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to return (0 = no limit)
//
// Returns list of message IDs that are stale.
func (s *MemoryStateManager) ListStale(_ context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cutoff := time.Now().Add(-staleTimeout)
	var stale []string

	for id, entry := range s.states {
		if entry.state == stateProcessing && entry.updatedAt.Before(cutoff) {
			stale = append(stale, id)
			if limit > 0 && len(stale) >= limit {
				break
			}
		}
	}

	return stale, nil
}

// StorePayload persists payload alongside a message ID.
func (s *MemoryStateManager) StorePayload(_ context.Context, messageID string, data *MessageData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.states[messageID]; exists {
		entry.payload = data
	}
	return nil
}

// LoadStalePayloads returns stale messages that have stored payload.
func (s *MemoryStateManager) LoadStalePayloads(_ context.Context, staleTimeout time.Duration, limit int) ([]*StaleMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cutoff := time.Now().Add(-staleTimeout)
	var result []*StaleMessage

	for id, entry := range s.states {
		if entry.state == stateProcessing && entry.updatedAt.Before(cutoff) && entry.payload != nil {
			sm := &StaleMessage{
				MessageID: id,
				Data:      *entry.payload,
				CreatedAt: entry.updatedAt,
			}
			result = append(result, sm)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, nil
}

// ClearPayload removes stored payload for a message.
func (s *MemoryStateManager) ClearPayload(_ context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.states[messageID]; exists {
		entry.payload = nil
	}
	return nil
}

// ResetStale resets all stale states, allowing them to be reacquired.
//
// This is a convenience method that combines ListStale and Reset.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to reset (0 = no limit)
//
// Returns the number of states reset.
func (s *MemoryStateManager) ResetStale(_ context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-staleTimeout)
	var reset int64

	for id, entry := range s.states {
		if entry.state == stateProcessing && entry.updatedAt.Before(cutoff) {
			delete(s.states, id)
			reset++
			if limit > 0 && reset >= int64(limit) {
				break
			}
		}
	}

	return reset, nil
}

// Close stops the background cleanup goroutine.
//
// Call this when the state manager is no longer needed to prevent goroutine leaks.
// After Close(), the state manager can still be used but expired entries won't
// be automatically cleaned up.
func (s *MemoryStateManager) Close() {
	select {
	case <-s.stopCleanup:
		// Already closed
	default:
		close(s.stopCleanup)
		<-s.cleanupDone
	}
}

// cleanup periodically removes expired entries.
func (s *MemoryStateManager) cleanup(period time.Duration) {
	defer close(s.cleanupDone)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCleanup:
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

// cleanupExpired removes all expired entries.
func (s *MemoryStateManager) cleanupExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, entry := range s.states {
		if now.After(entry.expiresAt) {
			delete(s.states, id)
		}
	}
}

// Compile-time interface checks
var (
	_ Coordinator   = (*MemoryStateManager)(nil)
	_ PayloadStore  = (*MemoryStateManager)(nil)
	_ StaleResetter = (*MemoryStateManager)(nil)
)

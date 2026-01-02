// Package poison provides poison message detection and quarantine.
//
// Poison messages are messages that repeatedly fail processing and waste
// resources by being retried indefinitely. This package provides mechanisms to:
//   - Track failure counts per message
//   - Quarantine messages that exceed a failure threshold
//   - Automatically skip quarantined messages
//   - Release messages from quarantine after investigation
//
// # Overview
//
// The package provides:
//   - Detector: Tracks failures and quarantines poison messages
//   - Store interface: For failure count and quarantine persistence
//   - MemoryStore: In-memory implementation for single-instance deployments
//   - RedisStore: Distributed implementation for multi-instance deployments
//
// # When to Use Poison Detection
//
// Use poison detection when:
//   - Messages may contain data that causes handler crashes
//   - Handler bugs could cause infinite retry loops
//   - You need to protect against malformed messages
//   - System resources should be preserved from bad messages
//
// # Basic Usage
//
// Create a detector and use it to check/record failures:
//
//	store := poison.NewMemoryStore()
//	detector := poison.NewDetector(store,
//	    poison.WithThreshold(5),
//	    poison.WithQuarantineTime(time.Hour),
//	)
//
//	func handleMessage(ctx context.Context, msg Message) error {
//	    // Check if message is quarantined
//	    if poisoned, _ := detector.Check(ctx, msg.ID); poisoned {
//	        return poison.NewError(msg.ID, "message is quarantined")
//	    }
//
//	    if err := process(msg); err != nil {
//	        // Record failure - returns true if now quarantined
//	        quarantined, _ := detector.RecordFailure(ctx, msg.ID)
//	        if quarantined {
//	            log.Warn("message quarantined", "id", msg.ID)
//	        }
//	        return err
//	    }
//
//	    // Clear failures on success
//	    detector.RecordSuccess(ctx, msg.ID)
//	    return nil
//	}
//
// # Distributed Deployments
//
// For multi-instance deployments, use RedisStore:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := poison.NewRedisStore(rdb)
//	detector := poison.NewDetector(store)
//
// # Best Practices
//
//   - Set appropriate thresholds (typically 3-10 failures)
//   - Use reasonable quarantine times (hours to days)
//   - Monitor quarantined messages for investigation
//   - Implement release mechanisms for fixed messages
//   - Clean up old quarantine entries periodically
package poison

import (
	"context"
	"sync"
	"time"
)

// Store tracks failure counts and quarantine status for messages.
//
// Implementations must be safe for concurrent use. The store provides
// the persistence layer for poison message detection.
//
// Implementations:
//   - MemoryStore: In-memory, single-instance only
//   - RedisStore: Distributed, for multi-instance deployments
type Store interface {
	// IncrementFailure increments the failure count for a message.
	// Returns the new count after incrementing.
	IncrementFailure(ctx context.Context, messageID string) (int, error)

	// GetFailureCount returns the current failure count for a message.
	// Returns 0 if the message has no recorded failures.
	GetFailureCount(ctx context.Context, messageID string) (int, error)

	// MarkPoison marks a message as poisoned/quarantined for the given duration.
	// After ttl expires, the message is automatically released.
	MarkPoison(ctx context.Context, messageID string, ttl time.Duration) error

	// IsPoison checks if a message is currently quarantined.
	// Returns false if the message was never quarantined or has expired.
	IsPoison(ctx context.Context, messageID string) (bool, error)

	// ClearPoison removes a message from quarantine immediately.
	// Use after investigating and fixing the issue.
	ClearPoison(ctx context.Context, messageID string) error

	// ClearFailures resets the failure count for a message.
	// Called after successful processing.
	ClearFailures(ctx context.Context, messageID string) error
}

// MemoryStore is an in-memory implementation of Store.
//
// MemoryStore provides poison message tracking for single-instance deployments.
// Data is not persisted across restarts. For distributed systems, use RedisStore.
//
// MemoryStore is safe for concurrent use.
//
// Example:
//
//	store := poison.NewMemoryStore()
//	detector := poison.NewDetector(store)
//
//	// Optionally run cleanup periodically
//	go func() {
//	    ticker := time.NewTicker(time.Hour)
//	    for range ticker.C {
//	        store.Cleanup()
//	    }
//	}()
type MemoryStore struct {
	mu          sync.RWMutex
	failures    map[string]int
	quarantined map[string]time.Time // messageID -> expiry time
}

// NewMemoryStore creates a new in-memory poison store.
//
// Example:
//
//	store := poison.NewMemoryStore()
//	detector := poison.NewDetector(store)
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		failures:    make(map[string]int),
		quarantined: make(map[string]time.Time),
	}
}

// IncrementFailure increments and returns the failure count for a message.
func (s *MemoryStore) IncrementFailure(ctx context.Context, messageID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.failures[messageID]++
	return s.failures[messageID], nil
}

// GetFailureCount returns the current failure count for a message.
// Returns 0 if the message has no recorded failures.
func (s *MemoryStore) GetFailureCount(ctx context.Context, messageID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.failures[messageID], nil
}

// MarkPoison marks a message as quarantined until the TTL expires.
func (s *MemoryStore) MarkPoison(ctx context.Context, messageID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.quarantined[messageID] = time.Now().Add(ttl)
	return nil
}

// IsPoison checks if a message is currently quarantined.
// Returns false if never quarantined or if the quarantine has expired.
func (s *MemoryStore) IsPoison(ctx context.Context, messageID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expiry, exists := s.quarantined[messageID]
	if !exists {
		return false, nil
	}

	// Check if quarantine has expired
	if time.Now().After(expiry) {
		return false, nil
	}

	return true, nil
}

// ClearPoison removes a message from quarantine immediately.
func (s *MemoryStore) ClearPoison(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.quarantined, messageID)
	return nil
}

// ClearFailures resets the failure count for a message to zero.
func (s *MemoryStore) ClearFailures(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.failures, messageID)
	return nil
}

// Cleanup removes expired quarantine entries to free memory.
//
// Call this periodically to prevent unbounded memory growth when using
// long quarantine times. Expired entries are automatically ignored by
// IsPoison, so this is purely for memory management.
//
// Example:
//
//	// Clean up every hour
//	go func() {
//	    ticker := time.NewTicker(time.Hour)
//	    for range ticker.C {
//	        store.Cleanup()
//	    }
//	}()
func (s *MemoryStore) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, expiry := range s.quarantined {
		if now.After(expiry) {
			delete(s.quarantined, id)
		}
	}
}

// Compile-time check
var _ Store = (*MemoryStore)(nil)

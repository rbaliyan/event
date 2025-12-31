package idempotency

import (
	"context"
	"sync"
	"time"
)

// MemoryStore implements Store using in-memory storage with TTL support.
//
// MemoryStore is ideal for:
//   - Single-instance deployments
//   - Development and testing
//   - High-performance scenarios where durability is not critical
//
// Limitations:
//   - Data is lost on process restart
//   - Does not work across multiple instances
//   - Memory usage grows with number of tracked messages
//
// For distributed deployments, use RedisStore instead.
//
// Example:
//
//	// Create store with 1-hour TTL for tracked messages
//	store := idempotency.NewMemoryStore(time.Hour)
//
//	// Use in message handler
//	func handleOrder(ctx context.Context, order Order) error {
//	    key := fmt.Sprintf("order:%s", order.ID)
//
//	    isDuplicate, _ := store.IsDuplicate(ctx, key)
//	    if isDuplicate {
//	        log.Info("duplicate order, skipping", "order_id", order.ID)
//	        return nil
//	    }
//
//	    if err := processOrder(order); err != nil {
//	        return err
//	    }
//
//	    return store.MarkProcessed(ctx, key)
//	}
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]time.Time // messageID -> expiry time
	ttl     time.Duration
	stopCh  chan struct{}
}

// NewMemoryStore creates a new in-memory idempotency store.
//
// The ttl parameter specifies how long to remember processed message IDs.
// After the TTL expires, entries are automatically cleaned up and the
// message ID can be processed again if redelivered.
//
// A background goroutine runs every minute to clean up expired entries.
// Call Close() when done to stop the cleanup goroutine.
//
// Parameters:
//   - ttl: Duration to keep processed message IDs (e.g., time.Hour)
//
// Example:
//
//	// Keep entries for 24 hours
//	store := idempotency.NewMemoryStore(24 * time.Hour)
//	defer store.Close() // Stop cleanup goroutine
//
//	// For testing, use shorter TTL
//	testStore := idempotency.NewMemoryStore(time.Second)
func NewMemoryStore(ttl time.Duration) *MemoryStore {
	s := &MemoryStore{
		entries: make(map[string]time.Time),
		ttl:     ttl,
		stopCh:  make(chan struct{}),
	}

	// Start cleanup goroutine
	go s.cleanup()

	return s
}

// IsDuplicate checks if a message ID has already been processed.
//
// Thread-safe: Uses read lock for concurrent access.
//
// Returns:
//   - (true, nil): Message was processed and TTL has not expired
//   - (false, nil): Message is new or TTL has expired
//
// Example:
//
//	isDuplicate, err := store.IsDuplicate(ctx, "msg-123")
//	if err != nil {
//	    return err
//	}
//	if isDuplicate {
//	    return nil // Skip duplicate
//	}
//	// Process message...
func (s *MemoryStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expiry, exists := s.entries[messageID]
	if !exists {
		return false, nil
	}

	// Check if expired
	if time.Now().After(expiry) {
		return false, nil
	}

	return true, nil
}

// MarkProcessed marks a message ID as processed using the default TTL.
//
// Thread-safe: Uses write lock for concurrent access.
//
// This stores the message ID with an expiry time of now + TTL.
// The entry will be automatically cleaned up after the TTL expires.
//
// Example:
//
//	if err := store.MarkProcessed(ctx, "order-456"); err != nil {
//	    log.Error("failed to mark as processed", "error", err)
//	}
func (s *MemoryStore) MarkProcessed(ctx context.Context, messageID string) error {
	return s.MarkProcessedWithTTL(ctx, messageID, s.ttl)
}

// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
//
// Thread-safe: Uses write lock for concurrent access.
//
// Use this when different message types require different retention periods.
//
// Parameters:
//   - ctx: Context (unused but kept for interface compatibility)
//   - messageID: The unique message identifier
//   - ttl: Custom time-to-live for this entry
//
// Example:
//
//	// High-value transactions: keep for 7 days
//	store.MarkProcessedWithTTL(ctx, "payment-789", 7*24*time.Hour)
//
//	// Ephemeral notifications: keep for 5 minutes
//	store.MarkProcessedWithTTL(ctx, "notification-abc", 5*time.Minute)
func (s *MemoryStore) MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[messageID] = time.Now().Add(ttl)
	return nil
}

// Remove removes a message ID from the store.
//
// Thread-safe: Uses write lock for concurrent access.
//
// After removal, the message ID is no longer considered a duplicate
// and can be processed again if redelivered.
//
// Use cases:
//   - Testing: Reset state between tests
//   - Manual intervention: Allow reprocessing after fixing an issue
//   - Cleanup: Remove specific entries without waiting for TTL
//
// Example:
//
//	// Allow reprocessing of a specific message
//	if err := store.Remove(ctx, "order-456"); err != nil {
//	    log.Error("failed to remove entry", "error", err)
//	}
func (s *MemoryStore) Remove(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, messageID)
	return nil
}

// Close stops the background cleanup goroutine.
//
// Should be called when the store is no longer needed to prevent
// goroutine leaks. Safe to call multiple times.
//
// Example:
//
//	store := idempotency.NewMemoryStore(time.Hour)
//	defer store.Close()
func (s *MemoryStore) Close() {
	select {
	case <-s.stopCh:
		// Already closed
	default:
		close(s.stopCh)
	}
}

// Len returns the number of entries currently in the store.
//
// Thread-safe: Uses read lock for concurrent access.
//
// Note: This includes entries that may have expired but haven't
// been cleaned up yet.
//
// Example:
//
//	count := store.Len()
//	log.Info("tracked messages", "count", count)
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// cleanup periodically removes expired entries.
//
// This runs in a background goroutine started by NewMemoryStore.
// It checks for expired entries every minute and removes them
// to prevent unbounded memory growth.
func (s *MemoryStore) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for id, expiry := range s.entries {
				if now.After(expiry) {
					delete(s.entries, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

// Compile-time check that MemoryStore implements Store interface
var _ Store = (*MemoryStore)(nil)

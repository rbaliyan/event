package distributed

import (
	"context"
	"sync"
	"time"
)

// claimState represents the state of a claimed message.
type claimState int

const (
	claimPending claimState = iota
	claimCompleted
)

// claimEntry stores claim information.
type claimEntry struct {
	state     claimState
	expiresAt time.Time
	updatedAt time.Time
}

// MemoryClaimer implements MessageClaimer using in-memory storage.
//
// MemoryClaimer is suitable for:
//   - Single-instance deployments where only one process handles messages
//   - Development and testing environments
//   - Local workloads where distributed coordination isn't needed
//
// IMPORTANT: MemoryClaimer does NOT provide distributed locking. In multi-instance
// deployments, each instance has its own independent claim state, so the same
// message could be processed by multiple instances. Use RedisClaimer for
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
//	// Create memory claimer
//	claimer := distributed.NewMemoryClaimer()
//
//	// With custom options
//	claimer := distributed.NewMemoryClaimer(
//	    distributed.WithClaimerTTL(5*time.Minute),
//	    distributed.WithCompletionTTL(24*time.Hour),
//	)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
//	    ),
//	)
type MemoryClaimer struct {
	mu            sync.RWMutex
	claims        map[string]*claimEntry
	completionTTL time.Duration
	stopCleanup   chan struct{}
	cleanupDone   chan struct{}
}

// NewMemoryClaimer creates a new in-memory message claimer.
//
// The claimer automatically starts a background goroutine for cleanup.
// Call Close() when done to stop the cleanup goroutine.
//
// Parameters:
//   - opts: Optional configuration (TTLs, cleanup settings)
//
// Returns a configured MemoryClaimer ready for use.
//
// Example:
//
//	claimer := distributed.NewMemoryClaimer()
//	defer claimer.Close()
//
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, ttl),
//	    ),
//	)
func NewMemoryClaimer(opts ...ClaimerOption) *MemoryClaimer {
	o := defaultClaimerOptions()
	for _, opt := range opts {
		opt(o)
	}

	c := &MemoryClaimer{
		claims:        make(map[string]*claimEntry),
		completionTTL: o.completionTTL,
		stopCleanup:   make(chan struct{}),
		cleanupDone:   make(chan struct{}),
	}

	if o.cleanupEnabled {
		go c.cleanup(o.cleanupPeriod)
	} else {
		close(c.cleanupDone)
	}

	return c
}

// TryClaim attempts to claim a message.
//
// If the message is not claimed or the existing claim has expired,
// creates a new claim and returns true. Otherwise returns false.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message to claim
//   - ttl: How long to hold the claim
//
// Returns:
//   - (true, nil): Claim succeeded
//   - (false, nil): Already claimed or completed
func (c *MemoryClaimer) TryClaim(_ context.Context, messageID string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	// Check existing claim
	if entry, exists := c.claims[messageID]; exists {
		// Check if expired
		if now.Before(entry.expiresAt) {
			// Active claim exists (pending or completed) - cannot claim
			return false, nil
		}
		// Expired - can reclaim
	}

	// Create new claim
	c.claims[messageID] = &claimEntry{
		state:     claimPending,
		expiresAt: now.Add(ttl),
		updatedAt: now,
	}

	return true, nil
}

// Complete marks a message as successfully processed.
//
// Updates the claim state to completed and extends the expiry time
// to prevent reprocessing within the completion window.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message that was successfully processed
//
// Returns nil (always succeeds for memory store).
func (c *MemoryClaimer) Complete(_ context.Context, messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if entry, exists := c.claims[messageID]; exists {
		entry.state = claimCompleted
		entry.expiresAt = now.Add(c.completionTTL)
		entry.updatedAt = now
	}

	return nil
}

// Release removes the claim to allow immediate retry.
//
// Deletes the claim entry so another TryClaim can succeed immediately.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - messageID: The message to release
//
// Returns nil (always succeeds for memory store).
func (c *MemoryClaimer) Release(_ context.Context, messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.claims, messageID)
	return nil
}

// Close stops the background cleanup goroutine.
//
// Call this when the claimer is no longer needed to prevent goroutine leaks.
// After Close(), the claimer can still be used but expired entries won't
// be automatically cleaned up.
func (c *MemoryClaimer) Close() {
	select {
	case <-c.stopCleanup:
		// Already closed
	default:
		close(c.stopCleanup)
		<-c.cleanupDone
	}
}

// cleanup periodically removes expired entries.
func (c *MemoryClaimer) cleanup(period time.Duration) {
	defer close(c.cleanupDone)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCleanup:
			return
		case <-ticker.C:
			c.cleanupExpired()
		}
	}
}

// cleanupExpired removes all expired entries.
func (c *MemoryClaimer) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for id, entry := range c.claims {
		if now.After(entry.expiresAt) {
			delete(c.claims, id)
		}
	}
}

// ListOrphanedClaims returns message IDs of claims that have been pending
// for longer than staleTimeout.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - staleTimeout: How long a claim can be pending before considered orphaned
//   - limit: Maximum number of orphans to return (0 = no limit)
//
// Returns list of message IDs that are orphaned.
func (c *MemoryClaimer) ListOrphanedClaims(_ context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	cutoff := time.Now().Add(-staleTimeout)
	var orphans []string

	for id, entry := range c.claims {
		if entry.state == claimPending && entry.updatedAt.Before(cutoff) {
			orphans = append(orphans, id)
			if limit > 0 && len(orphans) >= limit {
				break
			}
		}
	}

	return orphans, nil
}

// ReleaseOrphans releases all orphaned claims, allowing them to be reclaimed.
//
// This is a convenience method that combines ListOrphanedClaims and Release.
//
// Parameters:
//   - ctx: Context (unused but required for interface)
//   - staleTimeout: How long a claim can be pending before considered orphaned
//   - limit: Maximum number of orphans to release (0 = no limit)
//
// Returns the number of claims released.
func (c *MemoryClaimer) ReleaseOrphans(_ context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := time.Now().Add(-staleTimeout)
	var released int64

	for id, entry := range c.claims {
		if entry.state == claimPending && entry.updatedAt.Before(cutoff) {
			delete(c.claims, id)
			released++
			if limit > 0 && released >= int64(limit) {
				break
			}
		}
	}

	return released, nil
}

// Compile-time interface check
var _ MessageClaimer = (*MemoryClaimer)(nil)

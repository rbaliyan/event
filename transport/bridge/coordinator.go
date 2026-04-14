package bridge

import (
	"context"
	"sync"
	"time"
)

// Coordinator decides which bridge replica publishes a given message.
//
// Implementations MUST be safe for concurrent use across goroutines.
// Implementations SHOULD be safe for concurrent use across processes
// when they are the operative unit of coordination (which is the whole
// point). Single-process implementations are only useful for testing
// or single-replica deployments.
type Coordinator interface {
	// Claim atomically asserts ownership of the given dedup key for this
	// replica. Returns true if this call acquired the key, false if
	// another replica has already claimed it.
	//
	// TTL bounds how long the claim is remembered. The claim MUST expire
	// after TTL to cap memory/storage usage, but SHOULD live at least as
	// long as the source's redelivery window so replays don't trigger
	// double publishes.
	//
	// An error return indicates the coordinator itself is unhealthy;
	// the bridge applies its fail-open or fail-closed policy.
	Claim(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// NoopCoordinator disables deduplication: every call to Claim returns
// true. Every bridge replica publishes every source message, producing
// duplicates in the sink proportional to the replica count.
//
// Use this only when:
//   - Running a single replica, or
//   - The sink itself deduplicates by message ID (e.g. a keyed store), or
//   - Consumers are idempotent and duplicate publish cost is acceptable.
type NoopCoordinator struct{}

// Claim always returns true.
func (NoopCoordinator) Claim(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}

// MemoryCoordinator is a process-local deduplication coordinator backed
// by an in-memory map. It is safe for concurrent use within a single
// process but provides NO coordination across replicas.
//
// Use for tests, single-replica deployments, or as a local cache layer
// in front of a cross-process coordinator.
type MemoryCoordinator struct {
	mu    sync.Mutex
	keys  map[string]time.Time
	clock func() time.Time
}

// NewMemoryCoordinator returns a new in-process coordinator.
func NewMemoryCoordinator() *MemoryCoordinator {
	return &MemoryCoordinator{
		keys:  make(map[string]time.Time),
		clock: time.Now,
	}
}

// Claim records the key with an expiry of now+ttl. Returns true if the
// key was previously unclaimed or its prior claim has expired.
func (c *MemoryCoordinator) Claim(_ context.Context, key string, ttl time.Duration) (bool, error) {
	now := c.clock()
	c.mu.Lock()
	defer c.mu.Unlock()

	if exp, ok := c.keys[key]; ok && exp.After(now) {
		return false, nil
	}

	c.keys[key] = now.Add(ttl)

	// Opportunistic GC: if the map has grown noticeably, drop expired keys.
	// Bounded so Claim stays O(1) amortized.
	if len(c.keys) > memoryCoordinatorGCThreshold {
		c.gcLocked(now)
	}

	return true, nil
}

// memoryCoordinatorGCThreshold is the map size at which we sweep expired
// entries. Chosen to keep GC amortized cheap while avoiding unbounded growth.
const memoryCoordinatorGCThreshold = 1024

// gcLocked removes expired entries. Caller holds c.mu.
func (c *MemoryCoordinator) gcLocked(now time.Time) {
	for k, exp := range c.keys {
		if !exp.After(now) {
			delete(c.keys, k)
		}
	}
}

// Len reports the number of currently tracked keys. Intended for tests.
func (c *MemoryCoordinator) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.keys)
}

// Compile-time checks.
var (
	_ Coordinator = NoopCoordinator{}
	_ Coordinator = (*MemoryCoordinator)(nil)
)

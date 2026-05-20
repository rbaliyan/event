package bridge

import "time"

// SetClockForTesting swaps the clock function used by MemoryCoordinator to
// determine TTL expiry. Lives in an _test.go file so it compiles only in
// the test build and is invisible to production consumers.
//
// Tests use this to drive expiry deterministically via a fake clock instead
// of time.Sleep. The replacement function should return monotonically non-
// decreasing values within a single test to match real-clock semantics.
func (c *MemoryCoordinator) SetClockForTesting(fn func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock = fn
}

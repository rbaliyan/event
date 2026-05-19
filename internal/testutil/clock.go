package testutil

import (
	"sync"
	"time"
)

// Clock is the minimal time surface that production code can take as a
// dependency to enable deterministic tests. The default RealClock
// implementation delegates to the time package; tests inject FakeClock to
// drive time manually via Advance.
//
// Production code should only call methods defined here. Adding new methods
// (e.g. NewTimer, NewTicker) is fine but requires updating both
// implementations.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
}

// RealClock is the production Clock, backed by the standard time package.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// Since returns the duration elapsed since t.
func (RealClock) Since(t time.Time) time.Duration { return time.Since(t) }

// Sleep blocks for at least d.
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// FakeClock is a deterministic Clock for tests. Time only advances when
// Advance is called explicitly, so race-prone tests built around real
// time.Sleep timing can be rewritten to drive time directly.
//
// FakeClock is safe for concurrent use.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock constructs a FakeClock pinned to start. If start is the zero
// time, FakeClock starts at the Unix epoch — concrete enough that elapsed
// durations format readably in test failures.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Unix(0, 0).UTC()
	}
	return &FakeClock{now: start}
}

// Now returns the clock's current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Since returns the duration between the clock's current time and t.
func (c *FakeClock) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Sleep on a FakeClock advances the clock by d rather than blocking. Production
// code that calls Sleep for backoff or rate limiting becomes instantly
// testable.
func (c *FakeClock) Sleep(d time.Duration) {
	c.Advance(d)
}

// Advance moves the fake clock forward by d. Tests call Advance to simulate
// the passage of time without blocking.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set forces the fake clock to t. Useful for jumping to a known absolute
// timestamp (e.g. for testing TTL expiry from a specific epoch).
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Compile-time assertion that both clocks satisfy the interface.
var (
	_ Clock = RealClock{}
	_ Clock = (*FakeClock)(nil)
)

// Package clock provides a small time abstraction that production code can
// take as a dependency to enable deterministic tests.
//
// This package is a leaf — it has no other internal dependencies — so it
// can be imported from anywhere in the module (including transport, outbox,
// monitor, etc.) without risking an import cycle. Test code typically
// imports github.com/rbaliyan/event/v3/internal/testutil which re-exports
// type aliases for convenience.
package clock

import (
	"sync"
	"time"
)

// Clock is the minimal time surface that production code depends on.
// The default Real implementation delegates to the time package; tests
// inject Fake to drive time manually via Advance.
//
// Production code should only call methods defined here. Adding new methods
// (e.g. NewTimer, NewTicker) is fine but requires updating both
// implementations.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
}

// Real is the production Clock, backed by the standard time package.
type Real struct{}

// Now returns the current wall-clock time.
func (Real) Now() time.Time { return time.Now() }

// Since returns the duration elapsed since t.
func (Real) Since(t time.Time) time.Duration { return time.Since(t) }

// Sleep blocks for at least d.
func (Real) Sleep(d time.Duration) { time.Sleep(d) }

// Fake is a deterministic Clock for tests. Time only advances when Advance
// is called explicitly, so race-prone tests built around real time.Sleep
// timing can be rewritten to drive time directly.
//
// Fake is safe for concurrent use.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake constructs a Fake pinned to start. If start is the zero time,
// Fake starts at the Unix epoch — concrete enough that elapsed durations
// format readably in test failures.
func NewFake(start time.Time) *Fake {
	if start.IsZero() {
		start = time.Unix(0, 0).UTC()
	}
	return &Fake{now: start}
}

// Now returns the clock's current time.
func (c *Fake) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Since returns the duration between the clock's current time and t.
func (c *Fake) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

// Sleep on a Fake advances the clock by d rather than blocking. Production
// code that calls Sleep for backoff or rate limiting becomes instantly
// testable.
func (c *Fake) Sleep(d time.Duration) {
	c.Advance(d)
}

// Advance moves the fake clock forward by d. Tests call Advance to simulate
// the passage of time without blocking.
func (c *Fake) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set forces the fake clock to t. Useful for jumping to a known absolute
// timestamp (e.g. for testing TTL expiry from a specific epoch).
func (c *Fake) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// Compile-time assertion that both clocks satisfy the interface.
var (
	_ Clock = Real{}
	_ Clock = (*Fake)(nil)
)

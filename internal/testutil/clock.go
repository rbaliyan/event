package testutil

import (
	"time"

	"github.com/rbaliyan/event/v3/internal/clock"
)

// Clock is re-exported from internal/clock so tests already importing
// testutil don't need a second import. Production code should import
// internal/clock directly to avoid pulling in the rest of testutil
// (which depends on the root event package and would cause cycles when
// imported from event's own sub-packages).
type Clock = clock.Clock

// RealClock is an alias for clock.Real, the production Clock.
type RealClock = clock.Real

// FakeClock is an alias for clock.Fake, the test-only deterministic Clock.
type FakeClock = clock.Fake

// NewFakeClock constructs a new test clock pinned to start. If start is the
// zero time, it defaults to the Unix epoch.
func NewFakeClock(start time.Time) *FakeClock {
	return clock.NewFake(start)
}

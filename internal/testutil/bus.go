package testutil

import (
	"context"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
)

// busCloseTimeout is the deadline applied to bus.Close in MustNewBus's
// cleanup. Long enough to drain typical in-flight handlers; short enough that
// a leaked-handler test failure surfaces as a timeout rather than hanging the
// whole suite.
const busCloseTimeout = 5 * time.Second

// MustNewBus constructs a Bus with a collision-free name and registers
// t.Cleanup to close it. Tests should always prefer MustNewBus over calling
// event.NewBus directly so that:
//
//   - A panicking or t.Fatal-ing test cannot leak a name into the global bus
//     registry (which would poison subsequent tests sharing the same name).
//   - Parallel subtests cannot collide on names — UniqueName mixes 64 bits of
//     entropy into t.Name().
//   - Bus.Close runs even when the test fails mid-flight.
//
// The returned bus is ready to use; callers can Register events on it
// immediately.
func MustNewBus(t testing.TB, opts ...event.BusOption) *event.Bus {
	t.Helper()
	return MustNewBusNamed(t, UniqueName(t), opts...)
}

// MustNewBusNamed is like MustNewBus but lets the caller pick the name. Use
// this only when the test specifically asserts on bus name semantics — most
// tests should call MustNewBus.
func MustNewBusNamed(t testing.TB, name string, opts ...event.BusOption) *event.Bus {
	t.Helper()
	bus, err := event.NewBus(name, opts...)
	if err != nil {
		t.Fatalf("event.NewBus(%q): %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), busCloseTimeout)
		defer cancel()
		// Close errors during automatic cleanup are intentionally swallowed:
		// the test has already finished, and the bus.Close shutdown sequence
		// can race transport-closed errors that are benign in this context.
		// Tests that need to assert on bus.Close behavior should call it
		// explicitly inside the test body.
		_ = bus.Close(ctx)
	})
	return bus
}

// MustRegister calls event.Register and t.Fatals on error. It's a one-liner
// that keeps test setup readable: `e := testutil.MustRegister(t, ctx, bus,
// event.New[int]("foo"))`.
func MustRegister[T any](t testing.TB, ctx context.Context, bus *event.Bus, ev event.Event[T]) event.Event[T] {
	t.Helper()
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatalf("event.Register(%q): %v", ev.Name(), err)
	}
	return ev
}

package event

import (
	"sync/atomic"
	"testing"
	"time"
)

// This file collects the deterministic polling helpers used across the root
// event package's test suite. Each helper exists because the root package
// cannot import internal/testutil (that would create a cycle through
// internal/testutil/bus.go), so the same shape lives here instead.
//
// Use these instead of time.Sleep + assert. The benefits are the same as
// testutil.Eventually elsewhere in the repo:
//   - The test passes the instant the contract is met.
//   - Only failing runs burn the full timeout.
//   - There is no slack budget that grows stale on slow CI.

// eventuallyEqInt32 polls until counter reaches want or the deadline fires.
// Use for atomic.Int32 call counts / message counts.
func eventuallyEqInt32(t testing.TB, timeout time.Duration, counter *atomic.Int32, want int32, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for counter.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d, want %d (after %s)", msg, counter.Load(), want, timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// eventuallyTrue polls predicate until it returns true or the deadline fires.
// Use for atomic.Bool, *eventImpl.Subscribers(), mock-store probes, etc.
func eventuallyTrue(t testing.TB, timeout time.Duration, predicate func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatalf("%s (after %s)", msg, timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// consistentlyEqInt32 polls counter for the given window and fails the test
// if it ever observes a value other than want. Use for negative-stable
// assertions such as "after a duplicate publish, callCount must stay at N".
func consistentlyEqInt32(t testing.TB, window time.Duration, counter *atomic.Int32, want int32, msg string) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if got := counter.Load(); got != want {
			t.Fatalf("%s: got %d, want %d", msg, got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitInputsHandled blocks until counter reaches at least want or the
// deadline fires. Replaces the time.Sleep gaps that were used between
// sequential sends on coal.incoming: those sleeps existed because the
// coalescer's run() goroutine selects between incoming and done, and a
// queued done could be picked up before its preceding incoming messages
// without an explicit sync barrier.
func waitInputsHandled(t testing.TB, counter *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("inputsHandled did not reach %d (got %d)", want, counter.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

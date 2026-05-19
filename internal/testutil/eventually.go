// Package testutil provides shared helpers for unit, smoke, and integration
// tests across the event repository. It is intentionally internal so external
// consumers cannot depend on its evolving API.
//
// The package is split by concern: eventually.go (polling), clock.go (fake
// time), bus.go (Bus lifecycle), uniquename.go (collision-free per-run names),
// and backend harnesses (redis.go, postgres.go) that are gated behind the
// integration build tag.
package testutil

import (
	"testing"
	"time"
)

// defaultPollInterval is the interval Eventually uses when polling a condition.
// Short enough for sub-second total runs to feel responsive; long enough to
// avoid CPU-bound spin on a hot predicate.
const defaultPollInterval = 5 * time.Millisecond

// Eventually polls cond until it returns true or timeout elapses. On timeout
// the test fails (via t.Fatalf) with msg and args. It is the preferred
// replacement for time.Sleep-followed-by-assertion patterns: it succeeds as
// soon as the condition holds and only burns the timeout when something is
// actually wrong.
//
// Use a generous timeout (e.g. 2 * expected) — Eventually only blocks until
// the predicate flips, so over-budgeting the timeout costs nothing on the
// happy path but eliminates flakes on loaded CI runners.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	if EventuallyOK(timeout, cond) {
		return
	}
	if msg == "" {
		t.Fatalf("Eventually: condition did not hold within %s", timeout)
		return
	}
	t.Fatalf("Eventually: condition did not hold within %s: "+msg, append([]any{timeout}, args...)...)
}

// EventuallyOK is the non-fatal form of Eventually. It returns true if cond
// became true within timeout, false otherwise. Useful when the caller wants
// to attach a custom failure message that includes diagnostic state captured
// after the poll loop exits.
func EventuallyOK(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	if cond() {
		return true
	}
	for {
		select {
		case <-ticker.C:
			if cond() {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
		case <-time.After(time.Until(deadline)):
			return cond()
		}
	}
}

// WaitFor blocks until ch receives a value or timeout elapses. On timeout the
// test fails with msg. The channel-based alternative to Eventually for cases
// where the production code already signals completion via a channel.
func WaitFor[T any](t testing.TB, ch <-chan T, timeout time.Duration, msg string, args ...any) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		if msg == "" {
			t.Fatalf("WaitFor: no value on channel within %s", timeout)
		} else {
			t.Fatalf("WaitFor: no value on channel within %s: "+msg, append([]any{timeout}, args...)...)
		}
		var zero T
		return zero
	}
}

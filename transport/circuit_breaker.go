package transport

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/internal/clock"
)

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting calls.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Circuit breaker states.
const (
	cbClosed   int32 = 0
	cbOpen     int32 = 1
	cbHalfOpen int32 = 2
)

// CircuitBreaker implements a thread-safe circuit breaker using atomic operations.
// It transitions through three states: Closed → Open → Half-Open.
//
// When disabled (zero value or nil), all methods are no-ops with zero overhead.
type CircuitBreaker struct {
	enabled   bool
	threshold int32
	cooldown  time.Duration

	// clk supplies the current time. Defaults to clock.Real{} in production;
	// tests inject clock.Fake via withClock to drive cooldown deterministically
	// without time.Sleep.
	clk clock.Clock

	state    atomic.Int32
	failures atomic.Int32
	openedAt atomic.Int64 // clock.Now().UnixNano() when breaker opened
	probing  atomic.Int32 // CAS gate: 1 if a half-open probe is in-flight
}

// NewCircuitBreaker creates a circuit breaker that opens after threshold consecutive
// failures and transitions to half-open after cooldown elapses.
// Returns nil if threshold <= 0.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		return nil
	}
	return &CircuitBreaker{
		enabled:   true,
		threshold: int32(threshold), // #nosec G115 -- value is bounded
		cooldown:  cooldown,
		clk:       clock.Real{},
	}
}

// withClock swaps the clock for tests. Unexported on purpose — callers in
// other packages should not be able to install a clock and rely on its
// implementation detail. Used only from circuit_breaker_test.go, which
// golangci-lint excludes by default (--tests=false in CI); hence the
// explicit allow-list below.
//
//nolint:unused // used from circuit_breaker_test.go
func (cb *CircuitBreaker) withClock(c clock.Clock) *CircuitBreaker {
	cb.clk = c
	return cb
}

// Allow checks whether a call is permitted. Returns nil if allowed,
// ErrCircuitOpen if the breaker is open and rejecting calls.
func (cb *CircuitBreaker) Allow() error {
	if cb == nil || !cb.enabled {
		return nil
	}

	switch cb.state.Load() {
	case cbClosed:
		return nil

	case cbOpen:
		// Check if cooldown has elapsed
		opened := cb.openedAt.Load()
		if cb.clk.Now().Sub(time.Unix(0, opened)) < cb.cooldown {
			return ErrCircuitOpen
		}
		// Cooldown elapsed — try to become the probe caller
		if !cb.probing.CompareAndSwap(0, 1) {
			return ErrCircuitOpen
		}
		cb.state.Store(cbHalfOpen)
		return nil

	case cbHalfOpen:
		// Only the probe caller is allowed through
		return ErrCircuitOpen
	}

	return nil
}

// RecordSuccess records a successful call. Resets failures and closes the breaker
// if it was in half-open state.
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil || !cb.enabled {
		return
	}
	cb.failures.Store(0)
	if cb.state.CompareAndSwap(cbHalfOpen, cbClosed) {
		cb.probing.Store(0)
	}
}

// RecordFailure records a failed call. If consecutive failures reach the threshold,
// the breaker opens. If the breaker was in half-open state, it re-opens.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil || !cb.enabled {
		return
	}

	// Half-open probe failed — re-open immediately
	if cb.state.CompareAndSwap(cbHalfOpen, cbOpen) {
		cb.openedAt.Store(cb.clk.Now().UnixNano())
		cb.failures.Store(0)
		cb.probing.Store(0)
		return
	}

	n := cb.failures.Add(1)
	if n >= cb.threshold {
		if cb.state.CompareAndSwap(cbClosed, cbOpen) {
			cb.openedAt.Store(cb.clk.Now().UnixNano())
			cb.failures.Store(0)
		}
	}
}

// State returns the current state as a string: "closed", "open", or "half-open".
func (cb *CircuitBreaker) State() string {
	if cb == nil || !cb.enabled {
		return "closed"
	}
	switch cb.state.Load() {
	case cbOpen:
		return "open"
	case cbHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// IsEnabled reports whether the circuit breaker is active.
func (cb *CircuitBreaker) IsEnabled() bool {
	return cb != nil && cb.enabled
}

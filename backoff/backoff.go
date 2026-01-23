// Package backoff provides configurable retry backoff strategies.
//
// The package implements several common backoff strategies for retry logic:
//   - Constant: Fixed delay between retries
//   - Linear: Delay increases linearly with each attempt
//   - Exponential: Delay doubles (or multiplies) with optional jitter
//
// # Basic Usage
//
//	// Use the default exponential backoff
//	strategy := backoff.Default()
//
//	// Custom exponential backoff with jitter
//	strategy := &backoff.Exponential{
//	    Initial:    100 * time.Millisecond,
//	    Multiplier: 2.0,
//	    Max:        30 * time.Second,
//	    Jitter:     0.1,
//	}
//
//	// Get delay for each attempt
//	for attempt := 0; attempt < maxRetries; attempt++ {
//	    delay := strategy.NextDelay(attempt)
//	    time.Sleep(delay)
//	    if tryOperation() == nil {
//	        break
//	    }
//	}
//
// # Integration
//
// The Strategy interface is designed to be used with retry libraries and
// distributed systems. See the distributed, scheduler, saga, and dlq packages
// for integration examples.
package backoff

import (
	"math"
	"math/rand/v2"
	"time"
)

// Strategy defines a backoff strategy for retry logic.
//
// Implementations must be safe for concurrent use.
//
// Example:
//
//	strategy := backoff.Default()
//	for attempt := 0; attempt < maxRetries; attempt++ {
//	    delay := strategy.NextDelay(attempt)
//	    time.Sleep(delay)
//	    if err := operation(); err == nil {
//	        break
//	    }
//	}
type Strategy interface {
	// NextDelay returns the delay for the given attempt (0-indexed).
	// Attempt 0 is the first retry, not the initial attempt.
	NextDelay(attempt int) time.Duration

	// Reset resets the strategy state if any.
	// Most strategies are stateless and this is a no-op.
	Reset()
}

// Constant returns a fixed delay for all attempts.
//
// Use Constant when you want a simple, predictable delay between retries
// without any backoff behavior.
//
// Example:
//
//	strategy := &backoff.Constant{Delay: time.Second}
//	// All attempts will wait 1 second
type Constant struct {
	// Delay is the fixed delay for all attempts.
	Delay time.Duration
}

// NextDelay returns the constant delay for any attempt.
func (c *Constant) NextDelay(_ int) time.Duration {
	return c.Delay
}

// Reset is a no-op for Constant strategy.
func (c *Constant) Reset() {}

// Linear returns a delay that increases linearly with each attempt.
//
// The delay formula is: Initial + (Step * attempt), capped at Max.
//
// Example:
//
//	strategy := &backoff.Linear{
//	    Initial: 100 * time.Millisecond,
//	    Step:    100 * time.Millisecond,
//	    Max:     5 * time.Second,
//	}
//	// Attempt 0: 100ms, 1: 200ms, 2: 300ms, ...
type Linear struct {
	// Initial is the delay for the first retry (attempt 0).
	Initial time.Duration

	// Step is the amount added for each subsequent attempt.
	Step time.Duration

	// Max is the maximum delay. If zero, no maximum is enforced.
	Max time.Duration
}

// NextDelay returns the delay for the given attempt.
// The delay is Initial + (Step * attempt), capped at Max.
func (l *Linear) NextDelay(attempt int) time.Duration {
	delay := l.Initial + (l.Step * time.Duration(attempt))
	if l.Max > 0 && delay > l.Max {
		delay = l.Max
	}
	return delay
}

// Reset is a no-op for Linear strategy.
func (l *Linear) Reset() {}

// Exponential returns a delay that grows exponentially with each attempt.
//
// The delay formula is: Initial * (Multiplier ^ attempt), capped at Max.
// Optional jitter adds randomness to prevent thundering herd.
//
// Example:
//
//	strategy := &backoff.Exponential{
//	    Initial:    100 * time.Millisecond,
//	    Multiplier: 2.0,
//	    Max:        30 * time.Second,
//	    Jitter:     0.1, // +/- 10% randomness
//	}
//	// Attempt 0: ~100ms, 1: ~200ms, 2: ~400ms, 3: ~800ms, ...
type Exponential struct {
	// Initial is the delay for the first retry (attempt 0).
	Initial time.Duration

	// Multiplier is the factor by which delay increases each attempt.
	// Common values: 2.0 (doubling), 1.5, 3.0
	// If zero, defaults to 2.0.
	Multiplier float64

	// Max is the maximum delay. If zero, no maximum is enforced.
	Max time.Duration

	// Jitter is the percentage (0-1) of delay to randomize.
	// 0 = no jitter, 0.1 = +/- 10%, 0.5 = +/- 50%
	// Jitter helps prevent thundering herd when many clients retry simultaneously.
	Jitter float64
}

// NextDelay returns the delay for the given attempt.
// The delay is Initial * (Multiplier ^ attempt), with optional jitter, capped at Max.
func (e *Exponential) NextDelay(attempt int) time.Duration {
	multiplier := e.Multiplier
	if multiplier == 0 {
		multiplier = 2.0
	}

	// Calculate base delay: Initial * (Multiplier ^ attempt)
	delay := float64(e.Initial) * math.Pow(multiplier, float64(attempt))

	// Apply maximum
	if e.Max > 0 && delay > float64(e.Max) {
		delay = float64(e.Max)
	}

	// Apply jitter: +/- (Jitter * delay)
	if e.Jitter > 0 {
		jitterRange := delay * e.Jitter
		// Random value in [-jitterRange, +jitterRange]
		jitter := (rand.Float64()*2 - 1) * jitterRange
		delay += jitter
	}

	// Ensure delay is not negative
	if delay < 0 {
		delay = 0
	}

	return time.Duration(delay)
}

// Reset is a no-op for Exponential strategy.
func (e *Exponential) Reset() {}

// Default returns the recommended exponential backoff strategy.
//
// The default configuration is suitable for most retry scenarios:
//   - Initial: 100ms
//   - Multiplier: 2.0 (doubling)
//   - Max: 30 seconds
//   - Jitter: 10%
//
// Delay progression: ~100ms, ~200ms, ~400ms, ~800ms, ~1.6s, ~3.2s, ~6.4s, ~12.8s, ~25.6s, ~30s (max)
//
// Example:
//
//	strategy := backoff.Default()
func Default() Strategy {
	return &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        30 * time.Second,
		Jitter:     0.1,
	}
}

// None returns a zero-delay backoff strategy.
//
// Use this when you want immediate retries without any delay.
// Generally not recommended for production as it can overload services.
//
// Example:
//
//	strategy := backoff.None()
func None() Strategy {
	return &Constant{Delay: 0}
}

// Fixed returns a constant backoff strategy with the given delay.
//
// This is a convenience function for creating a Constant strategy.
//
// Example:
//
//	strategy := backoff.Fixed(time.Second)
func Fixed(delay time.Duration) Strategy {
	return &Constant{Delay: delay}
}

// Compile-time checks
var (
	_ Strategy = (*Constant)(nil)
	_ Strategy = (*Linear)(nil)
	_ Strategy = (*Exponential)(nil)
)

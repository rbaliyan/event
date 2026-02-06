package distributed

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/backoff"
)

// StaleResetter is an optional interface that state managers can implement
// for efficient batch stale state reset.
type StaleResetter interface {
	// ResetStale resets stale states in batch.
	// Returns the number of states reset.
	ResetStale(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error)
}

// RecoveryRunner provides active stale state detection and recovery.
//
// Instead of relying solely on TTL expiration (passive recovery), the runner
// periodically scans for stale states and resets them for faster failover.
//
// How it works:
//  1. Background goroutine runs at configurable interval (default: 30s)
//  2. Queries state manager for stale states (processing longer than staleTimeout)
//  3. Resets stale states so other workers can reacquire them
//
// Timing guidelines:
//
//	| Handler Timeout | Stale Timeout | Check Interval |
//	|-----------------|---------------|----------------|
//	| 30s             | 1m            | 15-30s         |
//	| 1m              | 2m            | 30-60s         |
//	| 5m              | 10m           | 1-2m           |
//
// Example:
//
//	sm := distributed.NewMongoStateManager(db)
//	runner := distributed.NewRecoveryRunner(sm,
//	    distributed.WithStaleTimeout(2*time.Minute),
//	    distributed.WithCheckInterval(30*time.Second),
//	)
//
//	// Start recovery in background
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go runner.Run(ctx)
//
//	// Or run once for manual recovery
//	reset, err := runner.RecoverOnce(ctx)
type RecoveryRunner struct {
	sm                StateManager
	staleTimeout      time.Duration
	checkInterval     time.Duration
	batchLimit        int
	logger            *slog.Logger
	backoff           backoff.Strategy
	consecutiveErrors atomic.Int32
}

// RecoveryOption configures a RecoveryRunner.
type RecoveryOption func(*RecoveryRunner)

// WithStaleTimeout sets how long a state can be processing before considered stale.
//
// This should be longer than the expected handler execution time but shorter
// than the state TTL. For example:
//   - Handler timeout: 30s → staleTimeout: 1-2m
//   - Handler timeout: 1m → staleTimeout: 2-3m
//
// Default: 2 minutes
func WithStaleTimeout(d time.Duration) RecoveryOption {
	return func(r *RecoveryRunner) {
		if d > 0 {
			r.staleTimeout = d
		}
	}
}

// WithCheckInterval sets how often to check for stale states.
//
// Shorter intervals mean faster failover but more load on the state manager.
// Longer intervals reduce load but delay recovery.
//
// Default: 30 seconds
func WithCheckInterval(d time.Duration) RecoveryOption {
	return func(r *RecoveryRunner) {
		if d > 0 {
			r.checkInterval = d
		}
	}
}

// WithBatchLimit sets the maximum number of stale states to process per check.
//
// This prevents the runner from overwhelming the state manager when many stale
// states exist (e.g., after a large-scale failure).
//
// Default: 100 (0 = no limit)
func WithBatchLimit(limit int) RecoveryOption {
	return func(r *RecoveryRunner) {
		r.batchLimit = limit
	}
}

// WithRecoveryLogger sets the logger for recovery operations.
//
// If not set, no logging is performed.
func WithRecoveryLogger(logger *slog.Logger) RecoveryOption {
	return func(r *RecoveryRunner) {
		r.logger = logger
	}
}

// WithBackoff sets a backoff strategy for handling recovery errors.
//
// When a recovery attempt fails, the runner uses this backoff strategy
// to determine how long to wait before the next attempt. The delay
// increases with consecutive errors and resets on success.
//
// If not set, the runner continues checking at the normal interval
// regardless of errors.
//
// Example:
//
//	runner := distributed.NewRecoveryRunner(sm,
//	    distributed.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        5 * time.Minute,
//	        Jitter:     0.1,
//	    }),
//	)
func WithBackoff(strategy backoff.Strategy) RecoveryOption {
	return func(r *RecoveryRunner) {
		r.backoff = strategy
	}
}

// NewRecoveryRunner creates a new stale state recovery runner.
//
// Parameters:
//   - sm: The state manager to monitor for stale states
//   - opts: Optional configuration
//
// Returns a configured runner. Call Run() to start background recovery
// or RecoverOnce() for manual recovery.
func NewRecoveryRunner(sm StateManager, opts ...RecoveryOption) *RecoveryRunner {
	r := &RecoveryRunner{
		sm:            sm,
		staleTimeout:  2 * time.Minute,
		checkInterval: 30 * time.Second,
		batchLimit:    100,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run starts the background recovery loop.
//
// The loop runs until the context is cancelled. It periodically checks for
// stale states and resets them.
//
// If a backoff strategy is configured and recovery fails, the runner waits
// an additional delay before the next attempt. The delay increases with
// consecutive errors and resets on success.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go runner.Run(ctx)
func (r *RecoveryRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reset, err := r.RecoverOnce(ctx)
			if err != nil {
				errCount := int(r.consecutiveErrors.Add(1))
				if r.logger != nil {
					r.logger.Warn("stale state recovery failed",
						"error", err,
						"consecutive_errors", errCount)
				}

				// Apply backoff on error if configured
				if r.backoff != nil {
					backoffDelay := r.backoff.NextDelay(errCount - 1)

					if backoffDelay > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(backoffDelay):
							// Continue after backoff delay
						}
					}
				}
			} else {
				// Reset consecutive errors on success
				r.consecutiveErrors.Store(0)

				if reset > 0 {
					if r.logger != nil {
						r.logger.Info("reset stale states",
							"count", reset,
							"stale_timeout", r.staleTimeout)
					}
				}
			}
		}
	}
}

// RecoverOnce performs a single recovery pass.
//
// This is useful for manual recovery or when you want more control over
// when recovery runs.
//
// Returns the number of states reset.
func (r *RecoveryRunner) RecoverOnce(ctx context.Context) (int64, error) {
	// If state manager implements StaleResetter, use the efficient batch method
	if resetter, ok := r.sm.(StaleResetter); ok {
		return resetter.ResetStale(ctx, r.staleTimeout, r.batchLimit)
	}

	// Fall back to list + reset
	stale, err := r.sm.ListStale(ctx, r.staleTimeout, r.batchLimit)
	if err != nil {
		return 0, err
	}

	var reset int64
	for _, msgID := range stale {
		if err := r.sm.Reset(ctx, msgID); err != nil {
			if r.logger != nil {
				r.logger.Warn("failed to reset stale state",
					"message_id", msgID,
					"error", err)
			}
			continue
		}
		reset++
	}

	return reset, nil
}

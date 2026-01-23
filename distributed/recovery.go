package distributed

import (
	"context"
	"log/slog"
	"time"
)

// OrphanReleaser is an optional interface that claimers can implement
// for efficient batch orphan release.
type OrphanReleaser interface {
	// ReleaseOrphans releases orphaned claims in batch.
	// Returns the number of claims released.
	ReleaseOrphans(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error)
}

// OrphanRecoveryRunner provides active orphan detection and recovery.
//
// Instead of relying solely on TTL expiration (passive recovery), the runner
// periodically scans for stale claims and releases them for faster failover.
//
// How it works:
//  1. Background goroutine runs at configurable interval (default: 30s)
//  2. Queries claimer for orphaned claims (pending longer than staleTimeout)
//  3. Releases orphaned claims so other workers can reclaim them
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
//	claimer := distributed.NewMongoClaimer(db)
//	runner := distributed.NewOrphanRecoveryRunner(claimer,
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
//	released, err := runner.RecoverOnce(ctx)
type OrphanRecoveryRunner struct {
	claimer       MessageClaimer
	staleTimeout  time.Duration
	checkInterval time.Duration
	batchLimit    int
	logger        *slog.Logger
}

// OrphanRecoveryOption configures an OrphanRecoveryRunner.
type OrphanRecoveryOption func(*OrphanRecoveryRunner)

// WithStaleTimeout sets how long a claim can be pending before considered orphaned.
//
// This should be longer than the expected handler execution time but shorter
// than the claim TTL. For example:
//   - Handler timeout: 30s → staleTimeout: 1-2m
//   - Handler timeout: 1m → staleTimeout: 2-3m
//
// Default: 2 minutes
func WithStaleTimeout(d time.Duration) OrphanRecoveryOption {
	return func(r *OrphanRecoveryRunner) {
		if d > 0 {
			r.staleTimeout = d
		}
	}
}

// WithCheckInterval sets how often to check for orphaned claims.
//
// Shorter intervals mean faster failover but more load on the claimer.
// Longer intervals reduce load but delay recovery.
//
// Default: 30 seconds
func WithCheckInterval(d time.Duration) OrphanRecoveryOption {
	return func(r *OrphanRecoveryRunner) {
		if d > 0 {
			r.checkInterval = d
		}
	}
}

// WithBatchLimit sets the maximum number of orphans to process per check.
//
// This prevents the runner from overwhelming the claimer when many orphans
// exist (e.g., after a large-scale failure).
//
// Default: 100 (0 = no limit)
func WithBatchLimit(limit int) OrphanRecoveryOption {
	return func(r *OrphanRecoveryRunner) {
		r.batchLimit = limit
	}
}

// WithRecoveryLogger sets the logger for recovery operations.
//
// If not set, no logging is performed.
func WithRecoveryLogger(logger *slog.Logger) OrphanRecoveryOption {
	return func(r *OrphanRecoveryRunner) {
		r.logger = logger
	}
}

// NewOrphanRecoveryRunner creates a new orphan recovery runner.
//
// Parameters:
//   - claimer: The claimer to monitor for orphaned claims
//   - opts: Optional configuration
//
// Returns a configured runner. Call Run() to start background recovery
// or RecoverOnce() for manual recovery.
func NewOrphanRecoveryRunner(claimer MessageClaimer, opts ...OrphanRecoveryOption) *OrphanRecoveryRunner {
	r := &OrphanRecoveryRunner{
		claimer:       claimer,
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
// orphaned claims and releases them.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	go runner.Run(ctx)
func (r *OrphanRecoveryRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			released, err := r.RecoverOnce(ctx)
			if err != nil {
				if r.logger != nil {
					r.logger.Warn("orphan recovery failed",
						"error", err)
				}
			} else if released > 0 {
				if r.logger != nil {
					r.logger.Info("released orphaned claims",
						"count", released,
						"stale_timeout", r.staleTimeout)
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
// Returns the number of claims released.
func (r *OrphanRecoveryRunner) RecoverOnce(ctx context.Context) (int64, error) {
	// If claimer implements OrphanReleaser, use the efficient batch method
	if releaser, ok := r.claimer.(OrphanReleaser); ok {
		return releaser.ReleaseOrphans(ctx, r.staleTimeout, r.batchLimit)
	}

	// Fall back to list + release
	orphans, err := r.claimer.ListOrphanedClaims(ctx, r.staleTimeout, r.batchLimit)
	if err != nil {
		return 0, err
	}

	var released int64
	for _, msgID := range orphans {
		if err := r.claimer.Release(ctx, msgID); err != nil {
			if r.logger != nil {
				r.logger.Warn("failed to release orphan",
					"message_id", msgID,
					"error", err)
			}
			continue
		}
		released++
	}

	return released, nil
}

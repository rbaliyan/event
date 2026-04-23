package distributed

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/backoff"
)

// RecoveryRunner provides active stale state detection and recovery.
//
// Delivery guarantee: at-least-once. If the process crashes between re-publishing
// an event and marking the old state as processed, the next recovery cycle may
// re-publish the same event again. Handlers MUST be idempotent.
//
// Instead of relying solely on TTL expiration (passive recovery), the runner
// periodically scans for stale states and resets them for faster failover.
//
// Recovery modes:
//
// Basic mode (no Publisher): resets stale states so other workers can reacquire
// them via the transport's own re-delivery mechanism.
//
// Payload-aware mode (with Publisher): if the Coordinator also implements
// PayloadStore, stale entries with stored payload are re-published via the
// Publisher before being marked as processed. Remaining stale entries without
// payload are reset normally.
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
//	coord, _ := distributed.NewRedisStateManager(redisClient)
//	runner := distributed.NewRecoveryRunner(coord,
//	    distributed.WithStaleTimeout(2*time.Minute),
//	    distributed.WithCheckInterval(30*time.Second),
//	    distributed.WithPublisher(bus), // enables payload-aware recovery
//	)
//
// For MongoDB-backed recovery, use NewMongoStateManager from the
// event-mongodb module (https://github.com/rbaliyan/event-mongodb).
//
//	// Start recovery in background
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go runner.Run(ctx)
//
//	// Or run once for manual recovery
//	reset, err := runner.RecoverOnce(ctx)
type RecoveryRunner struct {
	coord             Coordinator
	pub               Publisher
	staleTimeout      time.Duration
	checkInterval     time.Duration
	batchLimit        int
	logger            *slog.Logger
	backoff           backoff.Strategy
	metrics           *RecoveryMetrics
	consecutiveErrors atomic.Int32
}

// RecoveryOption configures a RecoveryRunner.
type RecoveryOption func(*recoveryOptions)

// recoveryOptions holds configuration for the RecoveryRunner.
type recoveryOptions struct {
	pub           Publisher
	staleTimeout  time.Duration
	checkInterval time.Duration
	batchLimit    int
	logger        *slog.Logger
	backoff       backoff.Strategy
	metrics       *RecoveryMetrics
}

// WithStaleTimeout sets how long a state can be processing before considered stale.
//
// This should be longer than the expected handler execution time but shorter
// than the state TTL. For example:
//   - Handler timeout: 30s → staleTimeout: 1-2m
//   - Handler timeout: 1m → staleTimeout: 2-3m
//
// Default: 2 minutes
func WithStaleTimeout(d time.Duration) RecoveryOption {
	return func(o *recoveryOptions) {
		if d > 0 {
			o.staleTimeout = d
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
	return func(o *recoveryOptions) {
		if d > 0 {
			o.checkInterval = d
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
	return func(o *recoveryOptions) {
		o.batchLimit = limit
	}
}

// WithRecoveryLogger sets the logger for recovery operations.
//
// If not set, no logging is performed.
func WithRecoveryLogger(logger *slog.Logger) RecoveryOption {
	return func(o *recoveryOptions) {
		o.logger = logger
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
//	runner := distributed.NewRecoveryRunner(coord,
//	    distributed.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        5 * time.Minute,
//	        Jitter:     0.1,
//	    }),
//	)
func WithBackoff(strategy backoff.Strategy) RecoveryOption {
	return func(o *recoveryOptions) {
		o.backoff = strategy
	}
}

// WithRecoveryMetrics sets OpenTelemetry metrics for recovery operations.
//
// Example:
//
//	metrics, _ := distributed.NewRecoveryMetrics()
//	runner := distributed.NewRecoveryRunner(coord,
//	    distributed.WithRecoveryMetrics(metrics),
//	)
func WithRecoveryMetrics(m *RecoveryMetrics) RecoveryOption {
	return func(o *recoveryOptions) {
		o.metrics = m
	}
}

// WithPublisher sets the publisher for payload-aware recovery.
//
// When a Publisher is provided and the Coordinator also implements PayloadStore,
// the recovery runner re-publishes stale events via the Publisher instead of
// just resetting the state. This is required for transports that don't support
// re-delivery (e.g., MongoDB Change Streams).
//
// The Publisher interface is satisfied by *event.Bus.
//
// Example:
//
//	runner := distributed.NewRecoveryRunner(coord,
//	    distributed.WithPublisher(bus),
//	)
func WithPublisher(pub Publisher) RecoveryOption {
	return func(o *recoveryOptions) {
		o.pub = pub
	}
}

// NewRecoveryRunner creates a new stale state recovery runner.
//
// Parameters:
//   - coord: The coordinator to monitor for stale states (must not be nil)
//   - opts: Optional configuration (Publisher, timeouts, logger, etc.)
//
// Returns a configured runner and an error if coord is nil.
// Call Run() to start background recovery or RecoverOnce() for manual recovery.
func NewRecoveryRunner(coord Coordinator, opts ...RecoveryOption) (*RecoveryRunner, error) {
	if coord == nil {
		return nil, fmt.Errorf("distributed: NewRecoveryRunner requires a non-nil Coordinator")
	}

	o := &recoveryOptions{
		staleTimeout:  2 * time.Minute,
		checkInterval: 30 * time.Second,
		batchLimit:    100,
	}
	for _, opt := range opts {
		opt(o)
	}
	return &RecoveryRunner{
		coord:         coord,
		pub:           o.pub,
		staleTimeout:  o.staleTimeout,
		checkInterval: o.checkInterval,
		batchLimit:    o.batchLimit,
		logger:        o.logger,
		backoff:       o.backoff,
		metrics:       o.metrics,
	}, nil
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
						timer := time.NewTimer(backoffDelay)
						select {
						case <-ctx.Done():
							timer.Stop()
							return
						case <-timer.C:
							// Continue after backoff delay
						}
					}
				}
			} else {
				// Reset consecutive errors on success
				r.consecutiveErrors.Store(0)

				if reset > 0 {
					if r.logger != nil {
						r.logger.Info("recovered stale states",
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
// Phase 1 (payload-aware): If the Coordinator implements PayloadStore and a
// Publisher is configured, stale entries with stored payload are re-published
// via the Publisher and marked as processed.
//
// Phase 2 (basic reset): Remaining stale entries (without payload or when no
// Publisher is configured) are reset so other workers can reacquire them.
// Entries that were handled in Phase 1 (including those where MarkProcessed
// failed) are excluded to prevent double-processing.
//
// Returns the number of states recovered (reset or re-published).
func (r *RecoveryRunner) RecoverOnce(ctx context.Context) (int64, error) {
	start := time.Now()
	var total int64

	// Phase 1: Re-publish stale entries that have stored payload.
	// Collect all handled message IDs to exclude from Phase 2.
	var phase1IDs map[string]struct{}
	ps, hasPayloadStore := r.coord.(PayloadStore)
	if hasPayloadStore && r.pub != nil {
		count, handled, err := r.recoverPayloadEntries(ctx, ps)
		phase1IDs = handled
		if err != nil {
			r.metrics.recordPassDuration(ctx, time.Since(start))
			return count, err
		}
		total += count
	}

	// Phase 2: Reset remaining stale entries (no payload).
	// When batchLimit is 0 (no limit), pass 0 to resetStaleEntries.
	remaining := 0
	if r.batchLimit > 0 {
		remaining = r.batchLimit - int(total)
	}
	if r.batchLimit == 0 || remaining > 0 {
		count, err := r.resetStaleEntries(ctx, remaining, phase1IDs)
		if err != nil {
			r.metrics.recordPassDuration(ctx, time.Since(start))
			return total, err
		}
		total += count
	}

	r.metrics.recordPassDuration(ctx, time.Since(start))
	return total, nil
}

// recoverPayloadEntries re-publishes stale entries that have stored payload.
// Returns the count of recovered entries and a set of ALL message IDs that
// were handled (including those where re-publish or MarkProcessed failed).
// Phase 2 uses this set to avoid double-processing.
func (r *RecoveryRunner) recoverPayloadEntries(ctx context.Context, ps PayloadStore) (int64, map[string]struct{}, error) {
	entries, err := ps.LoadStalePayloads(ctx, r.staleTimeout, r.batchLimit)
	if err != nil {
		r.metrics.recordError(ctx, "load_stale_payloads")
		return 0, nil, err
	}

	handled := make(map[string]struct{}, len(entries))
	var recovered int64
	for _, entry := range entries {
		// Track all entries seen by Phase 1 to exclude from Phase 2
		handled[entry.MessageID] = struct{}{}

		// Re-publish with a new event ID so WorkerPool can acquire fresh state
		newID := event.NewID()
		if err := r.pub.Send(ctx, entry.Data.EventName, newID, entry.Data.Payload, entry.Data.Metadata); err != nil {
			if r.logger != nil {
				r.logger.Warn("failed to re-publish stale event, leaving for next cycle",
					"message_id", entry.MessageID,
					"event_name", entry.Data.EventName,
					"error", err)
			}
			r.metrics.recordSkipped(ctx, "republish_failed", entry.Data.EventName)
			continue
		}

		// Re-publish succeeded — mark processed first, then clear payload.
		// Order matters: if MarkProcessed fails we keep the payload so the
		// next recovery cycle can retry. Clearing before marking would leave
		// the state stuck in "processing" with no payload, causing Phase 2
		// to reset it and allowing the original message to be reacquired.
		if err := r.coord.MarkProcessed(ctx, entry.MessageID); err != nil {
			if r.logger != nil {
				r.logger.Warn("failed to mark re-published state as processed, payload retained for next cycle",
					"message_id", entry.MessageID,
					"error", err)
			}
			r.metrics.recordError(ctx, "mark_processed")
			continue
		}
		_ = ps.ClearPayload(ctx, entry.MessageID)
		recovered++
		r.metrics.recordRepublished(ctx, entry.Data.EventName)
		r.metrics.recordRecovered(ctx)

		if r.logger != nil {
			r.logger.Info("re-published stale event",
				"old_message_id", entry.MessageID,
				"new_message_id", newID,
				"event_name", entry.Data.EventName)
		}
	}

	return recovered, handled, nil
}

// resetStaleEntries resets stale states so other workers can reacquire them.
// The exclude set contains message IDs already handled by Phase 1 (payload
// recovery) that should not be reset even if they're still in "processing"
// state (e.g., when MarkProcessed failed after successful re-publish).
//
// Uses ListStale + individual Reset to support the exclusion set. When no
// IDs need excluding, uses StaleResetter for efficient batch reset if available.
func (r *RecoveryRunner) resetStaleEntries(ctx context.Context, limit int, exclude map[string]struct{}) (int64, error) {
	// Use StaleResetter for efficient batch reset when no exclusions needed
	if len(exclude) == 0 {
		if sr, ok := r.coord.(StaleResetter); ok {
			count, err := sr.ResetStale(ctx, r.staleTimeout, limit)
			if err != nil {
				r.metrics.recordError(ctx, "reset_stale")
				return 0, err
			}
			r.metrics.recordResetN(ctx, count)
			r.metrics.recordRecoveredN(ctx, count)
			return count, nil
		}
	}

	// Fall back to ListStale + individual Reset (supports exclusion)
	stale, err := r.coord.ListStale(ctx, r.staleTimeout, limit)
	if err != nil {
		r.metrics.recordError(ctx, "list_stale")
		return 0, err
	}

	var count int64
	for _, messageID := range stale {
		// Skip entries already handled by Phase 1
		if _, excluded := exclude[messageID]; excluded {
			continue
		}
		if err := r.coord.Reset(ctx, messageID); err != nil {
			if r.logger != nil {
				r.logger.Warn("failed to reset stale state",
					"message_id", messageID,
					"error", err)
			}
			r.metrics.recordError(ctx, "reset")
			continue
		}
		count++
		r.metrics.recordReset(ctx)
		r.metrics.recordRecovered(ctx)
	}
	return count, nil
}

package poison

import (
	"context"
	"fmt"
	"time"
)

// Detector detects and quarantines poison messages.
//
// A Detector tracks message processing failures and automatically quarantines
// messages that exceed the failure threshold. Quarantined messages are skipped
// for processing until the quarantine period expires.
//
// Detector uses a Store for persistence, enabling distributed poison detection
// across multiple application instances.
//
// Example:
//
//	store := poison.NewMemoryStore()
//	detector := poison.NewDetector(store,
//	    poison.WithThreshold(5),
//	    poison.WithQuarantineTime(time.Hour),
//	)
//
//	// In message handler
//	if poisoned, _ := detector.Check(ctx, msg.ID); poisoned {
//	    return nil // Skip quarantined message
//	}
//
//	if err := process(msg); err != nil {
//	    detector.RecordFailure(ctx, msg.ID)
//	    return err
//	}
//	detector.RecordSuccess(ctx, msg.ID)
type Detector struct {
	store          Store
	threshold      int           // Failures before marking poison
	quarantineTime time.Duration // How long to quarantine
}

// Options configures the Detector behavior.
type Options struct {
	// Threshold is the number of failures before a message is quarantined.
	// Default: 5
	Threshold int

	// QuarantineTime is how long quarantined messages are blocked.
	// Default: 1 hour
	QuarantineTime time.Duration
}

// DefaultOptions returns default detector options.
//
// Defaults:
//   - Threshold: 5 failures
//   - QuarantineTime: 1 hour
func DefaultOptions() *Options {
	return &Options{
		Threshold:      5,
		QuarantineTime: time.Hour,
	}
}

// Option is a function that modifies Options.
type Option func(*Options)

// WithThreshold sets the number of failures required before quarantine.
//
// A message must fail this many times before being quarantined.
// Lower values quarantine faster but may catch transient errors.
// Higher values are more tolerant but may waste more resources.
//
// Typical values: 3-10
//
// Example:
//
//	// Quarantine after 3 failures
//	detector := poison.NewDetector(store, poison.WithThreshold(3))
func WithThreshold(threshold int) Option {
	return func(o *Options) {
		if threshold > 0 {
			o.Threshold = threshold
		}
	}
}

// WithQuarantineTime sets how long quarantined messages are blocked.
//
// After this duration, the message will be processed again. If it fails
// again, it will be re-quarantined for another period.
//
// Typical values: 1 hour to 7 days
//
// Example:
//
//	// Block poison messages for 24 hours
//	detector := poison.NewDetector(store, poison.WithQuarantineTime(24 * time.Hour))
func WithQuarantineTime(d time.Duration) Option {
	return func(o *Options) {
		if d > 0 {
			o.QuarantineTime = d
		}
	}
}

// NewDetector creates a new poison message detector.
//
// Parameters:
//   - store: Storage backend for failure counts and quarantine status
//   - opts: Optional configuration options
//
// Example:
//
//	store := poison.NewRedisStore(redisClient)
//	detector := poison.NewDetector(store,
//	    poison.WithThreshold(5),
//	    poison.WithQuarantineTime(time.Hour),
//	)
func NewDetector(store Store, opts ...Option) *Detector {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &Detector{
		store:          store,
		threshold:      o.Threshold,
		quarantineTime: o.QuarantineTime,
	}
}

// Check checks if a message is currently quarantined.
//
// Call this before processing a message to skip quarantined messages.
// Returns true if the message is quarantined and should be skipped.
//
// Example:
//
//	if poisoned, _ := detector.Check(ctx, msg.ID); poisoned {
//	    log.Debug("skipping quarantined message", "id", msg.ID)
//	    return nil
//	}
func (d *Detector) Check(ctx context.Context, messageID string) (bool, error) {
	return d.store.IsPoison(ctx, messageID)
}

// RecordFailure records a processing failure for a message.
//
// Increments the failure count and returns true if the message was just
// quarantined (count reached threshold). The message is automatically
// quarantined for QuarantineTime.
//
// Example:
//
//	if err := process(msg); err != nil {
//	    quarantined, _ := detector.RecordFailure(ctx, msg.ID)
//	    if quarantined {
//	        log.Warn("message quarantined", "id", msg.ID, "threshold", detector.Threshold())
//	    }
//	    return err
//	}
func (d *Detector) RecordFailure(ctx context.Context, messageID string) (bool, error) {
	count, err := d.store.IncrementFailure(ctx, messageID)
	if err != nil {
		return false, fmt.Errorf("increment failure: %w", err)
	}

	if count >= d.threshold {
		if err := d.store.MarkPoison(ctx, messageID, d.quarantineTime); err != nil {
			return true, fmt.Errorf("mark poison: %w", err)
		}
		return true, nil
	}

	return false, nil
}

// RecordSuccess records a successful processing and clears the failure count.
//
// Call this after successfully processing a message to reset its failure count.
// This prevents messages that occasionally fail from accumulating failures.
//
// Example:
//
//	if err := process(msg); err == nil {
//	    detector.RecordSuccess(ctx, msg.ID)
//	}
func (d *Detector) RecordSuccess(ctx context.Context, messageID string) error {
	return d.store.ClearFailures(ctx, messageID)
}

// Release releases a message from quarantine immediately.
//
// Use this after investigating and fixing the issue that caused the message
// to be quarantined. Clears both the quarantine status and failure count.
//
// Example:
//
//	// After fixing the bug that caused the message to fail
//	if err := detector.Release(ctx, messageID); err != nil {
//	    log.Error("failed to release message", "id", messageID, "error", err)
//	}
func (d *Detector) Release(ctx context.Context, messageID string) error {
	if err := d.store.ClearPoison(ctx, messageID); err != nil {
		return err
	}
	return d.store.ClearFailures(ctx, messageID)
}

// GetFailureCount returns the current failure count for a message.
//
// Use for monitoring and debugging to see how close a message is to
// being quarantined.
func (d *Detector) GetFailureCount(ctx context.Context, messageID string) (int, error) {
	return d.store.GetFailureCount(ctx, messageID)
}

// Threshold returns the configured failure threshold.
func (d *Detector) Threshold() int {
	return d.threshold
}

// QuarantineTime returns the configured quarantine duration.
func (d *Detector) QuarantineTime() time.Duration {
	return d.quarantineTime
}

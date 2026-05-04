// Package stack provides WithReliabilityStack, a convenience option that
// auto-configures Monitor, Idempotency, and Poison detection on a Bus with
// safe in-memory defaults — no external stores required for basic use.
//
// Producer-side publish audit reuses the monitor store: each successful
// Bus.Send writes an Entry with Status == StatusPublished alongside the
// subscriber entries, so a single GetByEventID returns the full lineage.
//
// # Quick start
//
//	bus, err := event.NewBus("mybus",
//	    event.WithTransport(t),
//	    stack.WithReliabilityStack(),
//	)
//
// # Custom stores
//
// Replace any default store with a production-grade backend. The monitor
// store, when it implements event.PublishAuditStore, also receives publish
// records (monitor.MemoryStore does this out of the box):
//
//	bus, err := event.NewBus("mybus",
//	    event.WithTransport(t),
//	    stack.WithReliabilityStack(
//	        stack.WithMonitorStore(monitor.NewPostgresStore(db)),
//	        stack.WithIdempotencyStore(idempotency.NewRedisStore(rdb, 24*time.Hour)),
//	    ),
//	)
package stack

import (
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/idempotency"
	"github.com/rbaliyan/event/v3/monitor"
	"github.com/rbaliyan/event/v3/poison"
)

const (
	defaultIdempotencyTTL   = 24 * time.Hour
	defaultPoisonThreshold  = 5
	defaultPoisonQuarantine = time.Hour
)

// options holds the configuration for WithReliabilityStack.
type options struct {
	monitorStore     event.MonitorStore
	idempotencyStore event.IdempotencyStore
	poisonDetector   event.PoisonDetector
	// Memory-store knobs (only used when the corresponding store is nil)
	idempotencyTTL   time.Duration
	poisonThreshold  int
	poisonQuarantine time.Duration
}

// Option configures WithReliabilityStack.
type Option func(*options)

// WithMonitorStore replaces the default in-memory monitor store.
// Use a production backend such as monitor.NewPostgresStore or
// monitor.NewMemoryStore for development.
//
// If the supplied store also implements event.PublishAuditStore (the
// in-memory store does), the stack wires it as the publish audit store
// as well, so producer-side publish entries land in the same backend.
func WithMonitorStore(store event.MonitorStore) Option {
	return func(o *options) {
		if store != nil {
			o.monitorStore = store
		}
	}
}

// WithIdempotencyStore replaces the default in-memory idempotency store.
// For distributed deployments use idempotency.NewRedisStore or
// idempotency.NewPostgresStore.
func WithIdempotencyStore(store event.IdempotencyStore) Option {
	return func(o *options) {
		if store != nil {
			o.idempotencyStore = store
		}
	}
}

// WithPoisonDetector replaces the default in-memory poison detector.
// For distributed deployments create a detector with a shared store
// (e.g. poison.NewRedisStore).
func WithPoisonDetector(detector event.PoisonDetector) Option {
	return func(o *options) {
		if detector != nil {
			o.poisonDetector = detector
		}
	}
}

// WithIdempotencyTTL sets the TTL for the default in-memory idempotency store.
// Has no effect when WithIdempotencyStore is also provided.
// Default: 24 hours.
func WithIdempotencyTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.idempotencyTTL = d
		}
	}
}

// WithPoisonThreshold sets the failure count before a message is quarantined
// by the default in-memory poison detector.
// Has no effect when WithPoisonDetector is also provided.
// Default: 5.
func WithPoisonThreshold(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.poisonThreshold = n
		}
	}
}

// WithPoisonQuarantine sets how long quarantined messages are blocked
// by the default in-memory poison detector.
// Has no effect when WithPoisonDetector is also provided.
// Default: 1 hour.
func WithPoisonQuarantine(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.poisonQuarantine = d
		}
	}
}

// WithReliabilityStack returns a BusOption that enables Monitor, Idempotency,
// and Poison detection with sensible defaults. All three features use in-memory
// stores unless replaced via the Option functions above.
//
// Producer-side publish audit reuses the monitor store: when the configured
// monitor store also implements event.PublishAuditStore (monitor.MemoryStore
// does), every successful Bus.Send writes a publish entry alongside subscriber
// entries — no extra option required.
//
// Default configuration:
//   - Monitor: in-memory store (also serves as publish audit)
//   - Idempotency: in-memory store, 24-hour TTL
//   - Poison detection: in-memory store, threshold=5, quarantine=1 hour
//
// Example (defaults):
//
//	bus, _ := event.NewBus("svc",
//	    event.WithTransport(t),
//	    stack.WithReliabilityStack(),
//	)
//
// Example (production stores):
//
//	bus, _ := event.NewBus("svc",
//	    event.WithTransport(t),
//	    stack.WithReliabilityStack(
//	        stack.WithMonitorStore(monitor.NewPostgresStore(db)),
//	        stack.WithIdempotencyStore(idempotency.NewRedisStore(rdb, 24*time.Hour)),
//	        stack.WithPoisonDetector(
//	            poison.NewDetector(poison.NewRedisStore(rdb),
//	                poison.WithThreshold(3),
//	                poison.WithQuarantineTime(2*time.Hour),
//	            ),
//	        ),
//	    ),
//	)
func WithReliabilityStack(opts ...Option) event.BusOption {
	o := &options{
		idempotencyTTL:   defaultIdempotencyTTL,
		poisonThreshold:  defaultPoisonThreshold,
		poisonQuarantine: defaultPoisonQuarantine,
	}
	for _, opt := range opts {
		opt(o)
	}

	ms := o.monitorStore
	if ms == nil {
		ms = monitor.NewMemoryStore()
	}

	is := o.idempotencyStore
	if is == nil {
		is = idempotency.NewMemoryStore(o.idempotencyTTL)
	}

	pd := o.poisonDetector
	if pd == nil {
		pd = poison.NewDetector(
			poison.NewMemoryStore(),
			poison.WithThreshold(o.poisonThreshold),
			poison.WithQuarantineTime(o.poisonQuarantine),
		)
	}

	busOpts := []event.BusOption{
		event.WithMonitor(ms),
		event.WithIdempotency(is),
		event.WithPoisonDetection(pd),
	}
	// If the monitor store also satisfies PublishAuditStore (e.g.
	// monitor.MemoryStore), reuse it for producer-side audit so every
	// publish lands alongside the subscriber entries it triggers.
	if pa, ok := ms.(event.PublishAuditStore); ok {
		busOpts = append(busOpts, event.WithPublishAudit(pa))
	}

	return event.WithAll(busOpts...)
}

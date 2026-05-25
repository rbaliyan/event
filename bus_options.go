package event

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// busOptions holds configuration for bus (unexported)
type busOptions struct {
	transport       transport.Transport
	logger          *slog.Logger
	tracingEnabled  bool
	recoveryEnabled bool
	metricsEnabled  bool
	drainTimeout    time.Duration
	// Subscriber middleware stores (applied automatically to all subscribers)
	idempotencyStore IdempotencyStore
	poisonDetector   PoisonDetector
	monitorStore     MonitorStore
	// Schema provider for dynamic event configuration
	schemaProvider SchemaProvider
	strictSchema   bool // If true, fail registration when schema provider errors occur
	// Outbox store for transactional event publishing
	outboxStore OutboxStore
	// DLQ store for automatic dead letter routing
	dlqStore DLQStore
	// Publish audit store for producer-side event tracking
	publishAuditStore PublishAuditStore
}

// BusOption option function for bus configuration
type BusOption func(*busOptions)

// WithTransport sets a custom transport for the bus
func WithTransport(t transport.Transport) BusOption {
	return func(o *busOptions) {
		if t != nil {
			o.transport = t
		}
	}
}

// WithTracing enables/disables tracing for all events on this bus
func WithTracing(enabled bool) BusOption {
	return func(o *busOptions) {
		o.tracingEnabled = enabled
	}
}

// WithRecovery enables/disables panic recovery for all events on this bus
func WithRecovery(enabled bool) BusOption {
	return func(o *busOptions) {
		o.recoveryEnabled = enabled
	}
}

// WithMetrics enables/disables metrics for all events on this bus
func WithMetrics(enabled bool) BusOption {
	return func(o *busOptions) {
		o.metricsEnabled = enabled
	}
}

// WithLogger sets a custom logger for the bus
func WithLogger(l *slog.Logger) BusOption {
	return func(o *busOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithIdempotency configures automatic idempotency checking for all subscribers.
// When set, all event handlers will automatically skip duplicate messages.
// This eliminates the need to manually check idempotency in each handler.
//
// Example:
//
//	store := idempotency.NewRedisStore(redisClient, time.Hour)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithIdempotency(store),
//	)
//
//	// Subscriber is simple - no manual idempotency check needed
//	orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
//	    return processOrder(ctx, order) // Just business logic!
//	})
func WithIdempotency(store IdempotencyStore) BusOption {
	return func(o *busOptions) {
		if store != nil {
			o.idempotencyStore = store
		}
	}
}

// WithPoisonDetection configures automatic poison message detection for all subscribers.
// When set, all event handlers will automatically skip quarantined messages and track failures.
// Messages that fail repeatedly will be quarantined and skipped until released.
//
// Example:
//
//	detector := poison.NewDetector(poison.NewRedisStore(redisClient),
//	    poison.WithThreshold(5),
//	    poison.WithQuarantineTime(time.Hour),
//	)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithPoisonDetection(detector),
//	)
//
//	// Subscriber is simple - no manual poison detection needed
//	orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
//	    return processOrder(ctx, order) // Just business logic!
//	})
func WithPoisonDetection(detector PoisonDetector) BusOption {
	return func(o *busOptions) {
		if detector != nil {
			o.poisonDetector = detector
		}
	}
}

// WithMonitor configures automatic event processing monitoring for all subscribers.
// When set, all event handlers will automatically record processing metrics including
// start time, duration, status, and any errors.
//
// Example:
//
//	store := monitor.NewPostgresStore(db)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithMonitor(store),
//	)
//
//	// Subscriber is simple - monitoring happens automatically
//	orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
//	    return processOrder(ctx, order) // Just business logic!
//	})
func WithMonitor(store MonitorStore) BusOption {
	return func(o *busOptions) {
		if store != nil {
			o.monitorStore = store
		}
	}
}

// WithSchemaProvider configures a schema provider for dynamic event configuration.
// When set, events will automatically load their configuration from the schema registry
// when registered, ensuring all subscribers have consistent settings.
//
// The schema provider also enables real-time configuration updates via the Watch mechanism.
//
// Example:
//
//	// Using in-memory provider for testing
//	provider := schema.NewMemoryProvider()
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithSchemaProvider(provider),
//	)
//
//	// Using PostgreSQL provider with notification callback
//	provider := schema.NewPostgresProvider(db, func(ctx context.Context, change schema.SchemaChangeEvent) error {
//	    return bus.publishSchemaChange(ctx, change)
//	})
func WithSchemaProvider(provider SchemaProvider) BusOption {
	return func(o *busOptions) {
		if provider != nil {
			o.schemaProvider = provider
		}
	}
}

// WithStrictSchema configures strict schema loading behavior.
// When enabled, event registration will fail if the schema provider
// returns an error (e.g., database connection failure).
//
// By default (strict=false):
//   - Schema not found: continue with event defaults (expected for new events)
//   - Schema provider error: log warning and continue with defaults
//
// With strict=true:
//   - Schema not found: continue with event defaults
//   - Schema provider error: fail registration with ErrSchemaLoadFailed
//
// Enable this when schema-defined settings (timeouts, retries, feature flags)
// are critical for correct operation and should not be silently ignored.
//
// Example:
//
//	bus, _ := event.NewBus("order-service",
//	    event.WithSchemaProvider(provider),
//	    event.WithStrictSchema(true), // Fail if schema provider errors
//	)
func WithStrictSchema(strict bool) BusOption {
	return func(o *busOptions) {
		o.strictSchema = strict
	}
}

// WithOutbox configures an outbox store for transactional event publishing.
// When set, calls to Publish() will automatically route to the outbox when
// inside a transaction (detected via WithOutboxTx context).
//
// Normal publishes (outside transactions) still go directly to the transport.
// This enables atomic "business operation + event publish" within database transactions.
//
// Example:
//
//	store, _ := outbox.NewPostgresStore(db)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithOutbox(store),
//	)
//
//	// Normal publish - goes directly to transport
//	orderEvent.Publish(ctx, order)
//
//	// Inside transaction - goes to outbox
//	tx, _ := db.BeginTx(ctx, nil)
//	ctx = event.WithOutboxTx(ctx, tx)
//	tx.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", "paid", orderID)
//	orderEvent.Publish(ctx, order) // Routed to outbox!
//
// For MongoDB outbox, use the separate event-mongodb module:
// https://github.com/rbaliyan/event-mongodb
func WithOutbox(store OutboxStore) BusOption {
	return func(o *busOptions) {
		if store != nil {
			o.outboxStore = store
		}
	}
}

// WithDLQ configures a DLQ store for automatic dead letter routing.
// When set, messages that fail permanently (rejected, max retries exhausted,
// or decode errors) are automatically stored in the DLQ. This eliminates the
// need for per-event DLQ configuration.
//
// Example:
//
//	dlqStore := dlq.NewPostgresStore(db)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithDLQ(dlq.NewStoreAdapter(dlqStore, "my-service")),
//	)
func WithDLQ(store DLQStore) BusOption {
	return func(o *busOptions) {
		if store != nil {
			o.dlqStore = store
		}
	}
}

// WithPublishAudit configures producer-side publish audit logging.
// When set, every successful transport.Publish call is recorded in the store.
// This closes the observability gap between "event published" and "event processed":
// if an event has no monitor entry but does have a publish audit entry, the fault
// lies in the transport or subscriber layer rather than the application code.
//
// Outbox-routed publishes (messages stored in the outbox for later relay) are
// not recorded here; they bypass the transport path and are tracked separately
// by the outbox store until the relay delivers them.
//
// Any monitor store satisfies PublishAuditStore, so the simplest setup
// reuses the monitor store you already pass to WithMonitor:
//
//	ms := monitor.NewMemoryStore() // or monitor.NewPostgresStore(db)
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithMonitor(ms),
//	    event.WithPublishAudit(ms),
//	)
//
// See monitor/DEBUGGING.md (#withpublishaudit) for the publish↔process
// fault-localization table.
func WithPublishAudit(store PublishAuditStore) BusOption {
	return func(o *busOptions) {
		if store != nil {
			o.publishAuditStore = store
		}
	}
}

// WithDrainTimeout sets the maximum time Bus.Close() will wait for in-flight
// message handlers to complete before proceeding with shutdown.
// A value of 0 (the default) means no waiting — current behavior is preserved.
// Negative values are coerced to the same "no wait" behavior; the option is
// idempotent for non-positive d.
func WithDrainTimeout(d time.Duration) BusOption {
	return func(o *busOptions) {
		if d > 0 {
			o.drainTimeout = d
		}
	}
}

// WithAll combines multiple BusOptions into a single option. This is useful
// when building helper functions outside the event package that compose
// several bus options together, since busOptions is unexported.
//
// Example:
//
//	func WithMyDefaults() event.BusOption {
//	    return event.WithAll(
//	        event.WithTracing(true),
//	        event.WithRecovery(true),
//	    )
//	}
func WithAll(opts ...BusOption) BusOption {
	return func(o *busOptions) {
		for _, opt := range opts {
			opt(o)
		}
	}
}

// newBusOptions creates options with defaults and applies provided options
func newBusOptions(opts ...BusOption) *busOptions {
	o := &busOptions{
		logger:          slog.Default(),
		tracingEnabled:  true,
		recoveryEnabled: true,
		metricsEnabled:  true,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

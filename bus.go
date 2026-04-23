package event

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NewID generates a new unique ID
func NewID() string {
	return transport.NewID()
}

const (
	busRunning = 1
	busStopped = 0
)

// DefaultBusName is the default name for bus and consumer groups
// Using the same name across distributed systems enables:
// - WorkerPool mode: load balancing across instances (one receives)
// - Broadcast mode: all instances receive messages
const DefaultBusName = "event-bus"

// Bus is an event bus that manages events and their lifecycle
type Bus struct {
	status          int32
	id              string
	name            string
	shutdownChan    chan struct{}
	inflightWG      sync.WaitGroup
	drainTimeout    time.Duration
	transport       transport.Transport
	logger          *slog.Logger
	tracingEnabled  bool
	recoveryEnabled bool
	metricsEnabled  bool
	events          map[string]any
	eventTypes      map[string]reflect.Type // Track registered types for type checking
	eventMutex      sync.RWMutex
	// Subscriber middleware (applied automatically to all subscribers)
	idempotencyStore IdempotencyStore
	poisonDetector   PoisonDetector
	monitorStore     MonitorStore
	// Schema provider for dynamic event configuration
	schemaProvider SchemaProvider
	strictSchema   bool // If true, fail registration when schema provider errors
	// Outbox store for transactional event publishing
	outboxStore OutboxStore
	// DLQ store for automatic dead letter routing
	dlqStore DLQStore
	// Cached OTel instruments (initialized once during construction)
	busAttr          attribute.KeyValue // bus="name" attribute for all metrics
	publishedCounter metric.Int64Counter
	subscribedCounter metric.Int64Counter
	publishDuration       metric.Float64Histogram
	handlerDuration       metric.Float64Histogram
	handlerErrors         metric.Int64Counter
	filterDroppedCounter  metric.Int64Counter
	receiveLag            metric.Float64Histogram
}

// NewBus creates a new event bus and registers it in the global registry.
// Returns error if:
//   - Transport is not provided via WithTransport()
//   - A bus with the same name already exists
//
// The bus is automatically registered in the global registry and can be
// retrieved using GetBus(name) or accessed via full event names like
// "busname://eventname".
func NewBus(name string, opts ...BusOption) (*Bus, error) {
	o := newBusOptions(opts...)

	if name == "" {
		name = DefaultBusName
	}

	// Transport is required - use WithTransport() to set it
	// For channel transport: NewBus(name, WithTransport(channel.New()))
	transport := o.transport
	if transport == nil {
		return nil, ErrTransportRequired
	}

	bus := &Bus{
		name:             name,
		status:           busRunning,
		id:               NewID(),
		shutdownChan:     make(chan struct{}),
		drainTimeout:     o.drainTimeout,
		transport:        transport,
		logger:           o.logger.With("component", "bus>"+name),
		tracingEnabled:   o.tracingEnabled,
		recoveryEnabled:  o.recoveryEnabled,
		metricsEnabled:   o.metricsEnabled,
		events:           make(map[string]any),
		eventTypes:       make(map[string]reflect.Type),
		idempotencyStore: o.idempotencyStore,
		poisonDetector:   o.poisonDetector,
		monitorStore:     o.monitorStore,
		schemaProvider:   o.schemaProvider,
		strictSchema:     o.strictSchema,
		outboxStore:      o.outboxStore,
		dlqStore:         o.dlqStore,
	}

	// Initialize OTel instruments if metrics enabled.
	// All buses share a single meter to avoid duplicate instrument registrations
	// that cause Prometheus exporter conflicts.
	if bus.metricsEnabled {
		bus.busAttr = attribute.String("bus", name)
		meter := sharedMeter()
		bus.publishedCounter, _ = meter.Int64Counter("event.published",
			metric.WithDescription("Total number of events published"))
		bus.subscribedCounter, _ = meter.Int64Counter("event.subscribed",
			metric.WithDescription("Total number of subscriptions"))
		bus.publishDuration, _ = meter.Float64Histogram("event.publish_duration_seconds",
			metric.WithDescription("Time to publish a message to the transport"),
			metric.WithUnit("s"))
		bus.handlerDuration, _ = meter.Float64Histogram("event.handler_duration_seconds",
			metric.WithDescription("Time spent executing a subscriber handler"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0))
		bus.handlerErrors, _ = meter.Int64Counter("event.handler_errors_total",
			metric.WithDescription("Total number of subscriber handler errors"))
		bus.filterDroppedCounter, _ = meter.Int64Counter("event_messages_filter_dropped_total",
			metric.WithDescription("Messages silently dropped by WithMessageFilter before handler dispatch"),
			metric.WithUnit("{message}"))
		bus.receiveLag, _ = meter.Float64Histogram("event.transport_receive_lag_seconds",
			metric.WithDescription("Time between message creation and handler start (queue residence time)"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0))
		initGauges(meter)
	}

	// Register in global registry (use LoadOrStore to handle race condition)
	if _, loaded := busRegistry.LoadOrStore(name, bus); loaded {
		return nil, fmt.Errorf("%w: %q", ErrBusExists, name)
	}

	return bus, nil
}

// ID returns the bus ID
func (b *Bus) ID() string {
	return b.id
}

// Name returns the bus name
func (b *Bus) Name() string {
	return b.name
}

// Running returns true if bus is running
func (b *Bus) Running() bool {
	return atomic.LoadInt32(&b.status) == busRunning
}

// Transport returns the bus transport for custom event implementations
func (b *Bus) Transport() transport.Transport {
	return b.transport
}

// Logger returns the bus logger for custom event implementations
func (b *Bus) Logger() *slog.Logger {
	return b.logger
}

// NewEventID generates a new event ID
func (b *Bus) NewEventID() string {
	return NewID()
}

// NewSubscriptionID generates a new subscription ID
func (b *Bus) NewSubscriptionID() string {
	return NewID()
}

// IdempotencyStore returns the bus-level idempotency store (may be nil)
func (b *Bus) IdempotencyStore() IdempotencyStore {
	return b.idempotencyStore
}

// PoisonDetector returns the bus-level poison detector (may be nil)
func (b *Bus) PoisonDetector() PoisonDetector {
	return b.poisonDetector
}

// MonitorStore returns the bus-level monitor store (may be nil)
func (b *Bus) MonitorStore() MonitorStore {
	return b.monitorStore
}

// recordFilterDrop increments the filter-drop counter when a message is
// rejected by WithMessageFilter before reaching the handler. Both event
// and operation are used as metric attributes so that cross-stream
// delivery bugs (e.g. an insert landing in an update-only consumer group)
// appear as {event="caserecord.updated.global", operation="insert"} spikes.
func (b *Bus) recordFilterDrop(eventName, operation string) {
	if b.filterDroppedCounter == nil {
		return
	}
	b.filterDroppedCounter.Add(context.Background(), 1,
		metric.WithAttributes(
			b.busAttr,
			attribute.String("event", eventName),
			attribute.String("operation", operation),
		),
	)
}

// SchemaProvider returns the bus-level schema provider (may be nil).
// When configured, events automatically load their configuration from the registry.
func (b *Bus) SchemaProvider() SchemaProvider {
	return b.schemaProvider
}

// OutboxStore returns the bus-level outbox store (may be nil).
// When configured, publishes inside transactions automatically route to the outbox.
func (b *Bus) OutboxStore() OutboxStore {
	return b.outboxStore
}

// DLQStore returns the bus-level DLQ store (may be nil).
// When configured, rejected messages are automatically routed to the DLQ.
func (b *Bus) DLQStore() DLQStore {
	return b.dlqStore
}

// sendToDLQ stores a message in the DLQ if configured.
// Returns nil if no DLQ store is configured.
func (b *Bus) sendToDLQ(ctx context.Context, eventName string, msg transport.Message, err error) error {
	if b.dlqStore == nil {
		return nil
	}
	return b.dlqStore.Store(ctx, &DLQMessage{
		EventName:  eventName,
		MessageID:  msg.ID(),
		Payload:    msg.Payload(),
		Metadata:   msg.Metadata(),
		Error:      err,
		RetryCount: msg.RetryCount(),
		Source:     b.name,
		CreatedAt:  time.Now(),
	})
}

// logFallbackDLQ logs the full raw message as a structured log entry when both
// decode fails and DLQ storage fails. This provides a last-resort recovery path
// via centralized logging (e.g., CloudWatch, Datadog, ELK).
func (b *Bus) logFallbackDLQ(logger *slog.Logger, eventName string, msg transport.Message, decodeErr, dlqErr error) {
	logger.Error("DLQ_FALLBACK: message preserved in log after DLQ store failure",
		"event", eventName,
		"msg_id", msg.ID(),
		"payload_b64", base64.StdEncoding.EncodeToString(msg.Payload()),
		"metadata", msg.Metadata(),
		"decode_error", decodeErr,
		"dlq_error", dlqErr,
		"retry_count", msg.RetryCount(),
		"timestamp", msg.Timestamp(),
	)
}

// SupportsRedelivery returns true if the underlying transport supports
// automatic re-delivery of unacknowledged messages.
// Returns false if transport does not implement transport.Redeliverable.
func (b *Bus) SupportsRedelivery() bool {
	if rd, ok := b.transport.(transport.Redeliverable); ok {
		return rd.SupportsRedelivery()
	}
	return false
}

// Get returns an event by name
func (b *Bus) Get(name string) any {
	b.eventMutex.RLock()
	defer b.eventMutex.RUnlock()
	return b.events[name]
}

// Close stops the bus and all registered events.
// Events are shut down before the transport to allow clean subscription cleanup.
// Returns a joined error if any unregister or transport close operations fail.
func (b *Bus) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&b.status, busRunning, busStopped) {
		return nil
	}

	// Unregister from global registry
	busRegistry.Delete(b.name)

	// Signal all subscriber goroutines to stop
	close(b.shutdownChan)

	// Wait for in-flight message handlers to complete
	if b.drainTimeout > 0 {
		done := make(chan struct{})
		go func() {
			b.inflightWG.Wait()
			close(done)
		}()
		timer := time.NewTimer(b.drainTimeout)
		defer timer.Stop()
		select {
		case <-done:
			b.logger.Info("all in-flight handlers completed")
		case <-timer.C:
			b.logger.Warn("drain timeout exceeded, proceeding with shutdown",
				"timeout", b.drainTimeout)
		}
	}

	var errs []error

	// Close the transport first so source watchers stop delivering before
	// events are unregistered. Reversing this order creates a race where
	// the watcher drains buffered change events against an empty
	// registeredEvents map, producing spurious "no registered events" warnings.
	if b.transport != nil {
		if err := b.transport.Close(ctx); err != nil {
			b.logger.Warn("failed to close transport during shutdown", "error", err)
			errs = append(errs, fmt.Errorf("close transport: %w", err))
		}
	}

	// Collect event names under lock, then unregister without holding it.
	// This avoids holding RLock during potentially slow transport operations.
	b.eventMutex.RLock()
	names := make([]string, 0, len(b.events))
	for name := range b.events {
		names = append(names, name)
	}
	b.eventMutex.RUnlock()

	for _, name := range names {
		if err := b.transport.UnregisterEvent(ctx, name); err != nil {
			b.logger.Warn("failed to unregister event during shutdown", "event", name, "error", err)
			errs = append(errs, fmt.Errorf("unregister %s: %w", name, err))
		}
	}

	return errors.Join(errs...)
}

// register adds an event to the bus (internal use)
func (b *Bus) register(name string, ev any, eventType reflect.Type) error {
	b.eventMutex.Lock()
	defer b.eventMutex.Unlock()

	// Re-check under lock: Close() sets status before acquiring eventMutex,
	// so this prevents registering events after bus shutdown.
	if !b.Running() {
		return ErrBusClosed
	}

	if existing, ok := b.events[name]; ok {
		// Check if types match
		if existingType, ok := b.eventTypes[name]; ok {
			if existingType != eventType {
				return fmt.Errorf("%w: event %q registered as %v, requested %v",
					ErrTypeMismatch, name, existingType, eventType)
			}
		}
		// Same type, return existing (this shouldn't happen in normal flow)
		_ = existing
		return ErrEventExists
	}

	b.events[name] = ev
	b.eventTypes[name] = eventType
	return nil
}

// unregister removes an event from the bus (internal use)
func (b *Bus) unregister(name string) {
	b.eventMutex.Lock()
	defer b.eventMutex.Unlock()

	delete(b.events, name)
	delete(b.eventTypes, name)
}

// getTyped returns existing event if it matches the type
func (b *Bus) getTyped(name string, eventType reflect.Type) (any, error) {
	b.eventMutex.RLock()
	defer b.eventMutex.RUnlock()

	if existing, ok := b.events[name]; ok {
		if existingType, ok := b.eventTypes[name]; ok {
			if existingType != eventType {
				return nil, fmt.Errorf("%w: event %q registered as %v, requested %v",
					ErrTypeMismatch, name, existingType, eventType)
			}
		}
		return existing, nil
	}
	return nil, nil
}

// Send publishes a message to the specified event with metrics and tracing.
// This is the low-level method that events should use instead of directly calling transport.
//
// When an outbox store is configured (via WithOutbox) and the context contains
// an active transaction (via WithOutboxTx), the message is routed to the outbox
// instead of the transport. This enables atomic "business operation + event publish"
// within database transactions.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: name of the event to publish to
//   - eventID: unique identifier for this event instance (can be empty to auto-generate)
//   - payload: the pre-encoded event data as bytes
//   - metadata: optional metadata to attach to the message (must include Content-Type)
//
// Returns error if the bus is closed, outbox store fails, or transport fails.
func (b *Bus) Send(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	if !b.Running() {
		return ErrBusClosed
	}

	// Generate event ID if not provided
	if eventID == "" {
		eventID = b.NewEventID()
	}

	// Check if we should route to outbox (inside transaction with outbox configured)
	if b.outboxStore != nil && InOutboxTx(ctx) {
		return b.outboxStore.Store(ctx, eventName, eventID, payload, metadata)
	}

	var spanCtx trace.SpanContext

	// Record publish metrics
	if b.metricsEnabled && b.publishedCounter != nil {
		b.publishedCounter.Add(ctx, 1, metric.WithAttributes(b.busAttr, attribute.String("event", eventName)))
	}

	// Add tracing
	if b.tracingEnabled {
		tracer := otel.Tracer(b.name)
		var span trace.Span
		ctx, span = tracer.Start(ctx, fmt.Sprintf("%s.publish", eventName),
			trace.WithAttributes(
				attribute.String(spanKeyEventID, eventID),
				attribute.String(spanKeyEventSource, b.ID()),
				attribute.String(spanKeyEventBus, b.name),
				attribute.String(spanKeyEventName, eventName)),
			trace.WithSpanKind(trace.SpanKindProducer))
		spanCtx = span.SpanContext()
		defer span.End()
	}

	// Copy metadata if provided
	var meta map[string]string
	if metadata != nil {
		meta = make(map[string]string, len(metadata))
		for k, v := range metadata {
			meta[k] = v
		}
	}

	// Create message
	msg := message.New(eventID, b.ID(), payload, meta, message.WithSpanContext(spanCtx))

	// Send via transport
	publishStart := time.Now()
	err := b.transport.Publish(ctx, eventName, msg)
	if b.metricsEnabled && b.publishDuration != nil {
		b.publishDuration.Record(ctx, time.Since(publishStart).Seconds(),
			metric.WithAttributes(b.busAttr, attribute.String("event", eventName)))
	}
	return err
}

// recordHandlerMetrics records handler duration, receive lag, and error metrics for a subscriber invocation.
// lag is the time from message creation to handler start (queue residence time).
func (b *Bus) recordHandlerMetrics(ctx context.Context, eventName string, duration, lag time.Duration, handlerErr error) {
	if !b.metricsEnabled {
		return
	}
	attrs := metric.WithAttributes(b.busAttr, attribute.String("event", eventName))
	if b.handlerDuration != nil {
		b.handlerDuration.Record(ctx, duration.Seconds(), attrs)
	}
	if b.receiveLag != nil && lag >= 0 {
		b.receiveLag.Record(ctx, lag.Seconds(), attrs)
	}
	if handlerErr != nil && b.handlerErrors != nil {
		b.handlerErrors.Add(ctx, 1, attrs)
	}
}

// Recv creates a subscription to receive messages for the specified event.
// This is the low-level method that events should use instead of directly calling transport.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: name of the event to subscribe to
//   - opts: subscription options (delivery mode, start position, etc.)
//
// Returns:
//   - Subscription for receiving messages
//   - error if the bus is closed or transport fails
func (b *Bus) Recv(ctx context.Context, eventName string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !b.Running() {
		return nil, ErrBusClosed
	}

	// Record subscription metrics
	if b.metricsEnabled && b.subscribedCounter != nil {
		b.subscribedCounter.Add(ctx, 1, metric.WithAttributes(b.busAttr, attribute.String("event", eventName)))
	}

	// Subscribe via transport
	return b.transport.Subscribe(ctx, eventName, opts...)
}

// Register binds an existing event to the bus.
// Returns error if:
// - Bus is closed
// - Event with same name exists with different type
// - Event is already bound to another bus
// - Transport fails to register the event
//
// If a schema provider is configured, the event's schema is loaded and applied.
// This ensures all subscribers have consistent settings defined by the publisher.
func Register[T any](ctx context.Context, bus *Bus, event Event[T]) error {
	if !bus.Running() {
		return ErrBusClosed
	}

	impl, ok := event.(*eventImpl[T])
	if !ok {
		return errors.New("invalid event type: must be created with event.New()")
	}

	// Get the type for T
	var zero T
	eventType := reflect.TypeOf(zero)

	// Check if event already exists with same name
	if existing, err := bus.getTyped(impl.name, eventType); err != nil {
		return err
	} else if existing != nil {
		return fmt.Errorf("%w: %q", ErrEventExists, impl.name)
	}

	// Register event with transport
	if err := bus.transport.RegisterEvent(ctx, impl.name); err != nil {
		// If event already exists in transport (race condition), that's ok
		if !errors.Is(err, transport.ErrEventAlreadyExists) {
			return fmt.Errorf("transport register failed: %w", err)
		}
	}

	// Load schema from provider if configured.
	// Must happen BEFORE Bind() so schema fields are visible to concurrent
	// Subscribe() calls that check status after Bind() sets it to active.
	if bus.schemaProvider != nil {
		schema, err := bus.schemaProvider.Get(ctx, impl.name)
		if err != nil {
			// Schema provider error (not "schema not found")
			if bus.strictSchema {
				// Strict mode: fail registration on provider errors
				return fmt.Errorf("%w: %s: %v", ErrSchemaLoadFailed, impl.name, err)
			}
			// Non-strict mode: log warning and continue with defaults
			bus.logger.Warn("failed to load schema, using defaults",
				"event", impl.name, "error", err)
		} else if schema != nil {
			impl.applySchema(schema)
			bus.logger.Debug("applied schema", "event", impl.name, "version", schema.Version)
		}
		// schema == nil means not found, which is fine - use event defaults
	}

	// Bind event to bus (sets status to active via atomic store,
	// establishing a happens-before relationship with Subscribe() reads)
	if err := impl.Bind(bus); err != nil {
		return err
	}

	// Register with bus
	if err := bus.register(impl.name, impl, eventType); err != nil {
		impl.Unbind() // rollback: restore event to unbound state
		return err
	}

	bus.logger.Debug("registered event", "event", impl.name)
	return nil
}

// Unregister removes an event from the bus and unregisters it from the transport.
// After unregistration, the event can no longer publish or receive messages.
// Returns error if:
// - Bus is closed
// - Event is not registered with this bus
// - Transport fails to unregister the event
func Unregister[T any](ctx context.Context, bus *Bus, event Event[T]) error {
	if !bus.Running() {
		return ErrBusClosed
	}

	impl, ok := event.(*eventImpl[T])
	if !ok {
		return errors.New("invalid event type: must be created with event.New()")
	}

	// Check if event is bound to this bus
	if impl.getBus() != bus {
		return ErrEventNotBound
	}

	// Unbind event from bus
	if !impl.Unbind() {
		return nil // Already unbound
	}

	// Remove from bus's event map
	bus.unregister(impl.name)

	// Unregister from transport
	if err := bus.transport.UnregisterEvent(ctx, impl.name); err != nil {
		// Log but don't fail - event is already marked inactive
		bus.logger.Warn("failed to unregister event from transport", "event", impl.name, "error", err)
	}

	bus.logger.Debug("unregistered event", "event", impl.name)
	return nil
}

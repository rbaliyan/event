package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
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

// FullNameSeparator is the separator between bus name and event name in full event names.
// Full name format: "<bus_name>://<event_name>"
const FullNameSeparator = "://"

// Global bus registry
var busRegistry sync.Map // map[string]*Bus

// GetBus returns a registered bus by name.
// Returns nil if no bus with that name exists.
func GetBus(name string) *Bus {
	if v, ok := busRegistry.Load(name); ok {
		return v.(*Bus)
	}
	return nil
}

// ListBuses returns the names of all registered buses.
func ListBuses() []string {
	var names []string
	busRegistry.Range(func(key, value any) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

// parseFullName splits a full event name into bus name and event name.
// Format: "<bus_name>://<event_name>"
// Returns error if format is invalid.
func parseFullName(fullName string) (busName, eventName string, err error) {
	idx := strings.Index(fullName, FullNameSeparator)
	if idx == -1 {
		return "", "", fmt.Errorf("%w: missing separator %q in %q", ErrInvalidFullName, FullNameSeparator, fullName)
	}
	busName = fullName[:idx]
	eventName = fullName[idx+len(FullNameSeparator):]
	if busName == "" {
		return "", "", fmt.Errorf("%w: empty bus name in %q", ErrInvalidFullName, fullName)
	}
	if eventName == "" {
		return "", "", fmt.Errorf("%w: empty event name in %q", ErrInvalidFullName, fullName)
	}
	return busName, eventName, nil
}

// Get retrieves a typed event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
// Returns ErrTypeMismatch if the types don't match.
//
// Example:
//
//	event, err := event.Get[Order]("mybus://order.created")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	event.Publish(ctx, Order{ID: "123"})
func Get[T any](fullName string) (Event[T], error) {
	busName, eventName, err := parseFullName(fullName)
	if err != nil {
		return nil, err
	}

	bus := GetBus(busName)
	if bus == nil {
		return nil, fmt.Errorf("%w: %q", ErrBusNotFound, busName)
	}

	// Get the type for T to validate
	var zero T
	eventType := reflect.TypeOf(zero)

	ev, err := bus.getTyped(eventName, eventType)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, fmt.Errorf("%w: %q", ErrEventNotFound, eventName)
	}

	typed, ok := ev.(Event[T])
	if !ok {
		return nil, fmt.Errorf("%w: cannot cast event %q to requested type", ErrTypeMismatch, eventName)
	}

	return typed, nil
}

// Publish sends data to an event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
//
// Example:
//
//	err := event.Publish(ctx, "mybus://order.created", Order{ID: "123"})
func Publish[T any](ctx context.Context, fullName string, data T) error {
	ev, err := Get[T](fullName)
	if err != nil {
		return err
	}
	return ev.Publish(ctx, data)
}

// Subscribe registers a handler for an event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
//
// Example:
//
//	err := event.Subscribe(ctx, "mybus://order.created", func(ctx context.Context, e event.Event[Order], order Order) error {
//	    fmt.Println("Received order:", order.ID)
//	    return nil
//	})
func Subscribe[T any](ctx context.Context, fullName string, handler Handler[T], opts ...SubscribeOption[T]) error {
	ev, err := Get[T](fullName)
	if err != nil {
		return err
	}
	return ev.Subscribe(ctx, handler, opts...)
}

// DefaultBusName is the default name for bus and consumer groups
// Using the same name across distributed systems enables:
// - WorkerPool mode: load balancing across instances (one receives)
// - Broadcast mode: all instances receive messages
var DefaultBusName = "event-bus"

// Bus errors
var (
	ErrBusClosed         = errors.New("bus is closed")
	ErrBusExists         = errors.New("bus already exists with this name")
	ErrBusNotFound       = errors.New("bus not found")
	ErrEventExists       = errors.New("event already exists")
	ErrEventNotFound     = errors.New("event not found")
	ErrTypeMismatch      = errors.New("event type mismatch")
	ErrAlreadyBound      = errors.New("event already bound to another bus")
	ErrTransportRequired = errors.New("transport is required: use WithBusTransport(channel.New()) or similar")
	ErrInvalidFullName   = errors.New("invalid full name format, expected: <bus_name>://<event_name>")
)

// StatusCode represents the health state of the bus
type StatusCode string

const (
	// StatusHealthy indicates the bus is functioning normally
	StatusHealthy StatusCode = "healthy"
	// StatusDegraded indicates the bus is functioning but with issues
	StatusDegraded StatusCode = "degraded"
	// StatusUnhealthy indicates the bus is not functioning
	StatusUnhealthy StatusCode = "unhealthy"
)

// Status contains detailed status information for the bus
type Status struct {
	Code       StatusCode         `json:"status"`
	Message    string             `json:"message,omitempty"`
	Latency    time.Duration      `json:"latency,omitempty"`
	Details    map[string]any     `json:"details,omitempty"`
	Components map[string]*Status `json:"components,omitempty"`
	CheckedAt  time.Time          `json:"checked_at"`
}

// IsHealthy returns true if the status code is healthy
func (s *Status) IsHealthy() bool {
	return s.Code == StatusHealthy
}

// ConsumerLag contains information about consumer lag for an event
type ConsumerLag struct {
	Event           string        `json:"event"`
	ConsumerGroup   string        `json:"consumer_group,omitempty"`
	Lag             int64         `json:"lag"`              // Number of unprocessed messages
	OldestPending   time.Duration `json:"oldest_pending"`   // Age of oldest unacknowledged message
	PendingMessages int64         `json:"pending_messages"` // Messages delivered but not yet acked
}

// busOptions holds configuration for bus (unexported)
type busOptions struct {
	transport       transport.Transport
	logger          *slog.Logger
	tracingEnabled  bool
	recoveryEnabled bool
	metricsEnabled  bool
}

// BusOption option function for bus configuration
type BusOption func(*busOptions)

// WithBusTransport sets a custom transport for the bus
func WithBusTransport(t transport.Transport) BusOption {
	return func(o *busOptions) {
		if t != nil {
			o.transport = t
		}
	}
}

// WithBusTracing enables/disables tracing for all events on this bus
func WithBusTracing(enabled bool) BusOption {
	return func(o *busOptions) {
		o.tracingEnabled = enabled
	}
}

// WithBusRecovery enables/disables panic recovery for all events on this bus
func WithBusRecovery(enabled bool) BusOption {
	return func(o *busOptions) {
		o.recoveryEnabled = enabled
	}
}

// WithBusMetrics enables/disables metrics for all events on this bus
func WithBusMetrics(enabled bool) BusOption {
	return func(o *busOptions) {
		o.metricsEnabled = enabled
	}
}

// WithBusLogger sets a custom logger for the bus
func WithBusLogger(l *slog.Logger) BusOption {
	return func(o *busOptions) {
		if l != nil {
			o.logger = l
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

// Bus is an event bus that manages events and their lifecycle
type Bus struct {
	status          int32
	id              string
	name            string
	shutdownChan    chan struct{}
	transport       transport.Transport
	logger          *slog.Logger
	tracingEnabled  bool
	recoveryEnabled bool
	metricsEnabled  bool
	events          map[string]any
	eventTypes      map[string]reflect.Type // Track registered types for type checking
	eventMutex      sync.RWMutex
}

// NewBus creates a new event bus and registers it in the global registry.
// Returns error if:
//   - Transport is not provided via WithBusTransport()
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

	// Check if bus already exists
	if _, exists := busRegistry.Load(name); exists {
		return nil, fmt.Errorf("%w: %q", ErrBusExists, name)
	}

	// Transport is required - use WithBusTransport() to set it
	// For channel transport: NewBus(name, WithBusTransport(channel.New()))
	transport := o.transport
	if transport == nil {
		return nil, ErrTransportRequired
	}

	bus := &Bus{
		name:            name,
		status:          busRunning,
		id:              NewID(),
		shutdownChan:    make(chan struct{}),
		transport:       transport,
		logger:          o.logger.With("component", "bus>"+name),
		tracingEnabled:  o.tracingEnabled,
		recoveryEnabled: o.recoveryEnabled,
		metricsEnabled:  o.metricsEnabled,
		events:          make(map[string]any),
		eventTypes:      make(map[string]reflect.Type),
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

// Get returns an event by name
func (b *Bus) Get(name string) any {
	b.eventMutex.RLock()
	defer b.eventMutex.RUnlock()
	return b.events[name]
}

// Close stops the bus and all registered events
func (b *Bus) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&b.status, busRunning, busStopped) {
		// Unregister from global registry
		busRegistry.Delete(b.name)

		close(b.shutdownChan)
		// Close the bus transport
		if b.transport != nil {
			b.transport.Close(ctx)
		}
	}
	return nil
}

// register adds an event to the bus (internal use)
func (b *Bus) register(name string, ev any, eventType reflect.Type) error {
	b.eventMutex.Lock()
	defer b.eventMutex.Unlock()

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

// Status returns detailed status information about the bus and its transport.
// Use this to inspect the bus state for monitoring dashboards.
// If the transport implements HealthChecker, its status is included.
func (b *Bus) Status(ctx context.Context) *Status {
	result := &Status{
		CheckedAt:  time.Now(),
		Details:    make(map[string]any),
		Components: make(map[string]*Status),
	}

	// Check bus status
	if !b.Running() {
		result.Code = StatusUnhealthy
		result.Message = "bus is closed"
		result.Details["bus_name"] = b.name
		return result
	}

	// Count events
	b.eventMutex.RLock()
	eventCount := len(b.events)
	b.eventMutex.RUnlock()

	result.Details["bus_name"] = b.name
	result.Details["events"] = eventCount

	// Check transport health if it implements HealthChecker
	if hc, ok := b.transport.(transport.HealthChecker); ok {
		transportHealth := hc.Health(ctx)
		// Convert transport health to bus status
		result.Components["transport"] = convertTransportStatus(transportHealth)

		// Aggregate status from transport
		switch transportHealth.Status {
		case transport.HealthStatusUnhealthy:
			result.Code = StatusUnhealthy
			result.Message = "transport is unhealthy"
		case transport.HealthStatusDegraded:
			result.Code = StatusDegraded
			result.Message = "transport is degraded"
		default:
			result.Code = StatusHealthy
			result.Message = "bus is healthy"
		}
	} else {
		// Transport doesn't implement health checks
		result.Code = StatusHealthy
		result.Message = "bus is healthy (transport health not available)"
	}

	return result
}

// Health performs a health check suitable for health probes.
// Returns nil if the bus is healthy, or an error describing the issue.
func (b *Bus) Health(ctx context.Context) error {
	status := b.Status(ctx)
	if status.Code == StatusUnhealthy {
		return errors.New(status.Message)
	}
	return nil
}

// convertTransportStatus converts transport.HealthCheckResult to bus Status
func convertTransportStatus(th *transport.HealthCheckResult) *Status {
	if th == nil {
		return nil
	}

	result := &Status{
		Code:      StatusCode(th.Status),
		Message:   th.Message,
		Latency:   th.Latency,
		Details:   th.Details,
		CheckedAt: th.CheckedAt,
	}

	// Convert nested components
	if len(th.Components) > 0 {
		result.Components = make(map[string]*Status, len(th.Components))
		for k, v := range th.Components {
			result.Components[k] = convertTransportStatus(v)
		}
	}

	return result
}

// ConsumerLag returns consumer lag metrics for all events if the transport supports it.
// Returns nil if the transport doesn't implement LagMonitor.
func (b *Bus) ConsumerLag(ctx context.Context) ([]ConsumerLag, error) {
	if !b.Running() {
		return nil, ErrBusClosed
	}

	if lm, ok := b.transport.(transport.LagMonitor); ok {
		transportLags, err := lm.ConsumerLag(ctx)
		if err != nil {
			return nil, err
		}

		// Convert transport lags to bus lags
		result := make([]ConsumerLag, len(transportLags))
		for i, tl := range transportLags {
			result[i] = ConsumerLag{
				Event:           tl.Event,
				ConsumerGroup:   tl.ConsumerGroup,
				Lag:             tl.Lag,
				OldestPending:   tl.OldestPending,
				PendingMessages: tl.PendingMessages,
			}
		}
		return result, nil
	}

	// Transport doesn't support lag monitoring
	return nil, nil
}

// Send publishes a message to the specified event with metrics and tracing.
// This is the low-level method that events should use instead of directly calling transport.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: name of the event to publish to
//   - eventID: unique identifier for this event instance (can be empty to auto-generate)
//   - payload: the event data to send
//   - metadata: optional metadata to attach to the message
//
// Returns error if the bus is closed or transport fails.
func (b *Bus) Send(ctx context.Context, eventName string, eventID string, payload any, metadata map[string]string) error {
	if !b.Running() {
		return ErrBusClosed
	}

	// Generate event ID if not provided
	if eventID == "" {
		eventID = b.NewEventID()
	}

	var spanCtx trace.SpanContext

	// Record publish metrics
	if b.metricsEnabled {
		meter := otel.Meter(b.name)
		published, _ := meter.Int64Counter("event.published",
			metric.WithDescription("Total number of events published"))
		published.Add(ctx, 1, metric.WithAttributes(attribute.String("event", eventName)))
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
	msg := message.New(eventID, b.ID(), payload, meta, spanCtx)

	// Send via transport
	return b.transport.Publish(ctx, eventName, msg)
}

// Recv creates a subscription to receive messages for the specified event.
// This is the low-level method that events should use instead of directly calling transport.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: name of the event to subscribe to
//   - mode: delivery mode (Broadcast or WorkerPool)
//
// Returns:
//   - Subscription for receiving messages
//   - error if the bus is closed or transport fails
func (b *Bus) Recv(ctx context.Context, eventName string, mode transport.DeliveryMode) (transport.Subscription, error) {
	if !b.Running() {
		return nil, ErrBusClosed
	}

	// Record subscription metrics
	if b.metricsEnabled {
		meter := otel.Meter(b.name)
		subscribed, _ := meter.Int64Counter("event.subscribed",
			metric.WithDescription("Total number of subscriptions"))
		subscribed.Add(ctx, 1, metric.WithAttributes(attribute.String("event", eventName)))
	}

	// Subscribe via transport (convert bus DeliveryMode to transport DeliveryMode)
	return b.transport.Subscribe(ctx, eventName, transport.DeliveryMode(mode))
}

// Register binds an existing event to the bus.
// Returns error if:
// - Bus is closed
// - Event with same name exists with different type
// - Event is already bound to another bus
// - Transport fails to register the event
func Register[T any](ctx context.Context, bus *Bus, event Event[T]) (Event[T], error) {
	if !bus.Running() {
		return nil, ErrBusClosed
	}

	impl, ok := event.(*eventImpl[T])
	if !ok {
		return nil, errors.New("invalid event type: must be created with event.New()")
	}

	// Get the type for T
	var zero T
	eventType := reflect.TypeOf(zero)

	// Check if event already exists with same name
	if existing, err := bus.getTyped(impl.name, eventType); err != nil {
		return nil, err
	} else if existing != nil {
		// Return existing event with correct type
		if typed, ok := existing.(Event[T]); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("%w: cannot cast existing event", ErrTypeMismatch)
	}

	// Register event with transport
	if err := bus.transport.RegisterEvent(ctx, impl.name); err != nil {
		// If event already exists in transport (race condition), that's ok
		if !errors.Is(err, transport.ErrEventAlreadyExists) {
			return nil, fmt.Errorf("transport register failed: %w", err)
		}
	}

	// Bind event to bus
	if err := impl.Bind(bus); err != nil {
		return nil, err
	}

	// Register with bus
	if err := bus.register(impl.name, impl, eventType); err != nil {
		if errors.Is(err, ErrEventExists) {
			// Race condition: another goroutine registered first
			if existing, err := bus.getTyped(impl.name, eventType); err != nil {
				return nil, err
			} else if existing != nil {
				if typed, ok := existing.(Event[T]); ok {
					return typed, nil
				}
			}
		}
		return nil, err
	}

	bus.logger.Debug("registered event", "event", impl.name)
	return event, nil
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
	if impl.bus != bus {
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

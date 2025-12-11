package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrDuplicateEvent ...
	ErrDuplicateEvent = errors.New("duplicate event")

	// default registry
	defaultRegistry *Registry
)

func init() {
	defaultRegistry = NewRegistry("event", nil)
}

// Handler generic event handler
type Handler[T any] func(context.Context, Event[T], T)

// Metrics interface for custom metrics implementations
type Metrics interface {
	Register(prometheus.Registerer) error
	Publishing()
	Processing()
	Processed()
	Published()
	Subscribed()
}

// BaseEvent non-generic interface for registry storage and heterogeneous collections
type BaseEvent interface {
	// Name returns the event name which uniquely identifies this event in Registry
	Name() string
	// Close closes the event
	Close() error
}

// Event generic interface for typed publish/subscribe
type Event[T any] interface {
	BaseEvent
	// Publish sends data to all subscribers (fire and forget)
	Publish(context.Context, T)
	// Subscribe registers a handler to receive published data (fire and forget)
	Subscribe(context.Context, Handler[T])
}

// discardEvent discard all events
type discardEvent[T any] struct{}

func (discardEvent[T]) Name() string                           { return "" }
func (discardEvent[T]) Close() error                           { return nil }
func (discardEvent[T]) Subscribe(_ context.Context, _ Handler[T]) {}
func (discardEvent[T]) Publish(_ context.Context, _ T)         {}

// eventImpl generic event implementation
type eventImpl[T any] struct {
	status          int32
	name            string
	size            int64
	transport       Transport
	metrics         Metrics
	registry        *Registry
	logger          *slog.Logger
	subTimeout      time.Duration
	recoveryEnabled bool
	tracingEnabled  bool
	onError         func(BaseEvent, error) // for panic recovery only
}

func (e *eventImpl[T]) String() string {
	return e.name
}

// Name event name
func (e *eventImpl[T]) Name() string {
	return e.name
}

// Subscribers events subscribers count
func (e *eventImpl[T]) Subscribers() int64 {
	return atomic.LoadInt64(&e.size)
}

// Tracer event tracer
func (e *eventImpl[T]) Tracer() trace.Tracer {
	if e.tracingEnabled {
		return otel.Tracer(e.registry.Name())
	}
	return nil
}

// internalHandler is a non-generic handler for middleware chain
type internalHandler func(context.Context, BaseEvent, any)

// WithMetrics enable metrics for handlers
func (e *eventImpl[T]) WithMetrics(handler internalHandler) internalHandler {
	return func(ctx context.Context, ev BaseEvent, data any) {
		e.metrics.Processing()
		defer e.metrics.Processed()
		handler(ctx, ev, data)
	}
}

// WithTimeout enable timeout for handlers
func (e *eventImpl[T]) WithTimeout(handler internalHandler) internalHandler {
	if e.subTimeout == 0 {
		return handler
	}
	return func(ctx context.Context, ev BaseEvent, data any) {
		ctx, cancel := context.WithTimeout(ctx, e.subTimeout)
		defer cancel()
		handler(ctx, ev, data)
	}
}

// WithTracing enable tracing for handler
func (e *eventImpl[T]) WithTracing(handler internalHandler) internalHandler {
	if !e.tracingEnabled {
		return handler
	}
	return func(ctx context.Context, ev BaseEvent, data any) {
		if tracer := e.Tracer(); tracer != nil {
			var span trace.Span
			ctx, span = tracer.Start(ctx, fmt.Sprintf("%s.subscribe", e.name),
				trace.WithAttributes(attribute.String(spanKeyEventID, ContextEventID(ctx)),
					attribute.String(spanKeyEventSource, ContextSource(ctx)),
					attribute.String(spanKeyEventName, e.name),
					attribute.String(spanKeyEventRegistry, e.registry.name),
					attribute.String(spanKeyEventSubscriptionID, ContextSubscriptionID(ctx))),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithLinks(trace.Link{
					SpanContext: trace.SpanContextFromContext(ctx),
				}))
			defer span.End()
		}
		handler(ctx, ev, data)
	}
}

// WithRecovery enable recovery for handlers
func (e *eventImpl[T]) WithRecovery(handler internalHandler) internalHandler {
	if !e.recoveryEnabled {
		return handler
	}
	return func(ctx context.Context, ev BaseEvent, data any) {
		logger := ContextLogger(ctx)
		if logger == nil {
			logger = e.logger
		}
		defer func() {
			_, file, l, _ := runtime.Caller(0)
			if err := recover(); err != nil {
				logger.Error("panic recovered in event handler",
					"event", ev.Name(),
					"line", l,
					"file", file,
					"error", err,
					"stack", string(debug.Stack()),
				)
				if e.onError != nil {
					e.onError(ev, fmt.Errorf("[%s]panic in %s:%d with : %v", ev.Name(), file, l, err))
				}
			}
		}()
		handler(ctx, ev, data)
	}
}

// Publish sends data to all subscribers (fire and forget)
func (e *eventImpl[T]) Publish(ctx context.Context, eventData T) {
	// Check for nil event or closed
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return
	}

	id := ContextEventID(ctx)
	if id == "" {
		id = e.registry.NewEventID()
	}
	data := message{
		data:     eventData,
		id:       id,
		source:   e.registry.ID(),
		metadata: ContextMetadata(ctx).Copy(),
	}
	// increment counter
	e.metrics.Publishing()

	// Add tracing
	if tracer := e.Tracer(); tracer != nil {
		var span trace.Span
		ctx, span = tracer.Start(ctx, fmt.Sprintf("%s.publish", e.name),
			trace.WithAttributes(attribute.String(spanKeyEventID, data.id),
				attribute.String(spanKeyEventSource, data.source),
				attribute.String(spanKeyEventRegistry, e.registry.name),
				attribute.String(spanKeyEventName, e.name)),
			trace.WithSpanKind(trace.SpanKindProducer))
		data.span = span.SpanContext()
		defer span.End()
	}

	// Send data to transport
	sendCh := e.transport.Send()
	if sendCh == nil {
		return // transport closed
	}
	sendCh <- &data
	e.metrics.Published()
}

// Close shutdown event handling
func (e *eventImpl[T]) Close() error {
	var combinedErr error
	if atomic.CompareAndSwapInt32(&e.status, 1, 0) {
		combinedErr = e.transport.Close()
	}
	return combinedErr
}

// Subscribe registers a handler to receive published data (fire and forget)
func (e *eventImpl[T]) Subscribe(ctx context.Context, handler Handler[T]) {
	// Check for nil event or closed
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return
	}

	atomic.AddInt64(&e.size, 1)
	subID := e.registry.NewSubscriptionID()

	// Wrap typed handler into internal handler
	internalH := func(ctx context.Context, ev BaseEvent, data any) {
		// Type assert and call the typed handler
		typedData, ok := data.(T)
		if !ok {
			// For nil interface values, use zero value
			var zero T
			typedData = zero
		}
		handler(ctx, e, typedData)
	}

	// Apply middleware chain
	wrappedHandler := e.WithTimeout(e.WithMetrics(e.WithTracing(e.WithRecovery(internalH))))

	ch := e.transport.Receive(subID)
	if ch == nil {
		atomic.AddInt64(&e.size, -1)
		return // transport closed
	}
	e.metrics.Subscribed()

	go func() {
		defer func() {
			atomic.AddInt64(&e.size, -1)
		}()
		for {
			select {
			case <-e.registry.shutdownChan:
				e.logger.Info("shutdown subscriber remove", "event", e.Name(), "subscriber_id", subID)
				e.transport.Delete(subID)
				return

			case <-ctx.Done():
				e.logger.Info("subscriber remove", "event", e.Name(), "subscriber_id", subID)
				e.transport.Delete(subID)
				return

			case data, ok := <-ch:
				if !ok {
					e.logger.Info("channel closed", "event", e.Name(), "subscriber_id", subID)
					return
				}
				// Update context values and call handler
				wrappedHandler(contextWithInfo(data.Context(), data.ID(), e.name, data.Source(), subID, data.Metadata(), e.logger, e.registry),
					e, data.Payload())
			}
		}
	}()
	e.logger.Info("installed subscriber", "event", e.Name(), "subscriber_id", subID)
}

// Events a group of events with same type
type Events[T any] []Event[T]

// Names event names
func (e Events[T]) Names() []string {
	names := make([]string, 0, len(e))
	for _, event := range e {
		names = append(names, event.Name())
	}
	return names
}

// Name event name
func (e Events[T]) Name() string {
	return strings.Join(e.Names(), ",")
}

// Close closes all events
func (e Events[T]) Close() error {
	var err error
	for _, event := range e {
		if cerr := event.Close(); cerr != nil {
			err = cerr
		}
	}
	return err
}

// Subscribe all events in the list
func (e Events[T]) Subscribe(ctx context.Context, handler Handler[T]) {
	for _, event := range e {
		event.Subscribe(ctx, handler)
	}
}

// Publish to all events in list
func (e Events[T]) Publish(ctx context.Context, data T) {
	for _, event := range e {
		event.Publish(ctx, data)
	}
}

// New create new instance of event
// and registers with given registry
func New[T any](name string, opts ...Option) Event[T] {
	// Create options with defaults and apply provided options
	o := newEventOptions(opts...)

	// Get event from registry if already exists
	if ev := o.registry.Get(name); ev != nil {
		// Try to return as typed event
		if typed, ok := ev.(Event[T]); ok {
			return typed
		}
		// Event exists but with different type - return it anyway
		// This is a user error but we don't panic
	}

	// Setup metrics
	metrics := o.metrics
	if metrics == nil {
		if o.metricsEnabled {
			metrics = o.registry.Metrics(name)
			_ = metrics.Register(o.registry.Registerer())
		} else {
			metrics = dummyMetrics{}
		}
	}

	// Setup transport
	transport := o.transport
	if transport == nil {
		transport = NewChannelTransport(
			WithTransportBufferSize(o.channelBufferSize),
		)
	}

	// Create new instance of event
	e := &eventImpl[T]{
		status:          1,
		name:            name,
		logger:          o.logger,
		registry:        o.registry,
		metrics:         metrics,
		recoveryEnabled: o.recoveryEnabled,
		tracingEnabled:  o.tracingEnabled,
		subTimeout:      o.subTimeout,
		onError:         o.onError,
		transport:       transport,
	}

	// Add in registry and check if it already exists
	if ev, ok := o.registry.Add(e); !ok {
		if typed, ok := ev.(Event[T]); ok {
			return typed
		}
	}
	return e
}

// Discard create new event which discard all data
func Discard[T any](_ string, _ ...Option) Event[T] {
	return discardEvent[T]{}
}

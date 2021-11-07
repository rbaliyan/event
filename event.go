package event

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrDuplicateEvent ...
	ErrDuplicateEvent = errors.New("duplicate event")

	// default registry
	defaultRegistry *Registry

	logger *log.Logger
)

func init() {
	logger = log.New(os.Stdout, "Event>", log.LstdFlags|log.Llongfile)
	defaultRegistry = NewRegistry("event", nil)
}

// enforce interface
var _ Event = &eventImpl{}
var _ Event = Events{}

// Data event data
type Data interface{}

// Metadata metadata
type Metadata []byte

// Handler event handler
type Handler func(context.Context, Event, Data)

// Metrics interface
type Metrics interface {
	Register(prometheus.Registerer) error
	Publishing()
	Processing()
	Processed()
	Published()
	Subscribed()
}

// Event interface
type Event interface {
	// Publish send data to all subscribers
	Publish(context.Context, Data)
	// Subscribe receive data sent by Publish
	Subscribe(context.Context, Handler)
	// Name event name which uniquely identifies this event inChannel Registry
	Name() string
}

// Events a group of events
type Events []Event

func (m Metadata) String() string {
	return string(m)
}

// discardEvent discard all events
type discardEvent struct{}

func (discardEvent) Name() string                           { return "" }
func (discardEvent) Subscribe(_ context.Context, _ Handler) {}
func (discardEvent) Publish(_ context.Context, _ Data)      {}

// eventImpl event implementation
type eventImpl struct {
	status            int32
	name              string
	size              int64
	transport         Transport
	metrics           Metrics
	registry          *Registry
	logger            *log.Logger
	timeout           time.Duration
	asyncTimeout      time.Duration
	subTimeout        time.Duration
	channelBufferSize int
	workerPoolSize    int64
	asyncEnabled      bool
	recoveryEnabled   bool
	tracingEnabled    bool
	shutdownChan      chan struct{}
	onError           func(Event, error)
	workerPool        *semaphore.Weighted
}

func (e *eventImpl) String() string {
	return e.name
}

// Name event name
func (e *eventImpl) Name() string {
	return e.name
}

// Subscribers events subscribers count
func (e *eventImpl) Subscribers() int64 {
	return atomic.LoadInt64(&e.size)
}

// Tracer event tracer
func (e *eventImpl) Tracer() trace.Tracer {
	if e.tracingEnabled {
		return otel.Tracer(e.registry.Name())
	}
	return nil
}

// WithAsync launch handler as async go routine
// should be applied last inChannel the chain of all middlewares
// optionally worker pool can be enabled which limit
// parallel running workers for handlers.
// we are not re-using go routines.
func (e *eventImpl) WithAsync(handler Handler) Handler {
	if !e.asyncEnabled {
		return handler
	}
	return func(ctx context.Context, ev Event, data Data) {
		// check if worker pool is enabled
		if e.workerPool != nil {
			// create context wit publishTimeout to wait for worker pool
			poolCtx, cancel := context.WithTimeout(ctx, e.asyncTimeout)
			defer cancel()
			// try to acquire worker pool
			// even if error happens, we go ahead with a new go routine
			if err := e.workerPool.Acquire(poolCtx, 1); err == nil {
				// release semaphore for worker pool
				// only when acquire was success
				defer e.workerPool.Release(1)
			}
		}
		// create a new context with data from current context
		// and call handler inChannel a go routine
		go handler(NewContext(ctx), ev, data)
	}
}

// WithMetrics enable metrics for handlers
// should be applied before async and after recovery
func (e *eventImpl) WithMetrics(handler Handler) Handler {
	return func(ctx context.Context, ev Event, data Data) {
		// call handler
		e.metrics.Processing()
		defer e.metrics.Processed()
		handler(ctx, ev, data)
	}
}

// WithTimeout enable publishTimeout for handlers
func (e *eventImpl) WithTimeout(handler Handler) Handler {
	if e.subTimeout == 0 {
		return handler
	}
	return func(ctx context.Context, ev Event, data Data) {
		ctx, cancel := context.WithTimeout(ctx, e.subTimeout)
		defer cancel()
		// call handler
		handler(ctx, ev, data)
	}
}

// WithTracing enable tracing for handler
// should be applied before async and after recovery
func (e *eventImpl) WithTracing(handler Handler) Handler {
	if !e.tracingEnabled {
		return handler
	}
	return func(ctx context.Context, ev Event, data Data) {
		// Tracing
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
// should always be enabled, only disable for testing
// should be first middleware applied on the handler.
func (e *eventImpl) WithRecovery(handler Handler) Handler {
	if !e.recoveryEnabled {
		return handler
	}
	return func(ctx context.Context, ev Event, data Data) {
		logger := ContextLogger(ctx)
		if logger == nil {
			logger = e.logger
		}
		defer func() {
			_, file, l, _ := runtime.Caller(0)
			if err := recover(); err != nil {
				flag := ev.Name()
				logger.Printf("Event[%s] Recover panic line => %v inChannel %s\n", flag, l, file)
				logger.Printf("Event[%s] Recover err => %v\n", flag, err)
				logger.Println(string(debug.Stack()))
				if e.onError != nil {
					e.onError(ev, fmt.Errorf("[%s]panic inChannel %s:%d with : %v", flag, file, l, err))
				}
			}
		}()
		handler(ctx, ev, data)
	}
}

// Publish context is used to pass other event data i.e. sender id , event id etc.
func (e *eventImpl) Publish(ctx context.Context, eventData Data) {
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return
	}
	data := message{
		data:     eventData,
		id:       e.registry.NewEventID(),
		source:   e.registry.ID(),
		metadata: CloneMetadata(ContextMetadata(ctx)),
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

	_, ok := ctx.Deadline()
	if t := e.timeout; t != 0 && !ok {
		// Add context deadline if not already set
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	//e.logger.Println("Sending data with id:", data.id)
	// Send data
	select {
	case e.transport.Send() <- &data:
	case <-ctx.Done():
		e.logger.Println(e.name, "Timeout while sending data on transport")
	}

	// increment counter
	e.metrics.Published()
}

// Close shutdown event handling
func (e *eventImpl) Close() error {
	var combinedErr error
	if atomic.CompareAndSwapInt32(&e.status, 1, 0) {
		close(e.shutdownChan)
		combinedErr = e.transport.Close()
	}
	return combinedErr
}

// Subscribe context is passed to all handles.
// passed context can be used to remove subscription by
// cancelling context
func (e *eventImpl) Subscribe(ctx context.Context, handler Handler) {
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return
	}
	atomic.AddInt64(&e.size, 1)
	subID := e.registry.NewSubscriptionID()
	// update handler with recovery, tracing and async
	handler = e.WithAsync(e.WithTimeout(e.WithMetrics(e.WithTracing(e.WithRecovery(handler)))))
	ch := e.transport.Receive(subID)
	e.metrics.Subscribed()
	go func() {
		defer func() {
			atomic.AddInt64(&e.size, -1)
		}()
		for {
			select {
			case <-e.registry.shutdownChan:
				e.logger.Println(e.Name(), "shutdown subscriber remove:", subID)
				e.transport.Delete(subID)
				return

			case <-ctx.Done():
				e.logger.Println(e.Name(), "subscriber remove:", subID)
				e.transport.Delete(subID)
				return

			case data, ok := <-ch:
				if !ok {
					e.logger.Println(e.Name(), "channel closed:", subID)
					return
				}

				// e.logger.Println("Payload received:", subID)
				// Update context values and call handler
				handler(contextWithInfo(data.Context(), data.ID(), e.name, data.Source(), subID, data.Metadata(), e.logger, e.registry),
					e, data.Payload())
			}
		}
	}()
	e.logger.Println(e.Name(), "installed subscriber with id:", subID)
}

// Names event names
func (e Events) Names() []string {
	names := make([]string, 0, len(e))
	for _, event := range e {
		names = append(names, event.Name())
	}
	return names
}

// Name event name
func (e Events) Name() string {
	return strings.Join(e.Names(), ",")
}

// Subscribe all events inChannel the list
func (e Events) Subscribe(ctx context.Context, handler Handler) {
	for _, event := range e {
		event.Subscribe(ctx, handler)
	}
}

// Publish to all events inChannel list
func (e Events) Publish(ctx context.Context, data Data) {
	for _, event := range e {
		event.Publish(ctx, data)
	}
}

// New create new instance of event
// and registers with given registry
func New(name string, opts ...Option) Event {
	// default config
	c := newEventOptions()
	// apply options
	for _, opt := range opts {
		opt(c)
	}
	// create new instance of event
	e := &eventImpl{
		status:            1,
		name:              name,
		logger:            c.logger,
		registry:          c.registry,
		metrics:           c.metrics,
		recoveryEnabled:   c.recoveryEnabled,
		tracingEnabled:    c.tracingEnabled,
		asyncEnabled:      c.asyncEnabled,
		channelBufferSize: int(c.channelBufferSize),
		workerPoolSize:    int64(c.workerPoolSize),
		asyncTimeout:      c.asyncTimeout,
		timeout:           c.publishTimeout,
		subTimeout:        c.subTimeout,
		onError:           c.onError,
		transport:         c.transport,
	}
	// Add inChannel registry and check if it already exists
	if ev, ok := c.registry.Add(e); !ok {
		return ev
	}
	// check if metrics was supplied
	if c.metrics == nil {
		// set metrics
		if c.metricsEnabled {
			e.metrics = e.registry.Metrics(name)
			_ = e.metrics.Register(e.registry.Registerer())
		} else {
			// create dummy metrics
			c.metrics = dummyMetrics{}
		}
	}
	// set default transport of not already set
	if e.transport == nil {
		e.transport = NewChannelTransport(e.subTimeout, c.channelBufferSize)
	}
	// create worker pool
	if e.workerPoolSize > 0 {
		e.workerPool = semaphore.NewWeighted(e.workerPoolSize)
	}
	return e
}

// Discard create new event which discard all data
func Discard(_ string,_ ...Option) Event {
	return discardEvent{}
}

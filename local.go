package event

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// DefaultNamespaceSep ...
	DefaultNamespaceSep = "/"

	// DefaultPublishTimeout default publish timeout in milliseconds if no timeout specified
	DefaultPublishTimeout = 1000

	// MessageBusSize ...
	MessageBusSize = 100

	// default registry
	defaultRegistry *Registry

	defaultSource string
)

func init() {
	defaultRegistry = NewRegistry()
	defaultSource = NewID()
}

// localDataWrapper ...
type localDataWrapper struct {
	id     string
	source string
	data   Data
	span   trace.Span
	attrs  []attribute.KeyValue
}

// localImpl ...
type localImpl struct {
	published prometheus.Counter
	handled   prometheus.Counter
	name      string
	size      int64
	channels  []chan *localDataWrapper
	mutex     sync.RWMutex
}

func (e *localImpl) String() string {
	return e.name
}

// Name ...
func (e *localImpl) Name() string {
	return e.name
}

func (e *localImpl) Subscribers() int64 {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.size
}

// Default ...
func Local(name string) Event {
	return &localImpl{
		name: name,
		published: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "event",
			Subsystem: name,
			Name:      "published",
			Help:      "Total messages published",
		}),
		handled: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "event",
			Subsystem: name,
			Name:      "handled",
			Help:      "Total messages handled by subscribers",
		}),
	}
}

// NewID generate new event id
func NewID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		return u.String()
	}
	return ""
}

// Publish context is used to pass other event data i.e. sender id , event id etc.
func (e *localImpl) Publish(ctx context.Context, eventData Data) {
	data := &localDataWrapper{
		data: eventData,
	}
	// increment counter
	e.published.Inc()

	// Set event id if not already set
	if data.id = EventIDFromContext(ctx); data.id == "" {
		data.id = NewID()
	}
	// Set sender id
	if data.source = SourceFromContext(ctx); data.source == "" {
		data.source = defaultSource
	}
	// Add tracing
	if tracer := otel.Tracer("event"); tracer != nil {
		data.attrs = AttributesFromContext(ctx)
		data.attrs = append(data.attrs, attribute.String("event.id", data.id))
		ctx, data.span = tracer.Start(ctx, fmt.Sprintf("%s.publish", e.name),
			trace.WithAttributes(data.attrs...),
			trace.WithSpanKind(trace.SpanKindProducer))
		defer data.span.End()
	}
	// Add context deadline if not already set
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Millisecond*time.Duration(DefaultPublishTimeout))
		defer cancel()
	}

	// Send data to all channels
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	for _, ch := range e.channels {
		if ch == nil {
			continue
		}
		select {
		case ch <- data:
		case <-ctx.Done():
		}
	}
}

// wrapRecover a wrapper for recovery
func (e *localImpl) wrapRecover(handler Handler) Handler {
	return func(ctx context.Context, e Event, data Data) {
		defer func() {
			_, _, l, _ := runtime.Caller(1)
			if err := recover(); err != nil {
				flag := e.Name()
				logger.Printf("Event[%s] Recover panic line => %v\n", flag, l)
				logger.Printf("Event[%s] Recover err => %v\n", flag, err)
				debug.PrintStack()
			}
		}()
		handler(ctx, e, data)
	}
}

// Subscribe ctx is passed to all handles, and can be used to remove subscription by
// cancelling context
func (e *localImpl) Subscribe(ctx context.Context, handler Handler) {
	subID := NewID()
	subIDAttribute := attribute.String("subscription.id", subID)
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.size += 1
	handler = e.wrapRecover(handler)
	ch := make(chan *localDataWrapper, MessageBusSize)
	index := len(e.channels)
	// try find unsued slot
	for i := 0; i < len(e.channels); i++ {
		if e.channels[i] == nil {
			index = i
			e.channels[i] = ch
		}
	}
	// append channel
	if index >= len(e.channels) {
		e.channels = append(e.channels, ch)
	}
	closed := false
	go func() {
		defer func() {
			// close channel and remove from list
			e.mutex.Lock()
			if e.channels[index] == ch {
				e.channels[index] = nil
			}
			e.size -= 1
			e.mutex.Unlock()
			if !closed {
				close(ch)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-ch:
				if !ok {
					closed = true
					return
				}
				var span trace.Span
				// Update context values
				ctx = WithSubscriptionID(
					WithSource(
						WithEventID(
							WithEventName(ctx, e.name), data.id), data.source), subID)
				// Tracing
				if tracer := otel.Tracer("event"); tracer != nil && data.span != nil {
					attrs := make([]attribute.KeyValue, 0, len(data.attrs)+1)
					copy(attrs, data.attrs)
					attrs = append(attrs, subIDAttribute)
					ctx, span = tracer.Start(ctx, fmt.Sprintf("%s.subscribe", e.name),
						trace.WithAttributes(attrs...),
						trace.WithSpanKind(trace.SpanKindConsumer),
						trace.WithLinks(trace.Link{
							SpanContext: data.span.SpanContext(),
							Attributes:  attrs,
						}))
				}
				// Call handler
				handler(ctx, e, data.data)
				if span != nil {
					span.End()
				}
				// update counter
				e.handled.Inc()
			}
		}
	}()
}

// Name ...
func Name(names ...string) string {
	name := ""
	if len(names) > 1 {
		name = strings.Join(names, DefaultNamespaceSep)
	}
	return name
}

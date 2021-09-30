package event

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	// ErrDuplicateEvent ...
	ErrDuplicateEvent = errors.New("duplicate event")
	// ErrUnknownEvent ...
	ErrUnknownEvent = errors.New("unknown event")
	// ErrEventDisabled ...
	ErrEventDisabled = errors.New("Event is disabled")

	logger *log.Logger
)

func init() {
	logger = log.New(os.Stdout, "Event>", log.LstdFlags|log.Llongfile)
}

// Data event data
type Data interface{}

// Metadata metadata
type Metadata []byte

// Handler event handler
type Handler func(context.Context, Event, Data)

// Event interface
type Event interface {
	Publish(context.Context, Data)
	Subscribe(context.Context, Handler)
	Name() string
	Subscribers() int64
}

// Registry event registry
type Registry struct {
	events map[string]Event
	sync.Mutex
}

// NewRegistry create a new registry
func NewRegistry() *Registry {
	return &Registry{
		events: make(map[string]Event),
	}
}

// Clone  metadata
func (m Metadata) Clone() Metadata {
	if len(m) == 0 {
		return nil
	}
	m1 := make([]byte, 0, len(m))
	copy(m1, m)
	return m1
}

// Register event by name in registry
func (m *Registry) Register(event Event) error {
	m.Lock()
	defer m.Unlock()
	name := event.Name()
	if _, ok := m.events[name]; ok {
		return ErrDuplicateEvent
	}
	m.events[name] = event
	return nil
}

// Get a new event registered with registry
func (m *Registry) New(name string, fn ...func(string) Event) Event {
	create := Local
	if fn != nil {
		create = fn[0]
	}
	m.Lock()
	defer m.Unlock()
	if obj, ok := m.events[name]; ok {
		return obj
	}
	event := create(name)
	m.events[name] = event
	return event
}

// Get event by name
func (m *Registry) Get(name string) Event {
	m.Lock()
	defer m.Unlock()
	if obj, ok := m.events[name]; ok {
		return obj
	}
	return nil
}

// Event get event by name
func (m *Registry) Event(name string) Event {
	return m.Get(name)
}

// New create new event
func New(name string, fn ...func(string) Event) Event {
	return defaultRegistry.New(name, fn...)
}

// Register event to default registry
func Register(event Event) error {
	return defaultRegistry.Register(event)
}

// Get event by name from default registry
func Get(name string) Event {
	return defaultRegistry.Event(name)
}

// AsyncHandler convert event handler to async
func AsyncHandler(handler Handler, copyContextFns ...func(to, from context.Context) context.Context) Handler {
	return func(ctx context.Context, ev Event, data Data) {
		// Call handler with go routine
		go func() {
			// Create a new copy of context
			attrs := ContextAttributes(ctx)
			spanCtx := trace.SpanContextFromContext(ctx)

			// Create a new context
			newCtx := NewContext(ctx)
			for _, fn := range copyContextFns {
				// Copy other data
				newCtx = fn(newCtx, ctx)
			}
			// enable tracing
			if tracer := otel.Tracer("event"); tracer != nil {
				var span trace.Span
				newCtx, span = tracer.Start(newCtx, fmt.Sprintf("%s.subscribe.async", ev.Name()),
					trace.WithAttributes(attrs...),
					trace.WithSpanKind(trace.SpanKindInternal),
					trace.WithLinks(trace.Link{
						SpanContext: spanCtx,
						Attributes:  attrs,
					}))
				defer span.End()
			}
			handler(newCtx, ev, data)
		}()
	}
}

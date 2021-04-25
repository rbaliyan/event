package event

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
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

// Data ...
type Data interface{}

// Handler ...
type Handler func(context.Context, Event, Data)

// Event ...
type Event interface {
	Publish(context.Context, Data)
	Subscribe(context.Context, Handler)
	Name() string
}

// Registry ...
type Registry struct {
	events map[string]Event
	sync.Mutex
}

// NewRegistry ...
func NewRegistry() *Registry {
	return &Registry{
		events: make(map[string]Event),
	}
}

// Register ...
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

func (m *Registry) New(name string) Event {
	m.Lock()
	defer m.Unlock()
	if obj, ok := m.events[name]; ok {
		return obj
	}
	event := Local(name)
	m.events[name] = event
	return event
}

// Get ...
func (m *Registry) Get(name string) Event {
	m.Lock()
	defer m.Unlock()
	if obj, ok := m.events[name]; ok {
		return obj
	}
	return nil
}

// Event ...
func (m *Registry) Event(name string) Event {
	return m.Get(name)
}

// New create new event
func New(name string) Event {
	return defaultRegistry.New(name)
}

// Register ...
func Register(event Event) error {
	return defaultRegistry.Register(event)
}

// Get ...
func Get(name string) Event {
	return defaultRegistry.Event(name)
}

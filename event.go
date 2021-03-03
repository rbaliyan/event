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
	ErrDuplicateEvent = errors.New("Duplicate event")
	// ErrUnknownEvent ...
	ErrUnknownEvent = errors.New("Unknown event")
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
	Subscribe(context.Context, Handler) int
	Stop(int) error
	Name() string
}

// Manager ...
type Manager struct {
	events map[string]Event
	sync.Mutex
}

// NewManager ...
func NewManager() *Manager {
	return &Manager{
		events: make(map[string]Event),
	}
}

// Register ...
func (m *Manager) Register(event Event) error {
	m.Lock()
	defer m.Unlock()
	name := event.Name()
	if _, ok := m.events[name]; ok {
		return ErrDuplicateEvent
	}
	m.events[name] = event
	return nil
}

// get ...
func (m *Manager) get(name string) Event {
	m.Lock()
	defer m.Unlock()
	if obj, ok := m.events[name]; ok {
		return obj
	}
	return nil
}

// Event ...
func (m *Manager) Event(names ...string) Event {
	return m.get(FullName(names...))
}

// New create new event
func New(names ...string) Event {
	e := Get(names...)
	if e != nil {
		return e
	}
	e = Default(names...)
	Register(e)
	return Get(names...)
}

// Register ...
func Register(event Event) error {
	return defaultManager.Register(event)
}

// Get ...
func Get(names ...string) Event {
	return defaultManager.Event(names...)
}

// Default ...
func Default(names ...string) Event {
	return &eventImpl{name: FullName(names...)}
}

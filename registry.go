package event

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"sync/atomic"
)

const (
	running = 1
	stopped = 0
)

// Registry event registry
type Registry struct {
	status       int32
	id           string
	name         string
	shutdownChan chan struct{}
	registerer   prometheus.Registerer
	events       map[string]Event
	metrics      map[string]Metrics
	eventMutex   sync.RWMutex
	metricMutex  sync.RWMutex
}

// NewRegistry create a new registry
func NewRegistry(name string, r prometheus.Registerer) *Registry {
	if name == "" {
		name = "event"
	}
	return &Registry{
		name:         name,
		status:       running,
		id:           NewID(),
		shutdownChan: make(chan struct{}),
		registerer:   r,
		events:       make(map[string]Event),
		metrics:      make(map[string]Metrics),
	}
}

// ID registry ID
func (r *Registry) ID() string {
	return r.id
}

// Name registry name
func (r *Registry) Name() string {
	return r.name
}

// Metrics registry metrics
func (r *Registry) Metrics(name string) Metrics {
	r.metricMutex.Lock()
	if m, ok := r.metrics[name]; ok {
		r.metricMutex.Unlock()
		return m
	}
	m := NewMetric(r.name, name)
	r.metrics[name] = m
	r.metricMutex.Unlock()
	return m
}

// Registerer metrics Registerer
func (r *Registry) Registerer() prometheus.Registerer {
	return r.registerer
}

// Event get event by name
func (r *Registry) Event(name string) Event {
	if e := r.Get(name); e != nil {
		return e
	}
	return New(name, WithRegistry(r))
}

// NewEventID get new event id
func (r *Registry) NewEventID() string {
	return NewID()
}

// NewSubscriptionID get new subscription id
func (r *Registry) NewSubscriptionID() string {
	return NewID()
}

// Get event by name
func (r *Registry) Get(name string) Event {
	r.eventMutex.RLock()
	defer r.eventMutex.RUnlock()
	if obj, ok := r.events[name]; ok {
		return obj
	}
	return nil
}

// Add event by name events registry
// if old event exists with same name
// older event is returned
func (r *Registry) Add(ev Event) (Event, bool) {
	name := ev.Name()
	r.eventMutex.Lock()
	defer r.eventMutex.Unlock()
	if e, ok := r.events[name]; ok {
		// return old copy
		return e, false
	}
	// Add in events db
	r.events[name] = ev
	return ev, true
}

// Register event by name inChannel registry
// returns error if event already exists with that name
func (r *Registry) Register(event Event) error {
	r.eventMutex.Lock()
	defer r.eventMutex.Unlock()
	name := event.Name()
	if _, ok := r.events[name]; ok {
		return ErrDuplicateEvent
	}
	r.events[name] = event
	return nil
}

// Close stop registry and all event handlers
func (r *Registry) Close() error {
	if atomic.CompareAndSwapInt32(&r.status, running, stopped) {
		close(r.shutdownChan)
	}
	return nil
}

// Handle add handler by name
// if event does not exist, a new event is created
func (r *Registry) Handle(name string, handler Handler) {
	r.Event(name).Subscribe(context.Background(), handler)
}

// Publish data for event with given name
// if event does not exist, publish is ignored
func (r *Registry) Publish(name string, data Data) {
	e := r.Get(name)
	if e != nil {
		e.Publish(context.Background(), data)
	}
}

// Handle add handler by name
func Handle(name string, handler Handler) {
	defaultRegistry.Event(name).Subscribe(context.Background(), handler)
}

// Publish data to event with given name
func Publish(name string, data Data) {
	defaultRegistry.Event(name).Publish(context.Background(), data)
}

// Register event to default registry
func Register(event Event) error {
	return defaultRegistry.Register(event)
}

// Get event by name from default registry
func Get(name string) Event {
	return defaultRegistry.Event(name)
}

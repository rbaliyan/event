package event

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	running = 1
	stopped = 0
)

// registryOptions holds configuration for registry (unexported)
type registryOptions struct {
	name       string
	registerer prometheus.Registerer
}

// RegistryOption option function for registry configuration
type RegistryOption func(*registryOptions)

// WithRegistryName sets the registry name
func WithRegistryName(name string) RegistryOption {
	return func(o *registryOptions) {
		if name != "" {
			o.name = name
		}
	}
}

// WithPrometheusRegisterer sets the prometheus registerer
func WithPrometheusRegisterer(r prometheus.Registerer) RegistryOption {
	return func(o *registryOptions) {
		o.registerer = r
	}
}

// newRegistryOptions creates options with defaults and applies provided options
func newRegistryOptions(opts ...RegistryOption) *registryOptions {
	o := &registryOptions{
		name:       "event",
		registerer: nil,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Registry event registry (unexported fields)
type Registry struct {
	status       int32
	id           string
	name         string
	shutdownChan chan struct{}
	registerer   prometheus.Registerer
	events       map[string]BaseEvent
	metrics      map[string]Metrics
	eventMutex   sync.RWMutex
	metricMutex  sync.RWMutex
}

// NewRegistry create a new registry
func NewRegistry(name string, r prometheus.Registerer, opts ...RegistryOption) *Registry {
	o := newRegistryOptions(opts...)

	// Override with positional args if provided (backward compatibility)
	if name != "" {
		o.name = name
	}
	if r != nil {
		o.registerer = r
	}

	return &Registry{
		name:         o.name,
		status:       running,
		id:           NewID(),
		shutdownChan: make(chan struct{}),
		registerer:   o.registerer,
		events:       make(map[string]BaseEvent),
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

// NewEventID get new event id
func (r *Registry) NewEventID() string {
	return NewID()
}

// NewSubscriptionID get new subscription id
func (r *Registry) NewSubscriptionID() string {
	return NewID()
}

// Get event by name
func (r *Registry) Get(name string) BaseEvent {
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
func (r *Registry) Add(ev BaseEvent) (BaseEvent, bool) {
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

// Register event by name in registry
// returns error if event already exists with that name
func (r *Registry) Register(event BaseEvent) error {
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
		// Close all event transports and wait for them to drain
		r.eventMutex.RLock()
		for _, ev := range r.events {
			ev.Close()
		}
		r.eventMutex.RUnlock()
	}
	return nil
}

// Handle add handler by name using any type
// Note: For type-safe handlers, use New[T]() directly
func Handle[T any](name string, handler Handler[T]) {
	New[T](name, WithRegistry(defaultRegistry)).Subscribe(context.Background(), handler)
}

// Publish data for event with given name using any type
// Note: For type-safe publishing, use New[T]() directly
func Publish[T any](name string, data T) {
	New[T](name, WithRegistry(defaultRegistry)).Publish(context.Background(), data)
}

// Register event to default registry
func Register(event BaseEvent) error {
	return defaultRegistry.Register(event)
}

// Get event by name from default registry
func Get(name string) BaseEvent {
	return defaultRegistry.Get(name)
}

// Package base provides shared utilities for transport implementations.
//
// This package contains common patterns used across channel, Redis, NATS, Kafka,
// and other transport implementations to reduce code duplication.
//
// Key components:
//   - Event: Tracks event-specific state including subscriber counts
//   - EventRegistry: Type-safe concurrent map for event management
//   - HealthCheckBuilder: Fluent builder for health check results
//
// Example usage:
//
//	// Create an event registry for your transport
//	registry := base.NewEventRegistry[*MyEvent]()
//
//	// Register an event
//	event, created := registry.Register("order.created", func() *MyEvent {
//	    return &MyEvent{Name: "order.created"}
//	})
//
//	// Build a health check result
//	result := base.NewHealthCheck().
//	    WithType("redis").
//	    WithEvents(registry.Count()).
//	    Healthy("connected").
//	    Build()
package base

import (
	"sync"
	"sync/atomic"
)

// Event tracks event-specific state including subscriber counts.
// This is a common pattern across transport implementations.
type Event struct {
	Name        string
	Subscribers sync.Map // map[string]Subscription - keyed by subscription ID
	SubCount    int64    // Atomic counter for fast subscriber counting
}

// NewEvent creates a new event tracker.
func NewEvent(name string) *Event {
	return &Event{
		Name: name,
	}
}

// AddSubscriber adds a subscriber and increments the count.
func (e *Event) AddSubscriber(id string, sub any) {
	e.Subscribers.Store(id, sub)
	atomic.AddInt64(&e.SubCount, 1)
}

// RemoveSubscriber removes a subscriber and decrements the count.
// Returns the removed subscriber or nil if not found.
func (e *Event) RemoveSubscriber(id string) any {
	if sub, ok := e.Subscribers.LoadAndDelete(id); ok {
		atomic.AddInt64(&e.SubCount, -1)
		return sub
	}
	return nil
}

// GetSubscriber returns a subscriber by ID.
func (e *Event) GetSubscriber(id string) (any, bool) {
	return e.Subscribers.Load(id)
}

// SubscriberCount returns the current subscriber count.
func (e *Event) SubscriberCount() int64 {
	return atomic.LoadInt64(&e.SubCount)
}

// HasSubscribers returns true if there are any subscribers.
func (e *Event) HasSubscribers() bool {
	return e.SubscriberCount() > 0
}

// RangeSubscribers iterates over all subscribers.
func (e *Event) RangeSubscribers(fn func(id string, sub any) bool) {
	e.Subscribers.Range(func(key, value any) bool {
		return fn(key.(string), value)
	})
}

// EventRegistry provides a typed wrapper around sync.Map for event management.
// It enforces consistent event creation and lookup patterns.
type EventRegistry[E any] struct {
	events sync.Map
}

// NewEventRegistry creates a new typed event registry.
func NewEventRegistry[E any]() *EventRegistry[E] {
	return &EventRegistry[E]{}
}

// Register registers an event if it doesn't exist.
// Returns the event (new or existing) and whether it was newly created.
func (r *EventRegistry[E]) Register(name string, createFn func() E) (E, bool) {
	if existing, ok := r.events.Load(name); ok {
		return existing.(E), false
	}

	event := createFn()
	actual, loaded := r.events.LoadOrStore(name, event)
	return actual.(E), !loaded
}

// Get retrieves an event by name.
func (r *EventRegistry[E]) Get(name string) (E, bool) {
	if val, ok := r.events.Load(name); ok {
		return val.(E), true
	}
	var zero E
	return zero, false
}

// Delete removes an event and returns it.
func (r *EventRegistry[E]) Delete(name string) (E, bool) {
	if val, ok := r.events.LoadAndDelete(name); ok {
		return val.(E), true
	}
	var zero E
	return zero, false
}

// Count returns the number of registered events.
func (r *EventRegistry[E]) Count() int {
	count := 0
	r.events.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// Range iterates over all events.
func (r *EventRegistry[E]) Range(fn func(name string, event E) bool) {
	r.events.Range(func(key, value any) bool {
		return fn(key.(string), value.(E))
	})
}

// TotalSubscribers returns the total subscriber count across all events.
// Events must implement a SubscriberCount() int64 method.
type SubscriberCounter interface {
	SubscriberCount() int64
}

// TotalSubscribers counts all subscribers across events that implement SubscriberCounter.
func (r *EventRegistry[E]) TotalSubscribers() int64 {
	var total int64
	r.events.Range(func(_, value any) bool {
		if counter, ok := value.(SubscriberCounter); ok {
			total += counter.SubscriberCount()
		}
		return true
	})
	return total
}

// Package noop provides a no-operation transport that discards all messages.
//
// This transport is useful for:
//   - Development and testing without external infrastructure
//   - Gradually introducing event-driven code before a broker is available
//   - Disabling event delivery while keeping publish/subscribe code intact
//
// All publish calls succeed but messages are dropped. Subscriptions are valid
// but never receive messages. When you're ready for real event delivery,
// simply swap the transport in your bus initialization.
//
// Usage:
//
//	// Development: events are dropped
//	bus, _ := event.NewBus("mybus",
//	    event.WithTransport(noop.New()),
//	)
//
//	// Production: swap to real transport, no other code changes needed
//	bus, _ := event.NewBus("mybus",
//	    event.WithTransport(redis.New(client)),
//	)
package noop

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
)

// Transport is a no-operation transport that discards all messages.
// It implements the transport.Transport interface but performs no actual
// message delivery - all publishes succeed silently and subscriptions
// never receive messages.
type Transport struct {
	status int32
	events sync.Map // map[string]*event
	logger *slog.Logger
}

// event tracks registered events and their subscriptions
type event struct {
	name        string
	subscribers sync.Map // map[string]*subscription
}

// subscription implements transport.Subscription for noop transport
type subscription struct {
	*base.Subscription
}

// Option configures the noop transport
type Option func(*Transport)

// WithLogger sets the logger for the transport.
func WithLogger(logger *slog.Logger) Option {
	return func(t *Transport) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// New creates a new no-operation transport.
// All messages published to this transport are silently discarded.
func New(opts ...Option) *Transport {
	t := &Transport{
		status: 1,
		logger: transport.Logger("transport>noop"),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent registers an event with the transport.
// This is a no-op but tracks the event for validation purposes.
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	ev := &event{name: name}

	if _, loaded := t.events.LoadOrStore(name, ev); loaded {
		return transport.ErrEventAlreadyExists
	}

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent removes an event from the transport.
// All subscriptions for this event are closed.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	val, ok := t.events.LoadAndDelete(name)
	if !ok {
		return transport.ErrEventNotRegistered
	}

	// Close all subscriptions
	ev := val.(*event)
	ev.subscribers.Range(func(key, value any) bool {
		sub := value.(*subscription)
		sub.Close(ctx)
		return true
	})

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish discards the message and returns nil.
// The message is not delivered to any subscribers.
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.Load(name); !ok {
		return transport.ErrEventNotRegistered
	}

	// Message is silently discarded
	t.logger.Debug("message discarded", "event", name, "msg_id", msg.ID())
	return nil
}

// Subscribe creates a subscription that never receives messages.
// The subscription is valid and can be closed, but no messages will arrive.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	val, ok := t.events.Load(name)
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}

	ev := val.(*event)

	subID := transport.NewID()
	sub := &subscription{
		Subscription: base.NewSubscription(subID, 0, 0), // buffer size 0 - no messages expected
	}

	ev.subscribers.Store(sub.ID(), sub)

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID())
	return sub, nil
}

// Close shuts down the transport and closes all subscriptions.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Close all subscriptions for all events
	t.events.Range(func(key, value any) bool {
		ev := value.(*event)
		ev.subscribers.Range(func(k, v any) bool {
			sub := v.(*subscription)
			sub.Close(ctx)
			return true
		})
		return true
	})

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the transport.
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Count events
	var eventCount int
	t.events.Range(func(key, value any) bool {
		eventCount++
		return true
	})

	result.Status = transport.HealthStatusHealthy
	result.Message = "noop transport is healthy (all messages discarded)"
	result.Latency = time.Since(start)
	result.Details["type"] = "noop"
	result.Details["events"] = eventCount

	return result
}

// subscription methods

func (s *subscription) Close(ctx context.Context) error {
	return s.Subscription.Close(func() error {
		return nil
	})
}

// Compile-time checks
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
	_ transport.Subscription  = (*subscription)(nil)
)

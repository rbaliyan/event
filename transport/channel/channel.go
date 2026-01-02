// Package channel provides an in-memory transport implementation using Go channels.
//
// IMPORTANT: Channel transport is suitable for local pub/sub within a single process.
// It does NOT provide at-least-once delivery guarantees:
//
//   - Messages are lost on process crash or restart
//   - Messages may be dropped if WithTimeout is set and handlers are slow
//   - No persistence or redelivery mechanism
//
// For at-least-once delivery, use Redis, NATS, or Kafka transports instead.
//
// The channel transport is ideal for:
//   - Local event-driven architectures within a single process
//   - Testing and development
//   - Scenarios where message loss is acceptable
package channel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Transport implements transport.Transport using Go channels
type Transport struct {
	status     int32
	events     sync.Map // map[string]*eventChannel
	bufferSize uint
	timeout    int64 // stored as nanoseconds for atomic access
	logger     *slog.Logger
	onError    func(error)

	// Metrics
	meter          metric.Meter
	droppedCounter metric.Int64Counter
}

// eventChannel manages subscribers for a single event
type eventChannel struct {
	name            string
	subscribers     sync.Map // map[string]*subscription
	subCount        int64    // atomic counter
	workerNextIndex int64    // atomic counter for round-robin among workers
	closed          int32
	bufferSize      uint
}

// subscription implements transport.Subscription
type subscription struct {
	id       string
	ch       chan transport.Message
	ev       *eventChannel
	mode     transport.DeliveryMode
	closed   int32
	closedCh chan struct{}
}

func (s *subscription) ID() string {
	return s.id
}

func (s *subscription) Messages() <-chan transport.Message {
	return s.ch
}

func (s *subscription) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.closedCh)
		// Remove from event's subscriber list
		if s.ev != nil {
			s.ev.subscribers.Delete(s.id)
			atomic.AddInt64(&s.ev.subCount, -1)
		}
		// Close the channel
		close(s.ch)
	}
	return nil
}

// New creates a new channel-based transport.
func New(opts ...Option) *Transport {
	o := newOptions(opts...)

	// Initialize OTel meter and metrics
	meter := otel.Meter("event.transport.channel")
	droppedCounter, _ := meter.Int64Counter("event.transport.channel.dropped",
		metric.WithDescription("Number of messages dropped by channel transport"),
		metric.WithUnit("{message}"),
	)

	return &Transport{
		status:         1,
		bufferSize:     o.bufferSize,
		timeout:        int64(o.timeout),
		logger:         o.logger,
		onError:        o.onError,
		meter:          meter,
		droppedCounter: droppedCounter,
	}
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent creates resources for an event
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	ec := &eventChannel{
		name:       name,
		bufferSize: t.bufferSize,
	}

	if _, loaded := t.events.LoadOrStore(name, ec); loaded {
		return transport.ErrEventAlreadyExists
	}

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent cleans up event resources and closes all subscriptions
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	val, ok := t.events.LoadAndDelete(name)
	if !ok {
		return transport.ErrEventNotRegistered
	}

	ec := val.(*eventChannel)
	atomic.StoreInt32(&ec.closed, 1)

	// Close all subscriptions for this event
	ec.subscribers.Range(func(key, value any) bool {
		sub := value.(*subscription)
		sub.Close(ctx)
		return true
	})

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish sends a message to an event's subscribers
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	val, ok := t.events.Load(name)
	if !ok {
		return transport.ErrEventNotRegistered
	}

	ec := val.(*eventChannel)
	if atomic.LoadInt32(&ec.closed) == 1 {
		return transport.ErrEventNotRegistered
	}

	// Count subscribers
	subCount := atomic.LoadInt64(&ec.subCount)
	if subCount == 0 {
		// No subscribers - drop message silently
		t.logger.Debug("dropping message, no subscribers", "event", name, "msg_id", msg.ID())
		// Record metric for dropped message
		if t.droppedCounter != nil {
			t.droppedCounter.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("event", name),
					attribute.String("reason", "no_subscribers"),
				))
		}
		return nil
	}

	// Collect subscribers by mode
	var broadcastSubs []*subscription
	var workerSubs []*subscription

	ec.subscribers.Range(func(key, value any) bool {
		sub := value.(*subscription)
		if atomic.LoadInt32(&sub.closed) == 0 {
			if sub.mode == transport.WorkerPool {
				workerSubs = append(workerSubs, sub)
			} else {
				broadcastSubs = append(broadcastSubs, sub)
			}
		}
		return true
	})

	// Send to ALL broadcast subscribers
	// NOTE: Channel transport does NOT guarantee delivery. If a subscriber is slow
	// and timeout is configured, the message will be dropped for that subscriber.
	// Use Redis/NATS/Kafka transports for at-least-once delivery guarantees.
	for _, sub := range broadcastSubs {
		if err := t.sendToSubscriber(ctx, sub, msg); err != nil {
			if errors.Is(err, transport.ErrPublishTimeout) {
				t.logger.Debug("broadcast message dropped due to timeout (subscriber too slow)",
					"event", ec.name,
					"subscriber", sub.id,
					"msg_id", msg.ID())
				// Record metric for dropped message
				if t.droppedCounter != nil {
					t.droppedCounter.Add(ctx, 1,
						metric.WithAttributes(
							attribute.String("event", ec.name),
							attribute.String("reason", "timeout"),
							attribute.String("mode", "broadcast"),
						))
				}
			} else {
				t.logger.Debug("failed to send to broadcast subscriber",
					"event", ec.name,
					"subscriber", sub.id,
					"error", err)
			}
			t.onError(err)
		}
	}

	// Send to ONE worker pool subscriber (round-robin with retry)
	if len(workerSubs) > 0 {
		startIdx := atomic.AddInt64(&ec.workerNextIndex, 1)
		numWorkers := int64(len(workerSubs))
		var lastErr error

		// Try each worker in round-robin order until one succeeds
		for i := range numWorkers {
			idx := (startIdx + i) % numWorkers
			sub := workerSubs[idx]
			if err := t.sendToSubscriber(ctx, sub, msg); err != nil {
				t.logger.Debug("failed to send to worker subscriber, trying next",
					"event", ec.name,
					"subscriber", sub.id,
					"error", err,
					"attempt", i+1,
					"total_workers", numWorkers)
				lastErr = err
				continue // Try next worker
			}
			return nil // Success
		}

		// All workers failed - message will be dropped
		t.logger.Warn("all worker pool subscribers failed, message dropped",
			"event", ec.name,
			"msg_id", msg.ID(),
			"workers_tried", numWorkers,
			"last_error", lastErr)
		// Record metric for dropped message
		if t.droppedCounter != nil {
			t.droppedCounter.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("event", ec.name),
					attribute.String("reason", "all_workers_failed"),
					attribute.String("mode", "worker_pool"),
				))
		}
		t.onError(lastErr)
		return lastErr
	}

	return nil
}

func (t *Transport) sendToSubscriber(ctx context.Context, sub *subscription, msg transport.Message) error {
	// Use timeout if configured
	timeout := atomic.LoadInt64(&t.timeout)
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout))
		defer cancel()

		select {
		case <-ctx.Done():
			return transport.ErrPublishTimeout
		case <-sub.closedCh:
			return transport.ErrSubscriptionClosed
		case sub.ch <- msg:
			return nil
		}
	}

	// No timeout - try non-blocking first, then block
	select {
	case <-sub.closedCh:
		return transport.ErrSubscriptionClosed
	case sub.ch <- msg:
		return nil
	default:
		// Channel full - block until ready (blocking mode)
		select {
		case <-sub.closedCh:
			return transport.ErrSubscriptionClosed
		case sub.ch <- msg:
			return nil
		}
	}
}

// Subscribe creates a subscription to receive messages for an event
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	subOpts := transport.ApplySubscribeOptions(opts...)

	val, ok := t.events.Load(name)
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}

	ec := val.(*eventChannel)
	if atomic.LoadInt32(&ec.closed) == 1 {
		return nil, transport.ErrEventNotRegistered
	}

	bufSize := ec.bufferSize
	if subOpts.BufferSize > 0 {
		bufSize = uint(subOpts.BufferSize)
	}

	sub := &subscription{
		id:       transport.NewID(),
		ch:       make(chan transport.Message, bufSize),
		ev:       ec,
		mode:     subOpts.DeliveryMode,
		closedCh: make(chan struct{}),
	}

	ec.subscribers.Store(sub.id, sub)
	atomic.AddInt64(&ec.subCount, 1)

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.id, "mode", subOpts.DeliveryMode)
	return sub, nil
}

// Close shuts down the transport and all events
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil // Already closed
	}

	// Close all events
	t.events.Range(func(key, value any) bool {
		ec := value.(*eventChannel)
		atomic.StoreInt32(&ec.closed, 1)

		// Close all subscriptions
		ec.subscribers.Range(func(k, v any) bool {
			sub := v.(*subscription)
			sub.Close(ctx)
			return true
		})
		return true
	})

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the channel transport
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	// Check if transport is open
	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Count events and subscribers
	var eventCount int
	var totalSubscribers int64
	t.events.Range(func(key, value any) bool {
		eventCount++
		ec := value.(*eventChannel)
		totalSubscribers += atomic.LoadInt64(&ec.subCount)
		return true
	})

	result.Status = transport.HealthStatusHealthy
	result.Message = "channel transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "channel"
	result.Details["events"] = eventCount
	result.Details["subscribers"] = totalSubscribers
	result.Details["buffer_size"] = t.bufferSize

	return result
}

// Compile-time interface checks
var _ transport.Transport = (*Transport)(nil)
var _ transport.HealthChecker = (*Transport)(nil)
var _ transport.Subscription = (*subscription)(nil)

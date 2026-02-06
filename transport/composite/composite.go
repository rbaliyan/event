// Package composite provides a transport that combines a durable store with
// a real-time signal transport for low-latency, reliable event delivery.
//
// Storage systems (MongoDB, PostgreSQL) provide excellent durability but poor
// real-time notification. Real-time systems (Redis, NATS) provide instant
// delivery but poor persistence. The composite transport combines both:
//
//   - Publish writes to the durable store first (source of truth), then sends
//     a lightweight signal via the real-time transport
//   - Subscribe waits for signals for fast delivery, with a poll fallback
//     for reliability when signals are lost
//
// Signal failures are non-fatal: the message is already in the durable store
// and will be delivered via the poll fallback.
//
// Usage:
//
//	store := persistent.NewMemoryStore() // or MongoStore, PostgresStore
//	signal := redis.New(redisClient)     // or nats.New, channel.New
//
//	t, _ := composite.New(store, signal,
//	    composite.WithPollInterval(5*time.Second),
//	    composite.WithCheckpointStore(cpStore),
//	)
//
//	bus, _ := event.NewBus("mybus", event.WithTransport(t))
package composite

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/persistent"
)

// Errors
var (
	ErrStoreRequired  = errors.New("durable store is required")
	ErrSignalRequired = errors.New("signal transport is required")
)

// Transport combines a durable store with a real-time signal transport.
//
// On publish, messages are written to the durable store first, then a
// lightweight signal is sent via the signal transport. On subscribe,
// the subscription waits for signals (fast path) or polls the store
// (fallback path) to fetch and deliver messages.
type Transport struct {
	status          int32
	store           persistent.Store
	checkpointStore persistent.CheckpointStore
	signal          transport.Transport
	codec           codec.Codec
	logger          *slog.Logger
	onError         func(error)
	signalPrefix    string
	pollInterval    time.Duration
	bufferSize      int
	events          sync.Map // map[string]*compositeEvent
}

// compositeEvent tracks per-event state.
type compositeEvent struct {
	name     string
	subs     sync.Map // map[string]*subscription
	subCount int64
}

// New creates a composite transport backed by a durable store and
// a real-time signal transport.
//
// The store persists all messages and provides at-least-once delivery.
// The signal transport sends lightweight wake-up notifications so
// subscribers can fetch messages without polling delay.
func New(store persistent.Store, signal transport.Transport, opts ...Option) (*Transport, error) {
	if store == nil {
		return nil, ErrStoreRequired
	}
	if signal == nil {
		return nil, ErrSignalRequired
	}

	t := &Transport{
		status:       1,
		store:        store,
		signal:       signal,
		codec:        codec.Default(),
		logger:       transport.Logger("transport>composite"),
		onError:      func(error) {},
		signalPrefix: "_sig:",
		pollInterval: 5 * time.Second,
		bufferSize:   1,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

func (t *Transport) signalName(name string) string {
	return t.signalPrefix + name
}

// RegisterEvent creates resources for an event in both the durable store
// and the signal transport.
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	ev := &compositeEvent{name: name}
	if _, loaded := t.events.LoadOrStore(name, ev); loaded {
		return transport.ErrEventAlreadyExists
	}

	// Register signal event (best-effort: failure degrades to poll-only)
	signalName := t.signalName(name)
	if err := t.signal.RegisterEvent(ctx, signalName); err != nil {
		t.logger.Warn("failed to register signal event, subscribers will poll",
			"event", name, "error", err)
	}

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent cleans up event resources and closes all subscriptions.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	val, ok := t.events.LoadAndDelete(name)
	if !ok {
		return transport.ErrEventNotRegistered
	}

	ev := val.(*compositeEvent)
	ev.subs.Range(func(key, value any) bool {
		sub := value.(*subscription)
		_ = sub.Close(ctx)
		return true
	})

	// Unregister signal event (best-effort)
	signalName := t.signalName(name)
	if err := t.signal.UnregisterEvent(ctx, signalName); err != nil {
		t.logger.Warn("failed to unregister signal event",
			"event", name, "error", err)
	}

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish writes a message to the durable store and sends a signal.
//
// The durable write must succeed for Publish to return nil. Signal failure
// is non-fatal: the message is already persisted and will be delivered
// via the poll fallback.
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.Load(name); !ok {
		return transport.ErrEventNotRegistered
	}

	// Encode message
	data, err := t.codec.Encode(msg)
	if err != nil {
		return err
	}

	// Write to durable store (MUST succeed)
	seqID, err := t.store.Append(ctx, name, data)
	if err != nil {
		t.onError(err)
		return err
	}

	// Send signal (best-effort)
	signalMsg := transport.NewMessageWithAck(
		msg.ID(),
		"composite-signal",
		nil, // No payload: subscriber fetches from store
		map[string]string{"event": name, "seq_id": seqID},
		0,
		func(error) error { return nil },
	)

	signalName := t.signalName(name)
	if err := t.signal.Publish(ctx, signalName, signalMsg); err != nil {
		t.logger.Warn("signal publish failed, subscribers will poll",
			"event", name, "msg_id", msg.ID(), "error", err)
		t.onError(err)
		// DO NOT return error: message is durable, contract is met
	}

	t.logger.Debug("published message",
		"event", name, "msg_id", msg.ID(), "seq_id", seqID)
	return nil
}

// Subscribe creates a subscription that reads from the durable store,
// triggered by real-time signals with poll fallback.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	val, ok := t.events.Load(name)
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}

	ev := val.(*compositeEvent)
	subOpts := transport.ApplySubscribeOptions(opts...)

	bufSize := t.bufferSize
	if subOpts.BufferSize > 0 {
		bufSize = subOpts.BufferSize
	}

	sub := newSubscription(subscriptionConfig{
		ev:              ev,
		eventName:       name,
		consumerID:      subOpts.ConsumerID,
		bufferSize:      bufSize,
		store:           t.store,
		checkpointStore: t.checkpointStore,
		codec:           t.codec,
		pollInterval:    t.pollInterval,
		logger:          t.logger,
	})

	// Load checkpoint if store available
	if t.checkpointStore != nil {
		checkpoint, err := t.checkpointStore.Load(ctx, name, sub.consumerID)
		if err != nil {
			t.logger.Warn("failed to load checkpoint",
				"event", name, "error", err)
		} else {
			sub.setCheckpoint(checkpoint)
		}
	}

	// Subscribe to signal transport for real-time wake-ups
	signalName := t.signalName(name)
	signalSub, err := t.signal.Subscribe(ctx, signalName,
		transport.WithDeliveryMode(subOpts.DeliveryMode),
		transport.WithWorkerGroup(subOpts.WorkerGroup),
		transport.WithStartFrom(transport.StartFromLatest),
	)
	if err != nil {
		t.logger.Warn("signal subscription failed, using poll-only mode",
			"event", name, "error", err)
		// Continue without signal: poll-only fallback
	}

	// Track subscription
	ev.subs.Store(sub.ID(), sub)
	atomic.AddInt64(&ev.subCount, 1)

	// Start background goroutines
	sub.start(ctx, signalSub)

	t.logger.Debug("added subscriber",
		"event", name,
		"subscriber", sub.ID(),
		"checkpoint", sub.getCheckpoint(),
		"signal", signalSub != nil)
	return sub, nil
}

// Close shuts down the transport and all subscriptions.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Close all subscriptions
	t.events.Range(func(key, value any) bool {
		ev := value.(*compositeEvent)
		ev.subs.Range(func(k, v any) bool {
			sub := v.(*subscription)
			_ = sub.Close(ctx)
			return true
		})
		return true
	})

	// Close signal transport
	if err := t.signal.Close(ctx); err != nil {
		t.logger.Warn("failed to close signal transport", "error", err)
	}

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the composite transport.
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt:  start,
		Details:    make(map[string]any),
		Components: make(map[string]*transport.HealthCheckResult),
	}

	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	result.Status = transport.HealthStatusHealthy
	result.Message = "composite transport is healthy"

	// Check store health (if it implements HealthChecker)
	if checker, ok := t.store.(transport.HealthChecker); ok {
		storeHealth := checker.Health(ctx)
		result.Components["store"] = storeHealth
		if storeHealth.Status == transport.HealthStatusUnhealthy {
			result.Status = transport.HealthStatusUnhealthy
			result.Message = "durable store unhealthy"
		}
	}

	// Check signal transport health
	if checker, ok := t.signal.(transport.HealthChecker); ok {
		signalHealth := checker.Health(ctx)
		result.Components["signal"] = signalHealth
		if signalHealth.Status == transport.HealthStatusUnhealthy {
			// Only degrade if store is still healthy
			if result.Status == transport.HealthStatusHealthy {
				result.Status = transport.HealthStatusDegraded
				result.Message = "signal transport unhealthy, using poll fallback"
			}
		}
	}

	// Count events and subscribers
	var eventCount int
	var totalSubs int64
	t.events.Range(func(key, value any) bool {
		eventCount++
		ev := value.(*compositeEvent)
		totalSubs += atomic.LoadInt64(&ev.subCount)
		return true
	})

	result.Latency = time.Since(start)
	result.Details["type"] = "composite"
	result.Details["events"] = eventCount
	result.Details["subscribers"] = totalSubs
	result.Details["poll_interval"] = t.pollInterval.String()

	return result
}

// Compile-time checks
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
)

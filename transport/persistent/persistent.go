// Package persistent provides a store-backed transport with at-least-once delivery.
//
// Unlike the channel transport (in-memory, at-most-once), this transport:
//   - Persists all published events to a store
//   - Delivers messages sequentially with acknowledgment
//   - Supports resume after restart via checkpoints
//   - Provides natural backpressure (poll-based)
//
// This is useful when you need persistence without external infrastructure
// like Redis or Kafka, using any storage backend (MongoDB, PostgreSQL, memory).
//
// Usage:
//
//	store := persistent.NewMemoryStore() // or NewMongoStore, NewPostgresStore
//	t := persistent.New(store)
//
//	bus, _ := event.NewBus("mybus", event.WithTransport(t))
//
//	// Publishing writes to store
//	orderEvent.Publish(ctx, order)
//
//	// Subscribing polls store and processes sequentially
//	orderEvent.Subscribe(ctx, handler)
package persistent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// Errors
var (
	ErrStoreRequired = errors.New("store is required")
)

// Store defines the interface for persistent event storage.
// Implementations must be safe for concurrent use.
type Store interface {
	// Append adds a message to the store for the given event.
	// Returns a unique sequence ID for the stored message.
	Append(ctx context.Context, eventName string, data []byte) (string, error)

	// Fetch retrieves the next unprocessed message after the given checkpoint.
	// Returns nil if no messages are available.
	// The checkpoint is typically the last processed sequence ID.
	Fetch(ctx context.Context, eventName string, checkpoint string) (*StoredMessage, error)

	// Ack acknowledges a message as successfully processed.
	Ack(ctx context.Context, eventName string, sequenceID string) error

	// Nack marks a message for redelivery (processing failed).
	Nack(ctx context.Context, eventName string, sequenceID string) error
}

// CheckpointStore persists consumer checkpoints for resume after restart.
type CheckpointStore interface {
	// Load retrieves the checkpoint for a consumer.
	Load(ctx context.Context, eventName, consumerID string) (string, error)

	// Save persists the checkpoint for a consumer.
	Save(ctx context.Context, eventName, consumerID string, checkpoint string) error
}

// StoredMessage represents a message retrieved from the store.
type StoredMessage struct {
	SequenceID string    // Unique sequence identifier
	Data       []byte    // Encoded message data
	Timestamp  time.Time // When the message was stored
	RetryCount int       // Number of delivery attempts
}

// Transport implements transport.Transport with store-backed persistence.
type Transport struct {
	status          int32
	store           Store
	checkpointStore CheckpointStore
	codec           codec.Codec
	events          sync.Map // map[string]*event
	logger          *slog.Logger
	onError         func(error)
	pollInterval    time.Duration
	bufferSize      int
}

// event tracks event-specific state
type event struct {
	name        string
	subscribers sync.Map // map[string]*subscription
	subCount    int64
}

// subscription implements transport.Subscription
type subscription struct {
	*base.Subscription
	ev              *event // Parent event for cleanup on close
	eventName       string
	consumerID      string
	store           Store
	checkpointStore CheckpointStore
	codec           codec.Codec
	pollInterval    time.Duration
	checkpoint      string // Last processed sequence ID
	checkpointMu    sync.RWMutex
	cancel          context.CancelFunc
}

// Option configures the persistent transport
type Option func(*Transport)

// WithCheckpointStore sets the checkpoint store for resume capability.
func WithCheckpointStore(store CheckpointStore) Option {
	return func(t *Transport) {
		t.checkpointStore = store
	}
}

// WithCodec sets the codec for message serialization.
func WithCodec(c codec.Codec) Option {
	return func(t *Transport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(t *Transport) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// WithErrorHandler sets the error callback.
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithPollInterval sets the interval for polling the store.
func WithPollInterval(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.pollInterval = d
		}
	}
}

// WithBufferSize sets the subscription buffer size.
func WithBufferSize(size int) Option {
	return func(t *Transport) {
		if size > 0 {
			t.bufferSize = size
		}
	}
}

// New creates a new persistent transport.
func New(store Store, opts ...Option) (*Transport, error) {
	if store == nil {
		return nil, ErrStoreRequired
	}

	t := &Transport{
		status:       1,
		store:        store,
		codec:        codec.Default(),
		logger:       transport.Logger("transport>persistent"),
		onError:      func(error) {},
		pollInterval: 100 * time.Millisecond,
		bufferSize:   1, // Sequential by default - no buffering
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent creates resources for an event.
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

// UnregisterEvent cleans up event resources.
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

// Publish writes a message to the store.
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

	// Append to store
	seqID, err := t.store.Append(ctx, name, data)
	if err != nil {
		t.onError(err)
		return err
	}

	t.logger.Debug("published message", "event", name, "msg_id", msg.ID(), "seq_id", seqID)
	return nil
}

// Subscribe creates a subscription that polls the store for messages.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	subOpts := transport.ApplySubscribeOptions(opts...)

	val, ok := t.events.Load(name)
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}

	ev := val.(*event)

	bufSize := t.bufferSize
	if subOpts.BufferSize > 0 {
		bufSize = subOpts.BufferSize
	}

	subID := transport.NewID()
	consumerID := subOpts.ConsumerID
	if consumerID == "" {
		consumerID = subID
	}
	subCtx, cancel := context.WithCancel(ctx)

	sub := &subscription{
		Subscription:    base.NewSubscription(subID, bufSize, 0),
		ev:              ev,
		eventName:       name,
		consumerID:      consumerID,
		store:           t.store,
		checkpointStore: t.checkpointStore,
		codec:           t.codec,
		pollInterval:    t.pollInterval,
		cancel:          cancel,
	}

	// Load checkpoint if store available
	if t.checkpointStore != nil {
		checkpoint, err := t.checkpointStore.Load(ctx, name, consumerID)
		if err != nil {
			t.logger.Warn("failed to load checkpoint", "error", err)
		} else {
			sub.setCheckpoint(checkpoint)
		}
	}

	ev.subscribers.Store(sub.ID(), sub)
	atomic.AddInt64(&ev.subCount, 1)

	// Start polling in background
	sub.WaitGroup().Add(1)
	go func() {
		defer sub.WaitGroup().Done()
		sub.pollLoop(subCtx, t.logger)
	}()

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID(), "checkpoint", sub.getCheckpoint())
	return sub, nil
}

// Close shuts down the transport.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Close all subscriptions
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

// Health performs a health check.
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

	// Count events and subscribers
	var eventCount int
	var totalSubs int64
	t.events.Range(func(key, value any) bool {
		eventCount++
		ev := value.(*event)
		totalSubs += atomic.LoadInt64(&ev.subCount)
		return true
	})

	result.Status = transport.HealthStatusHealthy
	result.Message = "persistent transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "persistent"
	result.Details["events"] = eventCount
	result.Details["subscribers"] = totalSubs

	return result
}

// subscription methods

func (s *subscription) getCheckpoint() string {
	s.checkpointMu.RLock()
	defer s.checkpointMu.RUnlock()
	return s.checkpoint
}

func (s *subscription) setCheckpoint(cp string) {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.checkpoint = cp
}

func (s *subscription) Close(ctx context.Context) error {
	return s.Subscription.Close(func() error {
		if s.ev != nil {
			s.ev.subscribers.Delete(s.ID())
			atomic.AddInt64(&s.ev.subCount, -1)
		}
		if s.cancel != nil {
			s.cancel()
		}
		return nil
	})
}

// pollLoop continuously polls the store for new messages.
func (s *subscription) pollLoop(ctx context.Context, logger *slog.Logger) {
	backoff := base.NewBackoffWithConfig(s.pollInterval, 5*time.Second, 0.1)

	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		default:
		}

		// Fetch next message
		stored, err := s.store.Fetch(ctx, s.eventName, s.getCheckpoint())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("fetch error", "error", err)
			backoff.Wait(s.ClosedCh())
			continue
		}

		if stored == nil {
			// No messages - wait and poll again
			select {
			case <-s.ClosedCh():
				return
			case <-ctx.Done():
				return
			case <-time.After(backoff.Next()):
			}
			continue
		}

		// Reset backoff on successful fetch
		backoff.Reset()

		// Decode message
		decoded, err := s.codec.Decode(stored.Data)
		if err != nil {
			logger.Error("decode error", "error", err, "seq_id", stored.SequenceID)
			// Ack to skip bad message (or could nack for retry)
			if ackErr := s.store.Ack(ctx, s.eventName, stored.SequenceID); ackErr != nil {
				logger.Error("ack error", "error", ackErr, "seq_id", stored.SequenceID)
			}
			s.setCheckpoint(stored.SequenceID)
			continue
		}

		// Wrap with ack function
		seqID := stored.SequenceID
		wrappedMsg := transport.NewMessageWithAck(
			decoded.ID(),
			decoded.Source(),
			decoded.Payload(),
			decoded.Metadata(),
			stored.RetryCount,
			func(err error) error {
				if err == nil {
					// Success - ack and advance checkpoint
					if ackErr := s.store.Ack(ctx, s.eventName, seqID); ackErr != nil {
						return ackErr
					}
					s.setCheckpoint(seqID)
					// Persist checkpoint
					if s.checkpointStore != nil {
						if saveErr := s.checkpointStore.Save(ctx, s.eventName, s.consumerID, seqID); saveErr != nil {
							logger.Error("checkpoint save error", "error", saveErr, "seq_id", seqID)
						}
					}
					return nil
				}
				// Failure - nack for redelivery
				return s.store.Nack(ctx, s.eventName, seqID)
			},
		)

		// Send to handler - BLOCKING (sequential processing)
		// This is the key difference from channel transport
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		case s.Ch() <- wrappedMsg:
			// Message sent to handler, will be acked/nacked when handler returns
		}
	}
}

// Compile-time checks
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
	_ transport.Subscription  = (*subscription)(nil)
)

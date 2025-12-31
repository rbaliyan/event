// NATS Core transport implementation.
//
// This file provides a simple NATS Core pub/sub transport with at-most-once
// delivery semantics. For at-least-once delivery with persistence, use
// NewJetStream() instead.
//
// NATS Core is suitable for:
//   - Ephemeral events where message loss is acceptable
//   - Low-latency requirements
//   - Simple pub/sub without persistence
//
// For reliability features (deduplication, DLQ, poison detection), inject
// optional stores via WithIdempotencyStore(), WithDLQHandler(), etc.
package nats

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
	"go.opentelemetry.io/otel/trace"
)

// CoreTransport implements transport.Transport using NATS Core pub/sub.
//
// This provides at-most-once delivery. Messages are not persisted and will
// be lost if no subscribers are connected when published.
//
// For at-least-once delivery, use JetStreamTransport via NewJetStream().
type CoreTransport struct {
	status  int32
	conn    *nats.Conn
	codec   codec.Codec
	logger  *slog.Logger
	onError func(error)

	// Optional reliability stores (library-level features)
	idempotencyStore IdempotencyStore
	dlqHandler       DLQHandler
	poisonDetector   PoisonDetector

	// Subscription management
	events sync.Map // map[string]*coreEvent
}

// coreEvent tracks event-specific state for NATS Core
type coreEvent struct {
	name   string
	subIdx int64
}

// coreSubscription implements transport.Subscription for NATS Core
type coreSubscription struct {
	id       string
	ch       chan transport.Message
	closedCh chan struct{}
	closed   int32
	sub      *nats.Subscription
	codec    codec.Codec
	wg       sync.WaitGroup

	// Optional reliability features
	idempotencyStore IdempotencyStore
	dlqHandler       DLQHandler
	poisonDetector   PoisonDetector
}

// CoreOption configures the NATS Core transport
type CoreOption func(*CoreTransport)

// New creates a new NATS Core transport.
//
// NATS Core provides simple pub/sub with at-most-once delivery.
// Messages are not persisted - if no subscribers are connected,
// messages are dropped.
//
// For reliability features, inject optional stores:
//
//	transport := nats.New(conn,
//	    nats.WithIdempotencyStore(store),  // Deduplication
//	    nats.WithDLQHandler(handler),       // Dead letter handling
//	    nats.WithPoisonDetector(detector),  // Poison message detection
//	)
//
// For at-least-once delivery with persistence, use NewJetStream() instead.
func New(conn *nats.Conn, opts ...CoreOption) (*CoreTransport, error) {
	if conn == nil {
		return nil, ErrConnRequired
	}

	t := &CoreTransport{
		status:  1,
		conn:    conn,
		codec:   codec.Default(),
		logger:  transport.Logger("transport>nats"),
		onError: func(error) {},
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

// WithCoreCodec sets the codec for message serialization
func WithCoreCodec(c codec.Codec) CoreOption {
	return func(t *CoreTransport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithCoreLogger sets the logger
func WithCoreLogger(l *slog.Logger) CoreOption {
	return func(t *CoreTransport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithCoreErrorHandler sets the error handler callback
func WithCoreErrorHandler(fn func(error)) CoreOption {
	return func(t *CoreTransport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// =============================================================================
// Optional Reliability Stores (Library-Level Features)
// =============================================================================
// Since NATS Core doesn't provide native reliability features, these stores
// can be injected to add deduplication, DLQ, and poison detection.

// IdempotencyStore checks for duplicate messages.
type IdempotencyStore interface {
	IsDuplicate(ctx context.Context, messageID string) (bool, error)
	MarkProcessed(ctx context.Context, messageID string) error
}

// DLQHandler is called when a message fails processing.
type DLQHandler func(ctx context.Context, eventName string, msgID string, payload []byte, err error) error

// PoisonDetector tracks and quarantines repeatedly failing messages.
type PoisonDetector interface {
	Check(ctx context.Context, messageID string) (bool, error)
	RecordFailure(ctx context.Context, messageID string) (bool, error)
	RecordSuccess(ctx context.Context, messageID string) error
}

// WithIdempotencyStore sets a store for deduplication.
//
// When set, the transport will check each incoming message against the store
// and skip duplicates. This provides library-level exactly-once semantics.
//
// Example:
//
//	store := idempotency.NewRedisStore(redisClient, time.Hour)
//	transport := nats.New(conn, nats.WithIdempotencyStore(store))
func WithIdempotencyStore(store IdempotencyStore) CoreOption {
	return func(t *CoreTransport) {
		t.idempotencyStore = store
	}
}

// WithDLQHandler sets a handler for failed messages.
//
// When set, messages that fail processing will be passed to this handler
// for dead letter queue storage.
//
// Example:
//
//	handler := func(ctx context.Context, event, msgID string, payload []byte, err error) error {
//	    return dlqStore.Store(ctx, event, msgID, payload, nil, err, 0, "nats-core")
//	}
//	transport := nats.New(conn, nats.WithDLQHandler(handler))
func WithDLQHandler(handler DLQHandler) CoreOption {
	return func(t *CoreTransport) {
		t.dlqHandler = handler
	}
}

// WithPoisonDetector sets a detector for poison messages.
//
// When set, the transport will check if messages are quarantined before
// delivery and track failures for poison detection.
//
// Example:
//
//	detector := poison.NewDetector(store, poison.WithThreshold(5))
//	transport := nats.New(conn, nats.WithPoisonDetector(detector))
func WithPoisonDetector(detector PoisonDetector) CoreOption {
	return func(t *CoreTransport) {
		t.poisonDetector = detector
	}
}

// =============================================================================
// Transport Interface Implementation
// =============================================================================

func (t *CoreTransport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent creates resources for an event
func (t *CoreTransport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	ev := &coreEvent{name: name}

	if _, loaded := t.events.LoadOrStore(name, ev); loaded {
		return transport.ErrEventAlreadyExists
	}

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent cleans up event resources
func (t *CoreTransport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.LoadAndDelete(name); !ok {
		return transport.ErrEventNotRegistered
	}

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish sends a message to an event's subscribers
func (t *CoreTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
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

	// Publish to NATS (fire-and-forget)
	if err := t.conn.Publish(name, data); err != nil {
		t.onError(err)
		return err
	}

	t.logger.Debug("published message", "event", name, "msg_id", msg.ID())
	return nil
}

// Subscribe creates a subscription to receive messages for an event
func (t *CoreTransport) Subscribe(ctx context.Context, name string, mode transport.DeliveryMode) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	if _, ok := t.events.Load(name); !ok {
		return nil, transport.ErrEventNotRegistered
	}

	sub := &coreSubscription{
		id:               transport.NewID(),
		ch:               make(chan transport.Message, 100),
		closedCh:         make(chan struct{}),
		codec:            t.codec,
		idempotencyStore: t.idempotencyStore,
		dlqHandler:       t.dlqHandler,
		poisonDetector:   t.poisonDetector,
	}

	var natsSub *nats.Subscription
	var err error

	if mode == transport.WorkerPool {
		// Queue group for load balancing
		natsSub, err = t.conn.QueueSubscribe(name, "workers", sub.handleMessage)
	} else {
		// Regular subscription for broadcast
		natsSub, err = t.conn.Subscribe(name, sub.handleMessage)
	}

	if err != nil {
		return nil, err
	}

	sub.sub = natsSub
	t.logger.Debug("subscribed", "event", name, "subscriber", sub.id, "mode", mode)

	return sub, nil
}

// Close shuts down the transport
func (t *CoreTransport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the NATS Core transport
func (t *CoreTransport) Health(ctx context.Context) *transport.HealthCheckResult {
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

	status := t.conn.Status()
	if status != nats.CONNECTED {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "nats connection not healthy"
		result.Details["connection_status"] = status.String()
		result.Latency = time.Since(start)
		return result
	}

	result.Status = transport.HealthStatusHealthy
	result.Message = "nats core transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "nats-core"
	result.Details["connection_status"] = status.String()
	result.Details["server_url"] = t.conn.ConnectedUrl()

	return result
}

// =============================================================================
// Subscription Implementation
// =============================================================================

func (s *coreSubscription) ID() string {
	return s.id
}

func (s *coreSubscription) Messages() <-chan transport.Message {
	return s.ch
}

func (s *coreSubscription) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.closedCh)
		if s.sub != nil {
			s.sub.Unsubscribe()
		}
		s.wg.Wait()
		close(s.ch)
	}
	return nil
}

func (s *coreSubscription) handleMessage(msg *nats.Msg) {
	select {
	case <-s.closedCh:
		return
	default:
	}

	ctx := context.Background()

	// Decode message
	decoded, err := s.codec.Decode(msg.Data)
	if err != nil {
		// Can't decode - nothing to do in Core mode
		return
	}

	msgID := decoded.ID()

	// Check poison (if detector configured)
	if s.poisonDetector != nil {
		if poisoned, _ := s.poisonDetector.Check(ctx, msgID); poisoned {
			return // Skip quarantined message
		}
	}

	// Check deduplication (if store configured)
	if s.idempotencyStore != nil {
		if dup, _ := s.idempotencyStore.IsDuplicate(ctx, msgID); dup {
			return // Skip duplicate
		}
	}

	// Create transport message (no span context in core - at-most-once doesn't trace)
	transportMsg := transport.NewMessage(
		decoded.ID(),
		decoded.Source(),
		decoded.Payload(),
		decoded.Metadata(),
		trace.SpanContext{},
	)

	// Send to channel (non-blocking in Core mode - drop if full)
	select {
	case <-s.closedCh:
		return
	case s.ch <- transportMsg:
		// Message delivered
	default:
		// Channel full - drop message (at-most-once semantics)
	}
}

// Compile-time checks
var (
	_ transport.Transport    = (*CoreTransport)(nil)
	_ transport.HealthChecker = (*CoreTransport)(nil)
)

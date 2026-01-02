package event

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/payload"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

// MetadataContentType is the metadata key for payload encoding.
const MetadataContentType = "Content-Type"

// DeliveryMode determines how messages are distributed to subscribers
type DeliveryMode int

const (
	// Broadcast delivers message to ALL subscribers (pub/sub fan-out)
	Broadcast DeliveryMode = iota
	// WorkerPool delivers message to ONE subscriber (load balancing across workers)
	WorkerPool
)

// Default event configuration values
var (
	// DefaultSubscriberTimeout default subscriber timeout (0 = no timeout)
	DefaultSubscriberTimeout time.Duration = 0
	// DefaultMaxRetries default max retry attempts (0 = unlimited)
	DefaultMaxRetries = 0
)

// eventOptions holds configuration for events (unexported)
// These are event-level concerns, not bus-level infrastructure
type eventOptions struct {
	subTimeout   time.Duration
	onError      func(*Bus, string, error)
	maxRetries   int                                                             // Max retry attempts (0 = unlimited)
	dlqHandler   func(ctx context.Context, msg message.Message, err error) error // Dead letter queue handler (returns error if storage fails)
	payloadCodec payload.Codec                                                   // Payload codec (nil = use JSON default)
}

// EventOption is an alias for Option (for API clarity)
type EventOption = Option

// newEventOptions creates options with defaults and applies provided options
func newEventOptions(opts ...Option) *eventOptions {
	o := &eventOptions{
		onError:    func(*Bus, string, error) {}, // no-op default
		subTimeout: DefaultSubscriberTimeout,
	}

	// Apply all options
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Option event options
type Option func(*eventOptions)

// WithSubscriberTimeout set subscriber timeout for event handlers
// if set to 0, timeout will be disabled and handlers will
// run indefinitely.
func WithSubscriberTimeout(v time.Duration) Option {
	return func(o *eventOptions) {
		o.subTimeout = v
	}
}

// WithErrorHandler set error handler for panic recovery.
// The handler receives the bus, event name, and error.
func WithErrorHandler(v func(*Bus, string, error)) Option {
	return func(o *eventOptions) {
		if v != nil {
			o.onError = v
		}
	}
}

// WithMaxRetries sets the maximum number of retry attempts for failed messages.
// After maxRetries attempts, the message is sent to DLQ (if configured) or acked.
// Set to 0 (default) for unlimited retries.
//
// Example:
//
//	event := New[Order]("orders", WithMaxRetries(3))
func WithMaxRetries(maxRetries int) Option {
	return func(o *eventOptions) {
		if maxRetries >= 0 {
			o.maxRetries = maxRetries
		}
	}
}

// WithDeadLetterQueue configures a handler for messages that fail permanently.
// Messages are sent to DLQ when:
//   - Handler returns ErrReject
//   - Max retries are exhausted (if WithMaxRetries is set)
//   - Message decode fails (malformed message)
//
// The handler receives the original message and the last error.
// IMPORTANT: If the DLQ handler returns an error, the message will NOT be acknowledged
// and will be retried. This ensures no message loss if DLQ storage fails.
//
// Use this for logging, alerting, or storing failed messages for manual review.
//
// Example:
//
//	event := New[Order]("orders",
//	    WithMaxRetries(3),
//	    WithDeadLetterQueue(func(ctx context.Context, msg Message, err error) error {
//	        if err := dlqStore.Save(ctx, msg, err); err != nil {
//	            return err // Don't ACK - retry later
//	        }
//	        log.Error("message failed permanently",
//	            "msg_id", msg.ID(),
//	            "error", err,
//	        )
//	        return nil // ACK - message safely stored
//	    }),
//	)
func WithDeadLetterQueue(handler func(ctx context.Context, msg message.Message, err error) error) Option {
	return func(o *eventOptions) {
		if handler != nil {
			o.dlqHandler = handler
		}
	}
}

// WithPayloadCodec sets the codec for event payload serialization.
// Default is JSON if not specified.
//
// The codec handles encoding/decoding of event data at the application level,
// separate from transport-level message serialization.
//
// Example:
//
//	// Use protobuf for this event
//	event := New[*pb.Order]("orders", WithPayloadCodec(payload.Proto{}))
//
//	// Use msgpack for this event
//	event := New[Order]("orders", WithPayloadCodec(payload.MsgPack{}))
//
//	// JSON is used by default (no option needed)
//	event := New[Order]("orders")
func WithPayloadCodec(codec payload.Codec) Option {
	return func(o *eventOptions) {
		if codec != nil {
			o.payloadCodec = codec
		}
	}
}

// Middleware wraps a handler to add cross-cutting concerns.
// Middleware is applied in order: first middleware wraps the outermost layer.
//
// Example:
//
//	func LoggingMiddleware[T any](next event.Handler[T]) event.Handler[T] {
//	    return func(ctx context.Context, ev event.Event[T], data T) error {
//	        start := time.Now()
//	        err := next(ctx, ev, data)
//	        log.Info("handler completed", "event", ev.Name(), "duration", time.Since(start), "error", err)
//	        return err
//	    }
//	}
type Middleware[T any] func(Handler[T]) Handler[T]

// subscribeOptions holds configuration for subscriptions
type subscribeOptions[T any] struct {
	mode        DeliveryMode
	workerGroup string
	startFrom   transport.StartPosition
	startTime   time.Time
	maxAge      time.Duration
	latestOnly  bool
	bufferSize  int
	middleware  []Middleware[T]
}

// SubscribeOption configures subscription behavior
type SubscribeOption[T any] func(*subscribeOptions[T])

// newSubscribeOptions creates options with defaults and applies provided options
func newSubscribeOptions[T any](opts ...SubscribeOption[T]) *subscribeOptions[T] {
	o := &subscribeOptions[T]{
		mode:      Broadcast,                    // Default to broadcast (all receive)
		startFrom: transport.StartFromBeginning, // Default to processing all historical messages
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// transportOptions converts event subscribe options to transport subscribe options
func (o *subscribeOptions[T]) transportOptions() []transport.SubscribeOption {
	// Convert event-level DeliveryMode to transport-level
	var transportMode transport.DeliveryMode
	switch o.mode {
	case WorkerPool:
		transportMode = transport.WorkerPool
	default:
		transportMode = transport.Broadcast
	}

	opts := []transport.SubscribeOption{
		transport.WithDeliveryMode(transportMode),
	}

	if o.workerGroup != "" {
		opts = append(opts, transport.WithWorkerGroup(o.workerGroup))
	}

	if o.startFrom != transport.StartFromBeginning {
		opts = append(opts, transport.WithStartFrom(o.startFrom))
	}

	if !o.startTime.IsZero() {
		opts = append(opts, transport.WithStartTime(o.startTime))
	}

	if o.maxAge > 0 {
		opts = append(opts, transport.WithMaxAge(o.maxAge))
	}

	if o.latestOnly {
		opts = append(opts, transport.WithLatestOnly())
	}

	if o.bufferSize > 0 {
		opts = append(opts, transport.WithBufferSize(o.bufferSize))
	}

	return opts
}

// WithDeliveryMode sets the message delivery mode.
//
// Modes:
//   - Broadcast (default): all subscribers receive every message
//   - WorkerPool: each message is delivered to only ONE subscriber (load balancing)
//
// When using WorkerPool mode, use WithWorkerGroup to create named worker groups.
// Workers in the same group compete for messages; different groups each receive all messages.
//
// Example:
//
//	// Broadcast mode (default)
//	event.Subscribe(ctx, handler)
//
//	// Worker pool with default group
//	event.Subscribe(ctx, handler, event.WithDeliveryMode[Order](event.WorkerPool))
//
//	// Worker pool with named group
//	event.Subscribe(ctx, handler,
//	    event.WithDeliveryMode[Order](event.WorkerPool),
//	    event.WithWorkerGroup[Order]("processors"))
func WithDeliveryMode[T any](mode DeliveryMode) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.mode = mode
	}
}

// WithWorkerGroup sets the worker group name and automatically enables WorkerPool mode.
// Workers with the same group name compete for messages (load balancing).
// Different groups each receive all messages (broadcast between groups).
//
// Message flow:
//
//	Event
//	  ├── Broadcast subscribers (no group) ──► ALL receive every message
//	  ├── WorkerGroup "A" ──► ONE worker receives each message
//	  │     ├── worker-a1
//	  │     └── worker-a2
//	  └── WorkerGroup "B" ──► ONE worker receives each message
//	        ├── worker-b1
//	        └── worker-b2
//
// Example:
//
//	// Order processors compete within their group
//	orderEvent.Subscribe(ctx, processOrder,
//	    event.WithWorkerGroup[Order]("order-processors"))
//
//	// Inventory updaters in separate group (also receive all messages)
//	orderEvent.Subscribe(ctx, updateInventory,
//	    event.WithWorkerGroup[Order]("inventory-updaters"))
//
//	// Broadcast subscriber (no group) - receives all messages
//	orderEvent.Subscribe(ctx, logOrder)
func WithWorkerGroup[T any](group string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.workerGroup = group
		o.mode = WorkerPool // Automatically enable worker pool mode
	}
}

// FromLatest configures the subscription to only receive new messages.
// Historical messages that existed before the subscription are skipped.
// Use this for real-time dashboards or notifications that don't need history.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler, event.FromLatest[Order]())
func FromLatest[T any]() SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.startFrom = transport.StartFromLatest
	}
}

// FromTimestamp configures the subscription to start from a specific time.
// Messages before this time are skipped.
// Use this to resume processing from a known checkpoint.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler, event.FromTimestamp[Order](lastProcessedTime))
func FromTimestamp[T any](t time.Time) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.startFrom = transport.StartFromTimestamp
		o.startTime = t
	}
}

// WithMaxAge filters out messages older than the specified duration.
// Messages older than (now - maxAge) are silently skipped.
// Use this to avoid processing stale events after a service restart.
//
// Example:
//
//	// Only process messages from the last 5 minutes
//	orderEvent.Subscribe(ctx, handler, event.WithMaxAge[Order](5*time.Minute))
func WithMaxAge[T any](maxAge time.Duration) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.maxAge = maxAge
	}
}

// WithLatestOnly enables sampling mode where only the most recent message
// is delivered. If multiple messages arrive while processing, intermediate
// messages are dropped and only the latest is kept.
// Use this for real-time state updates where only the current value matters.
//
// Example:
//
//	// Real-time price updates - only care about current price
//	priceEvent.Subscribe(ctx, handler, event.WithLatestOnly[Price]())
func WithLatestOnly[T any]() SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.latestOnly = true
	}
}

// WithBufferSize sets the message channel buffer size.
// Use this to control backpressure behavior.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler, event.WithBufferSize[Order](1000))
func WithBufferSize[T any](size int) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.bufferSize = size
	}
}

// WithMiddleware adds custom middleware to the subscription handler chain.
// Middleware is applied in order: first middleware wraps the outermost layer.
// Custom middleware runs AFTER the built-in middleware (recovery, tracing, metrics, timeout).
//
// Example:
//
//	// Logging middleware
//	func LoggingMiddleware[T any](next event.Handler[T]) event.Handler[T] {
//	    return func(ctx context.Context, ev event.Event[T], data T) error {
//	        log.Info("processing", "event", ev.Name())
//	        return next(ctx, ev, data)
//	    }
//	}
//
//	// Rate limiting middleware
//	func RateLimitMiddleware[T any](limiter *rate.Limiter) event.Middleware[T] {
//	    return func(next event.Handler[T]) event.Handler[T] {
//	        return func(ctx context.Context, ev event.Event[T], data T) error {
//	            if err := limiter.Wait(ctx); err != nil {
//	                return event.ErrDefer
//	            }
//	            return next(ctx, ev, data)
//	        }
//	    }
//	}
//
//	ev.Subscribe(ctx, handler, event.WithMiddleware(LoggingMiddleware[string], RateLimitMiddleware[string](limiter)))
func WithMiddleware[T any](middleware ...Middleware[T]) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.middleware = append(o.middleware, middleware...)
	}
}

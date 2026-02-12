package event

import (
	"context"
	"errors"
	"time"

	"github.com/rbaliyan/event/v3/payload"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

// MetadataContentType is the metadata key for payload encoding.
// Canonical definition is in payload.MetadataContentType.
const MetadataContentType = payload.MetadataContentType

// DeliveryMode determines how messages are distributed to subscribers.
// This is an alias for transport.DeliveryMode.
type DeliveryMode = transport.DeliveryMode

// AckPolicy controls when messages are acknowledged by the event layer.
// This is an alias for transport.AckPolicy.
type AckPolicy = transport.AckPolicy

const (
	// Broadcast delivers message to ALL subscribers (pub/sub fan-out)
	Broadcast = transport.Broadcast
	// WorkerPool delivers message to ONE subscriber (load balancing across workers)
	WorkerPool = transport.WorkerPool
)

const (
	// AckExplicit requires the handler to succeed before acknowledging.
	// This is the default behavior.
	AckExplicit = transport.AckExplicit
	// AckOnReceive acknowledges messages immediately upon delivery.
	// Handler errors are logged but never cause redelivery.
	AckOnReceive = transport.AckOnReceive
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
	subTimeout         time.Duration
	onError            func(*Bus, string, error)
	maxRetries         int                                                             // Max retry attempts (0 = unlimited)
	payloadCodec       payload.Codec                                                   // Payload codec (nil = use JSON default)
	messageFilter      func(map[string]string) bool                                    // Pre-decode message filter (nil = accept all)
	decodeErrorHandler func(ctx context.Context, msg message.Message, err error) error // Decode error handler (nil = default DLQ+ack)
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

// WithMessageFilter sets a pre-decode filter that inspects message metadata
// before payload decoding. Return true to process the message, false to skip it.
//
// This is useful when multiple event types share a single bus/transport
// (e.g., MongoDB change streams watching multiple collections). Without a filter,
// messages from unrelated collections would fail codec decode and flood the DLQ.
//
// The metadata map includes transport-specific keys. For MongoDB change streams:
//   - "collection": source collection name
//   - "namespace": "database.collection"
//   - "operation": insert, update, replace, delete
//
// Example:
//
//	orderEvent := event.New[Order]("orders",
//	    event.WithMessageFilter(func(meta map[string]string) bool {
//	        return meta["collection"] == "orders"
//	    }),
//	)
func WithMessageFilter(filter func(map[string]string) bool) Option {
	return func(o *eventOptions) {
		o.messageFilter = filter
	}
}

// WithDecodeErrorHandler sets a handler for codec decode failures during subscription.
// By default, decode errors route to DLQ (if configured) and acknowledge the message.
// This handler lets you control the behavior using the same sentinel errors as handlers:
//
//   - nil / ErrAck: silently acknowledge and skip (no DLQ)
//   - ErrReject: send to DLQ if configured, then acknowledge
//   - ErrNack: retry immediately
//   - ErrDefer: retry with backoff
//   - Other errors: treated as ErrDefer (retry with backoff)
//
// When maxRetries is configured, retry attempts are tracked and the message is sent
// to DLQ when retries are exhausted (same behavior as handler errors).
//
// This handler only applies to application-level codec.Decode failures (e.g., schema
// changes, field type mismatches). Transport-level decode errors and unknown content
// types are always routed to DLQ since they are not schema-related.
//
// Example:
//
//	event := New[Order]("orders",
//	    WithDecodeErrorHandler(func(ctx context.Context, msg message.Message, err error) error {
//	        if isSchemaEvolution(err) {
//	            return event.ErrDefer // Retry during rolling deployment
//	        }
//	        return fmt.Errorf("decode failed: %w", event.ErrReject) // Permanent — DLQ
//	    }),
//	)
func WithDecodeErrorHandler(handler func(ctx context.Context, msg message.Message, err error) error) Option {
	return func(o *eventOptions) {
		o.decodeErrorHandler = handler
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

// Chain is a composable middleware chain builder.
// It provides a fluent API for building middleware chains that can be reused
// across multiple subscriptions.
//
// Middleware execution order:
// The first middleware added to the chain wraps the outermost layer.
// Given chain.Use(A).Use(B).Use(C), the execution order is:
// A.before -> B.before -> C.before -> handler -> C.after -> B.after -> A.after
//
// Example:
//
//	// Build a reusable middleware chain
//	chain := event.NewChain[Order]().
//	    Use(LoggingMiddleware[Order]).
//	    Use(MetricsMiddleware[Order]).
//	    Use(RateLimitMiddleware[Order](limiter))
//
//	// Use the chain with WithMiddleware
//	orderEvent.Subscribe(ctx, handler, event.WithMiddlewareChain(chain))
//
//	// Or wrap a handler directly
//	wrappedHandler := chain.Wrap(myHandler)
type Chain[T any] struct {
	middleware []Middleware[T]
}

// NewChain creates a new empty middleware chain.
func NewChain[T any]() *Chain[T] {
	return &Chain[T]{
		middleware: make([]Middleware[T], 0),
	}
}

// Use adds middleware to the chain and returns the chain for method chaining.
// Middleware is applied in the order added: first added wraps the outermost layer.
func (c *Chain[T]) Use(m Middleware[T]) *Chain[T] {
	c.middleware = append(c.middleware, m)
	return c
}

// UseFunc is a convenience method that converts a handler wrapper function
// to middleware and adds it to the chain.
func (c *Chain[T]) UseFunc(fn func(Handler[T]) Handler[T]) *Chain[T] {
	return c.Use(Middleware[T](fn))
}

// Append adds all middleware from another chain to this chain.
// This allows composing chains together.
func (c *Chain[T]) Append(other *Chain[T]) *Chain[T] {
	if other != nil {
		c.middleware = append(c.middleware, other.middleware...)
	}
	return c
}

// Wrap applies the middleware chain to a handler, returning the wrapped handler.
// The chain is applied in order: first middleware wraps the outermost layer.
func (c *Chain[T]) Wrap(handler Handler[T]) Handler[T] {
	if len(c.middleware) == 0 {
		return handler
	}

	// Apply middleware in reverse order so first middleware is outermost
	wrapped := handler
	for i := len(c.middleware) - 1; i >= 0; i-- {
		wrapped = c.middleware[i](wrapped)
	}
	return wrapped
}

// Middleware returns the middleware slice for use with WithMiddleware.
func (c *Chain[T]) Middleware() []Middleware[T] {
	return c.middleware
}

// Len returns the number of middleware in the chain.
func (c *Chain[T]) Len() int {
	return len(c.middleware)
}

// WithMiddlewareChain adds all middleware from a chain to the subscription.
// This is equivalent to calling WithMiddleware with chain.Middleware()...
//
// Example:
//
//	chain := event.NewChain[Order]().
//	    Use(LoggingMiddleware[Order]).
//	    Use(MetricsMiddleware[Order])
//
//	orderEvent.Subscribe(ctx, handler, event.WithMiddlewareChain(chain))
func WithMiddlewareChain[T any](chain *Chain[T]) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		if chain != nil {
			o.middleware = append(o.middleware, chain.middleware...)
		}
	}
}

// defaultCoalesceMaxKeys is the default maximum number of unique keys
// tracked by the coalescer before evicting the oldest entry.
const defaultCoalesceMaxKeys = 10000

// subscribeOptions holds configuration for subscriptions
type subscribeOptions[T any] struct {
	mode                  DeliveryMode
	workerGroup           string
	startFrom             transport.StartPosition
	startTime             time.Time
	maxAge                time.Duration
	latestOnly            bool
	bufferSize            int
	middleware            []Middleware[T]
	subscriberName        string
	subscriberDescription string

	// ack policy
	ackPolicy AckPolicy

	// coalescing
	coalesceKeyFunc func(T) string // key from decoded payload (nil = no coalescing)
	coalesceMetaKey string         // key from message metadata (empty = no coalescing)
	coalesceMaxKeys int            // max unique keys tracked (0 = defaultCoalesceMaxKeys)
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

// coalescing returns true if any coalescing mode is configured.
func (o *subscribeOptions[T]) coalescing() bool {
	return o.coalesceKeyFunc != nil || o.coalesceMetaKey != ""
}

// effectiveCoalesceMaxKeys returns the coalesce max keys, applying the default.
func (o *subscribeOptions[T]) effectiveCoalesceMaxKeys() int {
	if o.coalesceMaxKeys > 0 {
		return o.coalesceMaxKeys
	}
	return defaultCoalesceMaxKeys
}

// validate checks for incompatible option combinations.
func (o *subscribeOptions[T]) validate() error {
	if o.coalesceKeyFunc != nil && o.coalesceMetaKey != "" {
		return errors.New("invalid subscribe options: WithCoalesceByKey and WithCoalesceByMetadata cannot be combined")
	}
	if o.coalescing() && o.latestOnly {
		return errors.New("invalid subscribe options: coalescing and WithLatestOnly cannot be combined")
	}
	return nil
}

// transportOptions converts event subscribe options to transport subscribe options
func (o *subscribeOptions[T]) transportOptions() []transport.SubscribeOption {
	opts := []transport.SubscribeOption{
		transport.WithDeliveryMode(o.mode),
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

	if o.ackPolicy != AckExplicit {
		opts = append(opts, transport.WithAckPolicy(transport.AckPolicy(o.ackPolicy)))
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

// WithSubscriberName sets a human-readable name for the subscriber.
// This name flows through to monitoring systems and dashboards for identification.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithSubscriberName[Order]("order-processor"),
//	)
func WithSubscriberName[T any](name string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.subscriberName = name
	}
}

// WithSubscriberDescription sets a human-readable description for the subscriber.
// This description flows through to monitoring systems and dashboards.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithSubscriberName[Order]("order-processor"),
//	    event.WithSubscriberDescription[Order]("Processes incoming orders and updates inventory"),
//	)
func WithSubscriberDescription[T any](desc string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.subscriberDescription = desc
	}
}

// WithAckPolicy sets the acknowledgment policy for this subscription.
// Default is AckExplicit (handler must succeed for acknowledgment).
//
// AckOnReceive is useful for:
//   - Real-time dashboards where stale retries are worse than gaps
//   - SSE/WebSocket push where clients will reconnect
//   - Metrics aggregation where occasional loss is acceptable
//
// AckOnReceive effectively disables retries and DLQ routing for this subscriber.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithAckPolicy[Order](event.AckOnReceive),
//	)
func WithAckPolicy[T any](policy AckPolicy) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.ackPolicy = policy
	}
}

// WithBestEffort is a convenience alias for WithAckPolicy[T](AckOnReceive).
// Messages are auto-acknowledged on receive, handler errors are logged but
// never cause redelivery.
//
// Example:
//
//	orderEvent.Subscribe(ctx, sseHandler,
//	    event.WithBestEffort[Order](),
//	)
func WithBestEffort[T any]() SubscribeOption[T] {
	return WithAckPolicy[T](AckOnReceive)
}

// WithCoalesceByKey enables key-based message coalescing.
// When multiple messages arrive for the same key while the handler is
// processing, intermediate messages are auto-acked and only the latest
// message per key is delivered.
//
// The keyFunc extracts a coalescing key from the decoded event data.
// Return "" to bypass coalescing for that message (deliver immediately).
//
// Coalescing state is ephemeral — it does not survive restarts.
// After restart, all messages are delivered without coalescing history.
//
// Cannot be combined with WithLatestOnly or WithCoalesceByMetadata.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithCoalesceByKey[Order](func(o Order) string {
//	        return o.ID
//	    }),
//	)
func WithCoalesceByKey[T any](keyFunc func(T) string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		if keyFunc != nil {
			o.coalesceKeyFunc = keyFunc
		}
	}
}

// WithCoalesceByMetadata enables metadata-based message coalescing.
// Like WithCoalesceByKey, but the coalescing key is extracted from message
// metadata before payload decoding. This avoids decoding messages that will
// be superseded, which is more efficient for high-throughput streams.
//
// The metadataKey specifies which metadata field to use as the coalescing key.
// For MongoDB change streams, use "document_key" (see event-mongodb.CoalesceByDocumentKey).
//
// Cannot be combined with WithLatestOnly or WithCoalesceByKey.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithCoalesceByMetadata[Order]("document_key"),
//	)
func WithCoalesceByMetadata[T any](metadataKey string) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		if metadataKey != "" {
			o.coalesceMetaKey = metadataKey
		}
	}
}

// WithCoalesceMaxKeys sets the maximum number of unique keys the coalescer
// will track. When exceeded, the oldest entry is evicted and delivered to
// the handler. Default: 10000.
//
// Use this to bound memory usage for high-cardinality key spaces.
//
// Example:
//
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithCoalesceByKey[Order](func(o Order) string { return o.ID }),
//	    event.WithCoalesceMaxKeys[Order](50000),
//	)
func WithCoalesceMaxKeys[T any](n int) SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		if n > 0 {
			o.coalesceMaxKeys = n
		}
	}
}

package event

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
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
	subTimeout time.Duration
	onError    func(*Bus, string, error)
	maxRetries int                                                             // Max retry attempts (0 = unlimited)
	dlqHandler func(ctx context.Context, msg message.Message, err error) error // Dead letter queue handler (returns error if storage fails)
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
	mode       transport.DeliveryMode
	middleware []Middleware[T]
}

// SubscribeOption configures subscription behavior
type SubscribeOption[T any] func(*subscribeOptions[T])

// newSubscribeOptions creates options with defaults and applies provided options
func newSubscribeOptions[T any](opts ...SubscribeOption[T]) *subscribeOptions[T] {
	o := &subscribeOptions[T]{
		mode: transport.Broadcast, // Default to broadcast (all receive)
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// AsWorker configures the subscription to use worker pool mode.
// In this mode, only one subscriber receives each message (load balancing).
// Multiple workers compete for messages - each message is processed by exactly one worker.
func AsWorker[T any]() SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.mode = transport.WorkerPool
	}
}

// AsBroadcast configures the subscription to use broadcast mode (default).
// In this mode, all subscribers receive every message (fan-out).
func AsBroadcast[T any]() SubscribeOption[T] {
	return func(o *subscribeOptions[T]) {
		o.mode = transport.Broadcast
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

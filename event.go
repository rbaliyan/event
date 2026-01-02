package event

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

var (
	// ErrEventNotBound returned when operating on unbound event
	ErrEventNotBound = errors.New("event not bound to bus")
)

// Handler generic event handler.
// Return values control message acknowledgment:
//   - nil: Success - message is acknowledged
//   - ErrAck: Same as nil - acknowledge with context
//   - ErrNack: Retry immediately
//   - ErrReject: Don't retry, send to DLQ if configured
//   - ErrDefer: Retry with backoff (default for unknown errors)
//   - Other errors: Treated as ErrDefer (retry with backoff)
//
// Use errors.Is() compatible wrapping for context:
//
//	return fmt.Errorf("validation failed: %w", event.ErrReject)
type Handler[T any] func(context.Context, Event[T], T) error

// Event generic interface for typed publish/subscribe
type Event[T any] interface {
	// Name returns the event name which uniquely identifies this event
	Name() string
	// Publish sends data to subscribers
	// Returns error if event not registered or transport fails
	Publish(context.Context, T) error
	// Subscribe registers a handler to receive published data.
	// Options control delivery mode:
	//   - Default (no options): Broadcast - all subscribers receive every message
	//   - AsWorker(): WorkerPool - only one subscriber receives each message
	// Returns error if event not registered or transport fails
	Subscribe(context.Context, Handler[T], ...SubscribeOption[T]) error
}

// discardEvent discard all events
type discardEvent[T any] struct{}

func (discardEvent[T]) Name() string                                                              { return "" }
func (discardEvent[T]) Subscribe(_ context.Context, _ Handler[T], _ ...SubscribeOption[T]) error { return nil }
func (discardEvent[T]) Publish(_ context.Context, _ T) error                                      { return nil }

// New creates a new unbound event.
// The event must be registered with a Bus before Publish/Subscribe can be used.
func New[T any](name string, opts ...EventOption) Event[T] {
	o := newEventOptions(opts...)
	return &eventImpl[T]{
		status:     0, // unbound - not active yet
		name:       name,
		subTimeout: o.subTimeout,
		onError:    o.onError,
		maxRetries: o.maxRetries,
		dlqHandler: o.dlqHandler,
		// bus is set by bus.Register()
	}
}

// schemaFlags stores which middleware features are enabled from schema.
// These override bus-level settings when set.
type schemaFlags struct {
	loaded            bool // true if schema was loaded from provider
	enableMonitor     bool
	enableIdempotency bool
	enablePoison      bool
	subTimeout        time.Duration
	maxRetries        int
	retryBackoff      time.Duration
}

// eventImpl generic event implementation
type eventImpl[T any] struct {
	status     int32
	name       string
	size       int64
	bus        *Bus
	subTimeout time.Duration
	onError    func(*Bus, string, error)                                        // for panic recovery only
	maxRetries int                                                              // max retry attempts (0 = unlimited)
	dlqHandler func(ctx context.Context, msg message.Message, err error) error  // dead letter queue handler (returns error if storage fails)
	schema     schemaFlags                                                      // schema-based configuration
}

func (e *eventImpl[T]) String() string {
	return e.name
}

// Name event name
func (e *eventImpl[T]) Name() string {
	return e.name
}

// Subscribers events subscribers count
func (e *eventImpl[T]) Subscribers() int64 {
	return atomic.LoadInt64(&e.size)
}

// Bind binds the event to a bus. Called by bus.Register().
// Returns error if already bound to another bus.
func (e *eventImpl[T]) Bind(bus *Bus) error {
	if e.bus != nil {
		return ErrAlreadyBound
	}
	e.bus = bus
	atomic.StoreInt32(&e.status, 1) // mark as active/bound
	return nil
}

// Unbind unbinds the event from its bus. Called by bus.Unregister().
// Returns false if already unbound.
func (e *eventImpl[T]) Unbind() bool {
	if !atomic.CompareAndSwapInt32(&e.status, 1, 0) {
		return false // Already unbound
	}
	e.bus = nil
	return true
}

// applySchema applies schema settings to the event.
// This is called during registration when a schema provider is configured.
func (e *eventImpl[T]) applySchema(schema *EventSchema) {
	if schema == nil {
		return
	}

	e.schema = schemaFlags{
		loaded:            true,
		enableMonitor:     schema.EnableMonitor,
		enableIdempotency: schema.EnableIdempotency,
		enablePoison:      schema.EnablePoison,
		subTimeout:        schema.SubTimeout,
		maxRetries:        schema.MaxRetries,
		retryBackoff:      schema.RetryBackoff,
	}

	// Apply schema timeout if event doesn't have one
	if e.subTimeout == 0 && schema.SubTimeout > 0 {
		e.subTimeout = schema.SubTimeout
	}

	// Apply schema max retries if event doesn't have one
	if e.maxRetries == 0 && schema.MaxRetries > 0 {
		e.maxRetries = schema.MaxRetries
	}
}

// WithTimeout enable timeout for handlers
func (e *eventImpl[T]) WithTimeout(handler Handler[T]) Handler[T] {
	if e.subTimeout == 0 {
		return handler
	}
	return func(ctx context.Context, ev Event[T], data T) error {
		ctx, cancel := context.WithTimeout(ctx, e.subTimeout)
		defer cancel()
		return handler(ctx, ev, data)
	}
}

// WithRecovery enable recovery for handlers
func (e *eventImpl[T]) WithRecovery(handler Handler[T]) Handler[T] {
	if !e.bus.recoveryEnabled {
		return handler
	}
	return func(ctx context.Context, ev Event[T], data T) (err error) {
		logger := ContextLogger(ctx)
		if logger == nil {
			logger = e.bus.logger.With("event", e.name)
		}
		defer func() {
			_, file, l, _ := runtime.Caller(0)
			if r := recover(); r != nil {
				logger.Error("panic recovered in event handler",
					"event", ev.Name(),
					"line", l,
					"file", file,
					"error", r,
					"stack", string(debug.Stack()),
				)
				if e.onError != nil {
					e.onError(e.bus, e.name, fmt.Errorf("[%s]panic in %s:%d with : %v", e.name, file, l, r))
				}
				// Panic treated as retriable error
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return handler(ctx, ev, data)
	}
}

// Publish sends data to subscribers
func (e *eventImpl[T]) Publish(ctx context.Context, eventData T) error {
	// Check for nil event or unregistered
	if e == nil || e.bus == nil {
		return ErrEventNotBound
	}
	// Check if closed
	if atomic.LoadInt32(&e.status) != 1 {
		return ErrEventNotBound
	}

	// Get event ID from context or let bus generate one
	id := ContextEventID(ctx)

	// Delegate to bus.Send which handles metrics and tracing
	return e.bus.Send(ctx, e.name, id, eventData, ContextMetadata(ctx))
}

// classifyResult determines how to handle the handler result.
// Returns the result classification and whether to send to DLQ.
func classifyResult(err error, retryCount, maxRetries int) (result HandlerResult, sendToDLQ bool) {
	if err == nil {
		return ResultAck, false
	}

	// Check sentinel errors
	result = ClassifyError(err)

	// Handle based on classification
	switch result {
	case ResultAck:
		return ResultAck, false
	case ResultReject:
		return ResultReject, true // Send to DLQ
	case ResultNack, ResultDefer:
		// Check max retries
		if maxRetries > 0 && retryCount >= maxRetries {
			return ResultReject, true // Max retries exhausted, send to DLQ
		}
		return result, false
	default:
		return ResultDefer, false
	}
}

// Subscribe registers a handler to receive published data
func (e *eventImpl[T]) Subscribe(ctx context.Context, handler Handler[T], opts ...SubscribeOption[T]) error {
	// Check for nil event or unregistered
	if e == nil || e.bus == nil {
		return ErrEventNotBound
	}
	// Check if closed
	if atomic.LoadInt32(&e.status) != 1 {
		return ErrEventNotBound
	}

	// Apply subscribe options
	subOpts := newSubscribeOptions(opts...)

	logger := e.bus.logger.With("event", e.name)

	// Convert event-level options to transport options
	transportOpts := subOpts.transportOptions()

	// Subscribe via bus.Recv which handles metrics
	sub, err := e.bus.Recv(ctx, e.name, transportOpts...)
	if err != nil {
		return err
	}

	atomic.AddInt64(&e.size, 1)
	subID := sub.ID()

	// Apply middleware chain (innermost to outermost):
	// 1. Recovery (innermost) - catch panics
	// 2. Timeout - enforce handler timeout
	// 3. Custom middleware (from WithMiddleware)
	// 4. Bus idempotency - skip duplicates
	// 5. Bus poison detection (outermost) - skip quarantined messages
	wrappedHandler := e.WithTimeout(e.WithRecovery(handler))

	// Apply custom middleware
	for i := len(subOpts.middleware) - 1; i >= 0; i-- {
		wrappedHandler = subOpts.middleware[i](wrappedHandler)
	}

	// Apply bus-level middleware (outermost - runs first)
	// When schema is loaded, use schema flags to control middleware.
	// Otherwise, fall back to bus-level stores (if configured).
	if e.schema.loaded {
		// Schema-controlled middleware: only apply if schema enables it AND store is configured
		if e.schema.enableIdempotency && e.bus.idempotencyStore != nil {
			wrappedHandler = IdempotencyMiddleware[T](e.bus.idempotencyStore)(wrappedHandler)
		}
		if e.schema.enablePoison && e.bus.poisonDetector != nil {
			wrappedHandler = PoisonMiddleware[T](e.bus.poisonDetector)(wrappedHandler)
		}
		if e.schema.enableMonitor && e.bus.monitorStore != nil {
			wrappedHandler = MonitorMiddleware[T](e.bus.monitorStore)(wrappedHandler)
		}
	} else {
		// No schema: fall back to bus-level middleware (if stores are configured)
		if e.bus.idempotencyStore != nil {
			wrappedHandler = IdempotencyMiddleware[T](e.bus.idempotencyStore)(wrappedHandler)
		}
		if e.bus.poisonDetector != nil {
			wrappedHandler = PoisonMiddleware[T](e.bus.poisonDetector)(wrappedHandler)
		}
		if e.bus.monitorStore != nil {
			wrappedHandler = MonitorMiddleware[T](e.bus.monitorStore)(wrappedHandler)
		}
	}

	go func() {
		defer func() {
			atomic.AddInt64(&e.size, -1)
			// Use background context for cleanup since subscription context is done
			sub.Close(context.Background())
		}()
		for {
			select {
			case <-e.bus.shutdownChan:
				logger.Info("shutdown subscriber remove", "event", e.Name(), "subscriber_id", subID)
				return

			case <-ctx.Done():
				logger.Info("subscriber remove", "event", e.Name(), "subscriber_id", subID)
				return

			case msg, ok := <-sub.Messages():
				if !ok {
					logger.Info("channel closed", "event", e.Name(), "subscriber_id", subID)
					return
				}

				// Check for decode errors (malformed messages) - route to DLQ
				if decodeErr, isDecodeErr := transport.IsDecodeError(msg.Payload()); isDecodeErr {
					logger.Error("decode error received, routing to DLQ",
						"event", e.Name(),
						"msg_id", msg.ID(),
						"error", decodeErr.Err)

					if e.dlqHandler != nil {
						// Create a context for the DLQ handler
						dlqCtx := contextWithInfo(context.Background(), msg.ID(), e.name, e.bus.ID(), subID, msg.Metadata(), msg.Timestamp(), logger, e.bus, subOpts.mode)
						if dlqErr := e.dlqHandler(dlqCtx, msg, decodeErr); dlqErr != nil {
							// DLQ storage failed - DON'T ACK, let message be redelivered
							logger.Error("DLQ handler failed for decode error, message will be retried",
								"event", e.Name(),
								"msg_id", msg.ID(),
								"error", dlqErr)
							msg.Ack(fmt.Errorf("DLQ storage failed: %w", dlqErr))
							continue
						}
					}
					// Successfully handled or no DLQ configured - ACK
					msg.Ack(nil)
					continue
				}

				// Type assert payload
				typedData, ok := msg.Payload().(T)
				if !ok {
					var zero T
					typedData = zero
				}

				// Update context values and call handler
				handlerCtx := contextWithInfo(msg.Context(), msg.ID(), e.name, e.bus.ID(), subID, msg.Metadata(), msg.Timestamp(), logger, e.bus, subOpts.mode)
				err := wrappedHandler(handlerCtx, e, typedData)

				// Classify result and determine action
				result, sendToDLQ := classifyResult(err, msg.RetryCount(), e.maxRetries)

				// Send to DLQ if configured and needed
				if sendToDLQ && e.dlqHandler != nil {
					if dlqErr := e.dlqHandler(handlerCtx, msg, err); dlqErr != nil {
						// DLQ storage failed - DON'T ACK, let message be redelivered
						logger.Error("DLQ handler failed, message will be retried",
							"event", e.Name(),
							"msg_id", msg.ID(),
							"error", dlqErr,
							"original_error", err)
						msg.Ack(fmt.Errorf("DLQ storage failed: %w", dlqErr))
						continue
					}
				}

				// Ack based on result
				switch result {
				case ResultAck, ResultReject:
					// Acknowledge (remove from queue)
					msg.Ack(nil)
				case ResultNack:
					// Retry immediately
					msg.Ack(err)
				case ResultDefer:
					// Retry with backoff (transport handles this)
					msg.Ack(err)
				}
			}
		}
	}()
	logger.Info("installed subscriber", "event", e.Name(), "subscriber_id", subID)
	return nil
}

// Events a group of events with same type
type Events[T any] []Event[T]

// Names event names
func (e Events[T]) Names() []string {
	names := make([]string, 0, len(e))
	for _, event := range e {
		names = append(names, event.Name())
	}
	return names
}

// Name event name
func (e Events[T]) Name() string {
	return strings.Join(e.Names(), ",")
}

// Subscribe all events in the list
func (e Events[T]) Subscribe(ctx context.Context, handler Handler[T], opts ...SubscribeOption[T]) error {
	var errs []error
	for _, event := range e {
		if err := event.Subscribe(ctx, handler, opts...); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Publish to all events in list
func (e Events[T]) Publish(ctx context.Context, data T) error {
	var errs []error
	for _, event := range e {
		if err := event.Publish(ctx, data); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Discard creates an event that discards all published data
func Discard[T any](_ string, _ ...Option) Event[T] {
	return discardEvent[T]{}
}

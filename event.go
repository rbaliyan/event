package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/payload"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

var (
	// ErrEventNotBound returned when operating on unbound event
	ErrEventNotBound = errors.New("event not bound to bus")
	// ErrInvalidSubscribeOptions returned when subscribe options are incompatible
	ErrInvalidSubscribeOptions = errors.New("invalid subscribe options: WorkerGroup requires WorkerPool mode")
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

func (discardEvent[T]) Name() string { return "" }
func (discardEvent[T]) Subscribe(_ context.Context, _ Handler[T], _ ...SubscribeOption[T]) error {
	return nil
}
func (discardEvent[T]) Publish(_ context.Context, _ T) error { return nil }

// New creates a new unbound event.
// The event must be registered with a Bus before Publish/Subscribe can be used.
func New[T any](name string, opts ...EventOption) Event[T] {
	o := newEventOptions(opts...)
	return &eventImpl[T]{
		status:             0, // unbound - not active yet
		name:               name,
		subTimeout:         o.subTimeout,
		onError:            o.onError,
		maxRetries:         o.maxRetries,
		payloadCodec:       o.payloadCodec,
		messageFilter:      o.messageFilter,
		decodeErrorHandler: o.decodeErrorHandler,
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
	status             int32
	name               string
	size               int64
	bus                atomic.Pointer[Bus]
	subTimeout         time.Duration
	onError            func(*Bus, string, error)                                       // for panic recovery only
	maxRetries         int                                                             // max retry attempts (0 = unlimited)
	payloadCodec       payload.Codec                                                   // payload codec (nil = use JSON default)
	messageFilter      func(map[string]string) bool                                    // pre-decode message filter (nil = accept all)
	decodeErrorHandler func(ctx context.Context, msg message.Message, err error) error // decode error handler (nil = default DLQ+ack)
	schema             schemaFlags                                                     // schema-based configuration
	subscriptions      subscriptionRegistry                                            // active subscription metadata for topology
}

func (e *eventImpl[T]) String() string {
	return e.name
}

// eventTopology interface implementation for generic-free topology access.
func (e *eventImpl[T]) eventName() string                  { return e.name }
func (e *eventImpl[T]) subscriberCount() int64             { return atomic.LoadInt64(&e.size) }
func (e *eventImpl[T]) subscriptionInfos() []SubscriptionInfo { return e.subscriptions.all() }

// Name event name
func (e *eventImpl[T]) Name() string {
	return e.name
}

// getBus returns the bus pointer atomically.
func (e *eventImpl[T]) getBus() *Bus {
	return e.bus.Load()
}

// codec returns the payload codec, defaulting to JSON if not set.
func (e *eventImpl[T]) codec() payload.Codec {
	if e.payloadCodec != nil {
		return e.payloadCodec
	}
	return payload.JSON{}
}

// Subscribers events subscribers count
func (e *eventImpl[T]) Subscribers() int64 {
	return atomic.LoadInt64(&e.size)
}

// Bind binds the event to a bus. Called by bus.Register().
// Returns error if already bound to another bus.
func (e *eventImpl[T]) Bind(bus *Bus) error {
	if !e.bus.CompareAndSwap(nil, bus) {
		return ErrAlreadyBound
	}
	atomic.StoreInt32(&e.status, 1) // mark as active/bound
	return nil
}

// Unbind unbinds the event from its bus. Called by bus.Unregister().
// Returns false if already unbound.
func (e *eventImpl[T]) Unbind() bool {
	if !atomic.CompareAndSwapInt32(&e.status, 1, 0) {
		return false // Already unbound
	}
	e.bus.Store(nil)
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
	bus := e.getBus()
	if !bus.recoveryEnabled {
		return handler
	}
	return func(ctx context.Context, ev Event[T], data T) (err error) {
		// Resolve bus at call time so recovery always uses the current bus
		currentBus := e.getBus()
		if currentBus == nil {
			currentBus = bus // fallback to bus captured at creation time
		}
		logger := ContextLogger(ctx)
		if logger == nil {
			logger = currentBus.logger.With("event", e.name)
		}
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				logger.Error("panic recovered in event handler",
					"event", ev.Name(),
					"error", r,
					"stack", string(stack),
				)
				if e.onError != nil {
					e.onError(currentBus, e.name, fmt.Errorf("[%s]panic: %v", e.name, r))
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
	// Check if closed
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return ErrEventNotBound
	}
	bus := e.getBus()
	if bus == nil {
		return ErrEventNotBound
	}

	// Get event ID from context or let bus generate one
	id := ContextEventID(ctx)

	// Encode payload using the event's codec
	codec := e.codec()
	payloadBytes, err := codec.Encode(eventData)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	// Copy context metadata and add Content-Type
	metadata := ContextMetadata(ctx)
	if metadata == nil {
		metadata = make(map[string]string)
	} else {
		// Make a copy to avoid modifying the original
		copied := make(map[string]string, len(metadata)+1)
		for k, v := range metadata {
			copied[k] = v
		}
		metadata = copied
	}
	metadata[MetadataContentType] = codec.ContentType()

	// Delegate to bus.Send which handles metrics and tracing
	return bus.Send(ctx, e.name, id, payloadBytes, metadata)
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
	// Check if closed
	if e == nil || atomic.LoadInt32(&e.status) != 1 {
		return ErrEventNotBound
	}
	bus := e.getBus()
	if bus == nil {
		return ErrEventNotBound
	}

	// Apply subscribe options
	subOpts := newSubscribeOptions(opts...)

	// Validate options: WorkerGroup requires WorkerPool mode
	if subOpts.workerGroup != "" && subOpts.mode == Broadcast {
		return ErrInvalidSubscribeOptions
	}

	// Validate coalescing options
	if err := subOpts.validate(); err != nil {
		return err
	}

	// Warn about incompatible combinations with AckOnReceive
	bestEffort := subOpts.ackPolicy == AckOnReceive

	logger := bus.logger.With("event", e.name)

	if bestEffort {
		if e.maxRetries > 0 {
			logger.Warn("AckOnReceive with WithMaxRetries: retries will never fire",
				"event", e.name, "max_retries", e.maxRetries)
		}
		if bus.dlqStore != nil {
			logger.Warn("AckOnReceive with DLQ: DLQ will never be reached",
				"event", e.name)
		}
	}

	// Convert event-level options to transport options
	transportOpts := subOpts.transportOptions()

	// Subscribe via bus.Recv which handles metrics
	sub, err := bus.Recv(ctx, e.name, transportOpts...)
	if err != nil {
		return err
	}

	atomic.AddInt64(&e.size, 1)
	subID := sub.ID()

	// Track subscription metadata for topology reporting
	e.subscriptions.add(&SubscriptionInfo{
		SubscriptionID:        subID,
		DeliveryMode:          subOpts.mode,
		WorkerGroup:           subOpts.workerGroup,
		SubscriberName:        subOpts.subscriberName,
		SubscriberDescription: subOpts.subscriberDescription,
		StartedAt:             time.Now(),
	})

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
		if e.schema.enableIdempotency && bus.idempotencyStore != nil {
			wrappedHandler = IdempotencyMiddleware[T](bus.idempotencyStore)(wrappedHandler)
		}
		if e.schema.enablePoison && bus.poisonDetector != nil {
			wrappedHandler = PoisonMiddleware[T](bus.poisonDetector)(wrappedHandler)
		}
		if e.schema.enableMonitor && bus.monitorStore != nil {
			wrappedHandler = MonitorMiddleware[T](bus.monitorStore)(wrappedHandler)
		}
	} else {
		// No schema: fall back to bus-level middleware (if stores are configured)
		if bus.idempotencyStore != nil {
			wrappedHandler = IdempotencyMiddleware[T](bus.idempotencyStore)(wrappedHandler)
		}
		if bus.poisonDetector != nil {
			wrappedHandler = PoisonMiddleware[T](bus.poisonDetector)(wrappedHandler)
		}
		if bus.monitorStore != nil {
			wrappedHandler = MonitorMiddleware[T](bus.monitorStore)(wrappedHandler)
		}
	}

	// Dispatch to the appropriate subscribe loop based on coalescing mode.
	if subOpts.coalesceMetaKey != "" {
		go e.subscribeWithRawCoalesce(ctx, bus, sub, subOpts, wrappedHandler, logger, bestEffort)
	} else if subOpts.coalesceKeyFunc != nil {
		go e.subscribeWithCoalesce(ctx, bus, sub, subOpts, wrappedHandler, logger, bestEffort)
	} else {
		go e.subscribeLoop(ctx, bus, sub, subOpts, wrappedHandler, logger, bestEffort)
	}

	logger.Info("installed subscriber", "event", e.Name(), "subscriber_id", subID,
		"ack_policy", subOpts.ackPolicy.String(),
		"coalescing", subOpts.coalescing())
	return nil
}

// subscribeLoop is the standard message processing loop (no coalescing).
func (e *eventImpl[T]) subscribeLoop(
	ctx context.Context,
	bus *Bus,
	sub transport.Subscription,
	subOpts *subscribeOptions[T],
	wrappedHandler Handler[T],
	logger *slog.Logger,
	bestEffort bool,
) {
	subID := sub.ID()
	defer func() {
		e.subscriptions.remove(subID)
		atomic.AddInt64(&e.size, -1)
		_ = sub.Close(context.Background())
	}()

	for {
		select {
		case <-bus.shutdownChan:
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

			func() {
				bus.inflightWG.Add(1)
				defer bus.inflightWG.Done()
				e.processMessage(msg, bus, sub, subOpts, wrappedHandler, logger, bestEffort, 0)
			}()
		}
	}
}

// subscribeWithRawCoalesce runs the pre-decode (metadata-based) coalescing loop.
func (e *eventImpl[T]) subscribeWithRawCoalesce(
	ctx context.Context,
	bus *Bus,
	sub transport.Subscription,
	subOpts *subscribeOptions[T],
	wrappedHandler Handler[T],
	logger *slog.Logger,
	bestEffort bool,
) {
	subID := sub.ID()
	coal := newRawCoalescer(subOpts.coalesceMetaKey, subOpts.effectiveCoalesceMaxKeys(), logger)

	defer func() {
		coal.Close()
		e.subscriptions.remove(subID)
		atomic.AddInt64(&e.size, -1)
		_ = sub.Close(context.Background())
	}()

	// Ingestion goroutine: reads from transport, filters, feeds coalescer.
	go func() {
		defer close(coal.incoming)
		for {
			select {
			case <-bus.shutdownChan:
				return
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Messages():
				if !ok {
					return
				}

				// Check for transport-level decode errors — handle inline, no coalescing possible.
				if decodeErrMsg, isDecodeErr := transport.IsDecodeError(msg.Metadata()); isDecodeErr {
					decodeErr := errors.New(decodeErrMsg)
					logger.Error("transport decode error, routing to DLQ",
						"event", e.Name(), "msg_id", msg.ID(), "error", decodeErrMsg)
					if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, decodeErr); dlqErr != nil {
						bus.logFallbackDLQ(logger, e.name, msg, decodeErr, dlqErr)
					}
					_ = msg.Ack(nil)
					continue
				}

				// Apply pre-decode message filter.
				if e.messageFilter != nil && !e.messageFilter(msg.Metadata()) {
					_ = msg.Ack(nil)
					continue
				}

				select {
				case coal.incoming <- rawCoalesceInput{msg: msg}:
				case <-bus.shutdownChan:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Delivery loop: pulls coalesced messages, decodes, calls handler.
	for {
		select {
		case <-bus.shutdownChan:
			logger.Info("shutdown subscriber remove", "event", e.Name(), "subscriber_id", subID)
			return
		case <-ctx.Done():
			logger.Info("subscriber remove", "event", e.Name(), "subscriber_id", subID)
			return
		case out, ok := <-coal.output:
			if !ok {
				return
			}

			func() {
				bus.inflightWG.Add(1)
				defer bus.inflightWG.Done()
				e.processMessage(out.msg, bus, sub, subOpts, wrappedHandler, logger, bestEffort, out.count)
			}()

			// Signal coalescer that this key is done.
			select {
			case coal.done <- out.key:
			case <-coal.stopped:
			}
		}
	}
}

// subscribeWithCoalesce runs the post-decode (key function) coalescing loop.
func (e *eventImpl[T]) subscribeWithCoalesce(
	ctx context.Context,
	bus *Bus,
	sub transport.Subscription,
	subOpts *subscribeOptions[T],
	wrappedHandler Handler[T],
	logger *slog.Logger,
	bestEffort bool,
) {
	subID := sub.ID()
	coal := newCoalescer[T](subOpts.effectiveCoalesceMaxKeys(), logger)

	defer func() {
		coal.Close()
		e.subscriptions.remove(subID)
		atomic.AddInt64(&e.size, -1)
		_ = sub.Close(context.Background())
	}()

	// Ingestion goroutine: reads from transport, decodes, feeds coalescer.
	go func() {
		defer close(coal.incoming)
		for {
			select {
			case <-bus.shutdownChan:
				return
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Messages():
				if !ok {
					return
				}

				// Check for transport-level decode errors — pass through.
				if decodeErrMsg, isDecodeErr := transport.IsDecodeError(msg.Metadata()); isDecodeErr {
					decodeErr := errors.New(decodeErrMsg)
					logger.Error("transport decode error, routing to DLQ",
						"event", e.Name(), "msg_id", msg.ID(), "error", decodeErrMsg)
					if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, decodeErr); dlqErr != nil {
						bus.logFallbackDLQ(logger, e.name, msg, decodeErr, dlqErr)
					}
					_ = msg.Ack(nil)
					continue
				}

				// Apply pre-decode message filter.
				if e.messageFilter != nil && !e.messageFilter(msg.Metadata()) {
					_ = msg.Ack(nil)
					continue
				}

				// Decode payload.
				var typedData T
				contentType := msg.Metadata()[MetadataContentType]
				if contentType == "" {
					contentType = "application/json"
				}

				codec, codecOk := payload.Get(contentType)
				if !codecOk {
					contentErr := fmt.Errorf("unknown content type: %s", contentType)
					logger.Error("unknown content type, routing to DLQ",
						"event", e.Name(), "msg_id", msg.ID(), "content_type", contentType)
					if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, contentErr); dlqErr != nil {
						bus.logFallbackDLQ(logger, e.name, msg, contentErr, dlqErr)
					}
					_ = msg.Ack(nil)
					continue
				}

				if err := codec.Decode(msg.Payload(), &typedData); err != nil {
					logger.Error("decode error, routing to DLQ",
						"event", e.Name(), "msg_id", msg.ID(), "error", err)
					if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, err); dlqErr != nil {
						bus.logFallbackDLQ(logger, e.name, msg, err, dlqErr)
					}
					_ = msg.Ack(nil)
					continue
				}

				// Extract coalesce key from decoded data.
				key := subOpts.coalesceKeyFunc(typedData)

				select {
				case coal.incoming <- coalesceInput[T]{key: key, msg: msg, value: typedData}:
				case <-bus.shutdownChan:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Delivery loop: pulls coalesced entries, calls handler directly (already decoded).
	for {
		select {
		case <-bus.shutdownChan:
			logger.Info("shutdown subscriber remove", "event", e.Name(), "subscriber_id", subID)
			return
		case <-ctx.Done():
			logger.Info("subscriber remove", "event", e.Name(), "subscriber_id", subID)
			return
		case out, ok := <-coal.output:
			if !ok {
				return
			}

			func() {
				bus.inflightWG.Add(1)
				defer bus.inflightWG.Done()

				// Build context and call handler directly (payload already decoded).
				handlerCtx := contextWithInfo(out.msg.Context(), contextInfo{
					id: out.msg.ID(), name: e.name, source: bus.ID(), subID: sub.ID(),
					metadata: out.msg.Metadata(), msgTime: out.msg.Timestamp(),
					logger: logger, bus: bus, mode: subOpts.mode,
					subscriberName: subOpts.subscriberName, subscriberDescription: subOpts.subscriberDescription,
					coalescedCount: out.count,
					workerGroup: subOpts.workerGroup,
				})
				handlerCtx = ContextWithRawPayload(handlerCtx, out.msg.Payload())

				// Best-effort: ack before handler (at-most-once delivery).
				if bestEffort {
					_ = out.msg.Ack(nil)
				}

				handlerErr := wrappedHandler(handlerCtx, e, out.value)

				if bestEffort {
					if handlerErr != nil {
						logger.Warn("best-effort handler error suppressed",
							"event", e.Name(), "msg_id", out.msg.ID(), "error", handlerErr)
					}
				} else {
					e.handleResult(out.msg, handlerCtx, handlerErr, logger)
				}
			}()

			// Signal coalescer that this key is done.
			select {
			case coal.done <- out.key:
			case <-coal.stopped:
			}
		}
	}
}

// processMessage handles a single message through the full decode + handler + ack pipeline.
// coalescedCount is the number of messages that were superseded (0 for non-coalesced).
func (e *eventImpl[T]) processMessage(
	msg transport.Message,
	bus *Bus,
	sub transport.Subscription,
	subOpts *subscribeOptions[T],
	wrappedHandler Handler[T],
	logger *slog.Logger,
	bestEffort bool,
	coalescedCount int,
) {
	subID := sub.ID()

	// Check for transport-level decode errors
	if decodeErrMsg, isDecodeErr := transport.IsDecodeError(msg.Metadata()); isDecodeErr {
		decodeErr := errors.New(decodeErrMsg)
		logger.Error("transport decode error, routing to DLQ",
			"event", e.Name(),
			"msg_id", msg.ID(),
			"error", decodeErrMsg)

		if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, decodeErr); dlqErr != nil {
			bus.logFallbackDLQ(logger, e.name, msg, decodeErr, dlqErr)
		}
		_ = msg.Ack(nil)
		return
	}

	// Apply pre-decode message filter
	if e.messageFilter != nil && !e.messageFilter(msg.Metadata()) {
		_ = msg.Ack(nil)
		return
	}

	// Best-effort: auto-ack on receive.
	if bestEffort {
		_ = msg.Ack(nil)
	}

	// Decode payload from bytes
	var typedData T
	contentType := msg.Metadata()[MetadataContentType]
	if contentType == "" {
		contentType = "application/json" // default
	}

	codec, codecOk := payload.Get(contentType)
	if !codecOk {
		contentErr := fmt.Errorf("unknown content type: %s", contentType)
		logger.Error("unknown content type, routing to DLQ",
			"event", e.Name(),
			"msg_id", msg.ID(),
			"content_type", contentType)

		if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, contentErr); dlqErr != nil {
			bus.logFallbackDLQ(logger, e.name, msg, contentErr, dlqErr)
		}
		if !bestEffort {
			_ = msg.Ack(nil)
		}
		return
	}

	if err := codec.Decode(msg.Payload(), &typedData); err != nil {
		if e.decodeErrorHandler == nil {
			// Default behavior: route to DLQ and ack
			logger.Error("decode error received, routing to DLQ",
				"event", e.Name(),
				"msg_id", msg.ID(),
				"error", err)

			if dlqErr := bus.sendToDLQ(context.Background(), e.name, msg, err); dlqErr != nil {
				bus.logFallbackDLQ(logger, e.name, msg, err, dlqErr)
			}
			if !bestEffort {
				_ = msg.Ack(nil)
			}
			return
		}

		// Custom decode error handler decides the action
		decodeCtx := contextWithInfo(detachedContext(msg.Context()), contextInfo{
			id: msg.ID(), name: e.name, source: bus.ID(), subID: subID,
			metadata: msg.Metadata(), msgTime: msg.Timestamp(),
			logger: logger, bus: bus, mode: subOpts.mode,
			subscriberName: subOpts.subscriberName, subscriberDescription: subOpts.subscriberDescription,
			workerGroup: subOpts.workerGroup,
		})
		decodeResult := e.decodeErrorHandler(decodeCtx, msg, err)

		if bestEffort {
			if decodeResult != nil {
				logger.Warn("best-effort decode error handler returned error, suppressed",
					"event", e.Name(), "msg_id", msg.ID(), "error", decodeResult)
			}
			return
		}

		result, sendToDLQ := classifyResult(decodeResult, msg.RetryCount(), e.maxRetries)

		if sendToDLQ {
			if dlqErr := bus.sendToDLQ(decodeCtx, e.name, msg, err); dlqErr != nil {
				logger.Error("DLQ store failed for decode error, message will be retried",
					"event", e.Name(),
					"msg_id", msg.ID(),
					"error", dlqErr,
					"decode_error", err)
				_ = msg.Ack(fmt.Errorf("DLQ storage failed: %w", dlqErr))
				return
			}
		}

		switch result {
		case ResultAck, ResultReject:
			_ = msg.Ack(nil)
		case ResultNack:
			_ = msg.Ack(err)
		case ResultDefer:
			_ = msg.Ack(err)
		}
		return
	}

	// Update context values and call handler
	handlerCtx := contextWithInfo(msg.Context(), contextInfo{
		id: msg.ID(), name: e.name, source: bus.ID(), subID: subID,
		metadata: msg.Metadata(), msgTime: msg.Timestamp(),
		logger: logger, bus: bus, mode: subOpts.mode,
		subscriberName: subOpts.subscriberName, subscriberDescription: subOpts.subscriberDescription,
		coalescedCount: coalescedCount,
		workerGroup: subOpts.workerGroup,
	})
	handlerCtx = ContextWithRawPayload(handlerCtx, msg.Payload())
	handlerErr := wrappedHandler(handlerCtx, e, typedData)

	if bestEffort {
		// Already acked. Log errors and move on.
		if handlerErr != nil {
			logger.Warn("best-effort handler error suppressed",
				"event", e.Name(), "msg_id", msg.ID(), "error", handlerErr)
		}
		return
	}

	e.handleResult(msg, handlerCtx, handlerErr, logger)
}

// handleResult classifies the handler error and performs ack/nack/DLQ routing.
func (e *eventImpl[T]) handleResult(
	msg transport.Message,
	handlerCtx context.Context,
	handlerErr error,
	logger *slog.Logger,
) {
	result, sendToDLQ := classifyResult(handlerErr, msg.RetryCount(), e.maxRetries)

	// Send to DLQ if needed
	if sendToDLQ {
		bus := e.bus.Load()
		if bus != nil {
			if dlqErr := bus.sendToDLQ(handlerCtx, e.name, msg, handlerErr); dlqErr != nil {
				// DLQ storage failed - DON'T ACK, let message be redelivered
				logger.Error("DLQ store failed, message will be retried",
					"event", e.Name(),
					"msg_id", msg.ID(),
					"error", dlqErr,
					"original_error", handlerErr)
				_ = msg.Ack(fmt.Errorf("DLQ storage failed: %w", dlqErr))
				return
			}
		}
	}

	// Ack based on result
	switch result {
	case ResultAck, ResultReject:
		// Acknowledge (remove from queue)
		_ = msg.Ack(nil)
	case ResultNack:
		// Retry immediately
		_ = msg.Ack(handlerErr)
	case ResultDefer:
		// Retry with backoff (transport handles this)
		_ = msg.Ack(handlerErr)
	}
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

// Name returns a comma-joined display name for all events in the collection.
// This is intended for logging and diagnostics only — it does not follow the
// busname://eventname convention used for individual events.
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

// Discard creates a no-op event that discards all published data.
// Useful as a default or placeholder when an event should be inactive.
func Discard[T any]() Event[T] {
	return discardEvent[T]{}
}

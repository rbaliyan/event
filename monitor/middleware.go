package monitor

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3"
	"go.opentelemetry.io/otel/trace"
)

// MiddlewareOption configures the monitor middleware.
type MiddlewareOption func(*middlewareOptions)

// middlewareOptions holds configuration for the monitor middleware.
type middlewareOptions struct {
	mode       *DeliveryMode
	instanceID string
}

// WithMiddlewareMode sets an explicit delivery mode for the middleware.
// When nil (default), the mode is auto-detected from the subscription context.
func WithMiddlewareMode(mode DeliveryMode) MiddlewareOption {
	return func(o *middlewareOptions) {
		o.mode = &mode
	}
}

// WithMiddlewareInstanceID sets the instance identifier recorded on each entry.
// This is typically the Kubernetes pod name or hostname, used to correlate
// monitor entries to a specific process instance.
func WithMiddlewareInstanceID(id string) MiddlewareOption {
	return func(o *middlewareOptions) {
		o.instanceID = id
	}
}

// NewMiddleware creates a middleware that records event processing in the monitor store.
//
// Options:
//   - WithMiddlewareMode: Set explicit delivery mode (default: auto-detect from context)
//   - WithMiddlewareInstanceID: Set instance identifier for pod/host correlation
//
// The middleware:
//  1. Records a pending entry when the handler starts
//  2. Executes the handler
//  3. Updates the entry with the final status based on error classification
//
// Example:
//
//	store := monitor.NewPostgresStore(db)
//	orderEvent.Subscribe(ctx, handler,
//	    event.AsWorker[Order](),
//	    event.WithMiddleware(monitor.NewMiddleware[Order](store,
//	        monitor.WithMiddlewareMode(monitor.WorkerPool),
//	        monitor.WithMiddlewareInstanceID(os.Getenv("POD_NAME")),
//	    )),
//	)
func NewMiddleware[T any](store Store, opts ...MiddlewareOption) event.Middleware[T] {
	o := &middlewareOptions{}
	for _, opt := range opts {
		opt(o)
	}

	return func(next event.Handler[T]) event.Handler[T] {
		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Extract context data
			eventID := event.ContextEventID(ctx)
			subscriptionID := event.ContextSubscriptionID(ctx)
			eventName := event.ContextName(ctx)
			busID := event.ContextSource(ctx)
			metadata := event.ContextMetadata(ctx)
			subscriberName := event.ContextSubscriberName(ctx)
			subscriberDescription := event.ContextSubscriberDescription(ctx)
			workerGroup := event.ContextWorkerGroup(ctx)

			// Determine delivery mode
			deliveryMode := detectDeliveryMode(ctx, o.mode)

			// For WorkerPool mode, subscription ID is recorded but not key
			subIDForEntry := subscriptionID
			if deliveryMode == WorkerPool {
				subIDForEntry = ""
			}

			// Create initial entry
			entry := &Entry{
				EventID:               eventID,
				SubscriptionID:        subIDForEntry,
				SubscriberName:        subscriberName,
				SubscriberDescription: subscriberDescription,
				EventName:             eventName,
				BusID:                 busID,
				InstanceID:            o.instanceID,
				DeliveryMode:          deliveryMode,
				Metadata:              metadata,
				Status:                StatusPending,
				StartedAt:             time.Now(),
				WorkerGroup:           workerGroup,
			}

			// Extract trace context if available
			if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
				entry.TraceID = span.SpanContext().TraceID().String()
				entry.SpanID = span.SpanContext().SpanID().String()
			}

			// Record start (best effort - don't fail the handler if monitor fails)
			if err := store.Record(ctx, entry); err != nil {
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor record failed", "error", err)
				}
			}

			// For WorkerPool mode, inject an acquisition signal so the
			// WorkerPoolMiddleware (which runs after us) can communicate
			// whether this worker actually acquired the message.
			var signal *event.AcquisitionSignal
			if deliveryMode == WorkerPool {
				signal = &event.AcquisitionSignal{}
				ctx = event.ContextWithAcquisitionSignal(ctx, signal)
			}

			// Execute handler
			start := time.Now()
			handlerErr := next(ctx, ev, data)
			duration := time.Since(start)

			// In WorkerPool mode, skip recording if this worker didn't acquire the message.
			// The winning worker will record the actual result.
			if signal != nil && signal.Result() == event.AcquisitionSkipped {
				return handlerErr
			}

			// Determine final status based on error classification
			status := StatusCompleted
			if handlerErr != nil {
				result := event.ClassifyError(handlerErr)
				switch result {
				case event.ResultNack, event.ResultDefer:
					status = StatusRetrying
				case event.ResultReject:
					status = StatusFailed
				case event.ResultAck:
					status = StatusCompleted
				default:
					status = StatusRetrying
				}
			}

			// Update entry with final status (best effort)
			if err := store.UpdateStatus(ctx, eventID, subIDForEntry, status, handlerErr, duration); err != nil {
				logger := event.ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor update failed", "error", err)
				}
			}

			return handlerErr
		}
	}
}

// Middleware creates a middleware that records event processing in the monitor store.
//
// The delivery mode is automatically detected from the subscription context.
// Use NewMiddleware for more configuration options.
//
// Example:
//
//	store := monitor.NewPostgresStore(db)
//	orderEvent.Subscribe(ctx, handler,
//	    event.AsWorker[Order](),
//	    event.WithMiddleware(monitor.Middleware[Order](store)),
//	)
func Middleware[T any](store Store) event.Middleware[T] {
	return NewMiddleware[T](store)
}

// MiddlewareWithMode creates a middleware with explicit mode configuration.
//
// Use this when mode auto-detection is not desired or when you want to
// force a specific mode regardless of the subscription configuration.
//
// Deprecated: Use NewMiddleware with WithMiddlewareMode instead.
func MiddlewareWithMode[T any](store Store, mode *DeliveryMode) event.Middleware[T] {
	if mode != nil {
		return NewMiddleware[T](store, WithMiddlewareMode(*mode))
	}
	return NewMiddleware[T](store)
}

// detectDeliveryMode determines the delivery mode from context or explicit configuration.
func detectDeliveryMode(ctx context.Context, explicitMode *DeliveryMode) DeliveryMode {
	if explicitMode != nil {
		return *explicitMode
	}

	// Auto-detect from context
	eventMode := event.ContextDeliveryMode(ctx)
	if eventMode == event.WorkerPool {
		return WorkerPool
	}
	return Broadcast
}

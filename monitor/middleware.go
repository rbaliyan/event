package monitor

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// Middleware creates a middleware that records event processing in the monitor store.
//
// The delivery mode is automatically detected from the subscription context.
// Use MiddlewareWithMode for explicit mode configuration.
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
//	    event.WithMiddleware(monitor.Middleware[Order](store)),
//	)
func Middleware[T any](store Store) event.Middleware[T] {
	return MiddlewareWithMode[T](store, nil)
}

// MiddlewareWithMode creates a middleware with explicit mode configuration.
//
// Use this when mode auto-detection is not desired or when you want to
// force a specific mode regardless of the subscription configuration.
//
// Parameters:
//   - store: The monitor store to write entries to
//   - mode: Explicit delivery mode (nil = auto-detect from context)
//
// Example:
//
//	// Force WorkerPool mode
//	middleware := monitor.MiddlewareWithMode[Order](store, &monitor.WorkerPool)
func MiddlewareWithMode[T any](store Store, mode *DeliveryMode) event.Middleware[T] {
	return func(next event.Handler[T]) event.Handler[T] {
		return func(ctx context.Context, ev event.Event[T], data T) error {
			// Extract context data
			eventID := event.ContextEventID(ctx)
			subscriptionID := event.ContextSubscriptionID(ctx)
			eventName := event.ContextName(ctx)
			busID := event.ContextSource(ctx)
			metadata := event.ContextMetadata(ctx)

			// Determine delivery mode
			deliveryMode := detectDeliveryMode(ctx, mode)

			// For WorkerPool mode, subscription ID is recorded but not key
			subIDForEntry := subscriptionID
			if deliveryMode == WorkerPool {
				subIDForEntry = ""
			}

			// Create initial entry
			entry := &Entry{
				EventID:        eventID,
				SubscriptionID: subIDForEntry,
				EventName:      eventName,
				BusID:          busID,
				DeliveryMode:   deliveryMode,
				Metadata:       metadata,
				Status:         StatusPending,
				StartedAt:      time.Now(),
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

			// Execute handler
			start := time.Now()
			handlerErr := next(ctx, ev, data)
			duration := time.Since(start)

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

// detectDeliveryMode determines the delivery mode from context or explicit configuration.
func detectDeliveryMode(ctx context.Context, explicitMode *DeliveryMode) DeliveryMode {
	if explicitMode != nil {
		return *explicitMode
	}

	// Auto-detect from context
	transportMode := event.ContextDeliveryMode(ctx)
	if transportMode == transport.WorkerPool {
		return WorkerPool
	}
	return Broadcast
}

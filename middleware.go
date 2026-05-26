package event

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/schema"
	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// IdempotencyStore is a minimal interface for idempotency tracking in middleware.
//
// This is an alias of transport.IdempotencyStore, the canonical definition.
//
// Implementations:
//   - idempotency.NewMemoryStore(): In-memory store for single-instance deployments
//   - idempotency.NewRedisStore(): Distributed store for multi-instance deployments
//   - idempotency.NewPostgresStore(): SQL-based store with transaction support
type IdempotencyStore = transport.IdempotencyStore

// PoisonDetector detects and quarantines repeatedly failing messages.
//
// This is an alias of transport.PoisonDetector, the canonical definition.
//
// Implementations:
//   - poison.NewDetector(poison.NewMemoryStore()): In-memory for single-instance
//   - poison.NewDetector(poison.NewRedisStore(client)): Distributed detection
//   - poison.NewDetector(poison.NewPostgresStore(db)): SQL-based detection
type PoisonDetector = transport.PoisonDetector

// DeduplicationStore is an interface for storing seen message IDs.
//
// DeduplicationStore provides simple TTL-based deduplication, which is lighter
// weight than IdempotencyStore. Use this when you need basic duplicate detection
// without the full exactly-once processing guarantees.
//
// The built-in implementation NewInMemoryDeduplicationStore provides an in-memory
// store with configurable TTL and max size.
//
// Example:
//
//	store := event.NewInMemoryDeduplicationStore(time.Hour, 10000)
//	ev.Subscribe(ctx, handler, event.WithMiddleware(
//	    event.DeduplicationMiddleware[Order](store),
//	))
type DeduplicationStore interface {
	// IsSeen checks if a message ID has been seen before.
	// Returns true if the message should be skipped (already processed).
	IsSeen(ctx context.Context, messageID string) (bool, error)

	// MarkSeen marks a message ID as seen.
	// The store should automatically expire entries after a configured TTL.
	MarkSeen(ctx context.Context, messageID string) error
}

// CloseableDeduplicationStore extends DeduplicationStore with a Close method
// for stores that use background resources (goroutines, timers).
type CloseableDeduplicationStore interface {
	DeduplicationStore
	// Close stops background cleanup resources. Call when the store is no longer needed.
	Close()
}

// inMemoryDeduplicationStore is a simple in-memory deduplication store with TTL
type inMemoryDeduplicationStore struct {
	mu      sync.RWMutex
	seen    map[string]time.Time
	ttl     time.Duration
	maxSize int
	stopCh  chan struct{}
}

// NewInMemoryDeduplicationStore creates a new in-memory deduplication store.
// ttl: how long to remember a message ID (default: 1 hour)
// maxSize: maximum number of entries to store (default: 10000)
//
// Call Close() when the store is no longer needed to stop the background
// cleanup goroutine.
func NewInMemoryDeduplicationStore(ttl time.Duration, maxSize int) CloseableDeduplicationStore {
	if ttl <= 0 {
		ttl = time.Hour
	}
	if maxSize <= 0 {
		maxSize = 10000
	}

	store := &inMemoryDeduplicationStore{
		seen:    make(map[string]time.Time),
		ttl:     ttl,
		maxSize: maxSize,
		stopCh:  make(chan struct{}),
	}

	// Start cleanup goroutine
	go store.cleanup()

	return store
}

func (s *inMemoryDeduplicationStore) IsSeen(ctx context.Context, messageID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seenAt, exists := s.seen[messageID]
	if !exists {
		return false, nil
	}

	// Check if entry has expired
	if time.Since(seenAt) > s.ttl {
		return false, nil
	}

	return true, nil
}

func (s *inMemoryDeduplicationStore) MarkSeen(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If at max capacity, remove oldest entries
	if s.maxSize > 0 && len(s.seen) >= s.maxSize {
		// Find and remove expired entries first
		now := time.Now()
		for id, seenAt := range s.seen {
			if now.Sub(seenAt) > s.ttl {
				delete(s.seen, id)
			}
		}

		// If still at capacity, remove oldest 10% using slices.SortFunc (O(n log n))
		// instead of O(n*k) selection sort.
		if len(s.seen) >= s.maxSize {
			toRemove := s.maxSize / 10
			if toRemove == 0 {
				toRemove = 1
			}
			type entry struct {
				id     string
				seenAt time.Time
			}
			entries := make([]entry, 0, len(s.seen))
			for id, seenAt := range s.seen {
				entries = append(entries, entry{id, seenAt})
			}
			slices.SortFunc(entries, func(a, b entry) int {
				return a.seenAt.Compare(b.seenAt)
			})
			for i := 0; i < toRemove && i < len(entries); i++ {
				delete(s.seen, entries[i].id)
			}
		}
	}

	s.seen[messageID] = time.Now()
	return nil
}

// Close stops the background cleanup goroutine.
func (s *inMemoryDeduplicationStore) Close() {
	select {
	case <-s.stopCh:
		// Already closed
	default:
		close(s.stopCh)
	}
}

func (s *inMemoryDeduplicationStore) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for id, seenAt := range s.seen {
				if now.Sub(seenAt) > s.ttl {
					delete(s.seen, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

// DeduplicationMiddleware creates a middleware that prevents duplicate message processing.
// Messages with the same ID will be skipped if they've been processed within the TTL window.
//
// Example usage:
//
//	store := event.NewInMemoryDeduplicationStore(time.Hour, 10000)
//	ev.Subscribe(ctx, handler, event.WithMiddleware(event.DeduplicationMiddleware[string](store)))
func DeduplicationMiddleware[T any](store DeduplicationStore) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			// Get message ID from context
			messageID := ContextEventID(ctx)
			if messageID == "" {
				// No message ID, can't deduplicate - proceed with handler
				return next(ctx, ev, data)
			}

			// Check if already seen
			seen, err := store.IsSeen(ctx, messageID)
			if err != nil {
				// Store error - log and proceed with processing
				ContextLogger(ctx).Warn("deduplication store error", "error", err)
				return next(ctx, ev, data)
			}

			if seen {
				// Duplicate message - skip processing and ack
				ContextLogger(ctx).Debug("skipping duplicate message", "message_id", messageID)
				return nil // Ack without processing
			}

			// Process message
			err = next(ctx, ev, data)

			// Only mark as seen if processing succeeded
			if err == nil {
				if markErr := store.MarkSeen(ctx, messageID); markErr != nil {
					ContextLogger(ctx).Warn("failed to mark message as seen", "error", markErr)
				}
			}

			return err
		}
	}
}

// IdempotencyMiddleware creates a middleware that prevents duplicate message processing.
// Uses IdempotencyStore to check and mark messages as processed.
//
// Example usage:
//
//	store, err := idempotency.NewRedisStore(redisClient, time.Hour)
//	if err != nil { log.Fatal(err) }
//	ev.Subscribe(ctx, handler, event.WithMiddleware(event.IdempotencyMiddleware[Order](store)))
func IdempotencyMiddleware[T any](store IdempotencyStore) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			messageID := ContextEventID(ctx)
			if messageID == "" {
				return next(ctx, ev, data)
			}

			// Check if already processed
			isDuplicate, err := store.IsDuplicate(ctx, messageID)
			if err != nil {
				ContextLogger(ctx).Warn("idempotency check failed", "error", err)
				return next(ctx, ev, data)
			}
			if isDuplicate {
				ContextLogger(ctx).Debug("skipping duplicate message", "message_id", messageID)
				return nil
			}

			// Process message
			err = next(ctx, ev, data)

			// Only mark as processed on success
			if err == nil {
				if markErr := store.MarkProcessed(ctx, messageID); markErr != nil {
					ContextLogger(ctx).Warn("failed to mark as processed", "error", markErr)
				}
			}

			return err
		}
	}
}

// RecordStartParams contains the parameters for recording the start of event processing.
type RecordStartParams struct {
	EventID               string
	SubscriptionID        string
	EventName             string
	BusID                 string
	WorkerPool            bool
	Metadata              map[string]string
	TraceID               string
	SpanID                string
	SubscriberName        string
	SubscriberDescription string
	WorkerGroup           string
}

// MonitorStore is a minimal interface for event processing monitoring in middleware.
//
// This interface provides the two methods needed by MonitorMiddleware to record
// event processing metrics. Monitor stores from the monitor package implement
// both this interface AND the full monitor.Store interface (which includes
// List, Get, Count, and other query methods).
//
// The separation allows middleware to use a simple interface while stores
// can provide full query capabilities for the monitor HTTP/gRPC APIs.
//
// Implementations:
//   - monitor.NewMemoryStore(): In-memory store for development/testing
//   - monitor.NewPostgresStore(db): PostgreSQL-based persistent store
//   - For MongoDB, use the separate event-mongodb module (https://github.com/rbaliyan/event-mongodb)
//
// Example:
//
//	store, err := monitor.NewPostgresStore(db)
//	if err != nil { log.Fatal(err) }
//	ev.Subscribe(ctx, handler, event.WithMiddleware(
//	    event.MonitorMiddleware[Order](store),
//	))
type MonitorStore interface {
	// RecordStart records when event processing begins.
	// params.WorkerPool indicates the delivery mode (true = WorkerPool, false = Broadcast)
	// params.SubscriberName and params.SubscriberDescription are optional human-readable identifiers
	// params.WorkerGroup is the worker group name (empty for broadcast or default group)
	RecordStart(ctx context.Context, params RecordStartParams) error

	// RecordComplete updates the entry with the final result.
	// Status: "completed" (success), "failed" (rejected), "retrying" (will retry)
	RecordComplete(ctx context.Context, params RecordCompleteParams) error
}

// RecordPublishParams contains the parameters for recording a successful event publish.
type RecordPublishParams struct {
	EventID     string
	EventName   string
	BusID       string // bus instance ID (unique per process)
	BusName     string // bus name (human-readable)
	PayloadSize int
	Metadata    map[string]string
	TraceID     string
	SpanID      string
}

// PublishAuditStore records successful publish attempts made through Bus.Send.
//
// The Bus calls RecordPublish after each successful transport.Publish call.
// This closes the gap in the monitoring system: if an event has no Entry in
// the monitor store, cross-referencing the publish audit reveals whether the
// event was ever published (transport fault) or never fired at all (app bug).
//
// Implementations:
//   - Any monitor.Store value (monitor.NewMemoryStore(),
//     monitor.NewPostgresStore(db)) satisfies this interface. The
//     stack.WithReliabilityStack convenience option promotes the
//     configured monitor store to also serve as the publish-audit store
//     automatically.
type PublishAuditStore interface {
	RecordPublish(ctx context.Context, params RecordPublishParams) error
}

// SchemaProvider is an alias for schema.SchemaProvider.
//
// This type alias exists for backward compatibility. New code should
// import and use schema.SchemaProvider directly from the schema package.
//
// SchemaProvider abstracts schema storage, implemented by transports
// (with retention) or database stores (PostgreSQL, MongoDB, Redis).
type SchemaProvider = schema.SchemaProvider

// EventSchema is an alias for schema.EventSchema.
//
// This type alias exists for backward compatibility. New code should
// import and use schema.EventSchema directly from the schema package.
//
// EventSchema defines processing configuration for an event including
// timeouts, retries, and feature flags (monitor, idempotency, poison).
type EventSchema = schema.EventSchema

// SchemaChangeEvent is an alias for schema.SchemaChangeEvent.
//
// This type alias exists for backward compatibility. New code should
// import and use schema.SchemaChangeEvent directly from the schema package.
//
// SchemaChangeEvent is published when a schema is updated, enabling
// real-time schema synchronization across distributed systems.
type SchemaChangeEvent = schema.SchemaChangeEvent

// RecordCompleteParams contains the parameters for recording the completion of event processing.
type RecordCompleteParams struct {
	EventID        string
	SubscriptionID string
	Status         string
	Error          error
	Duration       time.Duration
}

// MonitorMiddleware creates a middleware that records event processing metrics.
// Records start time, duration, status, and any errors for each event processed.
//
// Example usage:
//
//	store, err := monitor.NewPostgresStore(db)
//	if err != nil { log.Fatal(err) }
//	ev.Subscribe(ctx, handler, event.WithMiddleware(event.MonitorMiddleware[Order](store)))
func MonitorMiddleware[T any](store MonitorStore) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			eventID := ContextEventID(ctx)
			subscriptionID := ContextSubscriptionID(ctx)
			eventName := ContextName(ctx)
			busID := ContextSource(ctx)
			metadata := ContextMetadata(ctx)
			workerPool := ContextDeliveryMode(ctx) == WorkerPool
			subscriberName := ContextSubscriberName(ctx)
			subscriberDescription := ContextSubscriberDescription(ctx)

			// For WorkerPool mode, subscription ID is not part of the key
			subIDForEntry := subscriptionID
			if workerPool {
				subIDForEntry = ""
			}

			// Extract trace context from OpenTelemetry span
			var traceID, spanID string
			if span := trace.SpanContextFromContext(ctx); span.IsValid() {
				traceID = span.TraceID().String()
				spanID = span.SpanID().String()
			}

			// Record start (best effort)
			workerGroup := ContextWorkerGroup(ctx)
			if err := store.RecordStart(ctx, RecordStartParams{
				EventID:               eventID,
				SubscriptionID:        subIDForEntry,
				EventName:             eventName,
				BusID:                 busID,
				WorkerPool:            workerPool,
				Metadata:              metadata,
				TraceID:               traceID,
				SpanID:                spanID,
				SubscriberName:        subscriberName,
				SubscriberDescription: subscriberDescription,
				WorkerGroup:           workerGroup,
			}); err != nil {
				logger := ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor record start failed", "error", err)
				}
			}

			// For WorkerPool mode, inject an acquisition signal so the
			// WorkerPoolMiddleware can communicate whether this worker
			// actually acquired the message.
			var signal *AcquisitionSignal
			if workerPool {
				signal = &AcquisitionSignal{}
				ctx = ContextWithAcquisitionSignal(ctx, signal)
			}

			// Execute handler
			start := time.Now()
			handlerErr := next(ctx, ev, data)
			duration := time.Since(start)

			// In WorkerPool mode, skip recording if this worker didn't acquire the message.
			// The winning worker will record the actual result.
			if signal != nil && signal.Result() == AcquisitionSkipped {
				return handlerErr
			}

			// Determine status
			status := "completed"
			if handlerErr != nil {
				result := ClassifyError(handlerErr)
				switch result {
				case ResultNack, ResultDefer:
					status = "retrying"
				case ResultReject:
					status = "failed"
				case ResultAck:
					status = "completed"
				default:
					status = "retrying"
				}
			}

			// Record complete (best effort)
			if err := store.RecordComplete(ctx, RecordCompleteParams{
				EventID:        eventID,
				SubscriptionID: subIDForEntry,
				Status:         status,
				Error:          handlerErr,
				Duration:       duration,
			}); err != nil {
				logger := ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor record complete failed", "error", err)
				}
			}

			return handlerErr
		}
	}
}

// BestEffortMiddleware creates a middleware that suppresses all handler errors.
// The wrapped handler always returns nil (ack), and errors are logged at warn level.
//
// This is a composable alternative to WithBestEffort / WithAckPolicy(AckOnReceive).
// Use this when you want to suppress errors for a specific middleware position
// rather than changing the overall subscription ack policy.
//
// Example:
//
//	ev.Subscribe(ctx, handler, event.WithMiddleware(
//	    event.BestEffortMiddleware[Order](),
//	))
func BestEffortMiddleware[T any]() Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			if err := next(ctx, ev, data); err != nil {
				logger := ContextLogger(ctx)
				if logger != nil {
					logger.Warn("best-effort handler error suppressed",
						"event", ev.Name(), "error", err)
				}
			}
			return nil
		}
	}
}

// PoisonMiddleware creates a middleware that detects and quarantines poison messages.
// Poison messages are messages that repeatedly fail processing.
//
// Example usage:
//
//	detector := poison.NewDetector(poison.NewRedisStore(redisClient))
//	ev.Subscribe(ctx, handler, event.WithMiddleware(event.PoisonMiddleware[Order](detector)))
func PoisonMiddleware[T any](detector PoisonDetector) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			messageID := ContextEventID(ctx)
			if messageID == "" {
				return next(ctx, ev, data)
			}

			// Check if message is quarantined
			isPoisoned, err := detector.Check(ctx, messageID)
			if err != nil {
				ContextLogger(ctx).Warn("poison check failed", "error", err)
				// Continue processing on check failure
			} else if isPoisoned {
				ContextLogger(ctx).Debug("skipping quarantined message", "message_id", messageID)
				return nil // Ack and skip
			}

			// Process message
			err = next(ctx, ev, data)

			// Record result
			if err == nil {
				if successErr := detector.RecordSuccess(ctx, messageID); successErr != nil {
					ContextLogger(ctx).Warn("failed to record success", "error", successErr)
				}
			} else {
				quarantined, failErr := detector.RecordFailure(ctx, messageID)
				if failErr != nil {
					ContextLogger(ctx).Warn("failed to record failure", "error", failErr)
				} else if quarantined {
					ContextLogger(ctx).Warn("message quarantined after repeated failures", "message_id", messageID)
				}
			}

			return err
		}
	}
}

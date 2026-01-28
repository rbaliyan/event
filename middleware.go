package event

import (
	"context"
	"sync"
	"sync/atomic"
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
	mu       sync.RWMutex
	seen     map[string]time.Time
	ttl      time.Duration
	maxSize  int
	stopCh   chan struct{}
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

		// If still at capacity, remove oldest 10% by finding the oldest entries
		if len(s.seen) >= s.maxSize {
			toRemove := s.maxSize / 10
			if toRemove == 0 {
				toRemove = 1
			}
			// Collect entries and sort by time to find the oldest
			type entry struct {
				id     string
				seenAt time.Time
			}
			entries := make([]entry, 0, len(s.seen))
			for id, seenAt := range s.seen {
				entries = append(entries, entry{id, seenAt})
			}
			// Partial sort: find the toRemove oldest entries
			// Simple selection: iterate and find oldest toRemove entries
			for i := 0; i < toRemove && i < len(entries); i++ {
				minIdx := i
				for j := i + 1; j < len(entries); j++ {
					if entries[j].seenAt.Before(entries[minIdx].seenAt) {
						minIdx = j
					}
				}
				entries[i], entries[minIdx] = entries[minIdx], entries[i]
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

// CircuitState represents the state of the circuit breaker
type CircuitState int

const (
	// CircuitClosed means the circuit is functioning normally
	CircuitClosed CircuitState = iota
	// CircuitOpen means the circuit is open due to failures (requests fail fast)
	CircuitOpen
	// CircuitHalfOpen means the circuit is testing if the service recovered
	CircuitHalfOpen
)

// CircuitBreaker provides circuit breaker functionality for event handlers.
// When failures exceed a threshold, the circuit opens and requests fail fast.
// In half-open state, only one probe request is allowed at a time to prevent
// overwhelming a recovering service.
type CircuitBreaker struct {
	mu sync.RWMutex

	// configuration
	failureThreshold int           // number of failures before opening
	successThreshold int           // number of successes needed to close from half-open
	timeout          time.Duration // how long to wait before trying half-open

	// state (unexported to prevent external modification)
	state          CircuitState
	failures       int
	successes      int
	lastStateTime  time.Time
	halfOpenProbes int32 // atomic counter for concurrent half-open probes
}

// NewCircuitBreaker creates a new circuit breaker.
// failureThreshold: number of consecutive failures before opening (default: 5)
// successThreshold: number of consecutive successes in half-open before closing (default: 2)
// timeout: time to wait before attempting half-open (default: 30s)
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	if successThreshold <= 0 {
		successThreshold = 2
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		state:            CircuitClosed,
		lastStateTime:    time.Now(),
	}
}

// State returns the current circuit state
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// OpenUntil returns the time when the circuit breaker will transition to half-open.
// Returns zero time if the circuit is not open.
func (cb *CircuitBreaker) OpenUntil() time.Time {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	if cb.state == CircuitOpen {
		return cb.lastStateTime.Add(cb.timeout)
	}
	return time.Time{}
}

// Allow checks if a request should be allowed.
// Returns true if the request can proceed, false if it should fail fast.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if timeout has passed
		if time.Since(cb.lastStateTime) > cb.timeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
			cb.lastStateTime = time.Now()
			return true
		}
		return false
	case CircuitHalfOpen:
		// Limit concurrent probes in half-open state to prevent thundering herd
		if atomic.AddInt32(&cb.halfOpenProbes, 1) > int32(cb.successThreshold) {
			atomic.AddInt32(&cb.halfOpenProbes, -1)
			return false
		}
		return true
	default:
		return true
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0

	if cb.state == CircuitHalfOpen {
		atomic.AddInt32(&cb.halfOpenProbes, -1)
		cb.successes++
		if cb.successes >= cb.successThreshold {
			cb.state = CircuitClosed
			cb.successes = 0
			atomic.StoreInt32(&cb.halfOpenProbes, 0)
			cb.lastStateTime = time.Now()
		}
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.successes = 0
	cb.failures++

	if cb.state == CircuitClosed && cb.failures >= cb.failureThreshold {
		cb.state = CircuitOpen
		cb.lastStateTime = time.Now()
	} else if cb.state == CircuitHalfOpen {
		atomic.AddInt32(&cb.halfOpenProbes, -1)
		// Any failure in half-open goes back to open
		cb.state = CircuitOpen
		atomic.StoreInt32(&cb.halfOpenProbes, 0)
		cb.lastStateTime = time.Now()
	}
}

// CircuitBreakerMiddleware creates a middleware that implements circuit breaker pattern.
// When failures exceed the threshold, subsequent requests fail fast until the timeout.
//
// Example usage:
//
//	cb := event.NewCircuitBreaker(5, 2, 30*time.Second)
//	ev.Subscribe(ctx, handler, event.WithMiddleware(event.CircuitBreakerMiddleware[string](cb)))
func CircuitBreakerMiddleware[T any](cb *CircuitBreaker) Middleware[T] {
	return func(next Handler[T]) Handler[T] {
		return func(ctx context.Context, ev Event[T], data T) error {
			// Check if circuit allows request
			if !cb.Allow() {
				ContextLogger(ctx).Warn("circuit breaker open, failing fast",
					"event", ev.Name(),
					"state", cb.State())
				return &CircuitOpenError{
					Name:      ev.Name(),
					OpenUntil: cb.OpenUntil(),
				}
			}

			// Execute handler
			err := next(ctx, ev, data)

			// Record result
			if err == nil {
				cb.RecordSuccess()
			} else {
				cb.RecordFailure()
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
//	store := idempotency.NewRedisStore(redisClient, time.Hour)
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
//   - monitor.NewMongoStore(db): MongoDB-based persistent store
//
// Example:
//
//	store := monitor.NewPostgresStore(db)
//	ev.Subscribe(ctx, handler, event.WithMiddleware(
//	    event.MonitorMiddleware[Order](store),
//	))
type MonitorStore interface {
	// RecordStart records when event processing begins.
	// workerPool indicates the delivery mode (true = WorkerPool, false = Broadcast)
	RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string,
		workerPool bool, metadata map[string]string, traceID, spanID string) error

	// RecordComplete updates the entry with the final result.
	// status: "completed" (success), "failed" (rejected), "retrying" (will retry)
	RecordComplete(ctx context.Context, eventID, subscriptionID, status string,
		handlerErr error, duration time.Duration) error
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

// MonitorMiddleware creates a middleware that records event processing metrics.
// Records start time, duration, status, and any errors for each event processed.
//
// Example usage:
//
//	store := monitor.NewPostgresStore(db)
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
			if err := store.RecordStart(ctx, eventID, subIDForEntry, eventName, busID,
				workerPool, metadata, traceID, spanID); err != nil {
				logger := ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor record start failed", "error", err)
				}
			}

			// Execute handler
			start := time.Now()
			handlerErr := next(ctx, ev, data)
			duration := time.Since(start)

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
			if err := store.RecordComplete(ctx, eventID, subIDForEntry, status, handlerErr, duration); err != nil {
				logger := ContextLogger(ctx)
				if logger != nil {
					logger.Warn("monitor record complete failed", "error", err)
				}
			}

			return handlerErr
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

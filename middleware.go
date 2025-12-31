package event

import (
	"context"
	"sync"
	"time"
)

// IdempotencyStore is an interface for idempotency tracking.
// Implementations can use in-memory, Redis, PostgreSQL, or other storage backends.
// This interface is compatible with idempotency.Store from the idempotency package.
type IdempotencyStore interface {
	// IsDuplicate checks if a message ID has already been processed.
	// Returns true if the message should be skipped (already processed).
	IsDuplicate(ctx context.Context, messageID string) (bool, error)

	// MarkProcessed marks a message ID as successfully processed.
	MarkProcessed(ctx context.Context, messageID string) error
}

// PoisonDetector is an interface for detecting and handling poison messages.
// Poison messages are messages that repeatedly fail processing.
// This interface is compatible with poison.Detector from the poison package.
type PoisonDetector interface {
	// Check checks if a message is currently quarantined.
	// Returns true if the message is quarantined and should be skipped.
	Check(ctx context.Context, messageID string) (bool, error)

	// RecordFailure records a processing failure for a message.
	// Returns true if the message was just quarantined (threshold reached).
	RecordFailure(ctx context.Context, messageID string) (bool, error)

	// RecordSuccess records a successful processing and clears the failure count.
	RecordSuccess(ctx context.Context, messageID string) error
}

// DeduplicationStore is an interface for storing seen message IDs.
// Implementations can use in-memory, Redis, or other storage backends.
type DeduplicationStore interface {
	// IsSeen checks if a message ID has been seen before.
	// Returns true if the message should be skipped (already processed).
	IsSeen(ctx context.Context, messageID string) (bool, error)

	// MarkSeen marks a message ID as seen.
	// The store should automatically expire entries after a configured TTL.
	MarkSeen(ctx context.Context, messageID string) error
}

// inMemoryDeduplicationStore is a simple in-memory deduplication store with TTL
type inMemoryDeduplicationStore struct {
	mu      sync.RWMutex
	seen    map[string]time.Time
	ttl     time.Duration
	maxSize int
}

// NewInMemoryDeduplicationStore creates a new in-memory deduplication store.
// ttl: how long to remember a message ID (default: 1 hour)
// maxSize: maximum number of entries to store (default: 10000, 0 = unlimited)
func NewInMemoryDeduplicationStore(ttl time.Duration, maxSize int) DeduplicationStore {
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

		// If still at capacity, remove oldest 10%
		if len(s.seen) >= s.maxSize {
			toRemove := s.maxSize / 10
			if toRemove == 0 {
				toRemove = 1
			}
			count := 0
			for id := range s.seen {
				delete(s.seen, id)
				count++
				if count >= toRemove {
					break
				}
			}
		}
	}

	s.seen[messageID] = time.Now()
	return nil
}

func (s *inMemoryDeduplicationStore) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
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
type CircuitBreaker struct {
	mu sync.RWMutex

	// Configuration
	failureThreshold int           // Number of failures before opening
	successThreshold int           // Number of successes needed to close from half-open
	timeout          time.Duration // How long to wait before trying half-open

	// State
	state         CircuitState
	failures      int
	successes     int
	lastStateTime time.Time
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
		cb.successes++
		if cb.successes >= cb.successThreshold {
			cb.state = CircuitClosed
			cb.successes = 0
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
		// Any failure in half-open goes back to open
		cb.state = CircuitOpen
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
				return &CircuitOpenError{Name: ev.Name()}
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

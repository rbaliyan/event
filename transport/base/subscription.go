// Package base provides shared components for transport implementations.
//
// This package contains reusable building blocks that reduce code duplication
// across transport implementations (Redis, Kafka, NATS, etc.).
package base

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// SendResult indicates the outcome of sending a message to a channel.
type SendResult int

const (
	// SendOK indicates the message was sent successfully.
	SendOK SendResult = iota
	// SendClosed indicates the subscription was closed.
	SendClosed
	// SendTimeout indicates the send timed out (message NOT lost if using at-least-once transport).
	SendTimeout
)

// Subscription provides common subscription functionality that can be embedded
// in transport-specific subscription implementations.
//
// Example usage:
//
//	type redisSubscription struct {
//	    base.Subscription
//	    client Client
//	    stream string
//	    // ... redis-specific fields
//	}
//
//	func (s *redisSubscription) Close(ctx context.Context) error {
//	    return s.Subscription.Close(func() error {
//	        // Redis-specific cleanup
//	        if s.isBroadcast {
//	            s.client.XGroupDestroy(ctx, s.stream, s.group)
//	        }
//	        return nil
//	    })
//	}
type Subscription struct {
	id          string
	ch          chan transport.Message
	closedCh    chan struct{}
	closed      int32
	sendTimeout time.Duration
	wg          sync.WaitGroup
	chMu        sync.RWMutex // Protects channel close vs send operations
}

// NewSubscription creates a new base subscription with the given parameters.
func NewSubscription(id string, bufferSize int, sendTimeout time.Duration) *Subscription {
	return &Subscription{
		id:          id,
		ch:          make(chan transport.Message, bufferSize),
		closedCh:    make(chan struct{}),
		sendTimeout: sendTimeout,
	}
}

// ID returns the subscription identifier.
func (s *Subscription) ID() string {
	return s.id
}

// Messages returns the channel for receiving messages.
func (s *Subscription) Messages() <-chan transport.Message {
	return s.ch
}

// Ch returns the send side of the message channel (for internal use by transports).
func (s *Subscription) Ch() chan transport.Message {
	return s.ch
}

// ClosedCh returns the channel that is closed when subscription closes.
func (s *Subscription) ClosedCh() <-chan struct{} {
	return s.closedCh
}

// IsClosed returns true if the subscription has been closed.
func (s *Subscription) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) == 1
}

// WaitGroup returns the wait group for tracking goroutines.
func (s *Subscription) WaitGroup() *sync.WaitGroup {
	return &s.wg
}

// Close closes the subscription and executes the cleanup function.
// The cleanup function is called after signaling close but before waiting for goroutines.
// Returns immediately if already closed.
func (s *Subscription) Close(cleanup func() error) error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil // Already closed
	}

	// Signal close to all goroutines
	close(s.closedCh)

	// Run transport-specific cleanup
	var cleanupErr error
	if cleanup != nil {
		cleanupErr = cleanup()
	}

	// Wait for consumer goroutines to exit
	s.wg.Wait()

	// Acquire write lock to ensure no SendToChannel is in progress
	// This prevents a race where Close() closes s.ch while
	// SendToChannel() is blocked trying to send to it.
	s.chMu.Lock()
	close(s.ch)
	s.chMu.Unlock()

	return cleanupErr
}

// SendToChannel sends a message to the channel with optional timeout.
// Returns SendOK on success, SendClosed if subscription closed, SendTimeout on timeout.
func (s *Subscription) SendToChannel(msg transport.Message) SendResult {
	// Acquire read lock to prevent Close() from closing the channel
	// while we're trying to send to it.
	s.chMu.RLock()
	defer s.chMu.RUnlock()

	// Check if already closed (after acquiring lock to ensure visibility)
	if s.IsClosed() {
		return SendClosed
	}

	if s.sendTimeout > 0 {
		timer := time.NewTimer(s.sendTimeout)
		defer timer.Stop()
		select {
		case <-s.closedCh:
			return SendClosed
		case <-timer.C:
			return SendTimeout
		case s.ch <- msg:
			return SendOK
		}
	}
	select {
	case <-s.closedCh:
		return SendClosed
	case s.ch <- msg:
		return SendOK
	}
}

// SendWithRetry sends a message with exponential backoff on timeout.
// Returns true if message was sent, false if subscription was closed.
//
// The backoff starts at 100ms and doubles up to maxBackoff (default 5s).
// Jitter of ±30% is applied to prevent thundering herd.
func (s *Subscription) SendWithRetry(msg transport.Message, logger *slog.Logger, logFields ...any) bool {
	return s.SendWithRetryConfig(msg, logger, 100*time.Millisecond, 5*time.Second, logFields...)
}

// SendWithRetryConfig sends a message with configurable backoff parameters.
func (s *Subscription) SendWithRetryConfig(msg transport.Message, logger *slog.Logger, initialBackoff, maxBackoff time.Duration, logFields ...any) bool {
	backoff := initialBackoff

	for {
		switch s.SendToChannel(msg) {
		case SendClosed:
			return false
		case SendTimeout:
			jitteredBackoff := transport.Jitter(backoff, 0.3)
			fields := append([]any{"backoff", jitteredBackoff}, logFields...)
			logger.Warn("message send timeout, retrying with backoff", fields...)
			select {
			case <-s.closedCh:
				return false
			case <-time.After(jitteredBackoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		case SendOK:
			return true
		}
	}
}

// Backoff provides exponential backoff with jitter for retry loops.
//
// Backoff is NOT safe for concurrent use. It is designed to be used
// within a single goroutine (e.g., a poll loop or retry loop).
//
// This is distinct from backoff.Strategy which is a stateless interface
// for calculating delays from an attempt number. Backoff is a stateful
// helper that tracks its own current delay and provides convenience
// methods like Wait() for transport consumer loops. Both serve different
// use cases: Strategy for general retry logic, Backoff for transport internals.
type Backoff struct {
	current time.Duration
	initial time.Duration
	max     time.Duration
	factor  float64
}

// NewBackoff creates a new backoff helper with default settings.
// Default: initial=100ms, max=30s, jitter factor=0.3
func NewBackoff() *Backoff {
	return &Backoff{
		current: 100 * time.Millisecond,
		initial: 100 * time.Millisecond,
		max:     30 * time.Second,
		factor:  0.3,
	}
}

// NewBackoffWithConfig creates a new backoff helper with custom settings.
func NewBackoffWithConfig(initial, max time.Duration, jitterFactor float64) *Backoff {
	return &Backoff{
		current: initial,
		initial: initial,
		max:     max,
		factor:  jitterFactor,
	}
}

// Next returns the next backoff duration with jitter and increases the backoff.
func (b *Backoff) Next() time.Duration {
	d := transport.Jitter(b.current, b.factor)
	b.current *= 2
	if b.current > b.max {
		b.current = b.max
	}
	return d
}

// Reset resets the backoff to the initial value.
func (b *Backoff) Reset() {
	b.current = b.initial
}

// Wait waits for the next backoff duration or until closedCh is closed.
// Returns true if wait completed, false if closedCh was closed.
func (b *Backoff) Wait(closedCh <-chan struct{}) bool {
	d := b.Next()
	select {
	case <-closedCh:
		return false
	case <-time.After(d):
		return true
	}
}

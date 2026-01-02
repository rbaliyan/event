package event

import (
	"context"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

// TestBus creates a new bus configured for testing.
// The transport parameter is required - use channel.New() for in-memory testing.
// Has recovery/tracing/metrics disabled for simpler testing.
// Panics if transport is nil (test setup error).
//
// Example:
//
//	import "github.com/rbaliyan/event/v3/transport/channel"
//	bus := event.TestBus(channel.New())
func TestBus(t transport.Transport) *Bus {
	bus, err := NewBus("test-bus",
		WithTransport(t),
		WithRecovery(false),
		WithTracing(false),
		WithMetrics(false),
	)
	if err != nil {
		panic("event.TestBus: " + err.Error())
	}
	return bus
}

// RecordedMessage represents a message that was published during a test
type RecordedMessage struct {
	EventName string
	Message   message.Message
	Timestamp time.Time
}

// RecordingTransport wraps a transport and records all published messages.
// Useful for testing that events are published correctly.
type RecordingTransport struct {
	transport.Transport
	mu       sync.Mutex
	messages []RecordedMessage
}

// NewRecordingTransport creates a transport that records all published messages.
// It wraps the provided transport (which is required).
//
// Example:
//
//	import "github.com/rbaliyan/event/v3/transport/channel"
//	transport := event.NewRecordingTransport(channel.New())
func NewRecordingTransport(t transport.Transport) *RecordingTransport {
	if t == nil {
		panic("event: transport is required for NewRecordingTransport")
	}
	return &RecordingTransport{
		Transport: t,
		messages:  make([]RecordedMessage, 0),
	}
}

// Publish records the message and delegates to the underlying transport
func (t *RecordingTransport) Publish(ctx context.Context, name string, msg message.Message) error {
	t.mu.Lock()
	t.messages = append(t.messages, RecordedMessage{
		EventName: name,
		Message:   msg,
		Timestamp: time.Now(),
	})
	t.mu.Unlock()

	return t.Transport.Publish(ctx, name, msg)
}

// Messages returns a copy of all recorded messages
func (t *RecordingTransport) Messages() []RecordedMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make([]RecordedMessage, len(t.messages))
	copy(result, t.messages)
	return result
}

// MessagesFor returns recorded messages for a specific event
func (t *RecordingTransport) MessagesFor(eventName string) []RecordedMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	var result []RecordedMessage
	for _, m := range t.messages {
		if m.EventName == eventName {
			result = append(result, m)
		}
	}
	return result
}

// Reset clears all recorded messages
func (t *RecordingTransport) Reset() {
	t.mu.Lock()
	t.messages = make([]RecordedMessage, 0)
	t.mu.Unlock()
}

// Count returns the number of recorded messages
func (t *RecordingTransport) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.messages)
}

// CountFor returns the number of messages for a specific event
func (t *RecordingTransport) CountFor(eventName string) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := 0
	for _, m := range t.messages {
		if m.EventName == eventName {
			count++
		}
	}
	return count
}

// TestHandler is a helper for testing event handlers.
// It collects all events received by the handler for later assertions.
type TestHandler[T any] struct {
	mu       sync.Mutex
	received []TestHandlerCall[T]
	handler  func(context.Context, Event[T], T) error
}

// TestHandlerCall represents a single call to the test handler
type TestHandlerCall[T any] struct {
	Context context.Context
	Event   Event[T]
	Data    T
	Time    time.Time
}

// NewTestHandler creates a new test handler.
// If handler is nil, it will acknowledge all messages.
func NewTestHandler[T any](handler func(context.Context, Event[T], T) error) *TestHandler[T] {
	th := &TestHandler[T]{
		received: make([]TestHandlerCall[T], 0),
		handler:  handler,
	}
	return th
}

// Handler returns the handler function for use with Subscribe
func (h *TestHandler[T]) Handler() Handler[T] {
	return func(ctx context.Context, ev Event[T], data T) error {
		h.mu.Lock()
		h.received = append(h.received, TestHandlerCall[T]{
			Context: ctx,
			Event:   ev,
			Data:    data,
			Time:    time.Now(),
		})
		h.mu.Unlock()

		if h.handler != nil {
			return h.handler(ctx, ev, data)
		}
		return nil
	}
}

// Received returns a copy of all received calls
func (h *TestHandler[T]) Received() []TestHandlerCall[T] {
	h.mu.Lock()
	defer h.mu.Unlock()

	result := make([]TestHandlerCall[T], len(h.received))
	copy(result, h.received)
	return result
}

// Count returns the number of calls received
func (h *TestHandler[T]) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.received)
}

// Last returns the last received call, or nil if none
func (h *TestHandler[T]) Last() *TestHandlerCall[T] {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.received) == 0 {
		return nil
	}
	call := h.received[len(h.received)-1]
	return &call
}

// Reset clears all received calls
func (h *TestHandler[T]) Reset() {
	h.mu.Lock()
	h.received = make([]TestHandlerCall[T], 0)
	h.mu.Unlock()
}

// WaitFor waits until the handler has received at least n calls or timeout is reached.
// Returns true if the expected count was reached, false on timeout.
func (h *TestHandler[T]) WaitFor(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if h.Count() >= n {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// BlockingTransport is a transport that blocks publishes until manually released.
// Useful for testing timeout and concurrency scenarios.
type BlockingTransport struct {
	transport.Transport
	blockCh chan struct{}
	blocked bool
	mu      sync.Mutex
}

// NewBlockingTransport creates a transport that blocks all publishes.
// The transport parameter is required. Call Release() to unblock.
//
// Example:
//
//	import "github.com/rbaliyan/event/v3/transport/channel"
//	transport := event.NewBlockingTransport(channel.New())
func NewBlockingTransport(t transport.Transport) *BlockingTransport {
	if t == nil {
		panic("event: transport is required for NewBlockingTransport")
	}
	return &BlockingTransport{
		Transport: t,
		blockCh:   make(chan struct{}),
		blocked:   true,
	}
}

// Publish blocks until Release is called, then delegates to underlying transport
func (t *BlockingTransport) Publish(ctx context.Context, name string, msg message.Message) error {
	t.mu.Lock()
	blocked := t.blocked
	ch := t.blockCh
	t.mu.Unlock()

	if blocked {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// Unblocked
		}
	}

	return t.Transport.Publish(ctx, name, msg)
}

// Release unblocks all waiting publishes
func (t *BlockingTransport) Release() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.blocked {
		close(t.blockCh)
		t.blocked = false
	}
}

// Block creates a new block (for reuse after Release)
func (t *BlockingTransport) Block() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.blocked {
		t.blockCh = make(chan struct{})
		t.blocked = true
	}
}

// IsBlocked returns whether publishes are currently blocked
func (t *BlockingTransport) IsBlocked() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blocked
}

// FailingTransport is a transport that fails publishes with a configured error.
// Useful for testing error handling.
type FailingTransport struct {
	transport.Transport
	mu       sync.Mutex
	err      error
	failAll  bool
	failNext int
}

// NewFailingTransport creates a transport that can be configured to fail.
// The transport parameter is required.
//
// Example:
//
//	import "github.com/rbaliyan/event/v3/transport/channel"
//	transport := event.NewFailingTransport(channel.New())
func NewFailingTransport(t transport.Transport) *FailingTransport {
	if t == nil {
		panic("event: transport is required for NewFailingTransport")
	}
	return &FailingTransport{
		Transport: t,
	}
}

// Publish fails if configured, otherwise delegates to underlying transport
func (t *FailingTransport) Publish(ctx context.Context, name string, msg message.Message) error {
	t.mu.Lock()
	shouldFail := t.failAll || t.failNext > 0
	err := t.err
	if t.failNext > 0 {
		t.failNext--
	}
	t.mu.Unlock()

	if shouldFail {
		if err != nil {
			return err
		}
		return transport.ErrPublishTimeout
	}

	return t.Transport.Publish(ctx, name, msg)
}

// FailAll makes all publishes fail with the given error
func (t *FailingTransport) FailAll(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failAll = true
	t.err = err
}

// FailNext makes the next n publishes fail with the given error
func (t *FailingTransport) FailNext(n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failNext = n
	t.err = err
}

// Reset clears all failure configuration
func (t *FailingTransport) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failAll = false
	t.failNext = 0
	t.err = nil
}

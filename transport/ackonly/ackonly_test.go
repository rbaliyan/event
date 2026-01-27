package ackonly

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// mockAckBackend implements AckBackend for testing
type mockAckBackend struct {
	mu      sync.Mutex
	stored  map[string][]string // eventName -> msgIDs
	acked   map[string][]string // eventName -> msgIDs
	storeErr error
	ackErr   error
}

func newMockAckBackend() *mockAckBackend {
	return &mockAckBackend{
		stored: make(map[string][]string),
		acked:  make(map[string][]string),
	}
}

func (m *mockAckBackend) Store(ctx context.Context, eventName, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.storeErr != nil {
		return m.storeErr
	}

	m.stored[eventName] = append(m.stored[eventName], msgID)
	return nil
}

func (m *mockAckBackend) Ack(ctx context.Context, eventName, msgID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ackErr != nil {
		return m.ackErr
	}

	m.acked[eventName] = append(m.acked[eventName], msgID)
	return nil
}

func (m *mockAckBackend) Pending(ctx context.Context, eventName string, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stored := m.stored[eventName]
	acked := make(map[string]bool)
	for _, id := range m.acked[eventName] {
		acked[id] = true
	}

	var pending []string
	for _, id := range stored {
		if !acked[id] {
			pending = append(pending, id)
			if limit > 0 && len(pending) >= limit {
				break
			}
		}
	}

	return pending, nil
}

func (m *mockAckBackend) getStored(eventName string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.stored[eventName]...)
}

func (m *mockAckBackend) getAcked(eventName string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.acked[eventName]...)
}

func TestNew_Defaults(t *testing.T) {
	tr := New()

	if tr == nil {
		t.Fatal("expected non-nil transport")
	}

	if !tr.isOpen() {
		t.Error("expected transport to be open")
	}

	if tr.bufferSize != 100 {
		t.Errorf("expected buffer size 100, got %d", tr.bufferSize)
	}

	if tr.logger == nil {
		t.Error("expected logger to be set")
	}

	if tr.onError == nil {
		t.Error("expected onError to be set")
	}

	if tr.ackBackend != nil {
		t.Error("expected ackBackend to be nil by default")
	}
}

func TestNew_WithBufferSize(t *testing.T) {
	tr := New(WithBufferSize(200))

	if tr.bufferSize != 200 {
		t.Errorf("expected buffer size 200, got %d", tr.bufferSize)
	}
}

func TestNew_WithBufferSize_Negative(t *testing.T) {
	tr := New(WithBufferSize(-10))

	if tr.bufferSize != 100 {
		t.Errorf("expected default buffer size 100, got %d", tr.bufferSize)
	}
}

func TestNew_WithLogger(t *testing.T) {
	logger := slog.Default()
	tr := New(WithLogger(logger))

	if tr.logger != logger {
		t.Error("expected custom logger to be set")
	}
}

func TestNew_WithLogger_Nil(t *testing.T) {
	tr := New(WithLogger(nil))

	if tr.logger == nil {
		t.Error("expected logger to remain set when nil is passed")
	}
}

func TestNew_WithAckBackend(t *testing.T) {
	backend := newMockAckBackend()
	tr := New(WithAckBackend(backend))

	if tr.ackBackend != backend {
		t.Error("expected ack backend to be set")
	}
}

func TestNew_WithErrorHandler(t *testing.T) {
	var called bool
	handler := func(err error) {
		called = true
	}

	tr := New(WithErrorHandler(handler))

	tr.onError(errors.New("test"))

	if !called {
		t.Error("expected error handler to be called")
	}
}

func TestNew_WithErrorHandler_Nil(t *testing.T) {
	tr := New(WithErrorHandler(nil))

	if tr.onError == nil {
		t.Error("expected onError to remain set when nil is passed")
	}
}

func TestRegisterEvent_Success(t *testing.T) {
	tr := New()
	ctx := context.Background()

	err := tr.RegisterEvent(ctx, "test-event")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify event exists
	_, ok := tr.events.Load("test-event")
	if !ok {
		t.Error("expected event to be registered")
	}
}

func TestRegisterEvent_Duplicate(t *testing.T) {
	tr := New()
	ctx := context.Background()

	err := tr.RegisterEvent(ctx, "test-event")
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	err = tr.RegisterEvent(ctx, "test-event")
	if !errors.Is(err, transport.ErrEventAlreadyExists) {
		t.Errorf("expected ErrEventAlreadyExists, got %v", err)
	}
}

func TestRegisterEvent_TransportClosed(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.Close(ctx)

	err := tr.RegisterEvent(ctx, "test-event")
	if !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestUnregisterEvent_Success(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	err := tr.UnregisterEvent(ctx, "test-event")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify event removed
	_, ok := tr.events.Load("test-event")
	if ok {
		t.Error("expected event to be unregistered")
	}
}

func TestUnregisterEvent_NotRegistered(t *testing.T) {
	tr := New()
	ctx := context.Background()

	err := tr.UnregisterEvent(ctx, "unknown-event")
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("expected ErrEventNotRegistered, got %v", err)
	}
}

func TestUnregisterEvent_TransportClosed(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	tr.Close(ctx)

	err := tr.UnregisterEvent(ctx, "test-event")
	if !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestUnregisterEvent_ClosesSubscriptions(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	tr.UnregisterEvent(ctx, "test-event")

	// Verify subscription is closed by checking if channel is closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Error("expected subscription channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected subscription channel to be closed immediately")
	}
}

func TestPublish_NotSupported(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Publish(ctx, "test-event", msg)

	if !errors.Is(err, ErrPublishNotSupported) {
		t.Errorf("expected ErrPublishNotSupported, got %v", err)
	}
}

func TestSubscribe_Success(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	sub, err := tr.Subscribe(ctx, "test-event")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}

	if sub.ID() == "" {
		t.Error("expected subscription to have ID")
	}

	if sub.Messages() == nil {
		t.Error("expected subscription to have channel")
	}
}

func TestSubscribe_EventNotRegistered(t *testing.T) {
	tr := New()
	ctx := context.Background()

	_, err := tr.Subscribe(ctx, "unknown-event")
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("expected ErrEventNotRegistered, got %v", err)
	}
}

func TestSubscribe_TransportClosed(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	tr.Close(ctx)

	_, err := tr.Subscribe(ctx, "test-event")
	if !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestSubscribe_CustomBufferSize(t *testing.T) {
	tr := New(WithBufferSize(100))
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	sub, err := tr.Subscribe(ctx, "test-event", transport.WithBufferSize(50))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify subscription was created successfully
	if sub.ID() == "" {
		t.Error("expected subscription to have ID")
	}
}

func TestDeliver_ToSubscribers(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	sub1, _ := tr.Subscribe(ctx, "test-event")
	sub2, _ := tr.Subscribe(ctx, "test-event")

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Both subscribers should receive the message
	select {
	case received := <-sub1.Messages():
		if received.ID() != "msg-1" {
			t.Errorf("expected msg-1, got %s", received.ID())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub1 did not receive message")
	}

	select {
	case received := <-sub2.Messages():
		if received.ID() != "msg-1" {
			t.Errorf("expected msg-1, got %s", received.ID())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub2 did not receive message")
	}
}

func TestDeliver_EventNotRegistered(t *testing.T) {
	tr := New()
	ctx := context.Background()

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "unknown-event", msg)

	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("expected ErrEventNotRegistered, got %v", err)
	}
}

func TestDeliver_TransportClosed(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	tr.Close(ctx)

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)

	if !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestDeliver_WithAckBackend(t *testing.T) {
	backend := newMockAckBackend()
	tr := New(WithAckBackend(backend))
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Message should be stored as pending
	stored := backend.getStored("test-event")
	if len(stored) != 1 || stored[0] != "msg-1" {
		t.Errorf("expected msg-1 to be stored, got %v", stored)
	}

	// Receive and ack the message
	select {
	case received := <-sub.Messages():
		err := received.Ack(nil)
		if err != nil {
			t.Errorf("expected no error on ack, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("did not receive message")
	}

	// Message should be acked
	acked := backend.getAcked("test-event")
	if len(acked) != 1 || acked[0] != "msg-1" {
		t.Errorf("expected msg-1 to be acked, got %v", acked)
	}
}

func TestDeliver_AckBackend_StoreError(t *testing.T) {
	backend := newMockAckBackend()
	backend.storeErr = errors.New("store failed")

	var errorHandlerCalled bool
	tr := New(
		WithAckBackend(backend),
		WithErrorHandler(func(err error) {
			errorHandlerCalled = true
		}),
	)
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)
	if err != nil {
		t.Fatalf("expected no error (should continue on store error), got %v", err)
	}

	if !errorHandlerCalled {
		t.Error("expected error handler to be called")
	}

	// Message should still be delivered despite store error
	select {
	case <-sub.Messages():
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("message should still be delivered despite store error")
	}
}

func TestDeliver_NoSubscribers(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)

	// Should not error when no subscribers
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestDeliver_ClosedSubscriber(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")
	sub.Close(ctx)

	msg := transport.NewMessage("msg-1", "test-source", []byte("data"), nil, trace.SpanContext{})
	err := tr.Deliver(ctx, "test-event", msg)

	// Should not error when subscriber is closed
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestClose_Success(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	err := tr.Close(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if tr.isOpen() {
		t.Error("expected transport to be closed")
	}

	// Verify subscription channel is closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Error("expected subscription channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected subscription channel to be closed")
	}
}

func TestClose_Idempotent(t *testing.T) {
	tr := New()
	ctx := context.Background()

	err1 := tr.Close(ctx)
	err2 := tr.Close(ctx)

	if err1 != nil {
		t.Errorf("first close should succeed, got %v", err1)
	}

	if err2 != nil {
		t.Errorf("second close should succeed, got %v", err2)
	}
}

func TestClose_MultipleSubscriptions(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "event1")
	tr.RegisterEvent(ctx, "event2")

	sub1, _ := tr.Subscribe(ctx, "event1")
	sub2, _ := tr.Subscribe(ctx, "event1")
	sub3, _ := tr.Subscribe(ctx, "event2")

	tr.Close(ctx)

	// Verify all subscription channels are closed
	checkClosed := func(sub transport.Subscription, name string) {
		select {
		case _, ok := <-sub.Messages():
			if ok {
				t.Errorf("expected %s to be closed", name)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("expected %s to be closed", name)
		}
	}

	checkClosed(sub1, "sub1")
	checkClosed(sub2, "sub2")
	checkClosed(sub3, "sub3")
}

func TestHealth_Healthy(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "event1")
	tr.RegisterEvent(ctx, "event2")
	tr.Subscribe(ctx, "event1")
	tr.Subscribe(ctx, "event1")
	tr.Subscribe(ctx, "event2")

	result := tr.Health(ctx)

	if result.Status != transport.HealthStatusHealthy {
		t.Errorf("expected healthy status, got %s", result.Status)
	}

	if result.Message != "ack-only transport is healthy" {
		t.Errorf("unexpected message: %s", result.Message)
	}

	if result.Details["type"] != "ack-only" {
		t.Errorf("expected type ack-only, got %v", result.Details["type"])
	}

	if result.Details["events"] != 2 {
		t.Errorf("expected 2 events, got %v", result.Details["events"])
	}

	if result.Details["subscribers"] != int64(3) {
		t.Errorf("expected 3 subscribers, got %v", result.Details["subscribers"])
	}

	if result.Details["has_ack_backend"] != false {
		t.Errorf("expected has_ack_backend false, got %v", result.Details["has_ack_backend"])
	}

	if result.CheckedAt.IsZero() {
		t.Error("expected CheckedAt to be set")
	}

	if result.Latency <= 0 {
		t.Error("expected positive latency")
	}
}

func TestHealth_WithAckBackend(t *testing.T) {
	backend := newMockAckBackend()
	tr := New(WithAckBackend(backend))
	ctx := context.Background()

	result := tr.Health(ctx)

	if result.Details["has_ack_backend"] != true {
		t.Errorf("expected has_ack_backend true, got %v", result.Details["has_ack_backend"])
	}
}

func TestHealth_Unhealthy(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.Close(ctx)

	result := tr.Health(ctx)

	if result.Status != transport.HealthStatusUnhealthy {
		t.Errorf("expected unhealthy status, got %s", result.Status)
	}

	if result.Message != "transport is closed" {
		t.Errorf("unexpected message: %s", result.Message)
	}
}

func TestSubscription_Close(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	err := sub.Close(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify subscription channel is closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Error("expected subscription channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected subscription channel to be closed")
	}
}

func TestDeliver_ConcurrentSubscribers(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")

	// Create 10 subscribers
	subs := make([]transport.Subscription, 10)
	for i := 0; i < 10; i++ {
		sub, _ := tr.Subscribe(ctx, "test-event")
		subs[i] = sub
	}

	// Deliver messages concurrently
	numMessages := 100
	var wg sync.WaitGroup
	wg.Add(numMessages)

	for i := 0; i < numMessages; i++ {
		go func(id int) {
			defer wg.Done()
			msg := transport.NewMessage(
				transport.NewID(),
				"test-source",
				[]byte("data"),
				nil,
				trace.SpanContext{},
			)
			tr.Deliver(ctx, "test-event", msg)
		}(i)
	}

	// Wait for all deliveries
	wg.Wait()

	// Each subscriber should receive all messages
	for i, sub := range subs {
		count := 0
		timeout := time.After(1 * time.Second)
		for count < numMessages {
			select {
			case <-sub.Messages():
				count++
			case <-timeout:
				t.Errorf("subscriber %d only received %d/%d messages", i, count, numMessages)
				return
			}
		}
	}
}

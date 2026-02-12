package distributed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3"
)

// mockCoordinator is a test double for Coordinator.
type mockCoordinator struct {
	acquireResult  bool
	acquireErr     error
	markProcessed  bool
	markErr        error
	resetCalled    bool
	resetErr       error
	acquiredMsgIDs []string
}

func (m *mockCoordinator) Acquire(_ context.Context, messageID string, _ time.Duration) (bool, error) {
	m.acquiredMsgIDs = append(m.acquiredMsgIDs, messageID)
	return m.acquireResult, m.acquireErr
}
func (m *mockCoordinator) MarkProcessed(_ context.Context, _ string) error {
	m.markProcessed = true
	return m.markErr
}
func (m *mockCoordinator) Reset(_ context.Context, _ string) error {
	m.resetCalled = true
	return m.resetErr
}
func (m *mockCoordinator) ListStale(_ context.Context, _ time.Duration, _ int) ([]string, error) {
	return nil, nil
}

// mockCoordWithPayload implements both Coordinator and PayloadStore.
type mockCoordWithPayload struct {
	mockCoordinator
	storedPayloads  []*MessageData
	storeErr        error
	clearedPayloads []string
}

func (m *mockCoordWithPayload) StorePayload(_ context.Context, messageID string, data *MessageData) error {
	m.storedPayloads = append(m.storedPayloads, data)
	return m.storeErr
}
func (m *mockCoordWithPayload) LoadStalePayloads(_ context.Context, _ time.Duration, _ int) ([]*StaleMessage, error) {
	return nil, nil
}
func (m *mockCoordWithPayload) ClearPayload(_ context.Context, messageID string) error {
	m.clearedPayloads = append(m.clearedPayloads, messageID)
	return nil
}

// testHandler is a simple handler that records calls.
type testHandler[T any] struct {
	called bool
	err    error
}

func (h *testHandler[T]) handle(ctx context.Context, ev event.Event[T], data T) error {
	h.called = true
	return h.err
}

func TestMiddleware_NoEventID_PassesThrough(t *testing.T) {
	coord := &mockCoordinator{acquireResult: true}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute)
	wrapped := mw(handler.handle)

	// Context without event ID - should pass through to handler
	ctx := context.Background()
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called when no event ID")
	}
	if len(coord.acquiredMsgIDs) > 0 {
		t.Fatal("expected no Acquire call when no event ID")
	}
}

func TestMiddleware_AcquireFails_FailOpen(t *testing.T) {
	coord := &mockCoordinator{acquireErr: errors.New("redis down")}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute)
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called on Acquire error (fail open)")
	}
}

func TestMiddleware_NotAcquired_Skips(t *testing.T) {
	coord := &mockCoordinator{acquireResult: false}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute)
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler.called {
		t.Fatal("expected handler to NOT be called when not acquired")
	}
}

func TestMiddleware_Acquired_CallsHandlerAndMarksProcessed(t *testing.T) {
	coord := &mockCoordinator{acquireResult: true}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute)
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}
	if !coord.markProcessed {
		t.Fatal("expected MarkProcessed to be called on success")
	}
	if coord.resetCalled {
		t.Fatal("expected Reset NOT to be called on success")
	}
}

func TestMiddleware_HandlerError_ResetsState(t *testing.T) {
	coord := &mockCoordinator{acquireResult: true}
	handlerErr := errors.New("processing failed")
	handler := &testHandler[string]{err: handlerErr}

	mw := WorkerPoolMiddleware[string](coord, time.Minute)
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error to be returned, got %v", err)
	}
	if coord.markProcessed {
		t.Fatal("expected MarkProcessed NOT to be called on error")
	}
	if !coord.resetCalled {
		t.Fatal("expected Reset to be called on handler error")
	}
}

func TestMiddleware_NoPayloadInContext_NoStore(t *testing.T) {
	coord := &mockCoordWithPayload{
		mockCoordinator: mockCoordinator{acquireResult: true},
	}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute, WithPayloadRecovery())
	wrapped := mw(handler.handle)

	// Context with event ID but NO raw payload
	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}

	// No raw payload in context → no StorePayload call → no ClearPayload
	if len(coord.storedPayloads) > 0 {
		t.Fatal("expected no StorePayload call when no raw payload in context")
	}
	if len(coord.clearedPayloads) > 0 {
		t.Fatal("expected no ClearPayload call when payload was not stored")
	}
}

func TestMiddleware_PayloadStored_ClearedOnSuccess(t *testing.T) {
	coord := &mockCoordWithPayload{
		mockCoordinator: mockCoordinator{acquireResult: true},
	}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute, WithPayloadRecovery())
	wrapped := mw(handler.handle)

	// Context with event ID AND raw payload
	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	ctx = event.ContextWithRawPayload(ctx, []byte(`{"order":"abc"}`))
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}

	// Raw payload in context → StorePayload called → ClearPayload on success
	if len(coord.storedPayloads) != 1 {
		t.Fatalf("expected 1 StorePayload call, got %d", len(coord.storedPayloads))
	}
	if string(coord.storedPayloads[0].Payload) != `{"order":"abc"}` {
		t.Fatalf("unexpected stored payload: %s", coord.storedPayloads[0].Payload)
	}
	if len(coord.clearedPayloads) != 1 || coord.clearedPayloads[0] != "msg-1" {
		t.Fatalf("expected ClearPayload for msg-1, got %v", coord.clearedPayloads)
	}
}

func TestMiddleware_PayloadStored_NotClearedOnHandlerError(t *testing.T) {
	coord := &mockCoordWithPayload{
		mockCoordinator: mockCoordinator{acquireResult: true},
	}
	handlerErr := errors.New("processing failed")
	handler := &testHandler[string]{err: handlerErr}

	mw := WorkerPoolMiddleware[string](coord, time.Minute, WithPayloadRecovery())
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	ctx = event.ContextWithRawPayload(ctx, []byte(`{"order":"abc"}`))
	err := wrapped(ctx, nil, "data")
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}

	// Payload stored but handler failed:
	// - ClearPayload NOT called (payload preserved for recovery)
	// - Reset NOT called (would delete payload, losing the event)
	// - State left in "processing" for RecoveryRunner to re-publish
	if len(coord.storedPayloads) != 1 {
		t.Fatalf("expected 1 StorePayload call, got %d", len(coord.storedPayloads))
	}
	if len(coord.clearedPayloads) > 0 {
		t.Fatal("expected no ClearPayload when handler failed")
	}
	if coord.resetCalled {
		t.Fatal("expected Reset NOT to be called when payload was stored (preserves recovery data)")
	}
}

func TestMiddleware_StorePayloadFails_NoClearOnSuccess(t *testing.T) {
	coord := &mockCoordWithPayload{
		mockCoordinator: mockCoordinator{acquireResult: true},
		storeErr:        errors.New("store failed"),
	}
	handler := &testHandler[string]{}

	mw := WorkerPoolMiddleware[string](coord, time.Minute, WithPayloadRecovery())
	wrapped := mw(handler.handle)

	// With raw payload in context, StorePayload is called but fails
	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	ctx = event.ContextWithRawPayload(ctx, []byte(`{"order":"abc"}`))
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// StorePayload failed → payloadStored=false → no ClearPayload
	if len(coord.clearedPayloads) > 0 {
		t.Fatal("expected no ClearPayload when StorePayload failed")
	}
	if !coord.markProcessed {
		t.Fatal("expected MarkProcessed on handler success")
	}
}

func TestMiddleware_MaxPayloadSize(t *testing.T) {
	coord := &mockCoordWithPayload{
		mockCoordinator: mockCoordinator{acquireResult: true},
	}
	handler := &testHandler[string]{}

	// Set max payload size very small (10 bytes)
	mw := WorkerPoolMiddleware[string](coord, time.Minute,
		WithPayloadRecovery(),
		WithMaxPayloadSize(10),
	)
	wrapped := mw(handler.handle)

	// Payload exceeds 10 bytes → skipped
	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	ctx = event.ContextWithRawPayload(ctx, []byte(`{"order":"abc-very-long-payload"}`))
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}
	if len(coord.storedPayloads) > 0 {
		t.Fatal("expected no StorePayload call when payload exceeds max size")
	}
	if !coord.markProcessed {
		t.Fatal("expected MarkProcessed on success")
	}
}

func TestMiddleware_CoordWithoutPayloadStore(t *testing.T) {
	// Plain coordinator without PayloadStore
	coord := &mockCoordinator{acquireResult: true}
	handler := &testHandler[string]{}

	// Even with WithPayloadRecovery, should work fine (just no payload storage)
	mw := WorkerPoolMiddleware[string](coord, time.Minute, WithPayloadRecovery())
	wrapped := mw(handler.handle)

	ctx := event.ContextWithEventID(context.Background(), "msg-1")
	err := wrapped(ctx, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}
	if !coord.markProcessed {
		t.Fatal("expected MarkProcessed on success")
	}
}

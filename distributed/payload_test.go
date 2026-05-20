package distributed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/clock"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// newSMWithFakeClock constructs a MemoryStateManager wired to a Fake clock
// pinned at the Unix epoch. Test bodies call clk.Advance to deterministically
// cross TTL / stale-timeout boundaries instead of time.Sleep.
//
// Cleanup is disabled (WithCleanup(false, 0)) so the background goroutine
// doesn't race with the test's clock manipulation.
func newSMWithFakeClock(opts ...Option) (*MemoryStateManager, *clock.Fake) {
	clk := clock.NewFake(time.Time{})
	all := append([]Option{WithCleanup(false, 0), withClock(clk)}, opts...)
	return NewMemoryStateManager(all...), clk
}

func TestMemoryCoordinator_AcquireAndStorePayload(t *testing.T) {
	ctx := context.Background()
	sm, _ := newSMWithFakeClock()
	defer sm.Close()

	// Acquire state
	acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Store payload separately
	data := &MessageData{
		Payload:   []byte(`{"key":"value"}`),
		Metadata:  map[string]string{"source": "test"},
		EventName: "order.created",
	}
	if err := sm.StorePayload(ctx, "msg-1", data); err != nil {
		t.Fatalf("unexpected StorePayload error: %v", err)
	}

	// Second acquisition for same message should fail
	acquired, err = sm.Acquire(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if acquired {
		t.Fatal("expected second acquisition to fail")
	}

	// Different message should succeed
	acquired, err = sm.Acquire(ctx, "msg-2", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Fatal("expected different message acquisition to succeed")
	}
}

func TestMemoryCoordinator_Acquire_Expiry(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// Acquire with very short TTL
	acquired, _ := sm.Acquire(ctx, "msg-1", 10*time.Millisecond)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Cross the TTL boundary via the fake clock — instant, deterministic.
	clk.Advance(20 * time.Millisecond)

	// Should be acquirable again
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed after expiry")
	}
}

func TestMemoryPayloadStore_LoadStalePayloads(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// Acquire with payload
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})

	// Acquire without payload (regular acquire)
	sm.Acquire(ctx, "msg-2", time.Hour)

	// Acquire and complete (should not appear in stale)
	sm.Acquire(ctx, "msg-3", time.Hour)
	sm.StorePayload(ctx, "msg-3", &MessageData{Payload: []byte(`completed`)})
	sm.MarkProcessed(ctx, "msg-3")

	// Make processing entries stale by backdating updatedAt relative to the
	// FAKE clock — not real wall-clock — since LoadStalePayloads compares
	// against clk.Now() under the injected fake.
	sm.mu.Lock()
	for id, entry := range sm.states {
		if id != "msg-3" {
			entry.updatedAt = clk.Now().Add(-5 * time.Minute)
		}
	}
	sm.mu.Unlock()

	// LoadStalePayloads should only return entries WITH payload
	stale, err := sm.LoadStalePayloads(ctx, time.Minute, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should find only msg-1 (has payload and stale), not msg-2 (no payload) or msg-3 (completed)
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale entry with payload, got %d", len(stale))
	}

	if stale[0].MessageID != "msg-1" {
		t.Errorf("expected message ID 'msg-1', got %q", stale[0].MessageID)
	}
	if !stale[0].HasPayload() {
		t.Error("expected payload to be present")
	}
	if stale[0].Data.EventName != "order.created" {
		t.Errorf("expected event name 'order.created', got %q", stale[0].Data.EventName)
	}

	// Test limit
	sm.Acquire(ctx, "msg-4", time.Hour)
	sm.StorePayload(ctx, "msg-4", &MessageData{Payload: []byte(`another`)})
	sm.mu.Lock()
	sm.states["msg-4"].updatedAt = clk.Now().Add(-5 * time.Minute)
	sm.mu.Unlock()

	stale, err = sm.LoadStalePayloads(ctx, time.Minute, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale with limit, got %d", len(stale))
	}
}

func TestMemoryPayloadStore_ClearPayload(t *testing.T) {
	ctx := context.Background()
	sm, _ := newSMWithFakeClock()
	defer sm.Close()

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"key":"value"}`),
		EventName: "test.event",
	})

	// Verify payload is stored
	sm.mu.RLock()
	entry := sm.states["msg-1"]
	if entry.payload == nil {
		t.Fatal("expected payload to be stored")
	}
	sm.mu.RUnlock()

	// Clear payload
	if err := sm.ClearPayload(ctx, "msg-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify payload is cleared
	sm.mu.RLock()
	entry = sm.states["msg-1"]
	if entry.payload != nil {
		t.Fatal("expected payload to be cleared after ClearPayload")
	}
	sm.mu.RUnlock()
}

func TestStaleMessage_HasPayload(t *testing.T) {
	tests := []struct {
		name     string
		msg      StaleMessage
		expected bool
	}{
		{
			name:     "with payload",
			msg:      StaleMessage{Data: MessageData{Payload: []byte(`data`)}},
			expected: true,
		},
		{
			name:     "empty payload",
			msg:      StaleMessage{Data: MessageData{Payload: []byte{}}},
			expected: false,
		},
		{
			name:     "nil payload",
			msg:      StaleMessage{Data: MessageData{}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.msg.HasPayload(); got != tt.expected {
				t.Errorf("HasPayload() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestCompileTimeChecks(t *testing.T) {
	// Verify interface implementations
	var _ Coordinator = (*MemoryStateManager)(nil)
	var _ PayloadStore = (*MemoryStateManager)(nil)
}

// mockPublisher records Send calls for testing.
type mockPublisher struct {
	calls []publishCall
}

type publishCall struct {
	eventName string
	eventID   string
	payload   []byte
	metadata  map[string]string
}

func (m *mockPublisher) Send(_ context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error {
	m.calls = append(m.calls, publishCall{
		eventName: eventName,
		eventID:   eventID,
		payload:   payload,
		metadata:  metadata,
	})
	return nil
}

func TestRecoveryRunner_BasicReset(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// No Publisher → basic reset mode
	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered, got %d", recovered)
	}

	// Both should be acquirable again after reset
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected msg-1 to be acquirable after reset")
	}
	acquired, _ = sm.Acquire(ctx, "msg-2", time.Hour)
	if !acquired {
		t.Fatal("expected msg-2 to be acquirable after reset")
	}
}

func TestRecoveryRunner_Phase1And2Exclusion(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	pub := &mockPublisher{}
	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
		WithPublisher(pub),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// msg-1: has payload (Phase 1 re-publishes)
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})

	// msg-2: has payload (Phase 1 re-publishes)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.StorePayload(ctx, "msg-2", &MessageData{
		Payload:   []byte(`{"order":2}`),
		EventName: "order.created",
	})

	// msg-3: no payload (Phase 2 resets)
	sm.Acquire(ctx, "msg-3", time.Hour)

	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if recovered != 3 {
		t.Fatalf("expected 3 recovered, got %d", recovered)
	}

	// Publisher should have 2 calls (msg-1 and msg-2)
	if len(pub.calls) != 2 {
		t.Fatalf("expected 2 publish calls, got %d", len(pub.calls))
	}

	// msg-3 should be acquirable (was reset in Phase 2)
	acquired, _ := sm.Acquire(ctx, "msg-3", time.Hour)
	if !acquired {
		t.Fatal("expected msg-3 to be acquirable after Phase 2 reset")
	}
}

func TestRecoveryRunner_BatchLimitZero(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(0), // No limit
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.Acquire(ctx, "msg-3", time.Hour)

	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	// All 3 should be recovered with no batch limit
	if recovered != 3 {
		t.Fatalf("expected 3 recovered, got %d", recovered)
	}
}

func TestRecoveryRunner_PayloadRepublish(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	pub := &mockPublisher{}

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
		WithPublisher(pub),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Acquire and store payload
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		Metadata:  map[string]string{"source": "test"},
		EventName: "order.created",
	})

	// Acquire without payload (should be reset, not re-published)
	sm.Acquire(ctx, "msg-2", time.Hour)

	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// msg-1 re-published via Publisher, msg-2 reset via StaleResetter
	if recovered != 2 {
		t.Fatalf("expected 2 recovered, got %d", recovered)
	}

	// Publisher should have been called once (for msg-1)
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	if pub.calls[0].eventName != "order.created" {
		t.Errorf("expected event name 'order.created', got %q", pub.calls[0].eventName)
	}
	if string(pub.calls[0].payload) != `{"order":1}` {
		t.Errorf("unexpected payload: %s", pub.calls[0].payload)
	}

	// msg-1 should be marked as processed (not reset)
	sm.mu.RLock()
	entry, exists := sm.states["msg-1"]
	sm.mu.RUnlock()
	if !exists {
		t.Fatal("expected msg-1 state to still exist (marked processed)")
	}
	if entry.state != stateCompleted {
		t.Fatalf("expected msg-1 state to be completed, got %d", entry.state)
	}

	// msg-2 should be acquirable after reset
	acquired, _ := sm.Acquire(ctx, "msg-2", time.Hour)
	if !acquired {
		t.Fatal("expected msg-2 to be acquirable after reset")
	}
}

func TestRecoveryRunner_PublishFailure_SkipsEntry(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// Publisher that always fails
	failPub := &failingPublisher{err: errors.New("publish failed")}

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithPublisher(failPub),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})

	clk.Advance(60 * time.Millisecond)

	// Phase 1 should skip the entry (publish failed), Phase 2 should NOT
	// reset it because it was handled by Phase 1 (in the exclusion set)
	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	// No entries recovered — publish failed, and the entry is excluded from Phase 2
	if recovered != 0 {
		t.Fatalf("expected 0 recovered (publish failed), got %d", recovered)
	}
}

// failingPublisher always returns an error.
type failingPublisher struct {
	err error
}

func (p *failingPublisher) Send(_ context.Context, _, _ string, _ []byte, _ map[string]string) error {
	return p.err
}

func TestRecoveryRunner_WithRealBus(t *testing.T) {
	ctx := context.Background()

	// Create a real bus with channel transport
	ch := channel.New()
	bus, err := event.NewBus("test-recovery", event.WithTransport(ch))
	if err != nil {
		t.Fatalf("failed to create bus: %v", err)
	}
	defer bus.Close(ctx)

	if err := ch.RegisterEvent(ctx, "order.created"); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// Bus satisfies Publisher interface
	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
		WithPublisher(bus),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		Metadata:  map[string]string{"source": "test"},
		EventName: "order.created",
	})

	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered, got %d", recovered)
	}

	sm.mu.RLock()
	entry, exists := sm.states["msg-1"]
	sm.mu.RUnlock()
	if !exists {
		t.Fatal("expected msg-1 state to still exist (marked processed)")
	}
	if entry.state != stateCompleted {
		t.Fatalf("expected msg-1 state to be completed, got %d", entry.state)
	}
}

func TestBus_SupportsRedelivery(t *testing.T) {
	ctx := context.Background()

	ch := channel.New()
	bus, err := event.NewBus("test-redelivery", event.WithTransport(ch))
	if err != nil {
		t.Fatalf("failed to create bus: %v", err)
	}
	defer bus.Close(ctx)

	if bus.SupportsRedelivery() {
		t.Error("channel transport should not support redelivery")
	}
}

func TestPoolOption_WithPayloadRecovery(t *testing.T) {
	o := &poolOptions{}

	if o.storePayload != nil {
		t.Fatal("expected storePayload to be nil by default")
	}

	WithPayloadRecovery()(o)
	if o.storePayload == nil || !*o.storePayload {
		t.Fatal("expected storePayload to be true after WithPayloadRecovery")
	}
}

func TestPoolOption_WithMaxPayloadSize(t *testing.T) {
	o := &poolOptions{}

	if o.maxPayloadSize != 0 {
		t.Fatal("expected maxPayloadSize to be 0 by default")
	}

	WithMaxPayloadSize(1024 * 1024)(o)
	if o.maxPayloadSize != 1024*1024 {
		t.Fatalf("expected maxPayloadSize 1048576, got %d", o.maxPayloadSize)
	}

	WithMaxPayloadSize(0)(o)
	if o.maxPayloadSize != 1024*1024 {
		t.Fatal("expected maxPayloadSize unchanged with 0")
	}
	WithMaxPayloadSize(-1)(o)
	if o.maxPayloadSize != 1024*1024 {
		t.Fatal("expected maxPayloadSize unchanged with -1")
	}
}

func TestRecoveryMetrics_NilSafe(t *testing.T) {
	ctx := context.Background()
	var m *RecoveryMetrics
	m.recordRecovered(ctx)
	m.recordRecoveredN(ctx, 5)
	m.recordRepublished(ctx, "test")
	m.recordReset(ctx)
	m.recordResetN(ctx, 3)
	m.recordError(ctx, "acquire")
	m.recordSkipped(ctx, "bus_not_found", "test")
	m.recordPassDuration(ctx, time.Second)
}

func TestRecoveryMetrics_BatchNilSafe(t *testing.T) {
	var m *RecoveryMetrics
	ctx := context.Background()
	// Batch methods with n <= 0 should be no-ops
	m.recordRecoveredN(ctx, 0)
	m.recordRecoveredN(ctx, -1)
	m.recordResetN(ctx, 0)
	m.recordResetN(ctx, -1)
}

func TestNewRecoveryRunner_NilCoordinator_ReturnsError(t *testing.T) {
	_, err := NewRecoveryRunner(nil)
	if err == nil {
		t.Fatal("expected error for nil coordinator")
	}
}

func TestRecoveryMetrics_Creation(t *testing.T) {
	m, err := NewRecoveryMetrics()
	if err != nil {
		t.Fatalf("unexpected error creating metrics: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}

	ctx := context.Background()
	m.recordRecovered(ctx)
	m.recordRepublished(ctx, "order.created")
	m.recordReset(ctx)
	m.recordError(ctx, "list_stale")
	m.recordSkipped(ctx, "bus_not_found", "order.created")
	m.recordPassDuration(ctx, 100*time.Millisecond)
}

func TestRecoveryMetrics_WithNamespace(t *testing.T) {
	m, err := NewRecoveryMetrics(WithRecoveryMetricsNamespace("myapp"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
}

func TestRecoveryOption_WithRecoveryLogger(t *testing.T) {
	o := &recoveryOptions{}
	WithRecoveryLogger(nil)(o)
	if o.logger != nil {
		t.Fatal("expected nil logger")
	}
}

func TestRecoveryOption_WithBackoff(t *testing.T) {
	o := &recoveryOptions{}
	WithBackoff(nil)(o)
	if o.backoff != nil {
		t.Fatal("expected nil backoff")
	}
}

func TestRecoveryRunner_RecoverOnce_NoStaleEntries(t *testing.T) {
	ctx := context.Background()
	sm, _ := newSMWithFakeClock()
	defer sm.Close()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(time.Minute),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// No entries at all
	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 0 {
		t.Fatalf("expected 0 recovered, got %d", recovered)
	}
}

func TestMemoryStateManager_CleanupExpired(t *testing.T) {
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	ctx := context.Background()
	sm.Acquire(ctx, "msg-1", 10*time.Millisecond)
	sm.Acquire(ctx, "msg-2", time.Hour)

	clk.Advance(20 * time.Millisecond)
	sm.cleanupExpired()

	sm.mu.RLock()
	count := len(sm.states)
	sm.mu.RUnlock()

	if count != 1 {
		t.Fatalf("expected 1 entry after cleanup, got %d", count)
	}
}

func TestRecoveryRunner_WithMetrics(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	metrics, err := NewRecoveryMetrics()
	if err != nil {
		t.Fatalf("failed to create metrics: %v", err)
	}

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithRecoveryMetrics(metrics),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	sm.Acquire(ctx, "msg-1", time.Hour)
	clk.Advance(60 * time.Millisecond)

	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered, got %d", recovered)
	}
}

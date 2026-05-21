package distributed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/internal/testutil"
)

func TestMemoryStateManager_Acquire(t *testing.T) {
	ctx := context.Background()
	sm := NewMemoryStateManager()
	defer sm.Close()

	// First acquisition should succeed
	acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquisition to succeed")
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

func TestMemoryStateManager_MarkProcessed(t *testing.T) {
	ctx := context.Background()
	sm := NewMemoryStateManager()
	defer sm.Close()

	// Acquire a message
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Mark it as processed
	if err := sm.MarkProcessed(ctx, "msg-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still not be acquirable (completed state blocks)
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if acquired {
		t.Fatal("expected acquisition to fail after mark processed")
	}
}

func TestMemoryStateManager_Reset(t *testing.T) {
	ctx := context.Background()
	sm := NewMemoryStateManager()
	defer sm.Close()

	// Acquire a message
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Reset it
	if err := sm.Reset(ctx, "msg-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be acquirable again after reset
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed after reset")
	}
}

func TestMemoryStateManager_Expiry(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	// Acquire with very short TTL
	acquired, _ := sm.Acquire(ctx, "msg-1", 10*time.Millisecond)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Cross the TTL boundary deterministically rather than sleeping.
	clk.Advance(20 * time.Millisecond)

	// Should be acquirable again after expiry
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed after expiry")
	}
}

func TestStateOptions(t *testing.T) {
	opts := defaultStateOptions()

	// Test defaults
	if opts.prefix != "state:" {
		t.Errorf("expected prefix 'state:', got %q", opts.prefix)
	}
	if opts.completionTTL != 24*time.Hour {
		t.Errorf("expected completionTTL 24h, got %v", opts.completionTTL)
	}

	// Test options
	WithPrefix("test:")(opts)
	if opts.prefix != "test:" {
		t.Errorf("expected prefix 'test:', got %q", opts.prefix)
	}

	WithCompletedTTL(48 * time.Hour)(opts)
	if opts.completionTTL != 48*time.Hour {
		t.Errorf("expected completionTTL 48h, got %v", opts.completionTTL)
	}

	// Test MongoDB-specific options
	WithCollection("custom_states")(opts)
	if opts.collectionName != "custom_states" {
		t.Errorf("expected collectionName 'custom_states', got %q", opts.collectionName)
	}

	WithCapped(1024*1024, 1000)(opts)
	if !opts.capped {
		t.Error("expected capped to be true")
	}
	if opts.cappedSize != 1024*1024 {
		t.Errorf("expected cappedSize 1048576, got %d", opts.cappedSize)
	}
	if opts.cappedMaxDocs != 1000 {
		t.Errorf("expected cappedMaxDocs 1000, got %d", opts.cappedMaxDocs)
	}
}

func TestMemoryStateManager_ListStale(t *testing.T) {
	ctx := context.Background()
	sm := NewMemoryStateManager(
		WithCleanup(false, 0), // Disable cleanup for this test
	)
	defer sm.Close()

	// Acquire some messages
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.Acquire(ctx, "msg-3", time.Hour)

	// Mark one as processed
	sm.MarkProcessed(ctx, "msg-2")

	// Artificially make states stale by setting updatedAt in the past
	sm.mu.Lock()
	for id, entry := range sm.states {
		if id != "msg-2" { // Don't touch completed one
			entry.updatedAt = time.Now().Add(-5 * time.Minute)
		}
	}
	sm.mu.Unlock()

	// List stale with 1 minute stale timeout
	stale, err := sm.ListStale(ctx, time.Minute, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should find msg-1 and msg-3 (processing and stale), not msg-2 (completed)
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale, got %d", len(stale))
	}

	// Test limit
	stale, err = sm.ListStale(ctx, time.Minute, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale with limit, got %d", len(stale))
	}
}

func TestMemoryStateManager_ResetStale(t *testing.T) {
	ctx := context.Background()
	sm := NewMemoryStateManager(
		WithCleanup(false, 0),
	)
	defer sm.Close()

	// Acquire some messages
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)

	// Make them stale
	sm.mu.Lock()
	for _, entry := range sm.states {
		entry.updatedAt = time.Now().Add(-5 * time.Minute)
	}
	sm.mu.Unlock()

	// Reset stale states
	reset, err := sm.ResetStale(ctx, time.Minute, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reset != 2 {
		t.Fatalf("expected 2 reset, got %d", reset)
	}

	// Should be acquirable again
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected acquisition to succeed after stale reset")
	}
}

func TestRecoveryRunner_RecoverOnce(t *testing.T) {
	ctx := context.Background()
	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Acquire a message
	sm.Acquire(ctx, "msg-1", time.Hour)

	// Not stale yet
	reset, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reset != 0 {
		t.Fatalf("expected 0 reset (not stale yet), got %d", reset)
	}

	// Cross the stale-timeout boundary deterministically.
	clk.Advance(60 * time.Millisecond)

	// Now should be reset
	reset, err = runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reset != 1 {
		t.Fatalf("expected 1 reset, got %d", reset)
	}

	// Message should be acquirable again
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected acquisition to succeed after recovery")
	}
}

func TestRecoveryRunner_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sm, clk := newSMWithFakeClock()
	defer sm.Close()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(30*time.Millisecond),
		WithCheckInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Start runner in background
	go runner.Run(ctx)

	// Acquire a message
	sm.Acquire(ctx, "msg-1", time.Hour)

	// Verify it's acquired
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if acquired {
		t.Fatal("expected acquisition to fail (already acquired)")
	}

	// Cross the stale-timeout boundary in the state manager's clock.
	// The runner's check-interval ticker still runs on the real clock,
	// so poll for the reset to land instead of sleeping for it.
	clk.Advance(50 * time.Millisecond)

	testutil.Eventually(t, 2*time.Second, func() bool {
		acquired, _ = sm.Acquire(ctx, "msg-1", time.Hour)
		return acquired
	}, "runner did not reset stale state")
}

// faultyCoord wraps MemoryStateManager: MarkProcessed can be made to fail,
// and ClearPayload calls are counted.
type faultyCoord struct {
	*MemoryStateManager
	failMark      bool
	clearPayloads atomic.Int32
}

func (f *faultyCoord) MarkProcessed(ctx context.Context, id string) error {
	if f.failMark {
		return errors.New("mark processed failed")
	}
	return f.MemoryStateManager.MarkProcessed(ctx, id)
}

func (f *faultyCoord) ClearPayload(ctx context.Context, id string) error {
	f.clearPayloads.Add(1)
	return f.MemoryStateManager.ClearPayload(ctx, id)
}

// countingSender counts Send calls and implements the Publisher (event.Sender) interface.
type countingSender struct{ n atomic.Int32 }

func (s *countingSender) Send(_ context.Context, eventName, _ string, payload []byte, _ map[string]string) error {
	if eventName == "" {
		return errors.New("Send: empty eventName")
	}
	if payload == nil {
		return errors.New("Send: nil payload")
	}
	s.n.Add(1)
	return nil
}

// TestRecovery_ClearPayloadAfterMarkProcessed verifies that ClearPayload is only
// called after MarkProcessed succeeds. If MarkProcessed fails, payload must be
// retained so the next recovery cycle can retry the re-publish.
func TestRecovery_ClearPayloadAfterMarkProcessed(t *testing.T) {
	ctx := context.Background()

	inner, clk := newSMWithFakeClock()
	defer inner.Close()

	coord := &faultyCoord{MemoryStateManager: inner, failMark: true}

	// Acquire and store payload so Phase 1 fires.
	_, _ = inner.Acquire(ctx, "msg-1", time.Hour)
	_ = inner.StorePayload(ctx, "msg-1", &MessageData{
		EventName: "test.event",
		Payload:   []byte(`"payload"`),
	})

	pub := &countingSender{}

	runner, err := NewRecoveryRunner(coord,
		WithStaleTimeout(10*time.Millisecond),
		WithPublisher(pub),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Cross the stale-timeout boundary deterministically.
	clk.Advance(20 * time.Millisecond)

	// First pass: MarkProcessed fails → payload must NOT be cleared.
	if _, err := runner.RecoverOnce(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := pub.n.Load(); n != 1 {
		t.Fatalf("expected 1 re-publish attempt, got %d", n)
	}
	if n := coord.clearPayloads.Load(); n != 0 {
		t.Fatalf("ClearPayload called %d time(s) when MarkProcessed failed; want 0", n)
	}

	// Second pass: MarkProcessed succeeds → payload must be cleared.
	coord.failMark = false
	if _, err := runner.RecoverOnce(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := coord.clearPayloads.Load(); n != 1 {
		t.Fatalf("ClearPayload called %d time(s) after successful MarkProcessed; want 1", n)
	}
}

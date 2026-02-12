package distributed

import (
	"context"
	"testing"
	"time"
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
	sm := NewMemoryStateManager(
		WithCleanup(false, 0), // Disable cleanup for this test
	)
	defer sm.Close()

	// Acquire with very short TTL
	acquired, _ := sm.Acquire(ctx, "msg-1", 10*time.Millisecond)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Wait for expiry
	time.Sleep(20 * time.Millisecond)

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
	sm := NewMemoryStateManager(
		WithCleanup(false, 0),
	)
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

	// Wait for it to become stale
	time.Sleep(60 * time.Millisecond)

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

	sm := NewMemoryStateManager(
		WithCleanup(false, 0),
	)
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

	// Wait for stale timeout + check interval
	time.Sleep(100 * time.Millisecond)

	// Should be acquirable now (runner reset it)
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected acquisition to succeed after runner reset stale state")
	}
}

package distributed

import (
	"context"
	"testing"
	"time"
)

func TestMemoryClaimer_TryClaim(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer()
	defer claimer.Close()

	// First claim should succeed
	claimed, err := claimer.TryClaim(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	// Second claim for same message should fail
	claimed, err = claimer.TryClaim(ctx, "msg-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claimed {
		t.Fatal("expected second claim to fail")
	}

	// Different message should succeed
	claimed, err = claimer.TryClaim(ctx, "msg-2", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected different message claim to succeed")
	}
}

func TestMemoryClaimer_Complete(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer()
	defer claimer.Close()

	// Claim a message
	claimed, _ := claimer.TryClaim(ctx, "msg-1", time.Minute)
	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	// Complete it
	if err := claimer.Complete(ctx, "msg-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still not be claimable (completed state blocks)
	claimed, _ = claimer.TryClaim(ctx, "msg-1", time.Minute)
	if claimed {
		t.Fatal("expected claim to fail after complete")
	}
}

func TestMemoryClaimer_Release(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer()
	defer claimer.Close()

	// Claim a message
	claimed, _ := claimer.TryClaim(ctx, "msg-1", time.Minute)
	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	// Release it
	if err := claimer.Release(ctx, "msg-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be claimable again after release
	claimed, _ = claimer.TryClaim(ctx, "msg-1", time.Minute)
	if !claimed {
		t.Fatal("expected claim to succeed after release")
	}
}

func TestMemoryClaimer_Expiry(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer(
		WithCleanup(false, 0), // Disable cleanup for this test
	)
	defer claimer.Close()

	// Claim with very short TTL
	claimed, _ := claimer.TryClaim(ctx, "msg-1", 10*time.Millisecond)
	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	// Wait for expiry
	time.Sleep(20 * time.Millisecond)

	// Should be claimable again after expiry
	claimed, _ = claimer.TryClaim(ctx, "msg-1", time.Minute)
	if !claimed {
		t.Fatal("expected claim to succeed after expiry")
	}
}

func TestClaimerOptions(t *testing.T) {
	opts := defaultClaimerOptions()

	// Test defaults
	if opts.prefix != "claim:" {
		t.Errorf("expected prefix 'claim:', got %q", opts.prefix)
	}
	if opts.ttl != 5*time.Minute {
		t.Errorf("expected ttl 5m, got %v", opts.ttl)
	}
	if opts.completionTTL != 24*time.Hour {
		t.Errorf("expected completionTTL 24h, got %v", opts.completionTTL)
	}

	// Test options
	WithClaimerPrefix("test:")(opts)
	if opts.prefix != "test:" {
		t.Errorf("expected prefix 'test:', got %q", opts.prefix)
	}

	WithClaimerTTL(10 * time.Minute)(opts)
	if opts.ttl != 10*time.Minute {
		t.Errorf("expected ttl 10m, got %v", opts.ttl)
	}

	WithCompletionTTL(48 * time.Hour)(opts)
	if opts.completionTTL != 48*time.Hour {
		t.Errorf("expected completionTTL 48h, got %v", opts.completionTTL)
	}
}

func TestMemoryClaimer_ListOrphanedClaims(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer(
		WithCleanup(false, 0), // Disable cleanup for this test
	)
	defer claimer.Close()

	// Claim some messages
	claimer.TryClaim(ctx, "msg-1", time.Hour)
	claimer.TryClaim(ctx, "msg-2", time.Hour)
	claimer.TryClaim(ctx, "msg-3", time.Hour)

	// Complete one
	claimer.Complete(ctx, "msg-2")

	// Artificially make claims stale by setting updatedAt in the past
	claimer.mu.Lock()
	for id, entry := range claimer.claims {
		if id != "msg-2" { // Don't touch completed one
			entry.updatedAt = time.Now().Add(-5 * time.Minute)
		}
	}
	claimer.mu.Unlock()

	// List orphans with 1 minute stale timeout
	orphans, err := claimer.ListOrphanedClaims(ctx, time.Minute, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should find msg-1 and msg-3 (pending and stale), not msg-2 (completed)
	if len(orphans) != 2 {
		t.Fatalf("expected 2 orphans, got %d", len(orphans))
	}

	// Test limit
	orphans, err = claimer.ListOrphanedClaims(ctx, time.Minute, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(orphans) != 1 {
		t.Fatalf("expected 1 orphan with limit, got %d", len(orphans))
	}
}

func TestMemoryClaimer_ReleaseOrphans(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer(
		WithCleanup(false, 0),
	)
	defer claimer.Close()

	// Claim some messages
	claimer.TryClaim(ctx, "msg-1", time.Hour)
	claimer.TryClaim(ctx, "msg-2", time.Hour)

	// Make them stale
	claimer.mu.Lock()
	for _, entry := range claimer.claims {
		entry.updatedAt = time.Now().Add(-5 * time.Minute)
	}
	claimer.mu.Unlock()

	// Release orphans
	released, err := claimer.ReleaseOrphans(ctx, time.Minute, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if released != 2 {
		t.Fatalf("expected 2 released, got %d", released)
	}

	// Should be claimable again
	claimed, _ := claimer.TryClaim(ctx, "msg-1", time.Hour)
	if !claimed {
		t.Fatal("expected claim to succeed after orphan release")
	}
}

func TestOrphanRecoveryRunner_RecoverOnce(t *testing.T) {
	ctx := context.Background()
	claimer := NewMemoryClaimer(
		WithCleanup(false, 0),
	)
	defer claimer.Close()

	runner := NewOrphanRecoveryRunner(claimer,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)

	// Claim a message
	claimer.TryClaim(ctx, "msg-1", time.Hour)

	// Not stale yet
	released, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if released != 0 {
		t.Fatalf("expected 0 released (not stale yet), got %d", released)
	}

	// Wait for it to become stale
	time.Sleep(60 * time.Millisecond)

	// Now should be released
	released, err = runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if released != 1 {
		t.Fatalf("expected 1 released, got %d", released)
	}

	// Message should be claimable again
	claimed, _ := claimer.TryClaim(ctx, "msg-1", time.Hour)
	if !claimed {
		t.Fatal("expected claim to succeed after recovery")
	}
}

func TestOrphanRecoveryRunner_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	claimer := NewMemoryClaimer(
		WithCleanup(false, 0),
	)
	defer claimer.Close()

	runner := NewOrphanRecoveryRunner(claimer,
		WithStaleTimeout(30*time.Millisecond),
		WithCheckInterval(20*time.Millisecond),
	)

	// Start runner in background
	go runner.Run(ctx)

	// Claim a message
	claimer.TryClaim(ctx, "msg-1", time.Hour)

	// Verify it's claimed
	claimed, _ := claimer.TryClaim(ctx, "msg-1", time.Hour)
	if claimed {
		t.Fatal("expected claim to fail (already claimed)")
	}

	// Wait for stale timeout + check interval
	time.Sleep(100 * time.Millisecond)

	// Should be claimable now (runner released it)
	claimed, _ = claimer.TryClaim(ctx, "msg-1", time.Hour)
	if !claimed {
		t.Fatal("expected claim to succeed after runner released orphan")
	}
}

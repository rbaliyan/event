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

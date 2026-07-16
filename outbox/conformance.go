package outbox

import (
	"context"
	"testing"
	"time"
)

// RunStoreConformance verifies a Store honors the claim/ack/fail/close contract.
// Backends call it from an integration test with a live store and a seed func
// that writes one pending message carrying the given EventID.
func RunStoreConformance(t *testing.T, ctx context.Context, store Store, seed func(ctx context.Context, eventID string) error) {
	t.Helper()

	t.Run("claim empty returns resource-free batch", func(t *testing.T) {
		b, err := store.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if b == nil {
			t.Fatal("empty claim must return non-nil Batch")
		}
		if len(b.Messages()) != 0 {
			t.Fatalf("expected 0 messages, got %d", len(b.Messages()))
		}
		if err := b.Close(ctx); err != nil {
			t.Fatalf("close empty batch: %v", err)
		}
	})

	t.Run("ack removes from claimable set", func(t *testing.T) {
		if err := seed(ctx, "conf-ack"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		b, err := store.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		var target *Message
		for i := range b.Messages() {
			if b.Messages()[i].EventID == "conf-ack" {
				target = &b.Messages()[i]
			}
		}
		if target == nil {
			t.Fatal("seeded message not claimed")
		}
		if err := b.Ack(ctx, *target); err != nil {
			t.Fatalf("ack: %v", err)
		}
		if err := b.Close(ctx); err != nil {
			t.Fatalf("close: %v", err)
		}
		// A fresh claim must not return the acked message.
		b2, err := store.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("re-claim: %v", err)
		}
		for _, m := range b2.Messages() {
			if m.EventID == "conf-ack" {
				t.Fatal("acked message re-claimed")
			}
		}
		_ = b2.Close(ctx)
	})

	t.Run("fail increments retry and re-claims", func(t *testing.T) {
		if err := seed(ctx, "conf-fail"); err != nil {
			t.Fatalf("seed: %v", err)
		}
		b, err := store.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		msgs := b.Messages()
		for i := range msgs {
			m := msgs[i]
			if m.EventID == "conf-fail" {
				if err := b.Fail(ctx, m, context.DeadlineExceeded); err != nil {
					t.Fatalf("fail: %v", err)
				}
			}
		}
		_ = b.Close(ctx)

		// Re-claim (Redis needs idle time before XAUTOCLAIM; allow a short wait
		// only if the store advertises stuck recovery).
		if sr, ok := store.(StuckRecoverer); ok {
			time.Sleep(50 * time.Millisecond)
			_, _ = sr.RecoverStuck(ctx, 0)
		}
		b2, err := store.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("re-claim: %v", err)
		}
		found := false
		for _, m := range b2.Messages() {
			if m.EventID == "conf-fail" {
				found = true
				if m.RetryCount < 1 {
					t.Fatalf("expected RetryCount>=1 after fail, got %d", m.RetryCount)
				}
			}
		}
		_ = b2.Close(ctx)
		if !found {
			t.Fatal("failed message was not re-claimable")
		}
	})
}

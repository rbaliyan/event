package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// setupRedis spins up an in-process miniredis and returns a RedisStore wired
// to it with the consumer group already created. Each test gets isolated Redis
// state via miniredis.RunT(t).
func setupRedis(t *testing.T, opts ...RedisStoreOption) (*RedisStore, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	// Pin consumer name and group so PEL behavior is reproducible across runs.
	allOpts := append([]RedisStoreOption{
		WithConsumerName("test-consumer"),
		WithGroupName("test-group"),
	}, opts...)
	s, err := NewRedisStore(client, allOpts...)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	if err := s.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	return s, mr, client
}

func TestNewRedisStore_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisStore(nil); err == nil {
		t.Error("NewRedisStore(nil): expected error")
	}
}

func TestNewRedisStore_Defaults(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	s, err := NewRedisStore(c)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	if s.pendingKey != "outbox:pending" {
		t.Errorf("default pendingKey: got %q", s.pendingKey)
	}
	if s.groupName != "outbox-relay" {
		t.Errorf("default groupName: got %q", s.groupName)
	}
	if s.failedPrefix != "outbox:failed:" {
		t.Errorf("default failedPrefix: got %q", s.failedPrefix)
	}
	if s.consumerName == "" {
		t.Error("default consumerName should be set (UUID fallback)")
	}
}

func TestNewRedisStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t, WithKeyPrefix("custom:"), WithMaxLen(1000))
	if s.pendingKey != "custom:pending" {
		t.Errorf("WithKeyPrefix.pendingKey: got %q", s.pendingKey)
	}
	if s.publishedKey != "custom:published" {
		t.Errorf("WithKeyPrefix.publishedKey: got %q", s.publishedKey)
	}
	if s.failedPrefix != "custom:failed:" {
		t.Errorf("WithKeyPrefix.failedPrefix: got %q", s.failedPrefix)
	}
	if s.maxLen != 1000 {
		t.Errorf("WithMaxLen: got %d", s.maxLen)
	}
	if s.consumerName != "test-consumer" || s.groupName != "test-group" {
		t.Errorf("WithConsumerName/WithGroupName: got consumer=%q group=%q", s.consumerName, s.groupName)
	}
}

func TestRedisStore_EnsureGroup_TolerateBusygroup(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t) // setupRedis already created the group once
	ctx := context.Background()

	// A second EnsureGroup must NOT fail — BUSYGROUP is the documented
	// re-create case (relay restart against an existing stream).
	if err := s.EnsureGroup(ctx); err != nil {
		t.Errorf("second EnsureGroup must tolerate BUSYGROUP; got %v", err)
	}
}

func TestRedisStore_EnsureReady_WrapsEnsureGroup(t *testing.T) {
	t.Parallel()
	// A fresh store with no group yet: EnsureReady must create it so a
	// subsequent ClaimPending's XREADGROUP does not return NOGROUP.
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s, err := NewRedisStore(c, WithConsumerName("c"), WithGroupName("g"))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	ctx := context.Background()
	if err := s.EnsureReady(ctx); err != nil {
		t.Fatalf("EnsureReady: %v", err)
	}
	if _, err := s.ClaimPending(ctx, 10); err != nil {
		t.Fatalf("ClaimPending after EnsureReady: %v", err)
	}
}

func TestRedisStore_Store_AppendsToStream(t *testing.T) {
	t.Parallel()
	s, mr, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "order.created", "evt-1", []byte(`{"id":1}`), map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	entries, err := mr.Stream("outbox:pending")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Store did not append one entry; got %d", len(entries))
	}
}

func TestRedisStore_ClaimPending_EmptyReturnsResourceFreeBatch(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	// Block: -1 makes an empty XREADGROUP non-blocking — this must return
	// immediately with a non-nil empty batch, not hang.
	b, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending empty: %v", err)
	}
	if b == nil {
		t.Fatal("empty claim must return non-nil Batch")
	}
	if len(b.Messages()) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(b.Messages()))
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close empty batch: %v", err)
	}
}

func TestRedisStore_ClaimPending_ReturnsNewEntries(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	for _, id := range []string{"e1", "e2"} {
		if err := s.Store(ctx, "evt", id, []byte("p"), nil); err != nil {
			t.Fatalf("Store %s: %v", id, err)
		}
	}

	b, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	msgs := b.Messages()
	if len(msgs) != 2 {
		t.Fatalf("ClaimPending: got %d messages, want 2", len(msgs))
	}
	for _, m := range msgs {
		if m.EventName != "evt" {
			t.Errorf("EventName: got %q", m.EventName)
		}
		if m.RetryCount != 0 {
			t.Errorf("first delivery is not a retry; RetryCount=%d, want 0", m.RetryCount)
		}
		if m.Status != StatusProcessing {
			t.Errorf("claimed message Status=%v, want %v", m.Status, StatusProcessing)
		}
		if Token(m) == nil {
			t.Error("claimed message missing backend token")
		}
	}
}

func TestRedisStore_ClaimPending_ParsesPayloadAndMetadata(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "evt", "e1", []byte(`{"n":1}`), map[string]string{"trace": "abc"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	b, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	msgs := b.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.EventID != "e1" {
		t.Errorf("EventID: got %q", m.EventID)
	}
	if string(m.Payload) != `{"n":1}` {
		t.Errorf("Payload: got %q", string(m.Payload))
	}
	if m.Metadata["trace"] != "abc" {
		t.Errorf("Metadata: got %v", m.Metadata)
	}
}

func TestRedisStore_Ack_RemovesFromClaimableSet(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "evt", "e1", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	b, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	msgs := b.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d, want 1", len(msgs))
	}
	if err := b.Ack(ctx, msgs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	_ = b.Close(ctx)

	// After Ack (XACK + XDEL) a fresh claim must return nothing: the PEL is
	// cleared and the stream entry is deleted.
	b2, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("second ClaimPending: %v", err)
	}
	if len(b2.Messages()) != 0 {
		t.Errorf("acked message re-claimed; got %d", len(b2.Messages()))
	}
	_ = b2.Close(ctx)
}

func TestRedisStore_Fail_LeavesInPELAndReDelivers(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "evt", "e1", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	b, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if len(b.Messages()) != 1 {
		t.Fatalf("got %d, want 1", len(b.Messages()))
	}
	// Fail must NOT XACK — the entry stays in the PEL for re-delivery.
	if err := b.Fail(ctx, b.Messages()[0], context.DeadlineExceeded); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	_ = b.Close(ctx)

	// Bump the delivery-count via RecoverStuck (XAUTOCLAIM), mirroring the
	// crashed-consumer sweep, then re-claim: RetryCount must reflect the PEL
	// delivery-count (>=1 after the failure).
	if _, err := s.RecoverStuck(ctx, 0); err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	b2, err := s.ClaimPending(ctx, 10)
	if err != nil {
		t.Fatalf("second ClaimPending: %v", err)
	}
	msgs := b2.Messages()
	if len(msgs) != 1 {
		t.Fatalf("failed message not re-claimable; got %d", len(msgs))
	}
	if msgs[0].RetryCount < 1 {
		t.Errorf("expected RetryCount>=1 after fail+re-claim, got %d", msgs[0].RetryCount)
	}
	_ = b2.Close(ctx)
}

func TestRedisStore_ClaimPending_RetryCountProgression(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	failErr := errors.New("handler failed")

	if err := s.Store(ctx, "evt", "e1", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Re-claiming the same unacked entry via the PEL path (XPENDING + XCLAIM)
	// must report RetryCount as the number of PRIOR failed deliveries: 0 on
	// the first (XREADGROUP) read, then 1, then 2 on each subsequent re-claim
	// after a Fail. This mirrors Postgres, which increments retry_count on
	// each Fail. Deliberately does NOT use RecoverStuck/XAUTOCLAIM here: that
	// path injects its own delivery-count bump and would mask an off-by-one
	// in the plain re-claim path exercised by ClaimPending alone.
	for i, want := range []int{0, 1, 2} {
		b, err := s.ClaimPending(ctx, 10)
		if err != nil {
			t.Fatalf("ClaimPending iteration %d: %v", i, err)
		}
		msgs := b.Messages()
		if len(msgs) != 1 {
			t.Fatalf("ClaimPending iteration %d: got %d messages, want 1", i, len(msgs))
		}
		if msgs[0].RetryCount != want {
			t.Errorf("ClaimPending iteration %d: RetryCount=%d, want %d", i, msgs[0].RetryCount, want)
		}
		if err := b.Fail(ctx, msgs[0], failErr); err != nil {
			t.Fatalf("Fail iteration %d: %v", i, err)
		}
		if err := b.Close(ctx); err != nil {
			t.Fatalf("Close iteration %d: %v", i, err)
		}
	}
}

func TestRedisStore_Cleanup_IsNoOp(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	n, err := s.Cleanup(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if n != 0 {
		t.Errorf("Cleanup should be a no-op returning 0; got %d", n)
	}
}

func TestRedisStore_RecoverStuck_ClaimsIdleMessages(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "evt", "e1", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := s.ClaimPending(ctx, 10); err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	// Use a real (small) sleep rather than miniredis.FastForward — Stream PEL
	// idle accounting is keyed off wall-clock delivery time. A 60ms sleep with
	// a 25ms idle threshold gives comfortable headroom while staying fast.
	time.Sleep(60 * time.Millisecond)

	moved, err := s.RecoverStuck(ctx, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	if moved != 1 {
		t.Errorf("RecoverStuck: got %d, want 1", moved)
	}
}

func TestRedisStore_RecoverStuck_SkipsRecentMessages(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.Store(ctx, "evt", "e1", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := s.ClaimPending(ctx, 10); err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	// Entry is idle ~0ms; a 5s threshold must not reclaim it.
	moved, err := s.RecoverStuck(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	if moved != 0 {
		t.Errorf("RecoverStuck: recent message should NOT be reclaimed; got %d, want 0", moved)
	}
}

func TestRedisStore_RecoverStuck_EmptyPending(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	moved, err := s.RecoverStuck(ctx, time.Second)
	if err != nil {
		t.Fatalf("RecoverStuck on empty: %v", err)
	}
	if moved != 0 {
		t.Errorf("RecoverStuck on empty: got %d, want 0", moved)
	}
}

// TestRedisStore_Conformance_Miniredis runs the shared Store conformance harness
// against an in-memory miniredis, exercising the full claim/ack/fail/re-claim
// PEL path without a live server.
func TestRedisStore_Conformance_Miniredis(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	seed := func(ctx context.Context, eventID string) error {
		return s.Store(ctx, "conf.event", eventID, []byte(`{}`), nil)
	}
	RunStoreConformance(t, ctx, s, seed)
}

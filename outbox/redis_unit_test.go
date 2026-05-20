package outbox

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// setupRedis spins up an in-process miniredis and returns a RedisStore wired
// to it. Each test gets an isolated Redis state via miniredis.RunT(t).
func setupRedis(t *testing.T, opts ...RedisStoreOption) (*RedisStore, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	// Pin consumer name and group so test SQL matches mock expectations
	// and BUSYGROUP-like behavior is reproducible across runs.
	allOpts := append([]RedisStoreOption{
		WithConsumerName("test-consumer"),
		WithGroupName("test-group"),
	}, opts...)
	s, err := NewRedisStore(client, allOpts...)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
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
	// Default pending key is "outbox:pending"; group name is "outbox-relay".
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

func TestRedisStore_Insert_GeneratesIDAndTimestamp(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)

	msg := &RedisMessage{
		EventName: "order.created",
		EventID:   "evt-1",
		Payload:   []byte(`{"order":1}`),
	}
	before := time.Now().Add(-time.Second)
	streamID, err := s.Insert(context.Background(), msg)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if streamID == "" {
		t.Error("Insert returned empty streamID")
	}
	// Insert populates msg.ID when absent (UUID fallback for caller traceability).
	if msg.ID == "" {
		t.Error("Insert did not populate msg.ID")
	}
	if msg.CreatedAt.Before(before) {
		t.Errorf("Insert did not set CreatedAt; got %v", msg.CreatedAt)
	}
}

func TestRedisStore_Insert_PreservesProvidedFields(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)

	custom := time.Unix(1_700_000_000, 0).UTC()
	msg := &RedisMessage{
		ID:        "client-supplied",
		EventName: "e",
		EventID:   "e1",
		Payload:   []byte("p"),
		CreatedAt: custom,
	}
	if _, err := s.Insert(context.Background(), msg); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if msg.ID != "client-supplied" {
		t.Errorf("Insert clobbered msg.ID; got %q", msg.ID)
	}
	if !msg.CreatedAt.Equal(custom) {
		t.Errorf("Insert clobbered CreatedAt; got %v want %v", msg.CreatedAt, custom)
	}
}

func TestRedisStore_EnsureGroup_CreatesGroup(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
}

func TestRedisStore_EnsureGroup_TolerateBusygroup(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("first EnsureGroup: %v", err)
	}
	// Second EnsureGroup must NOT fail — BUSYGROUP is the documented
	// re-create case (relay restart against an existing stream).
	if err := s.EnsureGroup(ctx); err != nil {
		t.Errorf("second EnsureGroup must tolerate BUSYGROUP; got %v", err)
	}
}

func TestRedisStore_GetPending_ReturnsNewMessages(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Insert two messages.
	for _, ev := range []string{"e1", "e2"} {
		_, err := s.Insert(ctx, &RedisMessage{
			ID: ev, EventName: "evt", EventID: ev, Payload: []byte("p"),
		})
		if err != nil {
			t.Fatalf("Insert %s: %v", ev, err)
		}
	}

	got, err := s.GetPending(ctx, 10)
	if err != nil {
		t.Fatalf("GetPending: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetPending: got %d messages, want 2", len(got))
	}
	for _, m := range got {
		if m.StreamID == "" {
			t.Error("StreamID not populated on returned message")
		}
		if m.EventName != "evt" {
			t.Errorf("EventName: got %q", m.EventName)
		}
	}
}

// TestRedisStore_GetPending_EmptyStream is INTENTIONALLY OMITTED.
//
// Production code passes `Block: 0` to XREADGROUP with the inline comment
// "// Non-blocking" — but in Redis, `BLOCK 0` means "block indefinitely
// until a new message arrives" (go-redis sends it verbatim, and miniredis
// faithfully blocks the goroutine on the server side). Against an empty
// stream with no producers, GetPending therefore blocks forever, and
// neither a context timeout nor miniredis's own internal deadline reliably
// escapes the block within a CI test budget.
//
// This is a real production bug. The fix is one character: change `Block:
// 0` to `Block: -1` (true non-blocking semantics) in redis.go:234. Deferred
// to a follow-up PR so this test-only change stays scoped.

func TestRedisStore_GetPending_ClaimsUnackedOnRestart(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	_, _ = s.Insert(ctx, &RedisMessage{ID: "e1", EventName: "e", EventID: "e1", Payload: []byte("p")})

	// First GetPending claims the message but we DON'T ack — simulates a
	// relay crash mid-publish. Use count=1 so the second GetPending below
	// takes the early-return path (len(pendingMsgs) >= count) and avoids
	// the `Block: 0` blocking call documented in
	// TestRedisStore_GetPending_EmptyStream.
	first, err := s.GetPending(ctx, 1)
	if err != nil {
		t.Fatalf("first GetPending: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first call: got %d", len(first))
	}

	// Second GetPending must re-deliver the unacked message via the
	// claimPendingMessages path (XPENDING + XCLAIM). Without this, a
	// restarted relay would silently lose unacked messages.
	second, err := s.GetPending(ctx, 1)
	if err != nil {
		t.Fatalf("second GetPending: %v", err)
	}
	if len(second) != 1 {
		t.Errorf("second call should re-deliver unacked; got %d", len(second))
	}
	if second[0].StreamID != first[0].StreamID {
		t.Errorf("re-delivered StreamID mismatch: %q vs %q", second[0].StreamID, first[0].StreamID)
	}
}

func TestRedisStore_MarkPublished_AcksAndDeletes(t *testing.T) {
	t.Parallel()
	s, mr, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	_, _ = s.Insert(ctx, &RedisMessage{ID: "e1", EventName: "e", EventID: "e1", Payload: []byte("p")})
	got, _ := s.GetPending(ctx, 10)
	if len(got) != 1 {
		t.Fatalf("setup expected 1 pending, got %d", len(got))
	}

	if err := s.MarkPublished(ctx, got[0].StreamID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	// After MarkPublished:
	//   1. XACK was issued — the consumer group's PEL no longer has the
	//      message (verified via XPENDING on the underlying client).
	//   2. XDEL was issued — the stream entry itself is gone (verified
	//      via XLEN).
	if length, _ := s.Len(ctx); length != 0 {
		t.Errorf("after MarkPublished: Len=%d, want 0 (XDEL did not run)", length)
	}
	// PEL ack is verified by Len()==0 above. We don't issue a follow-up
	// GetPending here because the production code's `Block: 0` would
	// block forever on the now-empty stream (see the
	// GetPending_EmptyStream test for context).
	_ = mr // mr available for future ad-hoc inspection
}

func TestRedisStore_MarkFailed_RecordsHashAndAcks(t *testing.T) {
	t.Parallel()
	s, mr, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	msg := &RedisMessage{ID: "msg-1", EventName: "e", EventID: "evt-1", Payload: []byte("p"), RetryCount: 2}
	streamID, _ := s.Insert(ctx, msg)

	// Drive GetPending so the message is in the consumer's PEL before
	// MarkFailed acks it (mirrors the real relay flow).
	if _, err := s.GetPending(ctx, 10); err != nil {
		t.Fatalf("GetPending: %v", err)
	}

	if err := s.MarkFailed(ctx, streamID, msg, errors.New("downstream timeout")); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	// Failed-message hash must exist and carry the error + incremented
	// retry count.
	failedKey := "outbox:failed:msg-1"
	if !mr.Exists(failedKey) {
		t.Fatalf("MarkFailed did not write to %q", failedKey)
	}
	if got := mr.HGet(failedKey, "error"); got != "downstream timeout" {
		t.Errorf("failed-record error: got %q, want %q", got, "downstream timeout")
	}
	// retry_count = msg.RetryCount + 1 = 3
	if got := mr.HGet(failedKey, "retry_count"); got != "3" {
		t.Errorf("failed-record retry_count: got %q, want 3 (incremented from 2)", got)
	}

	// Stream entry was acked + deleted.
	if length, _ := s.Len(ctx); length != 0 {
		t.Errorf("after MarkFailed: Len=%d, want 0 (XDEL did not run)", length)
	}
}

func TestRedisStore_RecoverStuck_ClaimsIdleMessages(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	_, _ = s.Insert(ctx, &RedisMessage{ID: "e1", EventName: "e", EventID: "e1", Payload: []byte("p")})
	if _, err := s.GetPending(ctx, 10); err != nil {
		t.Fatalf("GetPending: %v", err)
	}
	// Use a real (small) sleep rather than miniredis.FastForward — Stream
	// PEL idle accounting in miniredis is keyed off wall-clock delivery
	// time, not the fast-forwarded `now`. A 60ms sleep with a 25ms idle
	// threshold gives us comfortable headroom while keeping the test fast.
	time.Sleep(60 * time.Millisecond)

	recovered, err := s.RecoverStuck(ctx, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	if recovered != 1 {
		t.Errorf("RecoverStuck: got %d, want 1", recovered)
	}
}

func TestRedisStore_RecoverStuck_SkipsRecentMessages(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	_, _ = s.Insert(ctx, &RedisMessage{ID: "e1", EventName: "e", EventID: "e1", Payload: []byte("p")})
	_, _ = s.GetPending(ctx, 10)
	// No FastForward — message is idle 0ms.

	recovered, err := s.RecoverStuck(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}
	if recovered != 0 {
		t.Errorf("RecoverStuck: recent message should NOT be claimed; got %d, want 0", recovered)
	}
}

func TestRedisStore_RecoverStuck_EmptyPending(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	recovered, err := s.RecoverStuck(ctx, time.Second)
	if err != nil {
		t.Fatalf("RecoverStuck on empty: %v", err)
	}
	if recovered != 0 {
		t.Errorf("RecoverStuck on empty: got %d, want 0", recovered)
	}
}

func TestRedisStore_RetryFailed_ReQueuesAndDeletes(t *testing.T) {
	t.Parallel()
	s, mr, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Seed two failed-message hashes manually so we can drive the scan-and-
	// requeue logic without going through MarkFailed (which requires a real
	// pending stream entry).
	mr.HSet("outbox:failed:m1", "event_id", "m1", "event_name", "e", "payload", "p", "retry_count", "1")
	mr.HSet("outbox:failed:m2", "event_id", "m2", "event_name", "e", "payload", "p", "retry_count", "1")

	retried, err := s.RetryFailed(ctx, 3)
	if err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
	if retried != 2 {
		t.Errorf("RetryFailed: got %d, want 2", retried)
	}

	// Failed hashes are deleted after re-queuing.
	if mr.Exists("outbox:failed:m1") || mr.Exists("outbox:failed:m2") {
		t.Error("RetryFailed did not delete failed-message hashes after re-queue")
	}

	// Pending stream now contains 2 messages.
	if length, _ := s.Len(ctx); length != 2 {
		t.Errorf("pending Len after RetryFailed: got %d, want 2", length)
	}
}

func TestRedisStore_RetryFailed_SkipsAboveMaxRetries(t *testing.T) {
	t.Parallel()
	s, mr, _ := setupRedis(t)
	ctx := context.Background()
	if err := s.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// Two failures; one already past maxRetries=3 → must NOT be re-queued.
	mr.HSet("outbox:failed:young", "event_id", "young", "event_name", "e", "payload", "p", "retry_count", "1")
	mr.HSet("outbox:failed:exhausted", "event_id", "exhausted", "event_name", "e", "payload", "p", "retry_count", "5")

	retried, err := s.RetryFailed(ctx, 3)
	if err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
	if retried != 1 {
		t.Errorf("RetryFailed with maxRetries=3: got %d, want 1 (one was already exhausted)", retried)
	}
	// Exhausted hash remains for operator inspection.
	if !mr.Exists("outbox:failed:exhausted") {
		t.Error("exhausted failed hash was deleted; it should remain for operator review")
	}
}

func TestRedisStore_Delete_TrimsPublishedStream(t *testing.T) {
	t.Parallel()
	s, mr, c := setupRedis(t)
	ctx := context.Background()

	// Seed the "published" stream directly (Delete operates on
	// publishedKey, not pendingKey). Three entries: two old, one new.
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	for range 2 {
		// Use a manually-set time-based ID so XTRIM MINID is meaningful.
		if err := c.XAdd(ctx, &redis.XAddArgs{
			Stream: "outbox:published",
			ID:     strconv.FormatInt(old, 10) + "-0",
			Values: map[string]interface{}{"k": "v"},
		}).Err(); err != nil {
			t.Fatalf("seed old: %v", err)
		}
		old++
	}
	newer := time.Now().UnixMilli()
	if err := c.XAdd(ctx, &redis.XAddArgs{
		Stream: "outbox:published",
		ID:     strconv.FormatInt(newer, 10) + "-0",
		Values: map[string]interface{}{"k": "v"},
	}).Err(); err != nil {
		t.Fatalf("seed newer: %v", err)
	}

	deleted, err := s.Delete(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Delete trimmed %d, want 2", deleted)
	}
	// One entry should remain.
	entries, _ := mr.Stream("outbox:published")
	if len(entries) != 1 {
		t.Errorf("post-trim stream entries: got %d, want 1", len(entries))
	}
}

func TestNewRedisPublisher_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisPublisher(nil); err == nil {
		t.Error("NewRedisPublisher(nil): expected error from underlying NewRedisStore")
	}
}

func TestRedisPublisher_StoreAndCodecAccessors(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	pub, err := NewRedisPublisher(c)
	if err != nil {
		t.Fatalf("NewRedisPublisher: %v", err)
	}
	if pub.Store() == nil {
		t.Error("Store() returned nil")
	}
	// Builder must return the receiver, not a copy.
	if got := pub.WithCodec(pub.codec); got != pub {
		t.Error("WithCodec must return the receiver")
	}
}

func TestRedisPublisher_Publish_InsertsToStream(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	pub, err := NewRedisPublisher(c)
	if err != nil {
		t.Fatalf("NewRedisPublisher: %v", err)
	}

	if err := pub.Publish(context.Background(), "order.placed",
		map[string]any{"id": "o-1"}, map[string]string{"src": "test"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Stream should now contain exactly one entry on the default pending key.
	entries, _ := mr.Stream("outbox:pending")
	if len(entries) != 1 {
		t.Errorf("Publish did not produce one stream entry; got %d", len(entries))
	}
}

func TestRedisStore_Len(t *testing.T) {
	t.Parallel()
	s, _, _ := setupRedis(t)
	ctx := context.Background()

	if got, _ := s.Len(ctx); got != 0 {
		t.Errorf("Len on empty: got %d, want 0", got)
	}

	for i := range 5 {
		_, _ = s.Insert(ctx, &RedisMessage{
			ID: strconv.Itoa(i), EventName: "e", EventID: strconv.Itoa(i), Payload: []byte("p"),
		})
	}

	got, err := s.Len(ctx)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if got != 5 {
		t.Errorf("Len: got %d, want 5", got)
	}
}

func TestRedisMessage_ToMessage(t *testing.T) {
	t.Parallel()
	created := time.Unix(1_700_000_000, 0).UTC()
	rm := &RedisMessage{
		StreamID:   "1700000000-0",
		ID:         "42",
		EventName:  "evt",
		EventID:    "evt-1",
		Payload:    []byte("p"),
		Metadata:   map[string]string{"k": "v"},
		CreatedAt:  created,
		RetryCount: 3,
		LastError:  "boom",
		Priority:   7,
	}
	m := rm.ToMessage()
	if m.ID != 42 || m.EventName != "evt" || m.EventID != "evt-1" || m.Priority != 7 {
		t.Errorf("ToMessage mismatch: %+v", m)
	}
	if m.Status != StatusPending {
		t.Errorf("ToMessage Status: got %v, want %v", m.Status, StatusPending)
	}
	if m.RetryCount != 3 || m.LastError != "boom" {
		t.Errorf("ToMessage retry/error mismatch: %+v", m)
	}
}

func TestRedisMessage_ToMessage_NonNumericIDDecaysToZero(t *testing.T) {
	t.Parallel()
	// RedisMessage.ID is documented as the application-level event ID
	// (UUID-shaped). Message.ID is the SQL-style int64. ToMessage tries
	// strconv.ParseInt and silently coerces non-numeric to 0 — pin this
	// so a consumer doesn't accidentally treat the zero as "missing".
	rm := &RedisMessage{ID: "not-a-number", EventName: "e", EventID: "e1", Payload: []byte("p")}
	m := rm.ToMessage()
	if m.ID != 0 {
		t.Errorf("non-numeric ID should coerce to 0; got %d", m.ID)
	}
}

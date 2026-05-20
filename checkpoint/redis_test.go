package checkpoint

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// setup spins up an in-process miniredis instance and returns a configured
// RedisStore plus the underlying miniredis handle. Each test gets its own
// independent Redis state.
func setup(t *testing.T, opts ...RedisOption) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	s, err := NewRedisStore(client, "test:checkpoints", opts...)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return s, mr
}

func TestNewRedisStore_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisStore(nil, "k"); err == nil {
		t.Error("NewRedisStore(nil): expected error")
	}
}

func TestNewRedisStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _ := setup(t, WithTTL(time.Hour))
	if s.ttl != time.Hour {
		t.Errorf("WithTTL: got %v, want 1h", s.ttl)
	}
	if s.key != "test:checkpoints" {
		t.Errorf("key: got %q", s.key)
	}
}

func TestRedisStore_SaveLoad_RoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	// Use a nanosecond-precise timestamp to verify the encoding preserves
	// full nanosecond resolution (not just milliseconds).
	want := time.Unix(1_700_000_000, 123_456_789).UTC()
	if err := s.Save(ctx, "sub-A", want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "sub-A")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("round-trip: got %v, want %v", got, want)
	}
}

func TestRedisStore_Save_OverwritesPreviousPosition(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	older := time.Unix(1_700_000_000, 0)
	newer := time.Unix(1_800_000_000, 0)

	if err := s.Save(ctx, "sub", older); err != nil {
		t.Fatalf("Save older: %v", err)
	}
	if err := s.Save(ctx, "sub", newer); err != nil {
		t.Fatalf("Save newer: %v", err)
	}

	got, _ := s.Load(ctx, "sub")
	if !got.Equal(newer) {
		t.Errorf("after overwrite: got %v, want %v (newer write must win)", got, newer)
	}
}

func TestRedisStore_Load_MissingReturnsZeroAndNil(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)

	// Documented contract: missing key returns (time.Time{}, nil) so
	// callers distinguish "first run" from a Redis error via the error
	// value, not via inspecting the timestamp.
	got, err := s.Load(context.Background(), "never-saved")
	if err != nil {
		t.Errorf("Load missing: returned err %v, want nil", err)
	}
	if !got.IsZero() {
		t.Errorf("Load missing: got %v, want zero time", got)
	}
}

func TestRedisStore_Load_CorruptValueReturnsParseError(t *testing.T) {
	t.Parallel()
	s, mr := setup(t)

	// Inject a non-numeric value directly into Redis to exercise the
	// strconv.ParseInt error path. This is a production failure-mode
	// (someone manually wrote to the hash from another tool, or a previous
	// version of the store used a different encoding).
	mr.HSet("test:checkpoints", "corrupted", "not-a-number")

	got, err := s.Load(context.Background(), "corrupted")
	if err == nil {
		t.Error("Load on corrupt value: expected error")
	}
	if !got.IsZero() {
		t.Errorf("Load on error: got %v, want zero", got)
	}
}

func TestRedisStore_Save_TTLAppliedToHashKey(t *testing.T) {
	t.Parallel()
	s, mr := setup(t, WithTTL(2*time.Hour))
	ctx := context.Background()

	if err := s.Save(ctx, "sub", time.Now()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// miniredis tracks per-key TTL; verify the hash key got expired set.
	ttl := mr.TTL("test:checkpoints")
	if ttl == 0 {
		t.Error("TTL not applied to hash key after Save with WithTTL")
	}
	if ttl > 2*time.Hour {
		t.Errorf("TTL exceeds configured value: got %v, want <= 2h", ttl)
	}
}

func TestRedisStore_Save_NoTTLLeavesKeyPersistent(t *testing.T) {
	t.Parallel()
	s, mr := setup(t) // no WithTTL
	ctx := context.Background()

	if err := s.Save(ctx, "sub", time.Now()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if ttl := mr.TTL("test:checkpoints"); ttl != 0 {
		t.Errorf("no WithTTL configured but TTL was set: %v", ttl)
	}
}

func TestRedisStore_Save_TTLRefreshedOnEachSave(t *testing.T) {
	t.Parallel()
	s, mr := setup(t, WithTTL(time.Hour))
	ctx := context.Background()

	if err := s.Save(ctx, "sub", time.Now()); err != nil {
		t.Fatalf("Save 1: %v", err)
	}
	mr.FastForward(45 * time.Minute) // Aged 45 minutes into the TTL

	if err := s.Save(ctx, "sub", time.Now()); err != nil {
		t.Fatalf("Save 2: %v", err)
	}

	// After the second Save, TTL should be refreshed to ~1h, NOT the
	// remaining 15 minutes from the original Save.
	if ttl := mr.TTL("test:checkpoints"); ttl <= 15*time.Minute {
		t.Errorf("TTL not refreshed; got %v, want close to 1h", ttl)
	}
}

func TestRedisStore_Delete(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	_ = s.Save(ctx, "sub-A", time.Now())
	_ = s.Save(ctx, "sub-B", time.Now())

	if err := s.Delete(ctx, "sub-A"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, _ := s.Load(ctx, "sub-A")
	if !got.IsZero() {
		t.Errorf("after Delete: Load returned %v, want zero", got)
	}
	// Untouched subscriber should still be there.
	got, _ = s.Load(ctx, "sub-B")
	if got.IsZero() {
		t.Error("Delete leaked beyond targeted subscriber")
	}
}

func TestRedisStore_Delete_MissingIsNoOp(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	// HDel on a non-existent field is a no-op in Redis — the contract
	// surfaced through the store is that Delete returns nil regardless.
	if err := s.Delete(context.Background(), "never-existed"); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
}

func TestRedisStore_DeleteAll(t *testing.T) {
	t.Parallel()
	s, mr := setup(t)
	ctx := context.Background()

	_ = s.Save(ctx, "sub-A", time.Now())
	_ = s.Save(ctx, "sub-B", time.Now())

	if err := s.DeleteAll(ctx); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	if mr.Exists("test:checkpoints") {
		t.Error("DeleteAll did not remove the hash key")
	}
}

func TestRedisStore_List(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	_ = s.Save(ctx, "sub-A", time.Now())
	_ = s.Save(ctx, "sub-B", time.Now())
	_ = s.Save(ctx, "sub-C", time.Now())

	got, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("List length: got %d, want 3", len(got))
	}
	// Convert to set for order-independent membership check.
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	for _, want := range []string{"sub-A", "sub-B", "sub-C"} {
		if !seen[want] {
			t.Errorf("List missing %q; got %v", want, got)
		}
	}
}

func TestRedisStore_List_EmptyReturnsEmpty(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	got, err := s.List(context.Background())
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List empty: got %d entries, want 0", len(got))
	}
}

func TestRedisStore_GetAll(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	pA := time.Unix(1_700_000_000, 0)
	pB := time.Unix(1_700_000_500, 0)
	_ = s.Save(ctx, "sub-A", pA)
	_ = s.Save(ctx, "sub-B", pB)

	got, err := s.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetAll: got %d entries, want 2", len(got))
	}
	if !got["sub-A"].Equal(pA) {
		t.Errorf("sub-A: got %v, want %v", got["sub-A"], pA)
	}
	if !got["sub-B"].Equal(pB) {
		t.Errorf("sub-B: got %v, want %v", got["sub-B"], pB)
	}
}

func TestRedisStore_GetAll_SkipsCorruptEntries(t *testing.T) {
	t.Parallel()
	s, mr := setup(t)
	ctx := context.Background()

	// Mix valid + corrupt entries. The valid entry must come through;
	// the corrupt one must be silently skipped (documented contract,
	// matches "skip invalid entries" comment in production).
	_ = s.Save(ctx, "good", time.Unix(1_700_000_000, 0))
	mr.HSet("test:checkpoints", "bad", "not-a-number")
	mr.HSet("test:checkpoints", "alsobad", "")

	got, err := s.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("GetAll: got %d entries, want 1 (corrupt entries skipped)", len(got))
	}
	if _, ok := got["good"]; !ok {
		t.Error("valid entry missing from GetAll output")
	}
}

func TestRedisStore_GetAll_EmptyReturnsEmptyMap(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	got, err := s.GetAll(context.Background())
	if err != nil {
		t.Fatalf("GetAll empty: %v", err)
	}
	if got == nil {
		t.Error("GetAll empty: got nil, want non-nil empty map")
	}
	if len(got) != 0 {
		t.Errorf("GetAll empty: got %d entries, want 0", len(got))
	}
}

func TestRedisStore_GetCheckpointInfo_Happy(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	ctx := context.Background()

	pos := time.Unix(1_700_000_000, 42)
	_ = s.Save(ctx, "sub", pos)

	info, err := s.GetCheckpointInfo(ctx, "sub")
	if err != nil {
		t.Fatalf("GetCheckpointInfo: %v", err)
	}
	if info == nil {
		t.Fatal("GetCheckpointInfo returned nil on existing checkpoint")
	}
	if info.SubscriberID != "sub" {
		t.Errorf("SubscriberID: got %q", info.SubscriberID)
	}
	if !info.Position.Equal(pos) {
		t.Errorf("Position: got %v, want %v", info.Position, pos)
	}
	// Documented: Redis store doesn't track UpdatedAt separately, so
	// UpdatedAt equals Position.
	if !info.UpdatedAt.Equal(info.Position) {
		t.Errorf("UpdatedAt should equal Position for Redis store; got %v vs %v",
			info.UpdatedAt, info.Position)
	}
}

func TestRedisStore_GetCheckpointInfo_MissingReturnsNilNil(t *testing.T) {
	t.Parallel()
	s, _ := setup(t)
	// No checkpoint saved — expect (nil, nil) per documented contract.
	info, err := s.GetCheckpointInfo(context.Background(), "absent")
	if err != nil {
		t.Errorf("GetCheckpointInfo missing: %v", err)
	}
	if info != nil {
		t.Errorf("GetCheckpointInfo missing: got %+v, want nil", info)
	}
}

func TestRedisStore_GetCheckpointInfo_CorruptValueErrors(t *testing.T) {
	t.Parallel()
	s, mr := setup(t)
	mr.HSet("test:checkpoints", "corrupt", "garbage")

	info, err := s.GetCheckpointInfo(context.Background(), "corrupt")
	if err == nil {
		t.Error("GetCheckpointInfo on corrupt entry: expected error")
	}
	if info != nil {
		t.Errorf("GetCheckpointInfo on error: got %+v, want nil", info)
	}
}

func TestRedisStore_Encoding_UsesUnixNanos(t *testing.T) {
	t.Parallel()
	// Pin the wire format: position is stored as Unix nanoseconds.
	// Operators inspecting Redis via redis-cli need this to be stable;
	// a switch to millisecond encoding would silently break dashboards
	// that decode the hash directly.
	s, mr := setup(t)
	pos := time.Unix(1_700_000_000, 123_456_789)
	_ = s.Save(context.Background(), "sub", pos)

	raw := mr.HGet("test:checkpoints", "sub")
	want := strconv.FormatInt(pos.UnixNano(), 10)
	if raw != want {
		t.Errorf("wire format drifted: got %q, want %q (UnixNano encoding)", raw, want)
	}
}

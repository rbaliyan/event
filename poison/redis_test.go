package poison

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// setupRedis spins up an in-process miniredis and returns a RedisStore plus
// the underlying mock and client. Each test owns its own isolated Redis
// state via miniredis.RunT(t).
func setupRedis(t *testing.T, opts ...RedisStoreOption) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	s, err := NewRedisStore(c, opts...)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return s, mr
}

func TestNewRedisStore_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisStore(nil); err == nil {
		t.Error("NewRedisStore(nil): expected error")
	}
}

func TestNewRedisStore_Defaults(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	if s.failurePrefix != "poison:failures:" {
		t.Errorf("default failurePrefix: got %q", s.failurePrefix)
	}
	if s.quarantinePrefix != "poison:quarantine:" {
		t.Errorf("default quarantinePrefix: got %q", s.quarantinePrefix)
	}
	if s.failureTTL != 24*time.Hour {
		t.Errorf("default failureTTL: got %v", s.failureTTL)
	}
}

func TestNewRedisStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t,
		WithFailurePrefix("myapp:failures:"),
		WithQuarantinePrefix("myapp:quarantine:"),
		WithFailureTTL(7*24*time.Hour),
	)
	if s.failurePrefix != "myapp:failures:" {
		t.Errorf("WithFailurePrefix: got %q", s.failurePrefix)
	}
	if s.quarantinePrefix != "myapp:quarantine:" {
		t.Errorf("WithQuarantinePrefix: got %q", s.quarantinePrefix)
	}
	if s.failureTTL != 7*24*time.Hour {
		t.Errorf("WithFailureTTL: got %v", s.failureTTL)
	}
}

func TestRedisStore_IncrementFailure_Pipeline(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	ctx := context.Background()

	// Sequential calls return the running INCR value (1, 2, 3, ...).
	// This contract is load-bearing — the detector compares the returned
	// count against the threshold immediately, with no second GET.
	for i := 1; i <= 3; i++ {
		got, err := s.IncrementFailure(ctx, "msg-1")
		if err != nil {
			t.Fatalf("IncrementFailure %d: %v", i, err)
		}
		if got != i {
			t.Errorf("call %d: got %d, want %d", i, got, i)
		}
	}

	// Different message IDs have independent counters.
	got, err := s.IncrementFailure(ctx, "msg-other")
	if err != nil {
		t.Fatalf("IncrementFailure msg-other: %v", err)
	}
	if got != 1 {
		t.Errorf("msg-other independent counter: got %d, want 1", got)
	}
}

func TestRedisStore_IncrementFailure_AppliesTTL(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t, WithFailureTTL(2*time.Hour))
	ctx := context.Background()

	if _, err := s.IncrementFailure(ctx, "msg"); err != nil {
		t.Fatalf("IncrementFailure: %v", err)
	}

	ttl := mr.TTL("poison:failures:msg")
	if ttl == 0 {
		t.Error("TTL not applied to failure key")
	}
	if ttl > 2*time.Hour {
		t.Errorf("TTL exceeds configured value: got %v, want <= 2h", ttl)
	}
}

func TestRedisStore_IncrementFailure_RefreshesTTLOnEachCall(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t, WithFailureTTL(time.Hour))
	ctx := context.Background()

	if _, err := s.IncrementFailure(ctx, "msg"); err != nil {
		t.Fatalf("IncrementFailure 1: %v", err)
	}
	mr.FastForward(45 * time.Minute)

	if _, err := s.IncrementFailure(ctx, "msg"); err != nil {
		t.Fatalf("IncrementFailure 2: %v", err)
	}
	// After the second increment, TTL must refresh to ~1h, not the
	// remaining 15min.
	if ttl := mr.TTL("poison:failures:msg"); ttl <= 15*time.Minute {
		t.Errorf("TTL not refreshed; got %v, want close to 1h", ttl)
	}
}

func TestRedisStore_GetFailureCount_Missing(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)

	// Documented contract: missing message returns (0, nil). The
	// detector relies on this to treat a never-seen message as "no
	// failures yet" without surfacing a Redis-not-found error.
	got, err := s.GetFailureCount(context.Background(), "never-failed")
	if err != nil {
		t.Errorf("GetFailureCount missing: returned err %v", err)
	}
	if got != 0 {
		t.Errorf("GetFailureCount missing: got %d, want 0", got)
	}
}

func TestRedisStore_GetFailureCount_Existing(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	ctx := context.Background()

	// Drive a few increments, then verify GetFailureCount agrees with
	// the IncrementFailure return value.
	for range 4 {
		_, _ = s.IncrementFailure(ctx, "msg")
	}
	got, err := s.GetFailureCount(ctx, "msg")
	if err != nil {
		t.Fatalf("GetFailureCount: %v", err)
	}
	if got != 4 {
		t.Errorf("GetFailureCount: got %d, want 4", got)
	}
}

func TestRedisStore_GetFailureCount_CorruptValue(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)

	// Inject a non-numeric value (someone wrote to the key from another
	// tool, or a previous version of the store used a different format).
	// The current code surfaces a parse error rather than silently
	// returning 0 — pin this so a future "swallow parse errors and
	// treat as 0" refactor is a deliberate decision.
	mr.Set("poison:failures:bad", "not-a-number")

	got, err := s.GetFailureCount(context.Background(), "bad")
	if err == nil {
		t.Error("GetFailureCount on corrupt value: expected error")
	}
	if got != 0 {
		t.Errorf("GetFailureCount on error: got %d, want 0", got)
	}
}

func TestRedisStore_MarkPoison_SetsKeyWithTTL(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)
	ctx := context.Background()

	if err := s.MarkPoison(ctx, "msg", time.Hour); err != nil {
		t.Fatalf("MarkPoison: %v", err)
	}

	if !mr.Exists("poison:quarantine:msg") {
		t.Fatal("MarkPoison did not create quarantine key")
	}
	ttl := mr.TTL("poison:quarantine:msg")
	if ttl == 0 || ttl > time.Hour {
		t.Errorf("MarkPoison TTL: got %v, want close to 1h", ttl)
	}
}

func TestRedisStore_MarkPoison_OverwritesPreviousMarker(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)
	ctx := context.Background()

	if err := s.MarkPoison(ctx, "msg", 30*time.Minute); err != nil {
		t.Fatalf("first MarkPoison: %v", err)
	}
	mr.FastForward(20 * time.Minute) // ~10 minutes left on the original TTL

	if err := s.MarkPoison(ctx, "msg", time.Hour); err != nil {
		t.Fatalf("second MarkPoison: %v", err)
	}
	// New TTL replaces the old one — investigated-and-extended quarantine.
	if ttl := mr.TTL("poison:quarantine:msg"); ttl <= 30*time.Minute {
		t.Errorf("MarkPoison did not extend TTL; got %v, want close to 1h", ttl)
	}
}

func TestRedisStore_IsPoison_True(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	ctx := context.Background()

	if err := s.MarkPoison(ctx, "msg", time.Hour); err != nil {
		t.Fatalf("MarkPoison: %v", err)
	}
	got, err := s.IsPoison(ctx, "msg")
	if err != nil {
		t.Fatalf("IsPoison: %v", err)
	}
	if !got {
		t.Errorf("IsPoison after MarkPoison: got false, want true")
	}
}

func TestRedisStore_IsPoison_False(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	got, err := s.IsPoison(context.Background(), "never-marked")
	if err != nil {
		t.Fatalf("IsPoison: %v", err)
	}
	if got {
		t.Errorf("IsPoison never-marked: got true, want false")
	}
}

func TestRedisStore_IsPoison_FalseAfterTTLExpiry(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)
	ctx := context.Background()

	if err := s.MarkPoison(ctx, "msg", 10*time.Minute); err != nil {
		t.Fatalf("MarkPoison: %v", err)
	}
	mr.FastForward(11 * time.Minute)

	got, err := s.IsPoison(ctx, "msg")
	if err != nil {
		t.Fatalf("IsPoison after expiry: %v", err)
	}
	if got {
		t.Error("IsPoison after TTL expiry: got true, want false (auto-release)")
	}
}

func TestRedisStore_ClearPoison(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)
	ctx := context.Background()

	_ = s.MarkPoison(ctx, "msg", time.Hour)
	if err := s.ClearPoison(ctx, "msg"); err != nil {
		t.Fatalf("ClearPoison: %v", err)
	}
	if mr.Exists("poison:quarantine:msg") {
		t.Error("ClearPoison did not remove quarantine key")
	}
}

func TestRedisStore_ClearPoison_MissingIsNoOp(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	// Documented contract: removing a non-quarantined message returns nil
	// (idempotent admin op, matches Postgres ClearPoison behavior).
	if err := s.ClearPoison(context.Background(), "never-marked"); err != nil {
		t.Errorf("ClearPoison missing: %v", err)
	}
}

func TestRedisStore_ClearFailures(t *testing.T) {
	t.Parallel()
	s, mr := setupRedis(t)
	ctx := context.Background()

	_, _ = s.IncrementFailure(ctx, "msg")
	if err := s.ClearFailures(ctx, "msg"); err != nil {
		t.Fatalf("ClearFailures: %v", err)
	}
	if mr.Exists("poison:failures:msg") {
		t.Error("ClearFailures did not remove failure key")
	}
	// After clear, subsequent GetFailureCount returns 0.
	if got, _ := s.GetFailureCount(ctx, "msg"); got != 0 {
		t.Errorf("post-clear GetFailureCount: got %d, want 0", got)
	}
}

func TestRedisStore_ClearFailures_MissingIsNoOp(t *testing.T) {
	t.Parallel()
	s, _ := setupRedis(t)
	if err := s.ClearFailures(context.Background(), "never-failed"); err != nil {
		t.Errorf("ClearFailures missing: %v", err)
	}
}

func TestRedisStore_KeyNamespaceIsolation(t *testing.T) {
	t.Parallel()
	// Two stores configured with different prefixes (multi-tenant scenario)
	// must not share state — incrementing a failure on one must not be
	// visible on the other.
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	tenantA, _ := NewRedisStore(c, WithFailurePrefix("a:failures:"))
	tenantB, _ := NewRedisStore(c, WithFailurePrefix("b:failures:"))

	ctx := context.Background()
	_, _ = tenantA.IncrementFailure(ctx, "msg")
	_, _ = tenantA.IncrementFailure(ctx, "msg")

	if got, _ := tenantB.GetFailureCount(ctx, "msg"); got != 0 {
		t.Errorf("tenant B leaked tenant A state; got %d failures, want 0", got)
	}
	if got, _ := tenantA.GetFailureCount(ctx, "msg"); got != 2 {
		t.Errorf("tenant A own state: got %d, want 2", got)
	}
}

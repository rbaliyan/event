package schema

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// setupRedis spins up an in-process miniredis and returns a RedisProvider
// with a no-op publisher. Each test owns its own isolated Redis state.
func setupRedis(t *testing.T, opts ...RedisOption) (*RedisProvider, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	p, err := NewRedisProvider(c, func(context.Context, SchemaChangeEvent) error { return nil }, opts...)
	if err != nil {
		t.Fatalf("NewRedisProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, mr
}

func TestNewRedisProvider_NilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewRedisProvider(nil, func(context.Context, SchemaChangeEvent) error { return nil }); err == nil {
		t.Error("NewRedisProvider(nil client): expected error")
	}
}

func TestNewRedisProvider_NilPublisher(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	if _, err := NewRedisProvider(c, nil); err == nil {
		t.Error("NewRedisProvider(nil publisher): expected error")
	}
}

func TestNewRedisProvider_DefaultsAndOptions(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	if p.key != "event:schemas" {
		t.Errorf("default key: got %q, want %q", p.key, "event:schemas")
	}

	p2, _ := setupRedis(t, WithKey("custom:schemas"))
	if p2.key != "custom:schemas" {
		t.Errorf("WithKey: got %q", p2.key)
	}
}

func TestRedisProvider_Get_NotFoundReturnsNilNil(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)

	s, err := p.Get(context.Background(), "missing")
	if err != nil {
		t.Errorf("Get(missing): %v", err)
	}
	if s != nil {
		t.Errorf("Get(missing): got %+v, want nil", s)
	}
}

func TestRedisProvider_Get_HappyPath(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	want := &EventSchema{
		Name:          "order.created",
		Version:       3,
		Description:   "Order created event",
		SubTimeout:    5 * time.Second,
		MaxRetries:    7,
		EnableMonitor: true,
		Metadata:      map[string]string{"owner": "orders-team"},
	}
	if err := p.Set(ctx, want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := p.Get(ctx, "order.created")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil for existing schema")
	}
	if got.Name != want.Name || got.Version != want.Version ||
		got.SubTimeout != want.SubTimeout || got.MaxRetries != want.MaxRetries ||
		!got.EnableMonitor {
		t.Errorf("Get round-trip mismatch: %+v", got)
	}
	if got.Metadata["owner"] != "orders-team" {
		t.Errorf("Metadata: got %v", got.Metadata)
	}
}

func TestRedisProvider_Get_CorruptJSONErrors(t *testing.T) {
	t.Parallel()
	p, mr := setupRedis(t)

	// Inject a non-JSON value into the schema field; production must
	// surface a clear unmarshal error rather than silently returning a
	// zero-valued schema.
	mr.HSet("event:schemas", "broken", "this-is-not-json")

	got, err := p.Get(context.Background(), "broken")
	if err == nil {
		t.Error("Get on corrupt JSON: expected error")
	}
	if got != nil {
		t.Errorf("Get on error: got %+v, want nil", got)
	}
}

func TestRedisProvider_Get_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	_ = p.Close()

	_, err := p.Get(context.Background(), "x")
	if !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Get on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestRedisProvider_Set_NewSchemaPopulatesTimestamps(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	before := time.Now().Add(-time.Second)
	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, _ := p.Get(ctx, "evt")
	if got.CreatedAt.Before(before) || got.CreatedAt.After(time.Now().Add(time.Second)) {
		t.Errorf("Set did not populate CreatedAt; got %v", got.CreatedAt)
	}
	if got.UpdatedAt.Before(before) {
		t.Errorf("Set did not populate UpdatedAt; got %v", got.UpdatedAt)
	}
}

func TestRedisProvider_Set_PreservesCreatedAtOnUpdate(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 1}); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	first, _ := p.Get(ctx, "evt")
	originalCreatedAt := first.CreatedAt

	// Brief real sleep so UpdatedAt definitely shifts.
	time.Sleep(5 * time.Millisecond)
	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 2, Description: "v2"}); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	second, _ := p.Get(ctx, "evt")

	if !second.CreatedAt.Equal(originalCreatedAt) {
		t.Errorf("Set must preserve CreatedAt across updates; got %v want %v",
			second.CreatedAt, originalCreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Errorf("Set must refresh UpdatedAt; first=%v second=%v",
			first.UpdatedAt, second.UpdatedAt)
	}
}

func TestRedisProvider_Set_VersionDowngradeRejected(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 5}); err != nil {
		t.Fatalf("Set v5: %v", err)
	}
	err := p.Set(ctx, &EventSchema{Name: "evt", Version: 3})
	if !errors.Is(err, ErrVersionDowngrade) {
		t.Errorf("Set downgrade: got %v, want ErrVersionDowngrade", err)
	}
}

func TestRedisProvider_Set_SameVersionAllowed(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 5}); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if err := p.Set(ctx, &EventSchema{Name: "evt", Version: 5, Description: "refreshed"}); err != nil {
		t.Errorf("same-version Set: %v", err)
	}
}

func TestRedisProvider_Set_ValidationFailureShortCircuits(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)

	if err := p.Set(context.Background(), &EventSchema{Name: "", Version: 1}); !errors.Is(err, ErrEmptyName) {
		t.Errorf("Set(empty name): got %v, want ErrEmptyName", err)
	}
	if err := p.Set(context.Background(), &EventSchema{Name: "x", Version: 0}); !errors.Is(err, ErrInvalidVersion) {
		t.Errorf("Set(version=0): got %v, want ErrInvalidVersion", err)
	}
}

func TestRedisProvider_Set_PublisherErrorSwallowed(t *testing.T) {
	t.Parallel()
	// If the publisher fails to broadcast the change, the row has already
	// been written to Redis; Set must NOT surface the publisher error and
	// confuse the caller into retrying. Pin this swallow contract.
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	publisherErr := errors.New("transport down")
	p, err := NewRedisProvider(c, func(context.Context, SchemaChangeEvent) error {
		return publisherErr
	})
	if err != nil {
		t.Fatalf("NewRedisProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.Set(context.Background(), &EventSchema{Name: "evt", Version: 1}); err != nil {
		t.Errorf("Set: publisher error should not surface; got %v", err)
	}
	// And the hash field WAS written despite the publisher failure.
	if !mr.Exists("event:schemas") {
		t.Error("Set did not write hash key despite publisher error")
	}
}

func TestRedisProvider_Set_NotifiesWatchers(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch, err := p.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := p.Set(context.Background(), &EventSchema{Name: "evt.notify", Version: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case change := <-ch:
		if change.EventName != "evt.notify" || change.Version != 1 {
			t.Errorf("watcher saw %+v, want EventName=evt.notify Version=1", change)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not receive change within 1s")
	}
}

func TestRedisProvider_Set_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	_ = p.Close()

	if err := p.Set(context.Background(), &EventSchema{Name: "x", Version: 1}); !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Set on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestRedisProvider_Delete(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	_ = p.Set(ctx, &EventSchema{Name: "evt", Version: 1})

	if err := p.Delete(ctx, "evt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, _ := p.Get(ctx, "evt")
	if got != nil {
		t.Errorf("Delete left schema behind: %+v", got)
	}
}

func TestRedisProvider_Delete_MissingIsNoOp(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	if err := p.Delete(context.Background(), "never-existed"); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
}

func TestRedisProvider_Delete_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	_ = p.Close()
	if err := p.Delete(context.Background(), "x"); !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Delete on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestRedisProvider_List(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	ctx := context.Background()

	_ = p.Set(ctx, &EventSchema{Name: "evt-a", Version: 1})
	_ = p.Set(ctx, &EventSchema{Name: "evt-b", Version: 2, Description: "second"})

	got, err := p.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List: got %d, want 2", len(got))
	}
	seen := map[string]int{}
	for _, s := range got {
		seen[s.Name] = s.Version
	}
	if seen["evt-a"] != 1 || seen["evt-b"] != 2 {
		t.Errorf("List entries incorrect: %v", seen)
	}
}

func TestRedisProvider_List_CorruptEntryErrors(t *testing.T) {
	t.Parallel()
	p, mr := setupRedis(t)
	ctx := context.Background()

	_ = p.Set(ctx, &EventSchema{Name: "good", Version: 1})
	mr.HSet("event:schemas", "broken", "not-json")

	// Documented behavior: List returns an unmarshal error on the first
	// corrupt entry rather than silently skipping it. Differs from
	// outbox/redis.RetryFailed which silently skips corrupt failed
	// records — pin this so future "be lenient" refactors are deliberate.
	if _, err := p.List(ctx); err == nil {
		t.Error("List with corrupt entry: expected error")
	}
}

func TestRedisProvider_List_Empty(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	got, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("List empty: got %d, want 0", len(got))
	}
}

func TestRedisProvider_List_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	_ = p.Close()
	if _, err := p.List(context.Background()); !errors.Is(err, ErrProviderClosed) {
		t.Errorf("List on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestRedisProvider_Watch_ContextCancelClosesChannel(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher channel was not closed after ctx.Cancel")
	}
}

func TestRedisProvider_Watch_CloseClosesChannel(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)

	ch, err := p.Watch(context.Background())
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher channel was not closed after Close")
	}
}

func TestRedisProvider_Watch_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	_ = p.Close()
	_, err := p.Watch(context.Background())
	if !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Watch on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestRedisProvider_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p, _ := setupRedis(t)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Close is documented as idempotent (guarded by the `closed` flag),
	// unlike the monitor/poison Postgres stores. Pin this so a refactor
	// that drops the guard is caught here rather than as a runtime panic.
	if err := p.Close(); err != nil {
		t.Errorf("second Close should be no-op; got %v", err)
	}
}

func TestRedisProvider_Set_StoresAsHashField(t *testing.T) {
	t.Parallel()
	// Pin the wire layout: schemas are stored as JSON values keyed by event
	// name in a single Redis hash. Operators inspecting Redis via
	// `HGETALL event:schemas` need this stable.
	p, mr := setupRedis(t)
	if err := p.Set(context.Background(), &EventSchema{Name: "wire.pin", Version: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	raw := mr.HGet("event:schemas", "wire.pin")
	if raw == "" {
		t.Fatal("Set did not write to hash field")
	}
	// Must be valid JSON containing the name (sanity check).
	if !contains(raw, `"name":"wire.pin"`) {
		t.Errorf("hash value not JSON-encoded schema; got %q", raw)
	}
}

// contains is a tiny helper to avoid pulling in strings just for one use.
func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

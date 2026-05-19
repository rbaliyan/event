//go:build smoke

// Env-gated bus-level smoke for the Redis Streams transport. Requires a real
// Redis instance because miniredis does not faithfully reproduce all stream
// semantics the transport depends on (XAUTOCLAIM, blocking XREADGROUP).
//
// Run with:
//
//	REDIS_ADDR=127.0.0.1:6379 just test-smoke
//	# or directly:
//	REDIS_ADDR=127.0.0.1:6379 go test -tags=smoke -race ./transport/redis/...

package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
)

// redisAddrEnv mirrors internal/testutil.RedisAddrEnv. Duplicated locally so
// this file does not depend on the build-tagged integration helper (smoke
// and integration are separate tags).
const redisAddrEnv = "REDIS_ADDR"

func setupSmokeClient(t testing.TB) *redis.Client {
	t.Helper()

	addr := os.Getenv(redisAddrEnv)
	if addr == "" {
		t.Skipf("Redis smoke skipped: %s not set", redisAddrEnv)
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis unreachable at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestSmokeRedisBus_RoundTrip(t *testing.T) {
	t.Parallel()
	client := setupSmokeClient(t)

	// Per-run consumer-group base so parallel smoke runs don't share group
	// state on the same Redis instance.
	groupID := "smoke-" + testutil.UniqueName(t)
	tr, err := New(client, WithConsumerGroup(groupID), WithBlockTime(50*time.Millisecond))
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	// Per-run event name keeps the stream key unique across parallel runs.
	eventName := "smoke_redis_" + testutil.UniqueName(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Del(ctx, tr.streamName(eventName)).Err()
	})

	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(tr))
	ev := testutil.MustRegister(t, ctx, bus, event.New[string](eventName))

	received := make(chan string, 1)
	if err := ev.Subscribe(ctx, func(_ context.Context, _ event.Event[string], v string) error {
		received <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := ev.Publish(ctx, "hello-redis"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := testutil.WaitFor(t, received, 5*time.Second, "payload should arrive on Redis")
	if got != "hello-redis" {
		t.Errorf("got %q, want %q", got, "hello-redis")
	}
}

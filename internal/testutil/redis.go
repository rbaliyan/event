//go:build integration

package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisAddrEnv is the environment variable consulted for the Redis address.
// Tests should not hardcode 127.0.0.1:6379; let SetupRedis resolve it so CI
// and local-dev can override without code changes.
const RedisAddrEnv = "REDIS_ADDR"

// defaultRedisAddr is used when RedisAddrEnv is unset. The CI service-container
// pattern and local docker compose both bind 6379 by default.
const defaultRedisAddr = "127.0.0.1:6379"

// SetupRedis returns a connected Redis client and a per-run key prefix. If
// Redis is unreachable the test is skipped — this lets `go test
// -tags=integration ./...` succeed on developer machines without a running
// Redis. CI brings Redis up via a services: block and never hits the skip.
//
// The returned prefix is collision-free across parallel test runs (see
// UniqueName). A t.Cleanup hook SCANs and DELs every key under the prefix on
// teardown, leaving the database in the state it was found.
//
// SetupRedis intentionally uses *redis.Client rather than the redis.Client
// interface from transport/redis, because integration tests assert against
// real Redis semantics that the abstraction interface hides.
func SetupRedis(t testing.TB) (*redis.Client, string) {
	t.Helper()

	addr := os.Getenv(RedisAddrEnv)
	if addr == "" {
		addr = defaultRedisAddr
	}

	client := redis.NewClient(&redis.Options{Addr: addr})

	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis unreachable at %s (%s=%s): %v",
			addr, RedisAddrEnv, os.Getenv(RedisAddrEnv), err)
	}

	prefix := "test:" + UniqueName(t) + ":"

	t.Cleanup(func() {
		// Scan-and-delete the prefix on teardown. SCAN is bounded by
		// COUNT=1000 hints; we drain all batches so leaked keys don't survive.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		iter := client.Scan(cleanupCtx, 0, prefix+"*", 1000).Iterator()
		var keys []string
		for iter.Next(cleanupCtx) {
			keys = append(keys, iter.Val())
			if len(keys) >= 500 {
				if err := client.Del(cleanupCtx, keys...).Err(); err != nil {
					t.Logf("redis cleanup DEL: %v", err)
				}
				keys = keys[:0]
			}
		}
		if len(keys) > 0 {
			if err := client.Del(cleanupCtx, keys...).Err(); err != nil {
				t.Logf("redis cleanup DEL: %v", err)
			}
		}
		_ = client.Close()
	})

	return client, prefix
}

package outbox

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreConformance(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	ctx := context.Background()
	client.FlushDB(ctx)

	store, err := NewRedisStore(client)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := store.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	seed := func(ctx context.Context, eventID string) error {
		return store.Store(ctx, "conf.event", eventID, []byte(`{}`), nil)
	}
	RunStoreConformance(t, ctx, store, seed)
}

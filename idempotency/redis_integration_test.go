//go:build integration

package idempotency

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func getRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func setupRedisStore(t *testing.T) (*RedisStore, func()) {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: getRedisAddr(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := "test:idemp:" + time.Now().Format("20060102150405") + ":"
	store, err := NewRedisStore(client, time.Hour, WithPrefix(prefix))
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	cleanup := func() {
		ctx := context.Background()
		// Clean up test keys
		iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	}

	return store, cleanup
}

func TestRedisStore_Integration_IsDuplicate(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("returns false for new message", func(t *testing.T) {
		isDuplicate, err := store.IsDuplicate(ctx, "msg-new-1")
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected false for new message")
		}
	})

	t.Run("returns true for processed message", func(t *testing.T) {
		msgID := "msg-processed-1"
		if err := store.MarkProcessed(ctx, msgID); err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected true for processed message")
		}
	})

	t.Run("atomic check-and-set", func(t *testing.T) {
		msgID := "msg-atomic-1"

		// First check should return false (not duplicate) and claim it
		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected false for new message")
		}

		// Second check should return true (duplicate)
		isDuplicate, err = store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected true after first check claimed it")
		}
	})
}

func TestRedisStore_Integration_MarkProcessed(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("marks message as processed", func(t *testing.T) {
		msgID := "msg-mark-1"
		if err := store.MarkProcessed(ctx, msgID); err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected message to be marked as processed")
		}
	})

	t.Run("MarkProcessedWithTTL stores message", func(t *testing.T) {
		msgID := "msg-mark-ttl-1"
		if err := store.MarkProcessedWithTTL(ctx, msgID, 30*time.Minute); err != nil {
			t.Fatalf("MarkProcessedWithTTL failed: %v", err)
		}

		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected message to be marked as processed")
		}
	})
}

func TestRedisStore_Integration_Remove(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("removes message", func(t *testing.T) {
		msgID := "msg-remove-1"
		if err := store.MarkProcessed(ctx, msgID); err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		if err := store.Remove(ctx, msgID); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}

		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected message to be removed")
		}
	})

	t.Run("remove non-existent message does not error", func(t *testing.T) {
		if err := store.Remove(ctx, "never-existed"); err != nil {
			t.Errorf("Remove non-existent should not error: %v", err)
		}
	})
}

func TestRedisStore_Integration_TTLExpiry(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create a store with very short TTL for testing expiry
	shortTTLStore, err := NewRedisStore(store.client, 100*time.Millisecond, WithPrefix(store.prefix+"short:"))
	if err != nil {
		t.Fatalf("failed to create short TTL store: %v", err)
	}

	msgID := "msg-expiry-1"
	if err := shortTTLStore.MarkProcessed(ctx, msgID); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	// Should be duplicate immediately
	isDuplicate, _ := shortTTLStore.IsDuplicate(ctx, msgID)
	if !isDuplicate {
		t.Error("expected duplicate before expiry")
	}

	// Wait for expiry
	time.Sleep(150 * time.Millisecond)

	// Should not be duplicate after expiry (key expired)
	isDuplicate, _ = shortTTLStore.IsDuplicate(ctx, msgID)
	if isDuplicate {
		t.Error("expected not duplicate after expiry")
	}
}

func TestRedisStore_Integration_DifferentMessages(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	if err := store.MarkProcessed(ctx, "msg-diff-1"); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	isDuplicate1, _ := store.IsDuplicate(ctx, "msg-diff-1")
	if !isDuplicate1 {
		t.Error("msg-diff-1 should be duplicate")
	}

	isDuplicate2, _ := store.IsDuplicate(ctx, "msg-diff-2")
	if isDuplicate2 {
		t.Error("msg-diff-2 should not be duplicate")
	}
}

func TestRedisStore_Integration_ConcurrentAccess(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)

		go func(id int) {
			defer wg.Done()
			_ = store.MarkProcessed(ctx, "msg-concurrent")
		}(i)

		go func(id int) {
			defer wg.Done()
			_, _ = store.IsDuplicate(ctx, "msg-concurrent")
		}(i)

		go func(id int) {
			defer wg.Done()
			_ = store.Remove(ctx, "msg-concurrent-remove")
		}(i)
	}

	wg.Wait()
}

func TestRedisStore_Integration_AtomicDeduplication(t *testing.T) {
	store, cleanup := setupRedisStore(t)
	defer cleanup()

	ctx := context.Background()
	msgID := "msg-atomic-race-1"

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to claim the same message simultaneously
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			isDuplicate, err := store.IsDuplicate(ctx, msgID)
			if err != nil {
				return
			}

			if !isDuplicate {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one goroutine should have successfully claimed the message
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful claim, got %d", successCount)
	}
}

//go:build integration

package distributed

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

func setupRedisStateManager(t *testing.T) (*RedisStateManager, func()) {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: getRedisAddr(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := "test:state:" + time.Now().Format("20060102150405") + ":"
	sm := NewRedisStateManager(client,
		WithPrefix(prefix),
		WithCompletedTTL(time.Hour),
	)

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

	return sm, cleanup
}

func TestRedisStateManager_Integration_Acquire(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("first acquisition succeeds", func(t *testing.T) {
		acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Error("expected acquisition to succeed")
		}
	})

	t.Run("second acquisition for same message fails", func(t *testing.T) {
		msgID := "msg-2"
		acquired, err := sm.Acquire(ctx, msgID, time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Error("expected first acquisition to succeed")
		}

		acquired, err = sm.Acquire(ctx, msgID, time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acquired {
			t.Error("expected second acquisition to fail")
		}
	})

	t.Run("different messages can be acquired", func(t *testing.T) {
		acquired1, _ := sm.Acquire(ctx, "msg-diff-1", time.Minute)
		acquired2, _ := sm.Acquire(ctx, "msg-diff-2", time.Minute)

		if !acquired1 || !acquired2 {
			t.Error("expected both different messages to be acquired")
		}
	})
}

func TestRedisStateManager_Integration_MarkProcessed(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("mark processed prevents reacquisition", func(t *testing.T) {
		msgID := "msg-processed-1"

		acquired, _ := sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		if err := sm.MarkProcessed(ctx, msgID); err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if acquired {
			t.Error("expected acquisition to fail after mark processed")
		}
	})
}

func TestRedisStateManager_Integration_Reset(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("reset allows reacquisition", func(t *testing.T) {
		msgID := "msg-reset-1"

		acquired, _ := sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		if err := sm.Reset(ctx, msgID); err != nil {
			t.Fatalf("Reset failed: %v", err)
		}

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Error("expected acquisition to succeed after reset")
		}
	})
}

func TestRedisStateManager_Integration_TTLExpiry(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("expired state allows reacquisition", func(t *testing.T) {
		msgID := "msg-expiry-1"

		acquired, _ := sm.Acquire(ctx, msgID, 100*time.Millisecond)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		// Should fail immediately
		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if acquired {
			t.Error("expected acquisition to fail before expiry")
		}

		// Wait for expiry
		time.Sleep(150 * time.Millisecond)

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Error("expected acquisition to succeed after expiry")
		}
	})
}

func TestRedisStateManager_Integration_ListStale(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "stale-1", time.Hour)
	sm.Acquire(ctx, "stale-2", time.Hour)
	sm.Acquire(ctx, "stale-3", time.Hour)

	// Mark one as processed
	sm.MarkProcessed(ctx, "stale-2")

	// Wait a bit so they become stale
	time.Sleep(100 * time.Millisecond)

	// List stale with 50ms stale timeout
	stale, err := sm.ListStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}

	// Should find stale-1 and stale-3 (processing and stale), not stale-2 (completed)
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale, got %d", len(stale))
	}

	// Test limit
	stale, err = sm.ListStale(ctx, 50*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("ListStale with limit failed: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale with limit, got %d", len(stale))
	}
}

func TestRedisStateManager_Integration_ResetStale(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "reset-stale-1", time.Hour)
	sm.Acquire(ctx, "reset-stale-2", time.Hour)

	// Wait a bit so they become stale
	time.Sleep(100 * time.Millisecond)

	// Reset stale states
	reset, err := sm.ResetStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ResetStale failed: %v", err)
	}
	if reset != 2 {
		t.Fatalf("expected 2 reset, got %d", reset)
	}

	// Should be acquirable again
	acquired, _ := sm.Acquire(ctx, "reset-stale-1", time.Hour)
	if !acquired {
		t.Error("expected acquisition to succeed after stale reset")
	}
}

func TestRedisStateManager_Integration_ConcurrentAcquire(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()
	msgID := "msg-concurrent-1"

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to acquire the same message
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			acquired, err := sm.Acquire(ctx, msgID, time.Minute)
			if err != nil {
				return
			}

			if acquired {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one goroutine should have successfully acquired the message
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful acquisition, got %d", successCount)
	}
}

func TestRedisStateManager_Integration_RecoveryRunner(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx := context.Background()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Acquire a message
	sm.Acquire(ctx, "recovery-1", time.Hour)

	// Not stale yet
	reset, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 0 {
		t.Fatalf("expected 0 reset (not stale yet), got %d", reset)
	}

	// Wait for it to become stale
	time.Sleep(100 * time.Millisecond)

	// Now should be reset
	reset, err = runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 1 {
		t.Fatalf("expected 1 reset, got %d", reset)
	}

	// Message should be acquirable again
	acquired, _ := sm.Acquire(ctx, "recovery-1", time.Hour)
	if !acquired {
		t.Error("expected acquisition to succeed after recovery")
	}
}

func TestRedisStateManager_Integration_RecoveryRunnerBackground(t *testing.T) {
	sm, cleanup := setupRedisStateManager(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(30*time.Millisecond),
		WithCheckInterval(20*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Start runner in background
	go runner.Run(ctx)

	// Acquire a message
	sm.Acquire(ctx, "bg-recovery-1", time.Hour)

	// Verify it's acquired
	acquired, _ := sm.Acquire(ctx, "bg-recovery-1", time.Hour)
	if acquired {
		t.Fatal("expected acquisition to fail (already acquired)")
	}

	// Wait for stale timeout + check interval
	time.Sleep(100 * time.Millisecond)

	// Should be acquirable now (runner reset it)
	acquired, _ = sm.Acquire(ctx, "bg-recovery-1", time.Hour)
	if !acquired {
		t.Error("expected acquisition to succeed after runner reset stale state")
	}
}

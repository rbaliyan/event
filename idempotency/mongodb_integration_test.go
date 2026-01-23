//go:build integration

package idempotency

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27017"
}

func setupMongoStore(t *testing.T) (*MongoStore, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("MongoDB not reachable: %v", err)
	}

	dbName := "event_test_idempotency"
	collName := "idempotency_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	store := NewMongoStoreWithCollection(db, collName,
		WithMongoTTL(time.Hour),
		WithMongoCleanupInterval(0), // Disable cleanup for tests
	)

	if err := store.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("failed to ensure indexes: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		store.Close()
		_ = store.collection.Drop(ctx)
		_ = client.Disconnect(ctx)
	}

	return store, cleanup
}

func TestMongoStore_Integration_IsDuplicate(t *testing.T) {
	store, cleanup := setupMongoStore(t)
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
}

func TestMongoStore_Integration_MarkProcessed(t *testing.T) {
	store, cleanup := setupMongoStore(t)
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

func TestMongoStore_Integration_Remove(t *testing.T) {
	store, cleanup := setupMongoStore(t)
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

func TestMongoStore_Integration_DifferentMessages(t *testing.T) {
	store, cleanup := setupMongoStore(t)
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

func TestMongoStore_Integration_ConcurrentAccess(t *testing.T) {
	store, cleanup := setupMongoStore(t)
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

func TestMongoStore_Integration_AtomicDeduplication(t *testing.T) {
	store, cleanup := setupMongoStore(t)
	defer cleanup()

	ctx := context.Background()
	msgID := "msg-atomic-1"

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to claim the same message
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

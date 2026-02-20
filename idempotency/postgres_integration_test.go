//go:build integration

package idempotency

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func getPostgresDSN() string {
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://localhost:5432/test?sslmode=disable"
}

func setupPostgresStore(t *testing.T) (*PostgresStore, func()) {
	t.Helper()

	db, err := sql.Open("postgres", getPostgresDSN())
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("PostgreSQL not reachable: %v", err)
	}

	tableName := "idempotency_test_" + time.Now().Format("20060102150405")
	store, err := NewPostgresStore(db,
		WithPostgresTTL(time.Hour),
		WithPostgresTable(tableName),
		WithPostgresCleanupInterval(0), // Disable cleanup for tests
	)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	if err := store.CreateTable(context.Background()); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	cleanup := func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
		store.Close()
		db.Close()
	}

	return store, cleanup
}

func TestPostgresStore_Integration_IsDuplicate(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
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

func TestPostgresStore_Integration_MarkProcessed(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
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

	t.Run("overwrites existing entry updates expiry", func(t *testing.T) {
		msgID := "msg-overwrite-1"

		if err := store.MarkProcessedWithTTL(ctx, msgID, 30*time.Minute); err != nil {
			t.Fatalf("MarkProcessedWithTTL failed: %v", err)
		}

		// Overwrite with longer TTL should not error
		if err := store.MarkProcessedWithTTL(ctx, msgID, time.Hour); err != nil {
			t.Fatalf("MarkProcessedWithTTL overwrite failed: %v", err)
		}

		isDuplicate, _ := store.IsDuplicate(ctx, msgID)
		if !isDuplicate {
			t.Error("expected message to still be processed")
		}
	})
}

func TestPostgresStore_Integration_Remove(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
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

func TestPostgresStore_Integration_DifferentMessages(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
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

func TestPostgresStore_Integration_ConcurrentAccess(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
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

func TestPostgresStore_Integration_TransactionalOperations(t *testing.T) {
	store, cleanup := setupPostgresStore(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("transactional duplicate check", func(t *testing.T) {
		msgID := "msg-tx-1"

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}
		defer tx.Rollback()

		isDuplicate, err := store.IsDuplicateTx(ctx, tx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicateTx failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected false for new message in transaction")
		}

		if err := store.MarkProcessedTx(ctx, tx, msgID); err != nil {
			t.Fatalf("MarkProcessedTx failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// After commit, should be duplicate
		isDuplicate, err = store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if !isDuplicate {
			t.Error("expected true after transaction commit")
		}
	})

	t.Run("transaction rollback does not mark as processed", func(t *testing.T) {
		msgID := "msg-tx-rollback-1"

		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx failed: %v", err)
		}

		if err := store.MarkProcessedTx(ctx, tx, msgID); err != nil {
			t.Fatalf("MarkProcessedTx failed: %v", err)
		}

		// Rollback the transaction
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// After rollback, should not be duplicate
		isDuplicate, err := store.IsDuplicate(ctx, msgID)
		if err != nil {
			t.Fatalf("IsDuplicate failed: %v", err)
		}
		if isDuplicate {
			t.Error("expected false after transaction rollback")
		}
	})
}

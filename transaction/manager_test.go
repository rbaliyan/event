package transaction

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func TestSQLManager_NewRequiresDB(t *testing.T) {
	_, err := NewSQLManager(nil)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

func TestSQLManager_Execute_HappyPath(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)
	ctx := context.Background()

	err := mgr.Execute(ctx, func(tx Transaction) error {
		sqlTx := tx.(SQLTransactionProvider).Tx()
		_, err := sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (1, 'hello')")
		return err
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Verify data was committed
	var value string
	err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	if err != nil {
		t.Fatalf("query after commit: %v", err)
	}
	if value != "hello" {
		t.Errorf("expected 'hello', got %q", value)
	}
}

func TestSQLManager_Execute_ErrorRollback(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)
	ctx := context.Background()

	fnErr := errors.New("validation failed")
	err := mgr.Execute(ctx, func(tx Transaction) error {
		sqlTx := tx.(SQLTransactionProvider).Tx()
		_, _ = sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (1, 'should_rollback')")
		return fnErr
	})

	if !errors.Is(err, fnErr) {
		t.Fatalf("expected fnErr, got %v", err)
	}

	// Verify data was NOT committed (rolled back)
	var count int
	db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after rollback, got %d", count)
	}
}

func TestSQLManager_Execute_PanicRecovery(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)
	ctx := context.Background()

	err := mgr.Execute(ctx, func(tx Transaction) error {
		sqlTx := tx.(SQLTransactionProvider).Tx()
		_, _ = sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (1, 'should_rollback')")
		panic("unexpected crash")
	})

	if err == nil {
		t.Fatal("expected error after panic")
	}
	if !errors.Is(err, ErrTransactionFailed) {
		t.Errorf("expected ErrTransactionFailed, got %v", err)
	}

	// Verify data was NOT committed (rolled back after panic)
	var count int
	db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after panic rollback, got %d", count)
	}
}

func TestSQLManager_Execute_MultipleOperations(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)
	ctx := context.Background()

	err := mgr.Execute(ctx, func(tx Transaction) error {
		sqlTx := tx.(SQLTransactionProvider).Tx()
		if _, err := sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (1, 'a')"); err != nil {
			return err
		}
		if _, err := sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (2, 'b')"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var count int
	db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestSQLManager_Execute_PartialFailureRollsBackAll(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)
	ctx := context.Background()

	err := mgr.Execute(ctx, func(tx Transaction) error {
		sqlTx := tx.(SQLTransactionProvider).Tx()
		// First insert succeeds
		if _, err := sqlTx.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (1, 'a')"); err != nil {
			return err
		}
		// Return error — should roll back both
		return errors.New("second operation failed")
	})

	if err == nil {
		t.Fatal("expected error")
	}

	// Both should be rolled back
	var count int
	db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 rows after partial failure rollback, got %d", count)
	}
}

func TestSQLManager_Begin(t *testing.T) {
	db := openTestDB(t)
	mgr, _ := NewSQLManager(db)

	tx, err := mgr.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// Verify it implements SQLTransactionProvider
	sqlTx, ok := tx.(SQLTransactionProvider)
	if !ok {
		t.Fatal("expected SQLTransactionProvider")
	}
	if sqlTx.Tx() == nil {
		t.Fatal("expected non-nil *sql.Tx")
	}

	_ = tx.Rollback()
}

func TestSQLTransaction_ImplementsInterfaces(t *testing.T) {
	var _ Transaction = (*SQLTransaction)(nil)
	var _ SQLTransactionProvider = (*SQLTransaction)(nil)
	var _ Manager = (*SQLManager)(nil)
}

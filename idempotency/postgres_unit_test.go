package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// setupMock returns a PostgresStore wired to sqlmock, with cleanup interval
// forced to zero so the background goroutine doesn't race the mock's
// expectations.
func setupMock(t *testing.T, opts ...PostgresOption) (*PostgresStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock unmet expectations: %v", err)
		}
	})

	allOpts := append([]PostgresOption{WithPostgresCleanupInterval(0)}, opts...)
	s, err := NewPostgresStore(db, allOpts...)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, mock
}

func TestNewPostgresStore_NilDB(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresStore(nil); err == nil {
		t.Error("NewPostgresStore(nil): expected error")
	}
}

func TestNewPostgresStore_Defaults(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t)
	if s.table != "event_idempotency" {
		t.Errorf("default table: got %q, want %q", s.table, "event_idempotency")
	}
	if s.ttl != 24*time.Hour {
		t.Errorf("default ttl: got %v, want 24h", s.ttl)
	}
}

func TestNewPostgresStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t,
		WithPostgresTable("custom_idem"),
		WithPostgresTTL(7*24*time.Hour),
	)
	if s.table != "custom_idem" {
		t.Errorf("WithPostgresTable: got %q", s.table)
	}
	if s.ttl != 7*24*time.Hour {
		t.Errorf("WithPostgresTTL: got %v", s.ttl)
	}
}

func TestNewPostgresStore_InvalidIdentifierKeepsDefault(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t, WithPostgresTable("evil; DROP TABLE x;--"))
	if s.table != "event_idempotency" {
		t.Errorf("invalid identifier should be rejected; got %q", s.table)
	}
}

func TestPostgresStore_IsDuplicate_True(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// SELECT EXISTS — the TTL-aware predicate (expires_at > NOW()) means
	// expired markers are filtered out at the DB layer. Pin the predicate.
	mock.ExpectQuery(`SELECT EXISTS\(\s*SELECT 1 FROM event_idempotency\s+WHERE message_id = .* AND expires_at > NOW`).
		WithArgs("msg-1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	got, err := s.IsDuplicate(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("IsDuplicate: %v", err)
	}
	if !got {
		t.Error("IsDuplicate: got false, want true")
	}
}

func TestPostgresStore_IsDuplicate_False(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("never-seen").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	got, err := s.IsDuplicate(context.Background(), "never-seen")
	if err != nil {
		t.Fatalf("IsDuplicate: %v", err)
	}
	if got {
		t.Error("IsDuplicate: got true, want false")
	}
}

func TestPostgresStore_IsDuplicate_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT EXISTS`).WillReturnError(errors.New("conn refused"))

	got, err := s.IsDuplicate(context.Background(), "x")
	if err == nil {
		t.Fatal("IsDuplicate: expected error")
	}
	// Load-bearing: query error must NOT return (true, nil) — that would
	// silently mark every message as a duplicate when the DB is down, dropping
	// legitimate work. Pin to (false, error).
	if got {
		t.Error("IsDuplicate on error: must return false (never true on DB failure)")
	}
}

func TestPostgresStore_IsDuplicateTx_WithSQLTx(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("msg-tx").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	got, err := s.IsDuplicateTx(context.Background(), tx, "msg-tx")
	if err != nil {
		t.Fatalf("IsDuplicateTx: %v", err)
	}
	if !got {
		t.Error("IsDuplicateTx: got false, want true")
	}
}

// fakeProvider implements the unexported `interface{ Tx() *sql.Tx }` so we
// can exercise extractSQLTx's second branch.
type fakeProvider struct{ tx *sql.Tx }

func (f *fakeProvider) Tx() *sql.Tx { return f.tx }

func TestPostgresStore_IsDuplicateTx_WithProvider(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("p-1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectRollback()

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	got, err := s.IsDuplicateTx(context.Background(), &fakeProvider{tx: tx}, "p-1")
	if err != nil {
		t.Fatalf("IsDuplicateTx via provider: %v", err)
	}
	if got {
		t.Error("IsDuplicateTx via provider: got true, want false")
	}
}

func TestPostgresStore_IsDuplicateTx_WrongTypeRejected(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t)

	// extractSQLTx must refuse a non-Tx value with a clear error rather
	// than panic, so the caller can fall back to a non-transactional path.
	_, err := s.IsDuplicateTx(context.Background(), "not a tx", "x")
	if err == nil {
		t.Fatal("IsDuplicateTx with non-Tx: expected error")
	}
}

func TestPostgresStore_IsDuplicateTx_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT EXISTS`).WillReturnError(errors.New("io"))
	mock.ExpectRollback()

	tx, _ := s.db.BeginTx(context.Background(), nil)
	defer tx.Rollback() //nolint:errcheck

	if _, err := s.IsDuplicateTx(context.Background(), tx, "x"); err == nil {
		t.Fatal("IsDuplicateTx: expected error")
	}
}

func TestPostgresStore_MarkProcessed_UsesDefaultTTL(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t, WithPostgresTTL(2*time.Hour))

	// MarkProcessed must delegate to MarkProcessedWithTTL with the store's
	// configured TTL — pinning the interval argument catches a regression
	// where the default falls back to a hardcoded value.
	mock.ExpectExec(`INSERT INTO event_idempotency.*ON CONFLICT \(message_id\) DO UPDATE`).
		WithArgs("msg", "2h0m0s").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkProcessed(context.Background(), "msg"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
}

func TestPostgresStore_MarkProcessedWithTTL_HappyPath(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`INSERT INTO event_idempotency.*ON CONFLICT.*DO UPDATE\s+SET processed_at = NOW.*expires_at = NOW`).
		WithArgs("msg-ttl", "30m0s").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkProcessedWithTTL(context.Background(), "msg-ttl", 30*time.Minute); err != nil {
		t.Fatalf("MarkProcessedWithTTL: %v", err)
	}
}

func TestPostgresStore_MarkProcessedWithTTL_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`INSERT INTO event_idempotency`).WillReturnError(errors.New("io"))
	if err := s.MarkProcessedWithTTL(context.Background(), "x", time.Hour); err == nil {
		t.Fatal("MarkProcessedWithTTL: expected error")
	}
}

func TestPostgresStore_MarkProcessedTx_WithSQLTx(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t, WithPostgresTTL(time.Hour))

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO event_idempotency`).
		WithArgs("msg-tx", "1h0m0s").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	if err := s.MarkProcessedTx(context.Background(), tx, "msg-tx"); err != nil {
		t.Fatalf("MarkProcessedTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestPostgresStore_MarkProcessedTx_WrongTypeRejected(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t)
	if err := s.MarkProcessedTx(context.Background(), 42, "x"); err == nil {
		t.Fatal("MarkProcessedTx with non-Tx: expected error")
	}
}

func TestPostgresStore_MarkProcessedWithTTLTx_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO event_idempotency`).WillReturnError(errors.New("io"))
	mock.ExpectRollback()

	tx, _ := s.db.BeginTx(context.Background(), nil)
	defer tx.Rollback() //nolint:errcheck

	if err := s.MarkProcessedWithTTLTx(context.Background(), tx, "x", time.Hour); err == nil {
		t.Fatal("MarkProcessedWithTTLTx: expected error")
	}
}

func TestPostgresStore_Remove(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM event_idempotency WHERE message_id =`).
		WithArgs("msg").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.Remove(context.Background(), "msg"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

func TestPostgresStore_Remove_NotFoundReturnsNil(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Documented contract: Remove returns nil even when the entry doesn't
	// exist (idempotent admin operation, like ClearPoison).
	mock.ExpectExec(`DELETE FROM event_idempotency`).
		WithArgs("never-seen").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.Remove(context.Background(), "never-seen"); err != nil {
		t.Errorf("Remove on non-existent: %v", err)
	}
}

func TestPostgresStore_Remove_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM event_idempotency`).WillReturnError(errors.New("io"))
	if err := s.Remove(context.Background(), "x"); err == nil {
		t.Fatal("Remove: expected error")
	}
}

func TestPostgresStore_CreateTable(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS event_idempotency.*CREATE INDEX IF NOT EXISTS idx_event_idempotency_expires`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.CreateTable(context.Background()); err != nil {
		t.Errorf("CreateTable: %v", err)
	}
}

func TestPostgresStore_CreateTable_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS event_idempotency`).
		WillReturnError(errors.New("perm denied"))

	if err := s.CreateTable(context.Background()); err == nil {
		t.Fatal("CreateTable: expected error")
	}
}

func TestPostgresStore_Cleanup_DeletesExpired(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM event_idempotency WHERE expires_at < NOW`).
		WillReturnResult(sqlmock.NewResult(0, 17))

	s.cleanup()
}

func TestPostgresStore_Cleanup_SwallowsErrors(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// cleanup runs on a timer; transient DB errors must not propagate or
	// crash the goroutine. The test passes if cleanup returns without panic
	// and ExpectationsWereMet sees the DELETE was attempted.
	mock.ExpectExec(`DELETE FROM event_idempotency`).WillReturnError(errors.New("locked"))
	s.cleanup()
}

func TestPostgresStore_Close(t *testing.T) {
	t.Parallel()
	// Direct construction so the t.Cleanup in setupMock doesn't double-close.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = mock.ExpectationsWereMet() })

	s, err := NewPostgresStore(db, WithPostgresCleanupInterval(0))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Note: Close is not idempotent — calling it twice panics on
	// close-of-closed-channel (same defect as monitor & poison PostgresStores;
	// fix is a coordinated sync.Once change across all three stores).
}

func TestExtractSQLTx_SQLTx(t *testing.T) {
	t.Parallel()
	// Build a *sql.Tx via sqlmock for direct extractor exercise.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = mock.ExpectationsWereMet() })

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	got, err := extractSQLTx(tx)
	if err != nil {
		t.Fatalf("extractSQLTx(*sql.Tx): %v", err)
	}
	if got != tx {
		t.Error("extractSQLTx returned a different Tx pointer")
	}
}

func TestExtractSQLTx_Provider(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = mock.ExpectationsWereMet() })

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	provider := &fakeProvider{tx: tx}
	got, err := extractSQLTx(provider)
	if err != nil {
		t.Fatalf("extractSQLTx(provider): %v", err)
	}
	if got != tx {
		t.Error("extractSQLTx via provider returned different Tx pointer")
	}
}

func TestExtractSQLTx_UnsupportedType(t *testing.T) {
	t.Parallel()
	cases := []any{
		"string-tx",
		42,
		nil,
		struct{}{},
	}
	for _, in := range cases {
		_, err := extractSQLTx(in)
		if err == nil {
			t.Errorf("extractSQLTx(%T) = nil, want error", in)
		}
	}
}

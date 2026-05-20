package poison

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
// expectations. The mock is in QueryMatcherRegexp mode so test query patterns
// survive whitespace and formatting tweaks.
func setupMock(t *testing.T, opts ...PostgresStoreOption) (*PostgresStore, sqlmock.Sqlmock) {
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

	allOpts := append([]PostgresStoreOption{WithPostgresCleanupInterval(0)}, opts...)
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
	if s.failuresTable != "poison_failures" {
		t.Errorf("default failuresTable: got %q, want %q", s.failuresTable, "poison_failures")
	}
	if s.quarantineTable != "poison_quarantine" {
		t.Errorf("default quarantineTable: got %q, want %q", s.quarantineTable, "poison_quarantine")
	}
	if s.failureTTL != 24*time.Hour {
		t.Errorf("default failureTTL: got %v, want 24h", s.failureTTL)
	}
}

func TestNewPostgresStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t,
		WithPostgresFailuresTable("custom_failures"),
		WithPostgresQuarantineTable("custom_quarantine"),
		WithPostgresFailureTTL(7*24*time.Hour),
	)
	if s.failuresTable != "custom_failures" {
		t.Errorf("WithPostgresFailuresTable: got %q", s.failuresTable)
	}
	if s.quarantineTable != "custom_quarantine" {
		t.Errorf("WithPostgresQuarantineTable: got %q", s.quarantineTable)
	}
	if s.failureTTL != 7*24*time.Hour {
		t.Errorf("WithPostgresFailureTTL: got %v", s.failureTTL)
	}
}

func TestNewPostgresStore_InvalidIdentifiersKeepDefaults(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t,
		WithPostgresFailuresTable("bad; DROP TABLE x;--"),
		WithPostgresQuarantineTable("--also-bad"),
	)
	if s.failuresTable != "poison_failures" {
		t.Errorf("invalid failures identifier should be rejected; got %q", s.failuresTable)
	}
	if s.quarantineTable != "poison_quarantine" {
		t.Errorf("invalid quarantine identifier should be rejected; got %q", s.quarantineTable)
	}
}

func TestPostgresStore_IncrementFailure_FirstAttempt(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// UPSERT with RETURNING failure_count. First insertion → count=1.
	mock.ExpectQuery(`INSERT INTO poison_failures.*ON CONFLICT \(message_id\) DO UPDATE.*RETURNING failure_count`).
		WithArgs("msg-1", "24h0m0s").
		WillReturnRows(sqlmock.NewRows([]string{"failure_count"}).AddRow(1))

	got, err := s.IncrementFailure(context.Background(), "msg-1")
	if err != nil {
		t.Fatalf("IncrementFailure: %v", err)
	}
	if got != 1 {
		t.Errorf("IncrementFailure first: got %d, want 1", got)
	}
}

func TestPostgresStore_IncrementFailure_Increment(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Subsequent UPSERT returns the incremented count.
	mock.ExpectQuery(`INSERT INTO poison_failures.*ON CONFLICT.*DO UPDATE.*RETURNING failure_count`).
		WithArgs("msg-2", "24h0m0s").
		WillReturnRows(sqlmock.NewRows([]string{"failure_count"}).AddRow(5))

	got, err := s.IncrementFailure(context.Background(), "msg-2")
	if err != nil {
		t.Fatalf("IncrementFailure: %v", err)
	}
	if got != 5 {
		t.Errorf("IncrementFailure: got %d, want 5", got)
	}
}

func TestPostgresStore_IncrementFailure_UsesConfiguredTTL(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t, WithPostgresFailureTTL(2*time.Hour))

	// The TTL passed to the SQL "interval" argument must reflect the option.
	mock.ExpectQuery(`INSERT INTO poison_failures`).
		WithArgs("msg", "2h0m0s").
		WillReturnRows(sqlmock.NewRows([]string{"failure_count"}).AddRow(1))

	if _, err := s.IncrementFailure(context.Background(), "msg"); err != nil {
		t.Fatalf("IncrementFailure: %v", err)
	}
}

func TestPostgresStore_IncrementFailure_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`INSERT INTO poison_failures`).WillReturnError(errors.New("deadlock"))
	if _, err := s.IncrementFailure(context.Background(), "x"); err == nil {
		t.Fatal("IncrementFailure: expected error")
	}
}

func TestPostgresStore_GetFailureCount_NoRowsReturnsZero(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// ErrNoRows must map to (0, nil) per documented contract — callers
	// distinguish "no failures yet" from "DB error" via the error value.
	mock.ExpectQuery(`SELECT failure_count FROM poison_failures\s+WHERE message_id = .* AND expires_at > NOW`).
		WithArgs("never-failed").
		WillReturnError(sql.ErrNoRows)

	got, err := s.GetFailureCount(context.Background(), "never-failed")
	if err != nil {
		t.Errorf("GetFailureCount on no-rows: returned error %v", err)
	}
	if got != 0 {
		t.Errorf("GetFailureCount on no-rows: got %d, want 0", got)
	}
}

func TestPostgresStore_GetFailureCount_ScansValue(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT failure_count FROM poison_failures`).
		WithArgs("msg").
		WillReturnRows(sqlmock.NewRows([]string{"failure_count"}).AddRow(3))

	got, err := s.GetFailureCount(context.Background(), "msg")
	if err != nil {
		t.Fatalf("GetFailureCount: %v", err)
	}
	if got != 3 {
		t.Errorf("GetFailureCount: got %d, want 3", got)
	}
}

func TestPostgresStore_GetFailureCount_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT failure_count FROM poison_failures`).WillReturnError(errors.New("io"))
	if _, err := s.GetFailureCount(context.Background(), "x"); err == nil {
		t.Fatal("GetFailureCount: expected error")
	}
}

func TestPostgresStore_MarkPoison_Upsert(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// UPSERT into quarantine table with the supplied TTL.
	mock.ExpectExec(`INSERT INTO poison_quarantine.*ON CONFLICT \(message_id\) DO UPDATE`).
		WithArgs("msg", "1h0m0s").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkPoison(context.Background(), "msg", time.Hour); err != nil {
		t.Fatalf("MarkPoison: %v", err)
	}
}

func TestPostgresStore_MarkPoison_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`INSERT INTO poison_quarantine`).WillReturnError(errors.New("io"))
	if err := s.MarkPoison(context.Background(), "x", time.Minute); err == nil {
		t.Fatal("MarkPoison: expected error")
	}
}

func TestPostgresStore_IsPoison_True(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT EXISTS\(\s*SELECT 1 FROM poison_quarantine\s+WHERE message_id = .* AND expires_at > NOW`).
		WithArgs("msg").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	got, err := s.IsPoison(context.Background(), "msg")
	if err != nil {
		t.Fatalf("IsPoison: %v", err)
	}
	if !got {
		t.Errorf("IsPoison: got false, want true")
	}
}

func TestPostgresStore_IsPoison_False(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT EXISTS`).
		WithArgs("msg").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	got, err := s.IsPoison(context.Background(), "msg")
	if err != nil {
		t.Fatalf("IsPoison: %v", err)
	}
	if got {
		t.Errorf("IsPoison: got true, want false")
	}
}

func TestPostgresStore_IsPoison_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT EXISTS`).WillReturnError(errors.New("io"))
	got, err := s.IsPoison(context.Background(), "x")
	if err == nil {
		t.Fatal("IsPoison: expected error")
	}
	if got {
		t.Error("IsPoison on error: must return false")
	}
}

func TestPostgresStore_ClearPoison(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM poison_quarantine WHERE message_id =`).
		WithArgs("msg").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.ClearPoison(context.Background(), "msg"); err != nil {
		t.Fatalf("ClearPoison: %v", err)
	}
}

func TestPostgresStore_ClearPoison_NotFoundIsOK(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Documented contract: ClearPoison returns nil even when the message
	// wasn't quarantined (idempotent admin operation).
	mock.ExpectExec(`DELETE FROM poison_quarantine`).
		WithArgs("never-poisoned").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.ClearPoison(context.Background(), "never-poisoned"); err != nil {
		t.Errorf("ClearPoison on non-existent: %v", err)
	}
}

func TestPostgresStore_ClearPoison_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM poison_quarantine`).WillReturnError(errors.New("io"))
	if err := s.ClearPoison(context.Background(), "x"); err == nil {
		t.Fatal("ClearPoison: expected error")
	}
}

func TestPostgresStore_ClearFailures(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM poison_failures WHERE message_id =`).
		WithArgs("msg").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.ClearFailures(context.Background(), "msg"); err != nil {
		t.Fatalf("ClearFailures: %v", err)
	}
}

func TestPostgresStore_ClearFailures_ExecError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM poison_failures`).WillReturnError(errors.New("io"))
	if err := s.ClearFailures(context.Background(), "x"); err == nil {
		t.Fatal("ClearFailures: expected error")
	}
}

func TestPostgresStore_GetQuarantinedMessages_NoLimit(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT message_id FROM poison_quarantine\s+WHERE expires_at > NOW\(\)\s+ORDER BY quarantined_at DESC\s*$`).
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}).
			AddRow("msg-3").
			AddRow("msg-1").
			AddRow("msg-2"))

	got, err := s.GetQuarantinedMessages(context.Background(), 0)
	if err != nil {
		t.Fatalf("GetQuarantinedMessages: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d messages, want 3", len(got))
	}
	if got[0] != "msg-3" {
		t.Errorf("ordering not preserved; got %v", got)
	}
}

func TestPostgresStore_GetQuarantinedMessages_WithLimit(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// The LIMIT is appended to the query as a literal — verify the pattern
	// to catch a refactor that switches to a parameterized LIMIT (which
	// would change the SQL surface clients/admin tooling sees).
	mock.ExpectQuery(`SELECT message_id FROM poison_quarantine.*ORDER BY quarantined_at DESC LIMIT 2`).
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}).
			AddRow("msg-1").
			AddRow("msg-2"))

	got, err := s.GetQuarantinedMessages(context.Background(), 2)
	if err != nil {
		t.Fatalf("GetQuarantinedMessages(limit=2): %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d, want 2", len(got))
	}
}

func TestPostgresStore_GetQuarantinedMessages_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT message_id FROM poison_quarantine`).WillReturnError(errors.New("io"))
	if _, err := s.GetQuarantinedMessages(context.Background(), 0); err == nil {
		t.Fatal("GetQuarantinedMessages: expected error")
	}
}

func TestPostgresStore_GetQuarantinedMessages_Empty(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT message_id FROM poison_quarantine`).
		WillReturnRows(sqlmock.NewRows([]string{"message_id"}))

	got, err := s.GetQuarantinedMessages(context.Background(), 0)
	if err != nil {
		t.Fatalf("GetQuarantinedMessages empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty quarantine: got %d, want 0", len(got))
	}
}

func TestPostgresStore_CreateTables_BothSucceed(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS poison_failures.*CREATE INDEX IF NOT EXISTS idx_poison_failures_expires`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS poison_quarantine.*CREATE INDEX IF NOT EXISTS idx_poison_quarantine_expires`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.CreateTables(context.Background()); err != nil {
		t.Errorf("CreateTables: %v", err)
	}
}

func TestPostgresStore_CreateTables_FailuresTableError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// First CREATE fails — second CREATE must NOT be issued.
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS poison_failures`).WillReturnError(errors.New("perm denied"))

	if err := s.CreateTables(context.Background()); err == nil {
		t.Fatal("CreateTables: expected error on failures-table create")
	}
}

func TestPostgresStore_CreateTables_QuarantineTableError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS poison_failures`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS poison_quarantine`).
		WillReturnError(errors.New("perm denied"))

	if err := s.CreateTables(context.Background()); err == nil {
		t.Fatal("CreateTables: expected error on quarantine-table create")
	}
}

func TestPostgresStore_Cleanup_DeletesBothTables(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// cleanup() runs both DELETEs sequentially and swallows errors.
	mock.ExpectExec(`DELETE FROM poison_failures WHERE expires_at < NOW`).
		WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec(`DELETE FROM poison_quarantine WHERE expires_at < NOW`).
		WillReturnResult(sqlmock.NewResult(0, 2))

	s.cleanup() // exported via package-internal access
}

func TestPostgresStore_Cleanup_SwallowsErrors(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Both DELETEs fail; cleanup must NOT propagate, since it runs on a
	// timer and a transient DB error should not crash the goroutine.
	mock.ExpectExec(`DELETE FROM poison_failures`).WillReturnError(errors.New("locked"))
	mock.ExpectExec(`DELETE FROM poison_quarantine`).WillReturnError(errors.New("locked"))

	// No assertion — the test passes if cleanup does not panic.
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
	// Note: PostgresStore.Close is not idempotent — calling it twice panics
	// on close-of-closed-channel (same issue documented for monitor.PostgresStore).
}


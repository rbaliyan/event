package outbox

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	event "github.com/rbaliyan/event/v3"
)

// setupMock returns a *sql.DB backed by sqlmock plus the mock controller.
// The mock is in QueryMatcherRegexp mode so test queries can use loose
// regexes that survive whitespace and formatting changes in the production
// SQL strings.
func setupMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
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
	return db, mock
}

func TestNewPostgresStore_NilDB(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresStore(nil); err == nil {
		t.Error("NewPostgresStore(nil): expected error, got nil")
	}
}

func TestNewPostgresStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)

	s, err := NewPostgresStore(db, WithTable("custom_outbox"), WithNotifyChannel("ch_outbox"))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if s.tableName != "custom_outbox" {
		t.Errorf("WithTable: got %q, want %q", s.tableName, "custom_outbox")
	}
	if s.NotifyChannel() != "ch_outbox" {
		t.Errorf("NotifyChannel: got %q, want %q", s.NotifyChannel(), "ch_outbox")
	}
}

func TestPostgresStore_WakerNilWithoutListener(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)

	s, err := NewPostgresStore(db)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if s.Notifications() != nil {
		t.Fatal("Notifications must be nil without a configured listener")
	}
}

func TestNewPostgresStore_RejectsInvalidIdentifier(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)

	// SQL-injection-shaped table name must be rejected and the default
	// preserved. base.ValidIdentifier handles the actual check; this test
	// pins that the option layer respects it.
	s, err := NewPostgresStore(db, WithTable("orders; DROP TABLE x;--"))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if s.tableName != "event_outbox" {
		t.Errorf("invalid identifier should be rejected; got %q", s.tableName)
	}
}

func TestPostgresStore_ClaimPending_Empty(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FOR UPDATE SKIP LOCKED`).
		WithArgs(StatusPending, StatusFailed, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_name", "event_id", "payload", "metadata", "created_at", "retry_count", "priority"}))
	mock.ExpectCommit()

	batch, err := s.ClaimPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if len(batch.Messages()) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(batch.Messages()))
	}
	// Close on the empty batch must be a no-op (no tx left open to commit).
	if err := batch.Close(context.Background()); err != nil {
		t.Fatalf("Close on empty batch: %v", err)
	}
}

func TestPostgresStore_ClaimPending_ReturnsMessages(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	created := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"id", "event_name", "event_id", "payload", "metadata", "created_at", "retry_count", "priority"}).
		AddRow(int64(1), "evt.a", "id-a", []byte("pa"), []byte(`{"k":"v"}`), created, 0, 10).
		AddRow(int64(2), "evt.b", "id-b", []byte("pb"), nil, created, 3, 0)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FOR UPDATE SKIP LOCKED`).
		WithArgs(StatusPending, StatusFailed, 50).
		WillReturnRows(rows)
	// Both messages Ack'd, then the batch is closed (commits the claim tx).
	mock.ExpectExec(`UPDATE event_outbox SET status=.* published_at=.* WHERE id=`).
		WithArgs(StatusPublished, sqlmock.AnyArg(), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE event_outbox SET status=.* last_error=.* retry_count=retry_count\+1 WHERE id=`).
		WithArgs(StatusFailed, "publish boom", int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	batch, err := s.ClaimPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	msgs := batch.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].EventID != "id-a" || msgs[0].Metadata["k"] != "v" {
		t.Errorf("first message decoded incorrectly: %+v", msgs[0])
	}
	if msgs[1].Metadata != nil {
		t.Errorf("nil metadata should yield nil map; got %v", msgs[1].Metadata)
	}

	if err := batch.Ack(context.Background(), msgs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := batch.Fail(context.Background(), msgs[1], errors.New("publish boom")); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if err := batch.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPostgresStore_ClaimPending_BeginErrPropagates(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin().WillReturnError(errors.New("cannot begin"))

	_, err := s.ClaimPending(context.Background(), 10)
	if err == nil || !regexp.MustCompile(`begin tx`).MatchString(err.Error()) {
		t.Errorf("ClaimPending: got %v, want wrapped begin error", err)
	}
}

func TestPostgresStore_ClaimPending_QueryErrRollsBack(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FOR UPDATE SKIP LOCKED`).WillReturnError(errors.New("query boom"))
	mock.ExpectRollback()

	_, err := s.ClaimPending(context.Background(), 5)
	if err == nil {
		t.Error("ClaimPending: expected query error to surface")
	}
}

func TestPostgresStore_Cleanup_ReturnsRowsAffected(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`DELETE FROM event_outbox WHERE status=.* AND published_at <`).
		WithArgs(StatusPublished, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 17))

	deleted, err := s.Cleanup(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if deleted != 17 {
		t.Errorf("Cleanup: got %d, want 17", deleted)
	}
}

func TestPostgresStore_Cleanup_ExecError(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`DELETE FROM event_outbox`).WillReturnError(errors.New("disk full"))

	deleted, err := s.Cleanup(context.Background(), time.Hour)
	if err == nil {
		t.Fatal("Cleanup: expected exec error")
	}
	if deleted != 0 {
		t.Errorf("Cleanup on error: got %d, want 0", deleted)
	}
}

func TestPostgresStore_Store_UsesContextTransaction(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// Store is the event.OutboxStore integration point. When the context
	// carries an active *sql.Tx, the INSERT must go through that tx so it
	// commits/rolls back atomically with the caller's business work.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO event_outbox`).
		WithArgs("e", "id", []byte("p"), sqlmock.AnyArg(), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	ctx := event.WithOutboxTx(context.Background(), tx)
	if err := s.Store(ctx, "e", "id", []byte("p"), map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	_ = tx.Commit()
}

func TestPostgresStore_Store_FallbackToDirectExec(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// No tx in context — Store falls back to direct db.ExecContext.
	mock.ExpectExec(`INSERT INTO event_outbox`).
		WithArgs("e", "id", []byte("p"), []byte(nil), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.Store(context.Background(), "e", "id", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
}

func TestPostgresStore_Store_EmitsNotifyOnTxPath(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// Regression test: Store must emit pg_notify on the outbox notify channel
	// after a successful insert so a NOTIFY Waker (WithNotifyListener) wakes
	// up the relay instead of silently degrading to polling. In the tx path,
	// the notify exec must run on the same *sql.Tx as the insert so it only
	// becomes visible to listeners once the caller commits.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO event_outbox`).
		WithArgs("e", "id", []byte("p"), sqlmock.AnyArg(), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, ''\)`).
		WithArgs(s.NotifyChannel()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	ctx := event.WithOutboxTx(context.Background(), tx)
	if err := s.Store(ctx, "e", "id", []byte("p"), map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	_ = tx.Commit()
}

func TestPostgresStore_Store_EmitsNotifyOnFallbackPath(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// Regression test: the non-transactional fallback must also emit
	// pg_notify after a successful insert, directly on s.db.
	mock.ExpectExec(`INSERT INTO event_outbox`).
		WithArgs("e", "id", []byte("p"), []byte(nil), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, ''\)`).
		WithArgs(s.NotifyChannel()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.Store(context.Background(), "e", "id", []byte("p"), nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
}

func TestPostgresStore_Store_WrongSessionTypeRejected(t *testing.T) {
	t.Parallel()
	db, _ := setupMock(t)
	s, _ := NewPostgresStore(db)

	// A non-*sql.Tx session in the outbox context — e.g., a MongoDB session
	// — must be rejected with a clear error rather than silently bypassed.
	// This prevents a cross-store type confusion that could break the
	// atomicity contract.
	ctx := event.WithOutboxTx(context.Background(), "not-a-tx")
	err := s.Store(ctx, "e", "id", []byte("p"), nil)
	if err == nil {
		t.Fatal("Store: expected error when ctx contains a non-*sql.Tx session")
	}
}

func TestPostgresTransaction_PiggybacksExistingTx(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)

	// When called inside a context that already has a *sql.Tx, the helper
	// must NOT open a second transaction — it invokes fn directly. Only one
	// Begin/Rollback pair (the outer one) is expected.
	mock.ExpectBegin()
	mock.ExpectRollback()

	outerTx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin: %v", err)
	}
	ctx := event.WithOutboxTx(context.Background(), outerTx)

	var called bool
	err = PostgresTransaction(ctx, db, func(ctx context.Context) error {
		called = true
		if got := event.OutboxTx(ctx); got != outerTx {
			t.Errorf("piggy-back tx changed: got %p want %p", got, outerTx)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("PostgresTransaction: %v", err)
	}
	if !called {
		t.Error("fn not invoked")
	}
	_ = outerTx.Rollback()
}

func TestPostgresTransaction_OpensNewTxAndCommits(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectCommit()

	var seen any
	err := PostgresTransaction(context.Background(), db, func(ctx context.Context) error {
		seen = event.OutboxTx(ctx)
		return nil
	})
	if err != nil {
		t.Fatalf("PostgresTransaction: %v", err)
	}
	if _, ok := seen.(*sql.Tx); !ok {
		t.Errorf("OutboxTx in fn context: got %T, want *sql.Tx", seen)
	}
}

func TestPostgresTransaction_FnErrorRollsBack(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)

	mock.ExpectBegin()
	mock.ExpectRollback()

	want := errors.New("business error")
	err := PostgresTransaction(context.Background(), db, func(context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Errorf("PostgresTransaction: got %v, want %v", err, want)
	}
}

func TestPostgresTransaction_BeginErrPropagates(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)

	mock.ExpectBegin().WillReturnError(errors.New("begin boom"))

	err := PostgresTransaction(context.Background(), db, func(context.Context) error { return nil })
	if err == nil || !regexp.MustCompile(`begin outbox tx`).MatchString(err.Error()) {
		t.Errorf("PostgresTransaction: got %v, want wrapped begin error", err)
	}
}

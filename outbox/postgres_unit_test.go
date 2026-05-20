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

func TestPostgresStore_Insert_HappyPath(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO event_outbox`).
		WithArgs("order.created", "evt-1", []byte("payload"), sqlmock.AnyArg(), StatusPending, sqlmock.AnyArg(), 5).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(42)))
	mock.ExpectExec(`pg_notify`).
		WithArgs("event_outbox_pending").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("db.Begin: %v", err)
	}
	msg := &Message{
		EventName: "order.created",
		EventID:   "evt-1",
		Payload:   []byte("payload"),
		Metadata:  map[string]string{"source": "test"},
		Priority:  5,
	}
	if err := s.Insert(context.Background(), tx, msg); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if msg.ID != 42 {
		t.Errorf("Insert did not populate msg.ID; got %d, want 42", msg.ID)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestPostgresStore_Insert_NoMetadata(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// When Metadata is nil, the JSON column is inserted as a typed-nil
	// []byte. Verify the production code does NOT try to Marshal(nil),
	// which would emit the four bytes "null" rather than NULL.
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO event_outbox`).
		WithArgs("evt", "id", []byte("p"), []byte(nil), StatusPending, sqlmock.AnyArg(), 0).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)))
	mock.ExpectExec(`pg_notify`).WithArgs("event_outbox_pending").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, _ := db.Begin()
	if err := s.Insert(context.Background(), tx, &Message{EventName: "evt", EventID: "id", Payload: []byte("p")}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	_ = tx.Commit()
}

func TestPostgresStore_Insert_QueryError(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO event_outbox`).WillReturnError(errors.New("constraint violation"))
	mock.ExpectRollback()

	tx, _ := db.Begin()
	err := s.Insert(context.Background(), tx, &Message{EventName: "e", EventID: "i", Payload: []byte("p")})
	if err == nil || err.Error() != "constraint violation" {
		t.Errorf("Insert: got %v, want unwrapped DB error", err)
	}
	_ = tx.Rollback()
}

func TestPostgresStore_GetPending(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	created := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"id", "event_name", "event_id", "payload", "metadata", "created_at", "retry_count", "priority"}).
		AddRow(int64(1), "evt.a", "id-a", []byte("pa"), []byte(`{"k":"v"}`), created, 0, 10).
		AddRow(int64(2), "evt.b", "id-b", []byte("pb"), nil, created, 3, 0)

	mock.ExpectQuery(`SELECT .* FROM event_outbox`).
		WithArgs(StatusPending, StatusFailed, 50).
		WillReturnRows(rows)

	msgs, err := s.GetPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("GetPending: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].ID != 1 || msgs[0].Metadata["k"] != "v" {
		t.Errorf("first message decoded incorrectly: %+v", msgs[0])
	}
	if msgs[1].Metadata != nil {
		t.Errorf("nil metadata should yield nil map; got %v", msgs[1].Metadata)
	}
}

func TestPostgresStore_ProcessPending_PublishMarksRow(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	created := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM event_outbox`).
		WithArgs(StatusPending, StatusFailed, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_name", "event_id", "payload", "metadata", "created_at", "retry_count", "priority"}).
			AddRow(int64(7), "e", "id", []byte("p"), nil, created, 0, 0))
	mock.ExpectExec(`UPDATE event_outbox SET status = .* published_at`).
		WithArgs(StatusPublished, sqlmock.AnyArg(), int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	var seen []*Message
	err := s.ProcessPending(context.Background(), 10, func(m *Message) error {
		seen = append(seen, m)
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	if len(seen) != 1 || seen[0].ID != 7 {
		t.Errorf("callback received %+v, want one msg id=7", seen)
	}
}

func TestPostgresStore_ProcessPending_FailureMarksRowAndContinues(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	created := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM event_outbox`).
		WithArgs(StatusPending, StatusFailed, 10).
		WillReturnRows(sqlmock.NewRows([]string{"id", "event_name", "event_id", "payload", "metadata", "created_at", "retry_count", "priority"}).
			AddRow(int64(1), "e", "id1", []byte("p"), nil, created, 0, 0).
			AddRow(int64(2), "e", "id2", []byte("p"), nil, created, 0, 0))
	// First message fails → UPDATE with last_error
	mock.ExpectExec(`UPDATE event_outbox SET status = .* last_error = .* retry_count = retry_count \+ 1`).
		WithArgs(StatusFailed, "publish boom", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Second message succeeds → UPDATE with published_at
	mock.ExpectExec(`UPDATE event_outbox SET status = .* published_at`).
		WithArgs(StatusPublished, sqlmock.AnyArg(), int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	call := 0
	err := s.ProcessPending(context.Background(), 10, func(m *Message) error {
		call++
		if m.ID == 1 {
			return errors.New("publish boom")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}
	if call != 2 {
		t.Errorf("callback called %d times, want 2 (one failure should not abort)", call)
	}
}

func TestPostgresStore_ProcessPending_BeginErrPropagates(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin().WillReturnError(errors.New("cannot begin"))

	err := s.ProcessPending(context.Background(), 10, func(*Message) error { return nil })
	if err == nil || !regexp.MustCompile(`begin tx`).MatchString(err.Error()) {
		t.Errorf("ProcessPending: got %v, want wrapped begin error", err)
	}
}

func TestPostgresStore_ProcessPending_SelectErrRollsBack(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT .* FROM event_outbox`).WillReturnError(errors.New("query boom"))
	mock.ExpectRollback()

	err := s.ProcessPending(context.Background(), 5, func(*Message) error { return nil })
	if err == nil {
		t.Error("ProcessPending: expected select error to surface")
	}
}

func TestPostgresStore_MarkPublished(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`UPDATE event_outbox\s+SET status = .* published_at = .* WHERE id = `).
		WithArgs(StatusPublished, sqlmock.AnyArg(), int64(99)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkPublished(context.Background(), 99); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
}

func TestPostgresStore_MarkFailed_RecordsError(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`UPDATE event_outbox\s+SET status = .* last_error = .* retry_count = retry_count \+ 1 WHERE id =`).
		WithArgs(StatusFailed, "transport down", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkFailed(context.Background(), 7, errors.New("transport down")); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

func TestPostgresStore_MarkFailed_NilErrorYieldsEmptyMessage(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	// Defensive: MarkFailed(nil) is permitted by the signature; verify the
	// empty string flows through rather than panicking.
	mock.ExpectExec(`UPDATE event_outbox`).
		WithArgs(StatusFailed, "", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.MarkFailed(context.Background(), 1, nil); err != nil {
		t.Fatalf("MarkFailed(nil): %v", err)
	}
}

func TestPostgresStore_Delete_ReturnsRowsAffected(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`DELETE FROM event_outbox\s+WHERE status = .* AND published_at <`).
		WithArgs(StatusPublished, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 17))

	deleted, err := s.Delete(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if deleted != 17 {
		t.Errorf("Delete: got %d, want 17", deleted)
	}
}

func TestPostgresStore_Delete_ExecError(t *testing.T) {
	t.Parallel()
	db, mock := setupMock(t)
	s, _ := NewPostgresStore(db)

	mock.ExpectExec(`DELETE FROM event_outbox`).WillReturnError(errors.New("disk full"))

	deleted, err := s.Delete(context.Background(), time.Hour)
	if err == nil {
		t.Fatal("Delete: expected exec error")
	}
	if deleted != 0 {
		t.Errorf("Delete on error: got %d, want 0", deleted)
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

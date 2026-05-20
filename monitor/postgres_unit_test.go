package monitor

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	event "github.com/rbaliyan/event/v3"
)

// setupMock returns a *sql.DB backed by sqlmock plus the mock controller, with
// cleanupInterval forced to zero so NewPostgresStore does not spawn a goroutine
// that would race the mock's expectations.
func setupMock(t *testing.T, opts ...StoreOption) (*PostgresStore, sqlmock.Sqlmock) {
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

	// Disable background cleanup so the store has no async goroutines that
	// could race with the mock during the test body.
	allOpts := append([]StoreOption{WithCleanupInterval(0)}, opts...)
	store, err := NewPostgresStore(db, allOpts...)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, mock
}

func TestNewPostgresStore_NilDB(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresStore(nil); err == nil {
		t.Error("NewPostgresStore(nil): expected error, got nil")
	}
}

func TestNewPostgresStore_AppliesOptions(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t, WithTableName("my_monitor"), WithSampling(0.5))
	if s.opts.tableName != "my_monitor" {
		t.Errorf("WithTableName: got %q, want %q", s.opts.tableName, "my_monitor")
	}
	if s.opts.samplingRate != 0.5 {
		t.Errorf("WithSampling: got %v, want 0.5", s.opts.samplingRate)
	}
}

func TestNewPostgresStore_InvalidIdentifierKeepsDefault(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t, WithTableName("dangerous; DROP TABLE x;--"))
	if s.opts.tableName != "monitor_entries" {
		t.Errorf("invalid identifier should be rejected; got %q", s.opts.tableName)
	}
}

func TestPostgresStore_Record_BroadcastUpsertsOnConflict(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Broadcast mode: ON CONFLICT DO UPDATE — each subscription has its own
	// row keyed by (event_id, subscription_id), and a later UpdateStatus
	// can refresh the row.
	mock.ExpectExec(`INSERT INTO monitor_entries.*ON CONFLICT.*DO UPDATE SET`).
		WithArgs(
			"evt-1",         // event_id
			"sub-1",         // subscription_id
			sqlmock.AnyArg(), // subscriber_name (*string)
			sqlmock.AnyArg(), // subscriber_description (*string)
			"order.created", // event_name
			"bus-1",         // bus_id
			sqlmock.AnyArg(), // instance_id (*string)
			"broadcast",     // delivery_mode
			sqlmock.AnyArg(), // metadata JSONB
			"pending",       // status
			sqlmock.AnyArg(), // error (*string)
			0,               // retry_count
			sqlmock.AnyArg(), // started_at
			sqlmock.AnyArg(), // completed_at
			sqlmock.AnyArg(), // duration_ms (*int64)
			sqlmock.AnyArg(), // trace_id (*string)
			sqlmock.AnyArg(), // span_id (*string)
			sqlmock.AnyArg(), // worker_group (*string)
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	entry := &Entry{
		EventID:        "evt-1",
		SubscriptionID: "sub-1",
		EventName:      "order.created",
		BusID:          "bus-1",
		DeliveryMode:   Broadcast,
		Status:         StatusPending,
		StartedAt:      time.Now(),
	}
	if err := s.Record(context.Background(), entry); err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestPostgresStore_Record_WorkerPoolDoNothingOnConflict(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// WorkerPool mode: ON CONFLICT DO NOTHING. Only one worker should record
	// each event; concurrent inserts must not overwrite each other. Also
	// verify subscription_id is forced to empty string per the documented
	// "EventID is the unique key for WorkerPool" contract.
	mock.ExpectExec(`INSERT INTO monitor_entries.*ON CONFLICT.*DO NOTHING`).
		WithArgs(
			"evt-2", "",
			sqlmock.AnyArg(), sqlmock.AnyArg(),
			"order.shipped", "bus-1",
			sqlmock.AnyArg(),
			"worker_pool",
			sqlmock.AnyArg(),
			"completed",
			sqlmock.AnyArg(),
			0,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	entry := &Entry{
		EventID:        "evt-2",
		SubscriptionID: "this-gets-clobbered",
		EventName:      "order.shipped",
		BusID:          "bus-1",
		DeliveryMode:   WorkerPool,
		Status:         StatusCompleted,
		StartedAt:      time.Now(),
	}
	if err := s.Record(context.Background(), entry); err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestPostgresStore_Record_SamplingSkipsBelowRate(t *testing.T) {
	t.Parallel()
	// Sampling rate of 0 means EVERY entry is skipped — no DB calls. The
	// mock would catch any spurious INSERT via ExpectationsWereMet.
	s, _ := setupMock(t, WithSampling(0))
	for i := range 5 {
		entry := &Entry{
			EventID:      "evt",
			EventName:    "e",
			BusID:        "b",
			DeliveryMode: Broadcast,
			Status:       StatusPending,
			StartedAt:    time.Now(),
		}
		_ = i
		if err := s.Record(context.Background(), entry); err != nil {
			t.Fatalf("Record with rate=0: %v", err)
		}
	}
}

func TestPostgresStore_Record_ExecErrorWrapped(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`INSERT INTO monitor_entries`).WillReturnError(errors.New("duplicate key"))

	err := s.Record(context.Background(), &Entry{
		EventID:      "e",
		EventName:    "n",
		BusID:        "b",
		DeliveryMode: Broadcast,
		Status:       StatusPending,
		StartedAt:    time.Now(),
	})
	if err == nil {
		t.Fatal("Record: expected exec error")
	}
}

func TestPostgresStore_Get_ReturnsNilForNoRows(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries\s+WHERE event_id = .* AND subscription_id =`).
		WithArgs("missing", "sub").
		WillReturnError(sql.ErrNoRows)

	entry, err := s.Get(context.Background(), "missing", "sub")
	if err != nil {
		t.Errorf("Get returned error on no-rows: %v", err)
	}
	if entry != nil {
		t.Errorf("Get returned non-nil entry on no-rows: %+v", entry)
	}
}

func TestPostgresStore_Get_ScansSingleRow(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	started := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{
		"event_id", "subscription_id", "subscriber_name", "subscriber_description",
		"event_name", "bus_id", "instance_id", "delivery_mode",
		"metadata", "status", "error", "retry_count", "started_at", "completed_at",
		"duration_ms", "trace_id", "span_id", "worker_group",
	}).AddRow(
		"evt-1", "sub-1", nil, nil,
		"order.created", "bus-1", nil, "broadcast",
		nil, "completed", nil, 0, started, nil,
		nil, nil, nil, nil,
	)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries`).
		WithArgs("evt-1", "sub-1").
		WillReturnRows(rows)

	entry, err := s.Get(context.Background(), "evt-1", "sub-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry == nil {
		t.Fatal("Get returned nil entry on existing row")
	}
	if entry.EventID != "evt-1" || entry.Status != StatusCompleted {
		t.Errorf("scanned entry mismatch: %+v", entry)
	}
}

func TestPostgresStore_GetByEventID(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	started := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"event_id", "subscription_id", "subscriber_name", "subscriber_description",
		"event_name", "bus_id", "instance_id", "delivery_mode",
		"metadata", "status", "error", "retry_count", "started_at", "completed_at",
		"duration_ms", "trace_id", "span_id", "worker_group",
	}).
		AddRow("evt-1", "sub-a", nil, nil, "e", "b", nil, "broadcast",
			nil, "completed", nil, 0, started, nil, nil, nil, nil, nil).
		AddRow("evt-1", "sub-b", nil, nil, "e", "b", nil, "broadcast",
			nil, "failed", nil, 1, started, nil, nil, nil, nil, nil)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries\s+WHERE event_id =`).
		WithArgs("evt-1").
		WillReturnRows(rows)

	entries, err := s.GetByEventID(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].SubscriptionID != "sub-a" || entries[1].SubscriptionID != "sub-b" {
		t.Errorf("entries out of expected order: %+v", entries)
	}
}

func TestPostgresStore_GetByEventID_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries\s+WHERE event_id =`).
		WillReturnError(errors.New("conn refused"))

	if _, err := s.GetByEventID(context.Background(), "x"); err == nil {
		t.Fatal("GetByEventID: expected error")
	}
}

func TestPostgresStore_UpdateStatus_WithError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`UPDATE monitor_entries\s+SET status = .* error = .* duration_ms`).
		WithArgs("failed", "boom", int64(250), "evt-1", "sub-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.UpdateStatus(context.Background(), "evt-1", "sub-1", StatusFailed, errors.New("boom"), 250*time.Millisecond); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
}

func TestPostgresStore_UpdateStatus_NilError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// nil error → NULL in the error column. sqlmock's argument matcher
	// compares against a typed nil; using sqlmock.AnyArg here would mask a
	// regression that accidentally writes the literal "<nil>" string.
	mock.ExpectExec(`UPDATE monitor_entries`).
		WithArgs("completed", nil, int64(100), "e", "s").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.UpdateStatus(context.Background(), "e", "s", StatusCompleted, nil, 100*time.Millisecond); err != nil {
		t.Fatalf("UpdateStatus(nil err): %v", err)
	}
}

func TestPostgresStore_DeleteOlderThan(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM monitor_entries\s+WHERE started_at < NOW\(\) - .*::interval`).
		WithArgs("24h0m0s").
		WillReturnResult(sqlmock.NewResult(0, 42))

	n, err := s.DeleteOlderThan(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan: %v", err)
	}
	if n != 42 {
		t.Errorf("DeleteOlderThan: got %d, want 42", n)
	}
}

func TestPostgresStore_DeleteOlderThan_Error(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`DELETE FROM monitor_entries`).
		WillReturnError(errors.New("io"))

	if _, err := s.DeleteOlderThan(context.Background(), time.Hour); err == nil {
		t.Fatal("DeleteOlderThan: expected exec error")
	}
}

func TestPostgresStore_CreateTable(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS monitor_entries`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := s.CreateTable(context.Background()); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

func TestPostgresStore_RecordStart_DelegatesToRecord(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// RecordStart must materialize into an Entry with status="pending" and
	// delivery_mode reflecting params.WorkerPool. Pin the resulting INSERT.
	mock.ExpectExec(`INSERT INTO monitor_entries.*ON CONFLICT.*DO NOTHING`).
		WithArgs(
			"evt-rs", "",
			sqlmock.AnyArg(), sqlmock.AnyArg(),
			"e", "b",
			sqlmock.AnyArg(),
			"worker_pool",
			sqlmock.AnyArg(),
			"pending",
			sqlmock.AnyArg(),
			0,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.RecordStart(context.Background(), event.RecordStartParams{
		EventID:    "evt-rs",
		EventName:  "e",
		BusID:      "b",
		WorkerPool: true,
	}); err != nil {
		t.Fatalf("RecordStart: %v", err)
	}
}

func TestPostgresStore_RecordComplete_DelegatesToUpdateStatus(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// RecordComplete must materialize into an UPDATE. Pin the column-write
	// shape and argument order so a refactor that flips them is caught here.
	mock.ExpectExec(`UPDATE monitor_entries\s+SET status =`).
		WithArgs("completed", nil, int64(50), "evt-rc", "sub-rc").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.RecordComplete(context.Background(), event.RecordCompleteParams{
		EventID:        "evt-rc",
		SubscriptionID: "sub-rc",
		Status:         "completed",
		Duration:       50 * time.Millisecond,
	}); err != nil {
		t.Fatalf("RecordComplete: %v", err)
	}
}

func TestPostgresStore_Close_StopsCleanupGoroutine(t *testing.T) {
	t.Parallel()
	// Close must stop the background cleanup goroutine. Construct a store
	// with a 1-hour cleanup interval (long enough not to fire during the
	// test) and verify Close returns nil. The setupMock helper's t.Cleanup
	// would otherwise double-close on the same channel — production code
	// does not guard against close-of-closed-channel — so we use a manual
	// constructor here that doesn't register a deferred Close.
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock unmet: %v", err)
		}
	})

	store, err := NewPostgresStore(db, WithCleanupInterval(time.Hour))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Note: PostgresStore.Close is not idempotent — calling it twice panics
	// on close-of-closed-channel. Callers must ensure single-close discipline.
}

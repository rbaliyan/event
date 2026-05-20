package schema

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// setupMock returns a *PostgresProvider wired to sqlmock, with a no-op
// publisher by default. The mock is in QueryMatcherRegexp mode so test
// patterns survive whitespace and formatting tweaks.
func setupMock(t *testing.T, opts ...PostgresOption) (*PostgresProvider, sqlmock.Sqlmock, func(context.Context, SchemaChangeEvent) error) {
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

	// Captures the most recent publisher invocation for tests that need to
	// assert on it. Tests that need to inject a publisher error replace it.
	var publisherErr error
	publisher := func(_ context.Context, _ SchemaChangeEvent) error { return publisherErr }

	p, err := NewPostgresProvider(db, publisher, opts...)
	if err != nil {
		t.Fatalf("NewPostgresProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Return a setter so a test can swap the captured publisher behavior.
	setErr := func(_ context.Context, _ SchemaChangeEvent) error { return publisherErr }
	return p, mock, setErr
}

// schemaRowCols matches the column order of the SELECT in Get / List.
var schemaRowCols = []string{
	"name", "version", "description", "sub_timeout_ms", "max_retries",
	"retry_backoff_ms", "enable_monitor", "enable_idempotency", "enable_poison",
	"metadata", "created_at", "updated_at",
}

func TestNewPostgresProvider_NilDB(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresProvider(nil, func(context.Context, SchemaChangeEvent) error { return nil }); err == nil {
		t.Error("NewPostgresProvider(nil): expected error")
	}
}

func TestNewPostgresProvider_NilPublisher(t *testing.T) {
	t.Parallel()
	db, _, _ := sqlmock.New()
	t.Cleanup(func() { _ = db.Close() })
	if _, err := NewPostgresProvider(db, nil); err == nil {
		t.Error("NewPostgresProvider(nil publisher): expected error")
	}
}

func TestNewPostgresProvider_WithTableName(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t, WithTableName("my_schemas"))
	if p.tableName != "my_schemas" {
		t.Errorf("WithTableName: got %q, want %q", p.tableName, "my_schemas")
	}
}

func TestNewPostgresProvider_InvalidIdentifierKeepsDefault(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t, WithTableName("bad; DROP TABLE x;--"))
	if p.tableName != "event_schemas" {
		t.Errorf("invalid identifier should be rejected; got %q", p.tableName)
	}
}

func TestPostgresProvider_Get_NotFoundReturnsNilNil(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM event_schemas\s+WHERE name =`).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	s, err := p.Get(context.Background(), "missing")
	if err != nil {
		t.Errorf("Get(missing) returned err: %v", err)
	}
	if s != nil {
		t.Errorf("Get(missing) returned non-nil schema: %+v", s)
	}
}

func TestPostgresProvider_Get_HappyPath(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT .* FROM event_schemas\s+WHERE name =`).
		WithArgs("order.created").
		WillReturnRows(sqlmock.NewRows(schemaRowCols).AddRow(
			"order.created", 3, "Order created event",
			int64(5000), int32(7), int64(250),
			true, true, false,
			[]byte(`{"owner":"orders-team"}`),
			now, now,
		))

	s, err := p.Get(context.Background(), "order.created")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s == nil {
		t.Fatal("Get returned nil schema on happy path")
	}
	if s.Name != "order.created" || s.Version != 3 || !s.EnableMonitor || !s.EnableIdempotency {
		t.Errorf("scanned schema mismatch: %+v", s)
	}
	if s.SubTimeout != 5*time.Second {
		t.Errorf("SubTimeout: got %v, want 5s", s.SubTimeout)
	}
	if s.MaxRetries != 7 {
		t.Errorf("MaxRetries: got %d, want 7", s.MaxRetries)
	}
	if s.RetryBackoff != 250*time.Millisecond {
		t.Errorf("RetryBackoff: got %v, want 250ms", s.RetryBackoff)
	}
	if s.Metadata["owner"] != "orders-team" {
		t.Errorf("Metadata: got %v, want owner=orders-team", s.Metadata)
	}
}

func TestPostgresProvider_Get_NullFieldsScanCleanly(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	// Optional columns (description, sub_timeout_ms, max_retries,
	// retry_backoff_ms, metadata) are nullable. Verify the scanner produces
	// the zero values for them without populating sentinel garbage.
	now := time.Now().UTC()
	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("min").
		WillReturnRows(sqlmock.NewRows(schemaRowCols).AddRow(
			"min", 1, nil, nil, nil, nil,
			false, false, false,
			nil,
			now, now,
		))

	s, err := p.Get(context.Background(), "min")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s.Description != "" || s.SubTimeout != 0 || s.MaxRetries != 0 || s.RetryBackoff != 0 {
		t.Errorf("null fields not scanned as zero: %+v", s)
	}
	if s.Metadata != nil {
		t.Errorf("nil metadata column should produce nil map; got %v", s.Metadata)
	}
}

func TestPostgresProvider_Get_QueryError(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("e").
		WillReturnError(errors.New("conn refused"))

	if _, err := p.Get(context.Background(), "e"); err == nil {
		t.Fatal("Get: expected error")
	}
}

func TestPostgresProvider_Get_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	_ = p.Close()
	_, err := p.Get(context.Background(), "x")
	if !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Get on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestPostgresProvider_Set_NewSchemaInserts(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	// First Set: existing schema lookup returns ErrNoRows (new schema),
	// then INSERT ... ON CONFLICT DO UPDATE fires with all the args.
	mock.ExpectQuery(`SELECT .* FROM event_schemas\s+WHERE name =`).
		WithArgs("order.created").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectExec(`INSERT INTO event_schemas.*ON CONFLICT \(name\) DO UPDATE SET`).
		WithArgs(
			"order.created",      // name
			3,                    // version
			sqlmock.AnyArg(),     // description (*string)
			sqlmock.AnyArg(),     // sub_timeout_ms (*int64)
			sqlmock.AnyArg(),     // max_retries (*int)
			sqlmock.AnyArg(),     // retry_backoff_ms (*int64)
			true, true, false,    // flags
			sqlmock.AnyArg(),     // metadata JSONB
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := p.Set(context.Background(), &EventSchema{
		Name:              "order.created",
		Version:           3,
		Description:       "Order created",
		SubTimeout:        5 * time.Second,
		MaxRetries:        7,
		RetryBackoff:      250 * time.Millisecond,
		EnableMonitor:     true,
		EnableIdempotency: true,
		Metadata:          map[string]string{"owner": "orders-team"},
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestPostgresProvider_Set_VersionDowngradeRejected(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	now := time.Now().UTC()
	// Existing schema is at version 5; attempt to set version 3.
	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("evt").
		WillReturnRows(sqlmock.NewRows(schemaRowCols).AddRow(
			"evt", 5, nil, nil, nil, nil, false, false, false, nil, now, now,
		))
	// No INSERT expectation: Set must short-circuit on downgrade.

	err := p.Set(context.Background(), &EventSchema{Name: "evt", Version: 3})
	if !errors.Is(err, ErrVersionDowngrade) {
		t.Errorf("Set with downgrade: got %v, want ErrVersionDowngrade", err)
	}
}

func TestPostgresProvider_Set_SameVersionAllowed(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	now := time.Now().UTC()
	// Equal versions are allowed (ON CONFLICT DO UPDATE will refresh
	// description / flags / metadata even when version stays the same).
	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("evt").
		WillReturnRows(sqlmock.NewRows(schemaRowCols).AddRow(
			"evt", 5, nil, nil, nil, nil, false, false, false, nil, now, now,
		))
	mock.ExpectExec(`INSERT INTO event_schemas.*ON CONFLICT \(name\) DO UPDATE`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := p.Set(context.Background(), &EventSchema{Name: "evt", Version: 5}); err != nil {
		t.Errorf("Set same-version: %v", err)
	}
}

func TestPostgresProvider_Set_ValidationFailureShortCircuits(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	// No SQL expectations — Validate runs before any DB call.

	err := p.Set(context.Background(), &EventSchema{Name: "", Version: 1})
	if !errors.Is(err, ErrEmptyName) {
		t.Errorf("Set(empty name): got %v, want ErrEmptyName", err)
	}
	err = p.Set(context.Background(), &EventSchema{Name: "x", Version: 0})
	if !errors.Is(err, ErrInvalidVersion) {
		t.Errorf("Set(version=0): got %v, want ErrInvalidVersion", err)
	}
}

func TestPostgresProvider_Set_PublisherErrorSwallowed(t *testing.T) {
	t.Parallel()
	// If the publisher fails to broadcast the change, the row has already
	// been written; Set must NOT surface the publisher error and confuse
	// the caller into retrying. Pin this swallow contract.
	publisherErr := errors.New("transport down")
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = mock.ExpectationsWereMet()
	})
	p, err := NewPostgresProvider(db, func(context.Context, SchemaChangeEvent) error {
		return publisherErr
	})
	if err != nil {
		t.Fatalf("NewPostgresProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("evt").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO event_schemas`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := p.Set(context.Background(), &EventSchema{Name: "evt", Version: 1}); err != nil {
		t.Errorf("Set: publisher error should not surface; got %v", err)
	}
}

func TestPostgresProvider_Set_NotifiesWatchers(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch, err := p.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	mock.ExpectQuery(`SELECT .* FROM event_schemas`).
		WithArgs("evt.notify").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(`INSERT INTO event_schemas`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := p.Set(context.Background(), &EventSchema{Name: "evt.notify", Version: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	select {
	case change := <-ch:
		if change.EventName != "evt.notify" || change.Version != 1 {
			t.Errorf("watcher saw %+v, want EventName=evt.notify Version=1", change)
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not receive change event within 1s")
	}
}

func TestPostgresProvider_Set_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	_ = p.Close()

	// Need to pass Validate so we exercise the closed-check, not the
	// validation-check.
	err := p.Set(context.Background(), &EventSchema{Name: "x", Version: 1})
	if !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Set on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestPostgresProvider_Delete(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectExec(`DELETE FROM event_schemas WHERE name =`).
		WithArgs("evt").
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := p.Delete(context.Background(), "evt"); err != nil {
		t.Errorf("Delete: %v", err)
	}
}

func TestPostgresProvider_Delete_ExecError(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectExec(`DELETE FROM event_schemas`).WillReturnError(errors.New("io"))
	if err := p.Delete(context.Background(), "x"); err == nil {
		t.Fatal("Delete: expected exec error")
	}
}

func TestPostgresProvider_Delete_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	_ = p.Close()
	if err := p.Delete(context.Background(), "x"); !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Delete on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestPostgresProvider_List_OrdersByName(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	now := time.Now().UTC()
	mock.ExpectQuery(`SELECT .* FROM event_schemas\s+ORDER BY name`).
		WillReturnRows(sqlmock.NewRows(schemaRowCols).
			AddRow("a", 1, nil, nil, nil, nil, false, false, false, nil, now, now).
			AddRow("b", 2, "second", nil, nil, nil, true, false, false, nil, now, now))

	out, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out))
	}
	if out[0].Name != "a" || out[1].Name != "b" {
		t.Errorf("entries out of expected order: %+v", out)
	}
	if !out[1].EnableMonitor {
		t.Errorf("entry b should have EnableMonitor=true; got %+v", out[1])
	}
}

func TestPostgresProvider_List_QueryError(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM event_schemas`).WillReturnError(errors.New("disk"))
	if _, err := p.List(context.Background()); err == nil {
		t.Fatal("List: expected error")
	}
}

func TestPostgresProvider_List_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	_ = p.Close()
	if _, err := p.List(context.Background()); !errors.Is(err, ErrProviderClosed) {
		t.Errorf("List on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestPostgresProvider_Watch_ContextCancelClosesChannel(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Watch(ctx)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	cancel()
	// The channel should be closed by the goroutine that observes ctx.Done.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher channel was not closed after context cancel")
	}
}

func TestPostgresProvider_Watch_CloseClosesChannel(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)

	ch, err := p.Watch(context.Background())
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel, got value")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher channel was not closed after Close")
	}
}

func TestPostgresProvider_Watch_ClosedProvider(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	_ = p.Close()
	_, err := p.Watch(context.Background())
	if !errors.Is(err, ErrProviderClosed) {
		t.Errorf("Watch on closed: got %v, want ErrProviderClosed", err)
	}
}

func TestPostgresProvider_Close_Idempotent(t *testing.T) {
	t.Parallel()
	p, _, _ := setupMock(t)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second Close should be no-op; got %v", err)
	}
}

func TestPostgresProvider_CreateTable(t *testing.T) {
	t.Parallel()
	p, mock, _ := setupMock(t)

	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS event_schemas.*CREATE INDEX IF NOT EXISTS idx_event_schemas_updated`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	if err := p.CreateTable(context.Background()); err != nil {
		t.Errorf("CreateTable: %v", err)
	}
}

func TestNullHelpers(t *testing.T) {
	t.Parallel()
	if nullString("") != nil {
		t.Error("nullString(\"\") must return nil")
	}
	if s := nullString("x"); s == nil || *s != "x" {
		t.Errorf("nullString(\"x\") = %v, want pointer to \"x\"", s)
	}
	if nullInt(0) != nil {
		t.Error("nullInt(0) must return nil")
	}
	if n := nullInt(7); n == nil || *n != 7 {
		t.Errorf("nullInt(7) = %v, want pointer to 7", n)
	}
}

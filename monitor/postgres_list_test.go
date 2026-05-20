package monitor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresStore_List_NoFilter_NoMore(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	started := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	// Limit=2 + the +1 page-probe row → expect a query for 3 rows; return 2
	// so HasMore is false.
	rows := sqlmock.NewRows([]string{
		"event_id", "subscription_id", "subscriber_name", "subscriber_description",
		"event_name", "bus_id", "instance_id", "delivery_mode",
		"metadata", "status", "error", "retry_count", "started_at", "completed_at",
		"duration_ms", "trace_id", "span_id", "worker_group",
	}).
		AddRow("e1", "s1", nil, nil, "evt", "b", nil, "broadcast", nil, "completed", nil, 0, started, nil, nil, nil, nil, nil).
		AddRow("e2", "s1", nil, nil, "evt", "b", nil, "broadcast", nil, "completed", nil, 0, started.Add(time.Second), nil, nil, nil, nil, nil)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries.*ORDER BY started_at ASC.*LIMIT 3`).
		WillReturnRows(rows)

	page, err := s.List(context.Background(), Filter{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(page.Entries))
	}
	if page.HasMore {
		t.Error("HasMore: expected false when result count <= limit")
	}
	if page.NextCursor != "" {
		t.Errorf("NextCursor should be empty when HasMore=false; got %q", page.NextCursor)
	}
}

func TestPostgresStore_List_HasMoreWhenLimitPlusOne(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	started := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	// Return limit+1 rows → the helper detects this and reports HasMore=true,
	// dropping the probe row from the page.
	r := sqlmock.NewRows([]string{
		"event_id", "subscription_id", "subscriber_name", "subscriber_description",
		"event_name", "bus_id", "instance_id", "delivery_mode",
		"metadata", "status", "error", "retry_count", "started_at", "completed_at",
		"duration_ms", "trace_id", "span_id", "worker_group",
	})
	for i := range 3 {
		r.AddRow("e", "s", nil, nil, "evt", "b", nil, "broadcast", nil, "completed", nil, 0, started.Add(time.Duration(i)*time.Second), nil, nil, nil, nil, nil)
	}

	mock.ExpectQuery(`SELECT .* FROM monitor_entries.*LIMIT 3`).
		WillReturnRows(r)

	page, err := s.List(context.Background(), Filter{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Errorf("page entries: got %d, want 2 (limit+1 detection should trim)", len(page.Entries))
	}
	if !page.HasMore {
		t.Error("HasMore: expected true when limit+1 rows returned")
	}
	if page.NextCursor == "" {
		t.Error("NextCursor should be populated when HasMore=true")
	}
}

func TestPostgresStore_List_OrderDescending(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries.*ORDER BY started_at DESC, event_id DESC, subscription_id DESC`).
		WillReturnRows(sqlmock.NewRows([]string{
			"event_id", "subscription_id", "subscriber_name", "subscriber_description",
			"event_name", "bus_id", "instance_id", "delivery_mode",
			"metadata", "status", "error", "retry_count", "started_at", "completed_at",
			"duration_ms", "trace_id", "span_id", "worker_group",
		}))

	if _, err := s.List(context.Background(), Filter{OrderDesc: true}); err != nil {
		t.Fatalf("List(OrderDesc): %v", err)
	}
}

func TestPostgresStore_List_FilterByEventNameAndStatus(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Verify that the filter's EventName and Status[] both make it into the
	// SQL parameter list. Use AnyArg for time-sensitive args.
	mock.ExpectQuery(`SELECT .* FROM monitor_entries.*WHERE.*event_name = .* status IN`).
		WithArgs("order.created", "completed", "failed").
		WillReturnRows(sqlmock.NewRows([]string{
			"event_id", "subscription_id", "subscriber_name", "subscriber_description",
			"event_name", "bus_id", "instance_id", "delivery_mode",
			"metadata", "status", "error", "retry_count", "started_at", "completed_at",
			"duration_ms", "trace_id", "span_id", "worker_group",
		}))

	_, err := s.List(context.Background(), Filter{
		EventName: "order.created",
		Status:    []Status{StatusCompleted, StatusFailed},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
}

func TestPostgresStore_List_InvalidCursorReturnsError(t *testing.T) {
	t.Parallel()
	s, _ := setupMock(t)

	// No SQL expectations: invalid cursor must short-circuit before query.
	_, err := s.List(context.Background(), Filter{Cursor: "not-a-base64-cursor"})
	if err == nil {
		t.Fatal("List with invalid cursor: expected error")
	}
}

func TestPostgresStore_List_QueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT .* FROM monitor_entries`).WillReturnError(errors.New("disk i/o"))

	if _, err := s.List(context.Background(), Filter{}); err == nil {
		t.Fatal("List: expected query error")
	}
}

func TestPostgresStore_Count(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor_entries`).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(123)))

	n, err := s.Count(context.Background(), Filter{})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 123 {
		t.Errorf("Count: got %d, want 123", n)
	}
}

func TestPostgresStore_Count_PassesFilterArgs(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// The same filter machinery that List uses must also wire arguments into
	// COUNT — otherwise Count diverges from List and dashboards lie.
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor_entries.*WHERE.*event_name`).
		WithArgs("evt").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(7)))

	n, err := s.Count(context.Background(), Filter{EventName: "evt"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 7 {
		t.Errorf("Count: got %d, want 7", n)
	}
}

func TestPostgresStore_Count_ScanError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM monitor_entries`).WillReturnError(errors.New("boom"))
	if _, err := s.Count(context.Background(), Filter{}); err == nil {
		t.Fatal("Count: expected error")
	}
}

func TestPostgresStore_Summary_PopulatesAllAggregates(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// Summary fires three queries: global aggregate, per-event-name, per-instance.
	// Pin the column layout returned by each so an accidental change to the
	// scan order is caught immediately rather than as a silently-wrong number
	// on a production dashboard.
	oldest := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	newest := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(`SELECT\s+COUNT\(\*\) AS total.*FROM monitor_entries`).
		WillReturnRows(sqlmock.NewRows([]string{
			"total", "avg_duration_ms", "failed_count", "completed_count",
			"retrying_count", "pending_count", "oldest", "newest",
		}).AddRow(
			int64(100), 250.0, int64(5), int64(80), int64(10), int64(5), oldest, newest,
		))
	mock.ExpectQuery(`SELECT\s+event_name,\s+COUNT\(\*\) AS total.*FROM monitor_entries`).
		WillReturnRows(sqlmock.NewRows([]string{
			"event_name", "total", "completed", "failed", "retrying", "pending", "avg_duration_ms",
		}).
			AddRow("order.created", int64(60), int64(50), int64(3), int64(5), int64(2), 100.0).
			AddRow("order.shipped", int64(40), int64(30), int64(2), int64(5), int64(3), 500.0))
	mock.ExpectQuery(`SELECT instance_id, COUNT\(\*\)`).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "count"}).
			AddRow("pod-1", int64(70)).
			AddRow("pod-2", int64(30)))

	sum, err := s.Summary(context.Background(), Filter{
		StartTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if sum.TotalEntries != 100 {
		t.Errorf("TotalEntries: got %d, want 100", sum.TotalEntries)
	}
	if sum.AvgDurationMs != 250 {
		t.Errorf("AvgDurationMs: got %d, want 250", sum.AvgDurationMs)
	}
	if sum.ErrorRate != 0.05 {
		t.Errorf("ErrorRate: got %v, want 0.05", sum.ErrorRate)
	}
	if sum.ByStatus[StatusCompleted] != 80 || sum.ByStatus[StatusFailed] != 5 {
		t.Errorf("ByStatus: got %v", sum.ByStatus)
	}
	if len(sum.ByEventName) != 2 {
		t.Errorf("ByEventName: got %d entries, want 2", len(sum.ByEventName))
	}
	if sum.ByEventName["order.created"].Completed != 50 {
		t.Errorf("order.created completed: got %d, want 50", sum.ByEventName["order.created"].Completed)
	}
	if len(sum.ByInstance) != 2 {
		t.Errorf("ByInstance: got %d, want 2", len(sum.ByInstance))
	}
	if sum.TimeRange.Oldest == nil || !sum.TimeRange.Oldest.Equal(oldest) {
		t.Errorf("TimeRange.Oldest: got %v, want %v", sum.TimeRange.Oldest, oldest)
	}
}

func TestPostgresStore_Summary_GlobalQueryError(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	mock.ExpectQuery(`SELECT\s+COUNT\(\*\) AS total`).WillReturnError(errors.New("planner crashed"))

	if _, err := s.Summary(context.Background(), Filter{}); err == nil {
		t.Fatal("Summary: expected error from global query")
	}
}

func TestPostgresStore_Summary_NoTimeRangeAddsDefaultWindow(t *testing.T) {
	t.Parallel()
	s, mock := setupMock(t)

	// When the filter has no StartTime/EndTime, Summary's implementation
	// adds a "last 24h" predicate. Verify by asserting that the global query
	// is issued with a time argument (default window applied) — the args
	// list is empty when no defaults are added.
	mock.ExpectQuery(`SELECT\s+COUNT\(\*\) AS total.*FROM monitor_entries.*WHERE started_at`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"total", "avg_duration_ms", "failed_count", "completed_count",
			"retrying_count", "pending_count", "oldest", "newest",
		}).AddRow(int64(0), 0.0, int64(0), int64(0), int64(0), int64(0), nil, nil))
	mock.ExpectQuery(`SELECT\s+event_name`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"event_name", "total", "completed", "failed", "retrying", "pending", "avg_duration_ms"}))
	mock.ExpectQuery(`SELECT instance_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"instance_id", "count"}))

	sum, err := s.Summary(context.Background(), Filter{})
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if sum.TotalEntries != 0 {
		t.Errorf("empty result: TotalEntries got %d, want 0", sum.TotalEntries)
	}
}

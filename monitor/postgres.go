package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/store/base"
)

// PostgresStore implements Store using PostgreSQL.
//
// PostgresStore provides durable monitor storage with cursor-based pagination.
// It uses a composite primary key (event_id, subscription_id) to support both
// Broadcast and WorkerPool delivery modes.
//
// Table Schema:
//
//	CREATE TABLE monitor_entries (
//	    event_id TEXT NOT NULL,
//	    subscription_id TEXT NOT NULL DEFAULT '',
//	    subscriber_name TEXT,
//	    subscriber_description TEXT,
//	    event_name TEXT NOT NULL,
//	    bus_id TEXT NOT NULL,
//	    instance_id TEXT,
//	    delivery_mode TEXT NOT NULL,
//	    metadata JSONB,
//	    status TEXT NOT NULL,
//	    error TEXT,
//	    retry_count INT DEFAULT 0,
//	    started_at TIMESTAMPTZ NOT NULL,
//	    completed_at TIMESTAMPTZ,
//	    duration_ms BIGINT,
//	    trace_id TEXT,
//	    span_id TEXT,
//	    PRIMARY KEY (event_id, subscription_id)
//	);
//	CREATE INDEX idx_monitor_event_name ON monitor_entries(event_name);
//	CREATE INDEX idx_monitor_status ON monitor_entries(status);
//	CREATE INDEX idx_monitor_started_at ON monitor_entries(started_at);
//	CREATE INDEX idx_monitor_delivery_mode ON monitor_entries(delivery_mode);
//	CREATE INDEX idx_monitor_subscriber_name ON monitor_entries(subscriber_name);
//	CREATE INDEX idx_monitor_instance_id ON monitor_entries(instance_id);
//
// Example:
//
//	db, _ := sql.Open("postgres", connString)
//	store := monitor.NewPostgresStore(db)
//	defer store.Close()
type PostgresStore struct {
	db          *sql.DB
	opts        *storeOptions
	stopCleanup chan struct{}
}

// NewPostgresStore creates a new PostgreSQL-based monitor store.
//
// The store requires a table with the schema described in the type documentation.
// Use CreateTable() to create it automatically in development.
//
// Parameters:
//   - db: An open PostgreSQL database connection
//   - opts: Optional configuration options
//
// The store starts a background goroutine for cleanup. Call Close() to stop it.
//
// Example:
//
//	db, _ := sql.Open("postgres", "postgres://localhost/mydb")
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithTableName("orders_monitor"),
//	    monitor.WithCleanupInterval(5 * time.Minute),
//	)
//	defer store.Close()
func NewPostgresStore(db *sql.DB, opts ...StoreOption) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("postgres: db is required")
	}

	o := defaultStoreOptions()
	for _, opt := range opts {
		opt(o)
	}

	s := &PostgresStore{
		db:          db,
		opts:        o,
		stopCleanup: make(chan struct{}),
	}

	// Start background cleanup if enabled
	if o.cleanupInterval > 0 {
		go base.SimpleCleanupLoop(o.cleanupInterval, s.stopCleanup, func() {
			// Default retention: 30 days
			_, _ = s.DeleteOlderThan(context.Background(), 30*24*time.Hour)
		})
	}

	return s, nil
}

// Record creates or updates a monitor entry.
func (s *PostgresStore) Record(ctx context.Context, entry *Entry) error {
	// Apply sampling
	if s.opts.samplingRate < 1.0 {
		// Simple deterministic sampling based on event ID hash
		// This ensures the same event is always sampled/not sampled
		hash := 0
		for _, c := range entry.EventID {
			hash = (hash*31 + int(c)) % 1000
		}
		if float64(hash)/1000.0 >= s.opts.samplingRate {
			return nil // Skip this entry
		}
	}

	// Serialize metadata
	metadataJSON, err := base.MarshalMetadata(entry.Metadata)
	if err != nil {
		return err
	}

	// Use empty string for subscription_id in WorkerPool mode
	subscriptionID := entry.SubscriptionID
	if entry.DeliveryMode == WorkerPool {
		subscriptionID = ""
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			event_id, subscription_id, subscriber_name, subscriber_description,
			event_name, bus_id, instance_id, delivery_mode,
			metadata, status, error, retry_count, started_at, completed_at,
			duration_ms, trace_id, span_id, worker_group
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT (event_id, subscription_id) DO UPDATE SET
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			retry_count = EXCLUDED.retry_count,
			completed_at = EXCLUDED.completed_at,
			duration_ms = EXCLUDED.duration_ms
	`, s.opts.tableName)

	var durationMs *int64
	if entry.Duration > 0 {
		ms := entry.Duration.Milliseconds()
		durationMs = &ms
	}

	_, err = s.db.ExecContext(ctx, query,
		entry.EventID,
		subscriptionID,
		base.StringPtr(entry.SubscriberName),
		base.StringPtr(entry.SubscriberDescription),
		entry.EventName,
		entry.BusID,
		base.StringPtr(entry.InstanceID),
		entry.DeliveryMode.String(),
		metadataJSON,
		string(entry.Status),
		base.StringPtr(entry.Error),
		entry.RetryCount,
		entry.StartedAt,
		entry.CompletedAt,
		durationMs,
		base.StringPtr(entry.TraceID),
		base.StringPtr(entry.SpanID),
		base.StringPtr(entry.WorkerGroup),
	)
	if err != nil {
		return fmt.Errorf("record monitor: %w", err)
	}

	return nil
}

// Get retrieves a monitor entry by its composite key.
func (s *PostgresStore) Get(ctx context.Context, eventID, subscriptionID string) (*Entry, error) {
	query := fmt.Sprintf(`
		SELECT event_id, subscription_id, subscriber_name, subscriber_description,
		       event_name, bus_id, instance_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id, worker_group
		FROM %s
		WHERE event_id = $1 AND subscription_id = $2
	`, s.opts.tableName)

	entry, err := s.scanEntry(s.db.QueryRowContext(ctx, query, eventID, subscriptionID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor: %w", err)
	}

	return entry, nil
}

// GetByEventID returns all entries for an event ID.
func (s *PostgresStore) GetByEventID(ctx context.Context, eventID string) ([]*Entry, error) {
	query := fmt.Sprintf(`
		SELECT event_id, subscription_id, subscriber_name, subscriber_description,
		       event_name, bus_id, instance_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id, worker_group
		FROM %s
		WHERE event_id = $1
		ORDER BY started_at ASC
	`, s.opts.tableName)

	rows, err := s.db.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("get by event id: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var entries []*Entry
	for rows.Next() {
		entry, err := s.scanEntryRows(rows)
		if err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// pgCursor represents the pagination cursor state for PostgreSQL.
type pgCursor struct {
	StartedAt time.Time `json:"s"`
	EventID   string    `json:"e"`
	SubID     string    `json:"u"`
}

// buildFilterQuery builds the WHERE clause and args for filter queries.
func (s *PostgresStore) buildFilterQuery(filter Filter) (*base.QueryBuilder, error) {
	qb := base.NewQueryBuilder()

	qb.AddIfNotEmpty("event_id = $%d", filter.EventID)
	qb.AddIfNotEmpty("subscription_id = $%d", filter.SubscriptionID)
	qb.AddIfNotEmpty("subscriber_name = $%d", filter.SubscriberName)
	qb.AddIfNotEmpty("worker_group = $%d", filter.WorkerGroup)
	qb.AddIfNotEmpty("event_name = $%d", filter.EventName)
	qb.AddIfNotEmpty("bus_id = $%d", filter.BusID)
	qb.AddIfNotEmpty("instance_id = $%d", filter.InstanceID)

	if filter.DeliveryMode != nil {
		qb.Add("delivery_mode = $%d", filter.DeliveryMode.String())
	}

	if len(filter.Status) > 0 {
		statusStrs := make([]string, len(filter.Status))
		for i, st := range filter.Status {
			statusStrs[i] = string(st)
		}
		qb.AddIn("status", statusStrs)
	}

	if filter.HasError != nil {
		if *filter.HasError {
			qb.AddRaw("error IS NOT NULL AND error != ''")
		} else {
			qb.AddRaw("(error IS NULL OR error = '')")
		}
	}

	qb.AddIfNotZero("started_at >= $%d", filter.StartTime)
	qb.AddIfNotZero("started_at < $%d", filter.EndTime)
	qb.AddIfPositiveDuration("duration_ms >= $%d", filter.MinDuration)
	qb.AddIfPositive("retry_count >= $%d", filter.MinRetries)

	return qb, nil
}

// List returns a page of entries matching the filter.
func (s *PostgresStore) List(ctx context.Context, filter Filter) (*Page, error) {
	qb, err := s.buildFilterQuery(filter)
	if err != nil {
		return nil, err
	}

	// Apply cursor for pagination
	if filter.Cursor != "" {
		cur, err := base.DecodeCursor[pgCursor](filter.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		argNum := qb.ArgNum()
		if filter.OrderDesc {
			qb.AddRaw(fmt.Sprintf(
				"(started_at < $%d OR (started_at = $%d AND (event_id < $%d OR (event_id = $%d AND subscription_id < $%d))))",
				argNum, argNum, argNum+1, argNum+1, argNum+2))
		} else {
			qb.AddRaw(fmt.Sprintf(
				"(started_at > $%d OR (started_at = $%d AND (event_id > $%d OR (event_id = $%d AND subscription_id > $%d))))",
				argNum, argNum, argNum+1, argNum+1, argNum+2))
		}
		// Manually add cursor args since they use multiple placeholders
		args := qb.Args()
		args = append(args, cur.StartedAt, cur.EventID, cur.SubID)
		// We need to rebuild with new args - use a workaround
		qb = base.NewQueryBuilderFrom(argNum + 3)
		for i := 0; i < len(args); i++ {
			// This is a bit hacky, let's just build the query directly
		}
	}

	// Build query directly for complex cursor handling
	whereClause := qb.WhereClause()
	args := qb.Args()

	// Handle cursor args separately if cursor was provided
	if filter.Cursor != "" {
		cur, _ := base.DecodeCursor[pgCursor](filter.Cursor)
		args = append(args, cur.StartedAt, cur.EventID, cur.SubID)
	}

	// Build ORDER BY
	orderBy := "ORDER BY started_at ASC, event_id ASC, subscription_id ASC"
	if filter.OrderDesc {
		orderBy = "ORDER BY started_at DESC, event_id DESC, subscription_id DESC"
	}

	// Query one extra row to check for more pages
	limit := filter.EffectiveLimit() + 1

	query := fmt.Sprintf(`
		SELECT event_id, subscription_id, subscriber_name, subscriber_description,
		       event_name, bus_id, instance_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id, worker_group
		FROM %s
		%s
		%s
		LIMIT %d
	`, s.opts.tableName, whereClause, orderBy, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list monitor: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var entries []*Entry
	for rows.Next() {
		entry, err := s.scanEntryRows(rows)
		if err != nil {
			return nil, fmt.Errorf("scan entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	// Use base.Paginate for consistent pagination handling
	pageResult := base.Paginate(entries, filter.EffectiveLimit(), func(e *Entry) pgCursor {
		return pgCursor{
			StartedAt: e.StartedAt,
			EventID:   e.EventID,
			SubID:     e.SubscriptionID,
		}
	})

	return &Page{
		Entries:    pageResult.Items,
		NextCursor: pageResult.NextCursor,
		HasMore:    pageResult.HasMore,
	}, nil
}

// Count returns the number of entries matching the filter.
func (s *PostgresStore) Count(ctx context.Context, filter Filter) (int64, error) {
	qb, err := s.buildFilterQuery(filter)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, s.opts.tableName, qb.WhereClause())

	var count int64
	err = s.db.QueryRowContext(ctx, query, qb.Args()...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count monitor: %w", err)
	}

	return count, nil
}

// UpdateStatus updates the status and related fields of an existing entry.
func (s *PostgresStore) UpdateStatus(ctx context.Context, eventID, subscriptionID string, status Status, err error, duration time.Duration) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $1, error = $2, duration_ms = $3, completed_at = NOW()
		WHERE event_id = $4 AND subscription_id = $5
	`, s.opts.tableName)

	var errStr *string
	if err != nil {
		s := err.Error()
		errStr = &s
	}

	_, execErr := s.db.ExecContext(ctx, query,
		string(status),
		errStr,
		duration.Milliseconds(),
		eventID,
		subscriptionID,
	)
	if execErr != nil {
		return fmt.Errorf("update status: %w", execErr)
	}

	return nil
}

// DeleteOlderThan removes entries older than the specified age.
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := fmt.Sprintf(`DELETE FROM %s WHERE started_at < NOW() - $1::interval`, s.opts.tableName)

	result, err := s.db.ExecContext(ctx, query, age.String())
	if err != nil {
		return 0, fmt.Errorf("delete old entries: %w", err)
	}

	return result.RowsAffected()
}

// Close stops the background cleanup goroutine.
func (s *PostgresStore) Close() error {
	close(s.stopCleanup)
	return nil
}

// CreateTable creates the monitor table if it doesn't exist.
//
// This is a convenience method for development and testing. In production,
// you should manage schema migrations separately.
func (s *PostgresStore) CreateTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			event_id TEXT NOT NULL,
			subscription_id TEXT NOT NULL DEFAULT '',
			subscriber_name TEXT,
			subscriber_description TEXT,
			event_name TEXT NOT NULL,
			bus_id TEXT NOT NULL,
			instance_id TEXT,
			delivery_mode TEXT NOT NULL,
			metadata JSONB,
			status TEXT NOT NULL,
			error TEXT,
			retry_count INT DEFAULT 0,
			started_at TIMESTAMPTZ NOT NULL,
			completed_at TIMESTAMPTZ,
			duration_ms BIGINT,
			trace_id TEXT,
			span_id TEXT,
			worker_group TEXT,
			PRIMARY KEY (event_id, subscription_id)
		);
		CREATE INDEX IF NOT EXISTS idx_%s_event_name ON %s(event_name);
		CREATE INDEX IF NOT EXISTS idx_%s_status ON %s(status);
		CREATE INDEX IF NOT EXISTS idx_%s_started_at ON %s(started_at);
		CREATE INDEX IF NOT EXISTS idx_%s_delivery_mode ON %s(delivery_mode);
		CREATE INDEX IF NOT EXISTS idx_%s_subscriber_name ON %s(subscriber_name);
		CREATE INDEX IF NOT EXISTS idx_%s_instance_id ON %s(instance_id);
	`, s.opts.tableName,
		s.opts.tableName, s.opts.tableName,
		s.opts.tableName, s.opts.tableName,
		s.opts.tableName, s.opts.tableName,
		s.opts.tableName, s.opts.tableName,
		s.opts.tableName, s.opts.tableName,
		s.opts.tableName, s.opts.tableName)

	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// scanEntry scans a single row into an Entry.
func (s *PostgresStore) scanEntry(row *sql.Row) (*Entry, error) {
	var entry Entry
	var metadataJSON []byte
	var deliveryMode string
	var status string
	var errStr sql.NullString
	var completedAt sql.NullTime
	var durationMs sql.NullInt64
	var traceID, spanID sql.NullString
	var subscriberName, subscriberDescription sql.NullString
	var instanceID, workerGroup sql.NullString

	err := row.Scan(
		&entry.EventID,
		&entry.SubscriptionID,
		&subscriberName,
		&subscriberDescription,
		&entry.EventName,
		&entry.BusID,
		&instanceID,
		&deliveryMode,
		&metadataJSON,
		&status,
		&errStr,
		&entry.RetryCount,
		&entry.StartedAt,
		&completedAt,
		&durationMs,
		&traceID,
		&spanID,
		&workerGroup,
	)
	if err != nil {
		return nil, err
	}

	entry.DeliveryMode = ParseDeliveryMode(deliveryMode)
	entry.Status = Status(status)
	entry.Error = base.NullString(errStr)
	entry.CompletedAt = base.NullTime(completedAt)
	entry.Duration = base.NullDurationMs(durationMs)
	entry.TraceID = base.NullString(traceID)
	entry.SpanID = base.NullString(spanID)
	entry.SubscriberName = base.NullString(subscriberName)
	entry.SubscriberDescription = base.NullString(subscriberDescription)
	entry.InstanceID = base.NullString(instanceID)
	entry.WorkerGroup = base.NullString(workerGroup)

	if len(metadataJSON) > 0 {
		metadata, err := base.UnmarshalMetadata(metadataJSON)
		if err != nil {
			return nil, err
		}
		entry.Metadata = metadata
	}

	return &entry, nil
}

// scanEntryRows scans a rows result into an Entry.
func (s *PostgresStore) scanEntryRows(rows *sql.Rows) (*Entry, error) {
	var entry Entry
	var metadataJSON []byte
	var deliveryMode string
	var status string
	var errStr sql.NullString
	var completedAt sql.NullTime
	var durationMs sql.NullInt64
	var traceID, spanID sql.NullString
	var subscriberName, subscriberDescription sql.NullString
	var instanceID, workerGroup sql.NullString

	err := rows.Scan(
		&entry.EventID,
		&entry.SubscriptionID,
		&subscriberName,
		&subscriberDescription,
		&entry.EventName,
		&entry.BusID,
		&instanceID,
		&deliveryMode,
		&metadataJSON,
		&status,
		&errStr,
		&entry.RetryCount,
		&entry.StartedAt,
		&completedAt,
		&durationMs,
		&traceID,
		&spanID,
		&workerGroup,
	)
	if err != nil {
		return nil, err
	}

	entry.DeliveryMode = ParseDeliveryMode(deliveryMode)
	entry.Status = Status(status)
	entry.Error = base.NullString(errStr)
	entry.CompletedAt = base.NullTime(completedAt)
	entry.Duration = base.NullDurationMs(durationMs)
	entry.TraceID = base.NullString(traceID)
	entry.SpanID = base.NullString(spanID)
	entry.SubscriberName = base.NullString(subscriberName)
	entry.SubscriberDescription = base.NullString(subscriberDescription)
	entry.InstanceID = base.NullString(instanceID)
	entry.WorkerGroup = base.NullString(workerGroup)

	if len(metadataJSON) > 0 {
		metadata, err := base.UnmarshalMetadata(metadataJSON)
		if err != nil {
			return nil, err
		}
		entry.Metadata = metadata
	}

	return &entry, nil
}

// RecordStart records when event processing begins.
// Implements event.MonitorStore interface.
func (s *PostgresStore) RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string,
	workerPool bool, metadata map[string]string, traceID, spanID string,
	subscriberName, subscriberDescription, workerGroup string) error {

	mode := Broadcast
	if workerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:               eventID,
		SubscriptionID:        subscriptionID,
		SubscriberName:        subscriberName,
		SubscriberDescription: subscriberDescription,
		EventName:             eventName,
		BusID:                 busID,
		DeliveryMode:          mode,
		Metadata:              metadata,
		Status:                StatusPending,
		StartedAt:             time.Now(),
		TraceID:               traceID,
		SpanID:                spanID,
		WorkerGroup:           workerGroup,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *PostgresStore) RecordComplete(ctx context.Context, eventID, subscriptionID, status string,
	handlerErr error, duration time.Duration) error {

	return s.UpdateStatus(ctx, eventID, subscriptionID, Status(status), handlerErr, duration)
}

// Summary returns aggregated statistics using SQL GROUP BY queries.
func (s *PostgresStore) Summary(ctx context.Context, filter Filter) (*Summary, error) {
	qb, err := s.buildFilterQuery(filter)
	if err != nil {
		return nil, err
	}

	// Default to last 24h if no time range specified
	if filter.StartTime.IsZero() && filter.EndTime.IsZero() {
		qb.Add("started_at >= $%d", time.Now().Add(-24*time.Hour))
	}

	where := qb.WhereClause()
	args := qb.Args()

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) AS total,
			COALESCE(AVG(duration_ms), 0) AS avg_duration_ms,
			COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
			COUNT(*) FILTER (WHERE status = 'retrying') AS retrying_count,
			COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
			MIN(started_at) AS oldest,
			MAX(started_at) AS newest
		FROM %s
		%s
	`, s.opts.tableName, where)

	var total, failedCount, completedCount, retryingCount, pendingCount int64
	var avgDurMs float64
	var oldest, newest sql.NullTime

	err = s.db.QueryRowContext(ctx, query, args...).Scan(
		&total, &avgDurMs, &failedCount, &completedCount, &retryingCount, &pendingCount,
		&oldest, &newest,
	)
	if err != nil {
		return nil, fmt.Errorf("summary global: %w", err)
	}

	summary := &Summary{
		TotalEntries: total,
		AvgDurationMs: int64(avgDurMs),
		ByStatus:     make(map[Status]int64),
		ByEventName:  make(map[string]*EventStats),
	}

	if total > 0 {
		summary.ErrorRate = float64(failedCount) / float64(total)
	}
	if completedCount > 0 {
		summary.ByStatus[StatusCompleted] = completedCount
	}
	if failedCount > 0 {
		summary.ByStatus[StatusFailed] = failedCount
	}
	if retryingCount > 0 {
		summary.ByStatus[StatusRetrying] = retryingCount
	}
	if pendingCount > 0 {
		summary.ByStatus[StatusPending] = pendingCount
	}
	if oldest.Valid {
		t := oldest.Time
		summary.TimeRange.Oldest = &t
	}
	if newest.Valid {
		t := newest.Time
		summary.TimeRange.Newest = &t
	}

	// Per-event stats
	eventQuery := fmt.Sprintf(`
		SELECT
			event_name,
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE status = 'completed') AS completed,
			COUNT(*) FILTER (WHERE status = 'failed') AS failed,
			COUNT(*) FILTER (WHERE status = 'retrying') AS retrying,
			COUNT(*) FILTER (WHERE status = 'pending') AS pending,
			COALESCE(AVG(duration_ms), 0) AS avg_duration_ms
		FROM %s
		%s
		GROUP BY event_name
	`, s.opts.tableName, where)

	rows, err := s.db.QueryContext(ctx, eventQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("summary by event: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name string
		var es EventStats
		var avgDur float64
		if err := rows.Scan(&name, &es.Total, &es.Completed, &es.Failed, &es.Retrying, &es.Pending, &avgDur); err != nil {
			return nil, fmt.Errorf("scan event stats: %w", err)
		}
		es.AvgDurationMs = int64(avgDur)
		if es.Total > 0 {
			es.ErrorRate = float64(es.Failed) / float64(es.Total)
		}
		summary.ByEventName[name] = &es
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("event rows: %w", err)
	}

	// Per-instance counts — use WHERE TRUE as baseline when where is empty
	// to safely append AND conditions
	instanceWhere := where
	if instanceWhere == "" {
		instanceWhere = "WHERE TRUE"
	}
	instanceQuery := fmt.Sprintf(`
		SELECT instance_id, COUNT(*) AS count
		FROM %s
		%s AND instance_id IS NOT NULL AND instance_id != ''
		GROUP BY instance_id
	`, s.opts.tableName, instanceWhere)

	instRows, err := s.db.QueryContext(ctx, instanceQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("summary by instance: %w", err)
	}
	defer func() { _ = instRows.Close() }()

	byInstance := make(map[string]int64)
	for instRows.Next() {
		var id string
		var count int64
		if err := instRows.Scan(&id, &count); err != nil {
			return nil, fmt.Errorf("scan instance: %w", err)
		}
		byInstance[id] = count
	}
	if err := instRows.Err(); err != nil {
		return nil, fmt.Errorf("instance rows: %w", err)
	}
	if len(byInstance) > 0 {
		summary.ByInstance = byInstance
	}

	return summary, nil
}

// Compile-time check that PostgresStore implements Store.
var _ Store = (*PostgresStore)(nil)
var _ SummaryProvider = (*PostgresStore)(nil)

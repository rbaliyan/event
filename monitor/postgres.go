package monitor

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
//	    event_name TEXT NOT NULL,
//	    bus_id TEXT NOT NULL,
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
func NewPostgresStore(db *sql.DB, opts ...StoreOption) *PostgresStore {
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
		go s.cleanupLoop()
	}

	return s
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
	var metadataJSON []byte
	if entry.Metadata != nil {
		var err error
		metadataJSON, err = json.Marshal(entry.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}

	// Use empty string for subscription_id in WorkerPool mode
	subscriptionID := entry.SubscriptionID
	if entry.DeliveryMode == WorkerPool {
		subscriptionID = ""
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			event_id, subscription_id, event_name, bus_id, delivery_mode,
			metadata, status, error, retry_count, started_at, completed_at,
			duration_ms, trace_id, span_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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

	_, err := s.db.ExecContext(ctx, query,
		entry.EventID,
		subscriptionID,
		entry.EventName,
		entry.BusID,
		entry.DeliveryMode.String(),
		metadataJSON,
		string(entry.Status),
		nullString(entry.Error),
		entry.RetryCount,
		entry.StartedAt,
		entry.CompletedAt,
		durationMs,
		nullString(entry.TraceID),
		nullString(entry.SpanID),
	)
	if err != nil {
		return fmt.Errorf("record monitor: %w", err)
	}

	return nil
}

// Get retrieves a monitor entry by its composite key.
func (s *PostgresStore) Get(ctx context.Context, eventID, subscriptionID string) (*Entry, error) {
	query := fmt.Sprintf(`
		SELECT event_id, subscription_id, event_name, bus_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id
		FROM %s
		WHERE event_id = $1 AND subscription_id = $2
	`, s.opts.tableName)

	entry, err := s.scanEntry(s.db.QueryRowContext(ctx, query, eventID, subscriptionID))
	if err == sql.ErrNoRows {
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
		SELECT event_id, subscription_id, event_name, bus_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id
		FROM %s
		WHERE event_id = $1
		ORDER BY started_at ASC
	`, s.opts.tableName)

	rows, err := s.db.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("get by event id: %w", err)
	}
	defer rows.Close()

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

// List returns a page of entries matching the filter.
func (s *PostgresStore) List(ctx context.Context, filter Filter) (*Page, error) {
	// Build query with filters
	var conditions []string
	var args []any
	argNum := 1

	if filter.EventID != "" {
		conditions = append(conditions, fmt.Sprintf("event_id = $%d", argNum))
		args = append(args, filter.EventID)
		argNum++
	}
	if filter.SubscriptionID != "" {
		conditions = append(conditions, fmt.Sprintf("subscription_id = $%d", argNum))
		args = append(args, filter.SubscriptionID)
		argNum++
	}
	if filter.EventName != "" {
		conditions = append(conditions, fmt.Sprintf("event_name = $%d", argNum))
		args = append(args, filter.EventName)
		argNum++
	}
	if filter.BusID != "" {
		conditions = append(conditions, fmt.Sprintf("bus_id = $%d", argNum))
		args = append(args, filter.BusID)
		argNum++
	}
	if filter.DeliveryMode != nil {
		conditions = append(conditions, fmt.Sprintf("delivery_mode = $%d", argNum))
		args = append(args, filter.DeliveryMode.String())
		argNum++
	}
	if len(filter.Status) > 0 {
		placeholders := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, string(status))
			argNum++
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ", ")))
	}
	if filter.HasError != nil {
		if *filter.HasError {
			conditions = append(conditions, "error IS NOT NULL AND error != ''")
		} else {
			conditions = append(conditions, "(error IS NULL OR error = '')")
		}
	}
	if !filter.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("started_at >= $%d", argNum))
		args = append(args, filter.StartTime)
		argNum++
	}
	if !filter.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("started_at < $%d", argNum))
		args = append(args, filter.EndTime)
		argNum++
	}
	if filter.MinDuration > 0 {
		conditions = append(conditions, fmt.Sprintf("duration_ms >= $%d", argNum))
		args = append(args, filter.MinDuration.Milliseconds())
		argNum++
	}
	if filter.MinRetries > 0 {
		conditions = append(conditions, fmt.Sprintf("retry_count >= $%d", argNum))
		args = append(args, filter.MinRetries)
		argNum++
	}

	// Apply cursor for pagination
	if filter.Cursor != "" {
		cur, err := s.decodePgCursor(filter.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		if filter.OrderDesc {
			conditions = append(conditions, fmt.Sprintf(
				"(started_at < $%d OR (started_at = $%d AND (event_id < $%d OR (event_id = $%d AND subscription_id < $%d))))",
				argNum, argNum, argNum+1, argNum+1, argNum+2))
		} else {
			conditions = append(conditions, fmt.Sprintf(
				"(started_at > $%d OR (started_at = $%d AND (event_id > $%d OR (event_id = $%d AND subscription_id > $%d))))",
				argNum, argNum, argNum+1, argNum+1, argNum+2))
		}
		args = append(args, cur.StartedAt, cur.EventID, cur.SubID)
		argNum += 3
	}

	// Build WHERE clause
	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	// Build ORDER BY
	orderBy := "ORDER BY started_at ASC, event_id ASC, subscription_id ASC"
	if filter.OrderDesc {
		orderBy = "ORDER BY started_at DESC, event_id DESC, subscription_id DESC"
	}

	// Query one extra row to check for more pages
	limit := filter.EffectiveLimit() + 1

	query := fmt.Sprintf(`
		SELECT event_id, subscription_id, event_name, bus_id, delivery_mode,
		       metadata, status, error, retry_count, started_at, completed_at,
		       duration_ms, trace_id, span_id
		FROM %s
		%s
		%s
		LIMIT %d
	`, s.opts.tableName, whereClause, orderBy, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list monitor: %w", err)
	}
	defer rows.Close()

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

	// Check if there are more pages
	hasMore := len(entries) > filter.EffectiveLimit()
	if hasMore {
		entries = entries[:filter.EffectiveLimit()]
	}

	// Create next cursor
	var nextCursor string
	if hasMore && len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		nextCursor = s.encodePgCursor(pgCursor{
			StartedAt: lastEntry.StartedAt,
			EventID:   lastEntry.EventID,
			SubID:     lastEntry.SubscriptionID,
		})
	}

	return &Page{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Count returns the number of entries matching the filter.
func (s *PostgresStore) Count(ctx context.Context, filter Filter) (int64, error) {
	// Build query with filters (same as List but simpler - no cursor/order/limit)
	var conditions []string
	var args []any
	argNum := 1

	if filter.EventID != "" {
		conditions = append(conditions, fmt.Sprintf("event_id = $%d", argNum))
		args = append(args, filter.EventID)
		argNum++
	}
	if filter.SubscriptionID != "" {
		conditions = append(conditions, fmt.Sprintf("subscription_id = $%d", argNum))
		args = append(args, filter.SubscriptionID)
		argNum++
	}
	if filter.EventName != "" {
		conditions = append(conditions, fmt.Sprintf("event_name = $%d", argNum))
		args = append(args, filter.EventName)
		argNum++
	}
	if filter.BusID != "" {
		conditions = append(conditions, fmt.Sprintf("bus_id = $%d", argNum))
		args = append(args, filter.BusID)
		argNum++
	}
	if filter.DeliveryMode != nil {
		conditions = append(conditions, fmt.Sprintf("delivery_mode = $%d", argNum))
		args = append(args, filter.DeliveryMode.String())
		argNum++
	}
	if len(filter.Status) > 0 {
		placeholders := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			placeholders[i] = fmt.Sprintf("$%d", argNum)
			args = append(args, string(status))
			argNum++
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeholders, ", ")))
	}
	if filter.HasError != nil {
		if *filter.HasError {
			conditions = append(conditions, "error IS NOT NULL AND error != ''")
		} else {
			conditions = append(conditions, "(error IS NULL OR error = '')")
		}
	}
	if !filter.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("started_at >= $%d", argNum))
		args = append(args, filter.StartTime)
		argNum++
	}
	if !filter.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("started_at < $%d", argNum))
		args = append(args, filter.EndTime)
		argNum++
	}
	if filter.MinDuration > 0 {
		conditions = append(conditions, fmt.Sprintf("duration_ms >= $%d", argNum))
		args = append(args, filter.MinDuration.Milliseconds())
		argNum++
	}
	if filter.MinRetries > 0 {
		conditions = append(conditions, fmt.Sprintf("retry_count >= $%d", argNum))
		args = append(args, filter.MinRetries)
		argNum++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s %s`, s.opts.tableName, whereClause)

	var count int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
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
			event_name TEXT NOT NULL,
			bus_id TEXT NOT NULL,
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
			PRIMARY KEY (event_id, subscription_id)
		);
		CREATE INDEX IF NOT EXISTS idx_%s_event_name ON %s(event_name);
		CREATE INDEX IF NOT EXISTS idx_%s_status ON %s(status);
		CREATE INDEX IF NOT EXISTS idx_%s_started_at ON %s(started_at);
		CREATE INDEX IF NOT EXISTS idx_%s_delivery_mode ON %s(delivery_mode);
	`, s.opts.tableName,
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

// cleanupLoop runs the periodic cleanup of old entries.
func (s *PostgresStore) cleanupLoop() {
	ticker := time.NewTicker(s.opts.cleanupInterval)
	defer ticker.Stop()

	// Default retention: 30 days
	retention := 30 * 24 * time.Hour

	for {
		select {
		case <-ticker.C:
			_, _ = s.DeleteOlderThan(context.Background(), retention)
		case <-s.stopCleanup:
			return
		}
	}
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

	err := row.Scan(
		&entry.EventID,
		&entry.SubscriptionID,
		&entry.EventName,
		&entry.BusID,
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
	)
	if err != nil {
		return nil, err
	}

	entry.DeliveryMode = ParseDeliveryMode(deliveryMode)
	entry.Status = Status(status)
	if errStr.Valid {
		entry.Error = errStr.String
	}
	if completedAt.Valid {
		entry.CompletedAt = &completedAt.Time
	}
	if durationMs.Valid {
		entry.Duration = time.Duration(durationMs.Int64) * time.Millisecond
	}
	if traceID.Valid {
		entry.TraceID = traceID.String
	}
	if spanID.Valid {
		entry.SpanID = spanID.String
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &entry.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
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

	err := rows.Scan(
		&entry.EventID,
		&entry.SubscriptionID,
		&entry.EventName,
		&entry.BusID,
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
	)
	if err != nil {
		return nil, err
	}

	entry.DeliveryMode = ParseDeliveryMode(deliveryMode)
	entry.Status = Status(status)
	if errStr.Valid {
		entry.Error = errStr.String
	}
	if completedAt.Valid {
		entry.CompletedAt = &completedAt.Time
	}
	if durationMs.Valid {
		entry.Duration = time.Duration(durationMs.Int64) * time.Millisecond
	}
	if traceID.Valid {
		entry.TraceID = traceID.String
	}
	if spanID.Valid {
		entry.SpanID = spanID.String
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &entry.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &entry, nil
}

// encodePgCursor encodes a cursor to a string.
func (s *PostgresStore) encodePgCursor(c pgCursor) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

// decodePgCursor decodes a cursor from a string.
func (s *PostgresStore) decodePgCursor(str string) (pgCursor, error) {
	var c pgCursor
	if str == "" {
		return c, nil
	}
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// nullString returns a pointer to s if non-empty, nil otherwise.
func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// RecordStart records when event processing begins.
// Implements event.MonitorStore interface.
func (s *PostgresStore) RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string,
	workerPool bool, metadata map[string]string, traceID, spanID string) error {

	mode := Broadcast
	if workerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:        eventID,
		SubscriptionID: subscriptionID,
		EventName:      eventName,
		BusID:          busID,
		DeliveryMode:   mode,
		Metadata:       metadata,
		Status:         StatusPending,
		StartedAt:      time.Now(),
		TraceID:        traceID,
		SpanID:         spanID,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *PostgresStore) RecordComplete(ctx context.Context, eventID, subscriptionID, status string,
	handlerErr error, duration time.Duration) error {

	return s.UpdateStatus(ctx, eventID, subscriptionID, Status(status), handlerErr, duration)
}

// Compile-time check that PostgresStore implements Store.
var _ Store = (*PostgresStore)(nil)

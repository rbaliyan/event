package dlq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

/*
PostgreSQL Schema:

CREATE TABLE event_dlq (
    id          VARCHAR(36) PRIMARY KEY,
    event_name  VARCHAR(255) NOT NULL,
    original_id VARCHAR(36) NOT NULL,
    payload     BYTEA NOT NULL,
    metadata    JSONB,
    error       TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    source      VARCHAR(255),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    retried_at  TIMESTAMP
);

CREATE INDEX idx_dlq_event_name ON event_dlq(event_name);
CREATE INDEX idx_dlq_created_at ON event_dlq(created_at);
CREATE INDEX idx_dlq_retried_at ON event_dlq(retried_at) WHERE retried_at IS NULL;
*/

// PostgresStore is a PostgreSQL-based DLQ store
type PostgresStore struct {
	db    *sql.DB
	table string
}

// NewPostgresStore creates a new PostgreSQL DLQ store
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{
		db:    db,
		table: "event_dlq",
	}
}

// WithTable sets a custom table name
func (s *PostgresStore) WithTable(table string) *PostgresStore {
	s.table = table
	return s
}

// Store adds a message to the DLQ
func (s *PostgresStore) Store(ctx context.Context, msg *Message) error {
	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, original_id, payload, metadata, error, retry_count, source, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		msg.ID,
		msg.EventName,
		msg.OriginalID,
		msg.Payload,
		metadata,
		msg.Error,
		msg.RetryCount,
		msg.Source,
		msg.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves a single message by ID
func (s *PostgresStore) Get(ctx context.Context, id string) (*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var retriedAt sql.NullTime
	var source sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.OriginalID,
		&msg.Payload,
		&metadata,
		&msg.Error,
		&msg.RetryCount,
		&source,
		&msg.CreatedAt,
		&retriedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	if retriedAt.Valid {
		msg.RetriedAt = &retriedAt.Time
	}

	if source.Valid {
		msg.Source = source.String
	}

	return &msg, nil
}

// List returns messages matching the filter
func (s *PostgresStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	query, args := s.buildListQuery(filter, false)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadata []byte
		var retriedAt sql.NullTime
		var source sql.NullString

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.OriginalID,
			&msg.Payload,
			&metadata,
			&msg.Error,
			&msg.RetryCount,
			&source,
			&msg.CreatedAt,
			&retriedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}

		if retriedAt.Valid {
			msg.RetriedAt = &retriedAt.Time
		}

		if source.Valid {
			msg.Source = source.String
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// Count returns the number of messages matching the filter
func (s *PostgresStore) Count(ctx context.Context, filter Filter) (int64, error) {
	query, args := s.buildListQuery(filter, true)

	var count int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}

	return count, nil
}

// buildListQuery builds the SQL query for List and Count
func (s *PostgresStore) buildListQuery(filter Filter, countOnly bool) (string, []interface{}) {
	var conditions []string
	var args []interface{}
	argIndex := 1

	if filter.EventName != "" {
		conditions = append(conditions, fmt.Sprintf("event_name = $%d", argIndex))
		args = append(args, filter.EventName)
		argIndex++
	}

	if !filter.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", argIndex))
		args = append(args, filter.StartTime)
		argIndex++
	}

	if !filter.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at <= $%d", argIndex))
		args = append(args, filter.EndTime)
		argIndex++
	}

	if filter.Error != "" {
		conditions = append(conditions, fmt.Sprintf("error ILIKE $%d", argIndex))
		args = append(args, "%"+filter.Error+"%")
		argIndex++
	}

	if filter.MaxRetries > 0 {
		conditions = append(conditions, fmt.Sprintf("retry_count <= $%d", argIndex))
		args = append(args, filter.MaxRetries)
		argIndex++
	}

	if filter.Source != "" {
		conditions = append(conditions, fmt.Sprintf("source = $%d", argIndex))
		args = append(args, filter.Source)
		argIndex++
	}

	if filter.ExcludeRetried {
		conditions = append(conditions, "retried_at IS NULL")
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	if countOnly {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s %s", s.table, whereClause), args
	}

	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at
		FROM %s
		%s
		ORDER BY created_at DESC
	`, s.table, whereClause)

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
		argIndex++
	}

	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, filter.Offset)
	}

	return query, args
}

// MarkRetried marks a message as replayed
func (s *PostgresStore) MarkRetried(ctx context.Context, id string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET retried_at = $1
		WHERE id = $2
	`, s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// Delete removes a message from the DLQ
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table)

	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE created_at < $1", s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now().Add(-age))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// DeleteByFilter removes messages matching the filter
func (s *PostgresStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	listQuery, args := s.buildListQuery(filter, false)
	// Convert SELECT to DELETE
	deleteQuery := strings.Replace(listQuery, "SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at", "DELETE", 1)
	// Remove ORDER BY and LIMIT for delete
	if idx := strings.Index(deleteQuery, "ORDER BY"); idx > 0 {
		deleteQuery = deleteQuery[:idx]
	}

	result, err := s.db.ExecContext(ctx, deleteQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// Stats returns DLQ statistics
func (s *PostgresStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)).Scan(&stats.TotalMessages)
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	// Pending count
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE retried_at IS NULL", s.table)).Scan(&stats.PendingMessages)
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}

	stats.RetriedMessages = stats.TotalMessages - stats.PendingMessages

	// Messages by event
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT event_name, COUNT(*) FROM %s GROUP BY event_name", s.table))
	if err != nil {
		return nil, fmt.Errorf("count by event: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventName string
		var count int64
		if err := rows.Scan(&eventName, &count); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		stats.MessagesByEvent[eventName] = count
	}

	// Oldest and newest
	var oldest, newest sql.NullTime
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(created_at), MAX(created_at) FROM %s", s.table)).Scan(&oldest, &newest)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("min/max: %w", err)
	}

	if oldest.Valid {
		stats.OldestMessage = &oldest.Time
	}
	if newest.Valid {
		stats.NewestMessage = &newest.Time
	}

	return stats, nil
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)
var _ StatsProvider = (*PostgresStore)(nil)

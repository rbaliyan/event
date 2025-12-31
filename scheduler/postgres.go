package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

/*
PostgreSQL Schema:

CREATE TABLE scheduled_messages (
    id           VARCHAR(36) PRIMARY KEY,
    event_name   VARCHAR(255) NOT NULL,
    payload      BYTEA NOT NULL,
    metadata     JSONB,
    scheduled_at TIMESTAMP NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scheduled_due ON scheduled_messages(scheduled_at);
*/

// PostgresScheduler uses PostgreSQL for scheduling
type PostgresScheduler struct {
	db        *sql.DB
	transport transport.Transport
	opts      *Options
	table     string
	logger    *slog.Logger
	stopCh    chan struct{}
	stoppedCh chan struct{}
}

// NewPostgresScheduler creates a new PostgreSQL-based scheduler
func NewPostgresScheduler(db *sql.DB, t transport.Transport, opts ...Option) *PostgresScheduler {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &PostgresScheduler{
		db:        db,
		transport: t,
		opts:      o,
		table:     "scheduled_messages",
		logger:    slog.Default().With("component", "scheduler.postgres"),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// WithTable sets a custom table name
func (s *PostgresScheduler) WithTable(table string) *PostgresScheduler {
	s.table = table
	return s
}

// WithLogger sets a custom logger
func (s *PostgresScheduler) WithLogger(l *slog.Logger) *PostgresScheduler {
	s.logger = l
	return s
}

// Schedule adds a message for future delivery
func (s *PostgresScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, metadata, scheduled_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		msg.ID,
		msg.EventName,
		msg.Payload,
		metadata,
		msg.ScheduledAt,
		msg.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	s.logger.Debug("scheduled message",
		"id", msg.ID,
		"event", msg.EventName,
		"scheduled_at", msg.ScheduledAt)

	return nil
}

// ScheduleAt schedules a message for a specific time
func (s *PostgresScheduler) ScheduleAt(ctx context.Context, eventName string, payload []byte, metadata map[string]string, at time.Time) (string, error) {
	msg := Message{
		ID:          uuid.New().String(),
		EventName:   eventName,
		Payload:     payload,
		Metadata:    metadata,
		ScheduledAt: at,
		CreatedAt:   time.Now(),
	}

	if err := s.Schedule(ctx, msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

// ScheduleAfter schedules a message after a delay
func (s *PostgresScheduler) ScheduleAfter(ctx context.Context, eventName string, payload []byte, metadata map[string]string, delay time.Duration) (string, error) {
	return s.ScheduleAt(ctx, eventName, payload, metadata, time.Now().Add(delay))
}

// Cancel cancels a scheduled message
func (s *PostgresScheduler) Cancel(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table)

	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	s.logger.Debug("cancelled scheduled message", "id", id)
	return nil
}

// Get retrieves a scheduled message by ID
func (s *PostgresScheduler) Get(ctx context.Context, id string) (*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.Payload,
		&metadata,
		&msg.ScheduledAt,
		&msg.CreatedAt,
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

	return &msg, nil
}

// List returns scheduled messages
func (s *PostgresScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at
		FROM %s
		WHERE 1=1
	`, s.table)

	var args []interface{}
	argIndex := 1

	if filter.EventName != "" {
		query += fmt.Sprintf(" AND event_name = $%d", argIndex)
		args = append(args, filter.EventName)
		argIndex++
	}

	if !filter.After.IsZero() {
		query += fmt.Sprintf(" AND scheduled_at >= $%d", argIndex)
		args = append(args, filter.After)
		argIndex++
	}

	if !filter.Before.IsZero() {
		query += fmt.Sprintf(" AND scheduled_at <= $%d", argIndex)
		args = append(args, filter.Before)
		argIndex++
	}

	query += " ORDER BY scheduled_at ASC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadata []byte

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.Payload,
			&metadata,
			&msg.ScheduledAt,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// Start begins the scheduler polling loop
func (s *PostgresScheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()

	s.logger.Info("scheduler started",
		"poll_interval", s.opts.PollInterval,
		"batch_size", s.opts.BatchSize)

	for {
		select {
		case <-ctx.Done():
			close(s.stoppedCh)
			return ctx.Err()
		case <-s.stopCh:
			close(s.stoppedCh)
			return nil
		case <-ticker.C:
			s.processDue(ctx)
		}
	}
}

// Stop gracefully stops the scheduler
func (s *PostgresScheduler) Stop(ctx context.Context) error {
	close(s.stopCh)

	select {
	case <-s.stoppedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processDue processes messages that are due for delivery
func (s *PostgresScheduler) processDue(ctx context.Context) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Error("failed to begin transaction", "error", err)
		return
	}
	defer tx.Rollback()

	// Lock and fetch due messages
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at
		FROM %s
		WHERE scheduled_at <= $1
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, s.table)

	rows, err := tx.QueryContext(ctx, query, time.Now(), s.opts.BatchSize)
	if err != nil {
		s.logger.Error("failed to query due messages", "error", err)
		return
	}
	defer rows.Close()

	var toDelete []string

	for rows.Next() {
		var msg Message
		var metadata []byte

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.Payload,
			&metadata,
			&msg.ScheduledAt,
			&msg.CreatedAt,
		)
		if err != nil {
			s.logger.Error("failed to scan message", "error", err)
			continue
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				s.logger.Error("failed to unmarshal metadata", "error", err)
				continue
			}
		}

		// Publish to transport
		if err := s.publishMessage(ctx, &msg); err != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			continue
		}

		toDelete = append(toDelete, msg.ID)

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
	}

	// Delete delivered messages
	if len(toDelete) > 0 {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.table)
		_, err = tx.ExecContext(ctx, deleteQuery, toDelete)
		if err != nil {
			s.logger.Error("failed to delete delivered messages", "error", err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error("failed to commit transaction", "error", err)
	}
}

// publishMessage publishes a scheduled message to the transport
func (s *PostgresScheduler) publishMessage(ctx context.Context, msg *Message) error {
	// Add scheduler metadata
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["scheduled_message_id"] = msg.ID
	metadata["scheduled_at"] = msg.ScheduledAt.Format(time.RFC3339)

	// Create transport message
	transportMsg := message.New(
		msg.ID,
		"scheduler",
		msg.Payload,
		metadata,
		trace.SpanContext{},
	)

	return s.transport.Publish(ctx, msg.EventName, transportMsg)
}

// Compile-time check
var _ Scheduler = (*PostgresScheduler)(nil)

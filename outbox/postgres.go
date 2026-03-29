package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/store/base"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// PostgresStoreOption configures a PostgresStore.
type PostgresStoreOption func(*postgresStoreOptions)

type postgresStoreOptions struct {
	table string
}

// WithTable sets a custom table name for the PostgreSQL outbox store.
// The name must be a valid SQL identifier (alphanumeric and underscores only).
func WithTable(table string) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if table != "" && base.ValidIdentifier(table) {
			o.table = table
		}
	}
}

// PostgresStore implements Store for PostgreSQL.
//
// PostgresStore uses PostgreSQL's transactional capabilities for reliable
// message storage. It supports concurrent relay instances using
// SELECT FOR UPDATE SKIP LOCKED to prevent duplicate processing.
//
// Required Schema:
//
//	CREATE TABLE event_outbox (
//	    id           BIGSERIAL PRIMARY KEY,
//	    event_name   VARCHAR(255) NOT NULL,
//	    event_id     VARCHAR(36) NOT NULL,
//	    payload      BYTEA NOT NULL,
//	    metadata     JSONB,
//	    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
//	    published_at TIMESTAMP,
//	    status       VARCHAR(20) NOT NULL DEFAULT 'pending',
//	    retry_count  INT NOT NULL DEFAULT 0,
//	    last_error   TEXT,
//	    priority     INT NOT NULL DEFAULT 0
//	);
//	CREATE INDEX idx_outbox_pending ON event_outbox(status, priority DESC, created_at)
//	    WHERE status IN ('pending', 'failed');
type PostgresStore struct {
	db        *sql.DB
	tableName string
}

// NewPostgresStore creates a new PostgreSQL outbox store.
//
// The provided database connection should be configured and connected.
// The default table name is "event_outbox".
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("postgres: db is required")
	}

	o := &postgresStoreOptions{
		table: "event_outbox",
	}
	for _, opt := range opts {
		opt(o)
	}

	return &PostgresStore{
		db:        db,
		tableName: o.table,
	}, nil
}

// Insert adds a message to the outbox within a transaction.
//
// The message is stored atomically with other database operations in the
// same transaction. On success, msg.ID is populated with the generated ID.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - tx: The active database transaction
//   - msg: The message to store (ID will be set on success)
//
// Example:
//
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    // Business logic
//	    _, err := tx.Exec("UPDATE orders SET status = 'shipped' WHERE id = $1", orderID)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Store event
//	    msg := &outbox.Message{
//	        EventName: "order.shipped",
//	        EventID:   uuid.New().String(),
//	        Payload:   payload,
//	    }
//	    return store.Insert(ctx, tx, msg)
//	})
func (s *PostgresStore) Insert(ctx context.Context, tx *sql.Tx, msg *Message) error {
	var metadataJSON []byte
	var err error
	if msg.Metadata != nil {
		metadataJSON, err = json.Marshal(msg.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (event_name, event_id, payload, metadata, status, created_at, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`, s.tableName)

	err = tx.QueryRowContext(ctx, query,
		msg.EventName,
		msg.EventID,
		msg.Payload,
		metadataJSON,
		StatusPending,
		time.Now(),
		msg.Priority,
	).Scan(&msg.ID)

	return err
}

// GetPending retrieves pending and failed messages for publishing.
//
// Returns messages with StatusPending or StatusFailed, ordered by creation
// time (oldest first). Uses FOR UPDATE SKIP LOCKED to prevent concurrent
// relays from processing the same messages.
//
// IMPORTANT: This method runs outside an explicit transaction, so the row
// locks from FOR UPDATE SKIP LOCKED are released when the query completes.
// For proper transactional safety with concurrent relays, use ProcessPending
// instead.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - limit: Maximum number of messages to retrieve
//
// Returns the messages and any error. Returns empty slice if no pending messages.
func (s *PostgresStore) GetPending(ctx context.Context, limit int) ([]*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, event_id, payload, metadata, created_at, retry_count, COALESCE(priority, 0)
		FROM %s
		WHERE status IN ($1, $2)
		ORDER BY priority DESC, created_at
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, s.tableName)

	rows, err := s.db.QueryContext(ctx, query, StatusPending, StatusFailed, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	return s.scanMessages(rows)
}

// ProcessPending retrieves pending and failed messages within a transaction
// and calls fn for each message. The transaction holds row locks via
// FOR UPDATE SKIP LOCKED, preventing concurrent relays from processing
// the same messages.
//
// The callback fn receives each message and should publish it. If fn returns
// nil, the message is marked as published. If fn returns an error, the message
// is marked as failed with the error recorded. All updates happen within the
// same transaction.
//
// This is the recommended method for relay implementations. Use GetPending
// only when you need to manage the transaction lifecycle yourself.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - limit: Maximum number of messages to process
//   - fn: Callback for each message; nil return marks published, error marks failed
//
// Returns error if the transaction fails. Individual message failures are
// recorded but do not abort the transaction.
func (s *PostgresStore) ProcessPending(ctx context.Context, limit int, fn func(msg *Message) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	query := fmt.Sprintf(`
		SELECT id, event_name, event_id, payload, metadata, created_at, retry_count, COALESCE(priority, 0)
		FROM %s
		WHERE status IN ($1, $2)
		ORDER BY priority DESC, created_at
		LIMIT $3
		FOR UPDATE SKIP LOCKED
	`, s.tableName)

	rows, err := tx.QueryContext(ctx, query, StatusPending, StatusFailed, limit)
	if err != nil {
		return fmt.Errorf("query pending: %w", err)
	}

	messages, err := s.scanMessages(rows)
	if err != nil {
		return fmt.Errorf("scan messages: %w", err)
	}

	publishQuery := fmt.Sprintf(`
		UPDATE %s SET status = $1, published_at = $2 WHERE id = $3
	`, s.tableName)

	failQuery := fmt.Sprintf(`
		UPDATE %s SET status = $1, last_error = $2, retry_count = retry_count + 1 WHERE id = $3
	`, s.tableName)

	now := time.Now()
	for _, msg := range messages {
		if fnErr := fn(msg); fnErr != nil {
			var errMsg string
			if fnErr != nil {
				errMsg = fnErr.Error()
			}
			if _, execErr := tx.ExecContext(ctx, failQuery, StatusFailed, errMsg, msg.ID); execErr != nil {
				return fmt.Errorf("mark failed id=%d: %w", msg.ID, execErr)
			}
			continue
		}
		if _, execErr := tx.ExecContext(ctx, publishQuery, StatusPublished, now, msg.ID); execErr != nil {
			return fmt.Errorf("mark published id=%d: %w", msg.ID, execErr)
		}
	}

	return tx.Commit()
}

// scanMessages reads rows into Message structs and closes the rows.
func (s *PostgresStore) scanMessages(rows *sql.Rows) ([]*Message, error) {
	defer func() { _ = rows.Close() }()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadataJSON []byte

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.EventID,
			&msg.Payload,
			&metadataJSON,
			&msg.CreatedAt,
			&msg.RetryCount,
			&msg.Priority,
		)
		if err != nil {
			return nil, err
		}

		if metadataJSON != nil {
			json.Unmarshal(metadataJSON, &msg.Metadata)
		}

		msg.Status = StatusPending
		messages = append(messages, &msg)
	}

	return messages, rows.Err()
}

// MarkPublished marks a message as successfully published.
//
// Sets the status to StatusPublished and records the current time as
// published_at. Called by the relay after successfully publishing.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: The message ID to mark as published
func (s *PostgresStore) MarkPublished(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $1, published_at = $2
		WHERE id = $3
	`, s.tableName)

	_, err := s.db.ExecContext(ctx, query, StatusPublished, time.Now(), id)
	return err
}

// MarkFailed marks a message as failed with an error.
//
// Sets the status to StatusFailed, increments retry_count, and stores
// the error message. The relay may retry failed messages later.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: The message ID to mark as failed
//   - err: The error that caused the failure
func (s *PostgresStore) MarkFailed(ctx context.Context, id int64, err error) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET status = $1, last_error = $2, retry_count = retry_count + 1
		WHERE id = $3
	`, s.tableName)

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	_, dbErr := s.db.ExecContext(ctx, query, StatusFailed, errMsg, id)
	return dbErr
}

// Delete removes old published messages.
//
// Deletes messages with StatusPublished that were published more than
// 'olderThan' ago. This prevents unbounded growth of the outbox table.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - olderThan: Age threshold for deletion
//
// Returns the number of deleted messages and any error.
//
// Example:
//
//	// Delete messages published more than 7 days ago
//	deleted, err := store.Delete(ctx, 7*24*time.Hour)
//	if err != nil {
//	    log.Error("cleanup failed", "error", err)
//	}
//	log.Info("cleaned up old messages", "count", deleted)
func (s *PostgresStore) Delete(ctx context.Context, olderThan time.Duration) (int64, error) {
	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE status = $1 AND published_at < $2
	`, s.tableName)

	result, err := s.db.ExecContext(ctx, query, StatusPublished, time.Now().Add(-olderThan))
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// PostgresPublisher implements Publisher for PostgreSQL.
//
// PostgresPublisher provides a high-level API for storing events in the
// outbox within a database transaction. It handles payload encoding and
// message ID generation automatically.
//
// Example:
//
//	db, _ := sql.Open("postgres", connString)
//	publisher := outbox.NewPostgresPublisher(db)
//
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    // Business logic
//	    _, err := tx.Exec("INSERT INTO orders ...")
//	    if err != nil {
//	        return err
//	    }
//
//	    // Store event
//	    return publisher.PublishInTransaction(ctx, tx, "order.created", order, nil)
//	})
type PostgresPublisher struct {
	store *PostgresStore
	codec codec.Codec
}

// NewPostgresPublisher creates a new PostgreSQL outbox publisher.
//
// Creates a publisher with the default JSON codec and "event_outbox" table.
//
// Parameters:
//   - db: An open PostgreSQL database connection
//
// Example:
//
//	publisher := outbox.NewPostgresPublisher(db)
func NewPostgresPublisher(db *sql.DB) (*PostgresPublisher, error) {
	store, err := NewPostgresStore(db)
	if err != nil {
		return nil, err
	}

	return &PostgresPublisher{
		store: store,
		codec: codec.Default(),
	}, nil
}

// WithCodec sets a custom codec for encoding payloads.
//
// Use this when you need a different encoding format (e.g., protobuf, msgpack).
//
// Parameters:
//   - c: The codec to use for encoding payloads
//
// Returns the publisher for method chaining.
func (p *PostgresPublisher) WithCodec(c codec.Codec) *PostgresPublisher {
	p.codec = c
	return p
}

// Store returns the underlying PostgresStore.
//
// Use this to access the store for the relay or advanced operations.
//
// Example:
//
//	publisher := outbox.NewPostgresPublisher(db)
//	relay := outbox.NewRelay(publisher.Store(), transport)
func (p *PostgresPublisher) Store() *PostgresStore {
	return p.store
}

// PublishInTransaction stores a message in the outbox within the caller's transaction.
//
// The message is stored atomically with other database operations. A unique
// EventID (UUID) is automatically generated for each message.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - tx: The active database transaction
//   - eventName: Event topic/name for routing (e.g., "order.created")
//   - payload: The event data (will be JSON encoded)
//   - metadata: Optional headers/context (can be nil)
//
// Example:
//
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    // Update order
//	    _, err := tx.Exec("UPDATE orders SET status = $1 WHERE id = $2", "shipped", orderID)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Store event for later publishing
//	    return publisher.PublishInTransaction(ctx, tx, "order.shipped", order,
//	        map[string]string{"source": "order-service"})
//	})
func (p *PostgresPublisher) PublishInTransaction(
	ctx context.Context,
	tx *sql.Tx,
	eventName string,
	payload any,
	metadata map[string]string,
) error {
	// Encode payload
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := &Message{
		EventName: eventName,
		EventID:   uuid.New().String(),
		Payload:   encoded,
		Metadata:  metadata,
	}

	return p.store.Insert(ctx, tx, msg)
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)
var _ Publisher = (*PostgresPublisher)(nil)

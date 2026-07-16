package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/store/base"
)

// PostgresStoreOption configures a PostgresStore.
type PostgresStoreOption func(*postgresStoreOptions)

type postgresStoreOptions struct {
	table         string
	notifyChannel string
	listener      *pq.Listener
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

// WithNotifyChannel sets the PostgreSQL NOTIFY channel name emitted on each stored event.
// Listeners using pq.NewListener can subscribe to this channel to be woken up
// immediately when new messages arrive instead of relying solely on polling.
// The default channel is "event_outbox_pending".
func WithNotifyChannel(channel string) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if channel != "" {
			o.notifyChannel = channel
		}
	}
}

// WithNotifyListener opts the store into the Waker interface: it wires the
// given pq.Listener to the store's notify channel so the relay wakes up
// immediately on PG NOTIFY instead of relying solely on polling.
//
// The listener must be created by the caller (e.g. via pq.NewListener) with
// the same connection string as db. NewPostgresStore starts listening on
// WithNotifyChannel's channel (default "event_outbox_pending") and closes
// over the listener's Notify channel for its lifetime; the caller owns
// closing the listener.
func WithNotifyListener(l *pq.Listener) PostgresStoreOption {
	return func(o *postgresStoreOptions) { o.listener = l }
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
	db            *sql.DB
	tableName     string
	notifyChannel string
	listener      *pq.Listener
	notifyCh      chan struct{}
}

// NotifyChannel returns the PostgreSQL NOTIFY channel name emitted on each Store.
func (s *PostgresStore) NotifyChannel() string { return s.notifyChannel }

// Notifications implements Waker. It returns nil unless a pq.Listener was
// configured via WithNotifyListener, in which case a bridging goroutine
// (started in NewPostgresStore) forwards pq notifications as wakeups. A nil
// channel is safe for the relay engine: the corresponding select case simply
// blocks forever, leaving polling as the only wakeup source.
func (s *PostgresStore) Notifications() <-chan struct{} { return s.notifyCh }

// NewPostgresStore creates a new PostgreSQL outbox store.
//
// The provided database connection should be configured and connected.
// The default table name is "event_outbox".
//
// If WithNotifyListener is supplied, the store starts listening on the
// configured NOTIFY channel and satisfies Waker so the relay can wake up
// immediately on new messages instead of waiting for the next poll.
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("postgres: db is required")
	}

	o := &postgresStoreOptions{
		table:         "event_outbox",
		notifyChannel: "event_outbox_pending",
	}
	for _, opt := range opts {
		opt(o)
	}

	s := &PostgresStore{
		db:            db,
		tableName:     o.table,
		notifyChannel: o.notifyChannel,
		listener:      o.listener,
	}

	if o.listener != nil {
		if err := o.listener.Listen(o.notifyChannel); err != nil {
			return nil, fmt.Errorf("postgres: listen on %q: %w", o.notifyChannel, err)
		}
		notifyCh := make(chan struct{}, 1)
		s.notifyCh = notifyCh
		go func(notify <-chan *pq.Notification) {
			// Ranging over Notify terminates cleanly once the caller closes
			// the listener, so this goroutine never leaks.
			for range notify {
				select {
				case notifyCh <- struct{}{}:
				default:
				}
			}
		}(o.listener.Notify)
	}

	return s, nil
}

// ClaimPending opens a tx and selects a batch FOR UPDATE SKIP LOCKED. The tx
// (and its row locks) is held by the returned pgBatch until Close commits.
// An empty result commits the idle tx immediately and returns a resource-free
// batch.
func (s *PostgresStore) ClaimPending(ctx context.Context, limit int) (Batch, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	// #nosec G201 -- table name set at construction
	q := fmt.Sprintf(`
		SELECT id, event_name, event_id, payload, metadata, created_at, retry_count, COALESCE(priority,0)
		FROM %s WHERE status IN ($1,$2)
		ORDER BY priority DESC, created_at
		LIMIT $3 FOR UPDATE SKIP LOCKED`, s.tableName)
	rows, err := tx.QueryContext(ctx, q, StatusPending, StatusFailed, limit)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("query pending: %w", err)
	}
	msgs, err := scanMessages(rows)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if len(msgs) == 0 {
		_ = tx.Commit() // release the idle tx; no locks held
		return &pgBatch{store: s}, nil
	}
	return &pgBatch{store: s, tx: tx, msgs: msgs}, nil
}

// Cleanup deletes published messages older than olderThan.
//
// This prevents unbounded growth of the outbox table.
func (s *PostgresStore) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	// #nosec G201
	q := fmt.Sprintf(`DELETE FROM %s WHERE status=$1 AND published_at < $2`, s.tableName)
	res, err := s.db.ExecContext(ctx, q, StatusPublished, time.Now().Add(-olderThan))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// pgBatch holds the claim tx; token is the int64 row id.
type pgBatch struct {
	store *PostgresStore
	tx    *sql.Tx // nil for empty batches
	msgs  []Message
}

func (b *pgBatch) Messages() []Message { return b.msgs }

func (b *pgBatch) Ack(ctx context.Context, msg Message) error {
	// #nosec G201
	q := fmt.Sprintf(`UPDATE %s SET status=$1, published_at=$2 WHERE id=$3`, b.store.tableName)
	_, err := b.tx.ExecContext(ctx, q, StatusPublished, time.Now(), msg.token)
	return err
}

func (b *pgBatch) Fail(ctx context.Context, msg Message, cause error) error {
	// #nosec G201
	q := fmt.Sprintf(`UPDATE %s SET status=$1, last_error=$2, retry_count=retry_count+1 WHERE id=$3`, b.store.tableName)
	var e string
	if cause != nil {
		e = cause.Error()
	}
	_, err := b.tx.ExecContext(ctx, q, StatusFailed, e, msg.token)
	return err
}

func (b *pgBatch) Close(context.Context) error {
	if b.tx == nil {
		return nil
	}
	return b.tx.Commit()
}

// scanMessages reads rows into Message with token=int64(id).
func scanMessages(rows *sql.Rows) ([]Message, error) {
	defer func() { _ = rows.Close() }()
	var out []Message
	for rows.Next() {
		var m Message
		var id int64
		var metadataJSON []byte
		if err := rows.Scan(&id, &m.EventName, &m.EventID, &m.Payload, &metadataJSON, &m.CreatedAt, &m.RetryCount, &m.Priority); err != nil {
			return nil, err
		}
		if metadataJSON != nil {
			if err := json.Unmarshal(metadataJSON, &m.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata for id=%d: %w", id, err)
			}
		}
		m.Status = StatusPending
		m.token = id
		out = append(out, m)
	}
	return out, rows.Err()
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)

var _ event.OutboxStore = (*PostgresStore)(nil)

var _ Waker = (*PostgresStore)(nil)

// Store implements event.OutboxStore for bus-level integration.
// When the bus is configured with WithOutbox(postgresStore), calls to Event.Publish()
// inside a transaction (marked by event.WithOutboxTx) are automatically routed here.
//
// The *sql.Tx is extracted from context via event.OutboxTx(). If no transaction
// is active, the message is inserted directly (non-transactional fallback).
func (s *PostgresStore) Store(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	var metadataJSON []byte
	var err error
	if metadata != nil {
		metadataJSON, err = json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}
	}

	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (event_name, event_id, payload, metadata, status, created_at, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, s.tableName)

	args := []any{eventName, eventID, payload, metadataJSON, StatusPending, time.Now().UTC(), 0}

	// Use the transaction from context if available.
	if session := event.OutboxTx(ctx); session != nil {
		tx, ok := session.(*sql.Tx)
		if !ok {
			return fmt.Errorf("outbox: expected *sql.Tx in context, got %T", session)
		}
		if _, err = tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert outbox event: %w", err)
		}
		// Best-effort wakeup: notifying inside the caller's tx means the
		// notification only fires once the tx commits, so subscribers never
		// wake for a row that isn't durably visible yet. The error is
		// intentionally ignored; the relay's poll ticker is the fallback.
		_, _ = tx.ExecContext(ctx, "SELECT pg_notify($1, '')", s.notifyChannel)
		return nil
	}

	// Non-transactional fallback (testing or non-transactional use).
	if _, err = s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	// Best-effort wakeup: fires immediately since there is no surrounding
	// tx to wait on. The error is intentionally ignored; the relay's poll
	// ticker is the fallback.
	_, _ = s.db.ExecContext(ctx, "SELECT pg_notify($1, '')", s.notifyChannel)
	return nil
}

// PostgresTransaction executes fn within a PostgreSQL transaction with outbox context.
// The context passed to fn contains the transaction via event.WithOutboxTx,
// so any Event.Publish() calls within fn are automatically routed to the outbox.
//
// If the context already contains an active outbox transaction (from a parent
// PostgresTransaction or similar), fn is called directly without starting a
// new transaction — piggy-backing on the existing one.
//
// Example:
//
//	store, _ := outbox.NewPostgresStore(db)
//	bus, _ := event.NewBus("mybus", event.WithTransport(t), event.WithOutbox(store))
//	orderEvent := event.New[Order]("order.created")
//	event.Register(ctx, bus, orderEvent)
//
//	err := outbox.PostgresTransaction(ctx, db, func(ctx context.Context) error {
//	    tx := event.OutboxTx(ctx).(*sql.Tx)
//	    if _, err := tx.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", "shipped", orderID); err != nil {
//	        return err
//	    }
//	    return orderEvent.Publish(ctx, order) // Routed to outbox automatically
//	})
func PostgresTransaction(ctx context.Context, db *sql.DB, fn func(ctx context.Context) error) error {
	// Piggy-back only if the existing transaction is a *sql.Tx.
	// Other session types (e.g., Mongo session) are ignored to prevent
	// cross-store type confusion that could silently break atomicity.
	if session := event.OutboxTx(ctx); session != nil {
		if _, ok := session.(*sql.Tx); ok {
			return fn(ctx)
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin outbox tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	txCtx := event.WithOutboxTx(ctx, tx)
	if err := fn(txCtx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit outbox tx: %w", err)
	}
	return nil
}

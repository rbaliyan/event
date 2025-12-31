package idempotency

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// PostgresStore implements Store and TransactionalStore using PostgreSQL.
//
// PostgresStore provides exactly-once processing guarantees when used with
// database transactions. It stores processed message IDs in a PostgreSQL table
// with automatic TTL-based expiration.
//
// Features:
//   - Transactional idempotency checks within the same DB transaction
//   - Automatic cleanup of expired entries via background goroutine
//   - Configurable table name for multi-tenant deployments
//   - Support for custom TTL per message
//
// Table Schema:
//
//	CREATE TABLE event_idempotency (
//	    message_id VARCHAR(255) PRIMARY KEY,
//	    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
//	    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
//	);
//	CREATE INDEX idx_event_idempotency_expires ON event_idempotency(expires_at);
//
// Example:
//
//	db, _ := sql.Open("postgres", connString)
//
//	// Create store with 24-hour TTL
//	store := idempotency.NewPostgresStore(db,
//	    idempotency.WithPostgresTTL(24*time.Hour),
//	)
//
//	// Use for exactly-once processing
//	func handleOrder(ctx context.Context, order Order) error {
//	    msgID := fmt.Sprintf("order:%s", order.ID)
//
//	    isDuplicate, err := store.IsDuplicate(ctx, msgID)
//	    if err != nil {
//	        return err
//	    }
//	    if isDuplicate {
//	        return nil // Already processed
//	    }
//
//	    // Process order...
//
//	    return store.MarkProcessed(ctx, msgID)
//	}
//
// Transactional Usage:
//
//	err := txManager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    // Check within transaction
//	    isDup, err := store.IsDuplicateTx(ctx, sqlTx, msgID)
//	    if isDup {
//	        return nil
//	    }
//
//	    // Process and update database...
//
//	    // Mark as processed in same transaction
//	    return store.MarkProcessedTx(ctx, sqlTx, msgID)
//	})
type PostgresStore struct {
	db              *sql.DB
	table           string
	ttl             time.Duration
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// PostgresOption configures a PostgresStore.
type PostgresOption func(*PostgresStore)

// WithPostgresTTL sets the default TTL for idempotency entries.
//
// Entries older than this duration are considered expired and will be
// cleaned up. Default is 24 hours.
//
// Example:
//
//	store := idempotency.NewPostgresStore(db,
//	    idempotency.WithPostgresTTL(7*24*time.Hour), // 7 days
//	)
func WithPostgresTTL(ttl time.Duration) PostgresOption {
	return func(s *PostgresStore) {
		s.ttl = ttl
	}
}

// WithPostgresTable sets the table name for storing idempotency entries.
//
// Default is "event_idempotency". Use this for multi-tenant deployments
// or when you need multiple idempotency stores.
//
// Example:
//
//	store := idempotency.NewPostgresStore(db,
//	    idempotency.WithPostgresTable("payments_idempotency"),
//	)
func WithPostgresTable(table string) PostgresOption {
	return func(s *PostgresStore) {
		s.table = table
	}
}

// WithPostgresCleanupInterval sets how often expired entries are removed.
//
// Default is 1 minute. Set to 0 to disable automatic cleanup.
//
// Example:
//
//	store := idempotency.NewPostgresStore(db,
//	    idempotency.WithPostgresCleanupInterval(5*time.Minute),
//	)
func WithPostgresCleanupInterval(interval time.Duration) PostgresOption {
	return func(s *PostgresStore) {
		s.cleanupInterval = interval
	}
}

// NewPostgresStore creates a new PostgreSQL-based idempotency store.
//
// The store requires a table with the following schema:
//
//	CREATE TABLE event_idempotency (
//	    message_id VARCHAR(255) PRIMARY KEY,
//	    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
//	    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
//	);
//	CREATE INDEX idx_event_idempotency_expires ON event_idempotency(expires_at);
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
//	store := idempotency.NewPostgresStore(db,
//	    idempotency.WithPostgresTTL(24*time.Hour),
//	    idempotency.WithPostgresTable("order_idempotency"),
//	)
//	defer store.Close()
func NewPostgresStore(db *sql.DB, opts ...PostgresOption) *PostgresStore {
	s := &PostgresStore{
		db:              db,
		table:           "event_idempotency",
		ttl:             24 * time.Hour,
		cleanupInterval: time.Minute,
		stopCleanup:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	// Start background cleanup
	if s.cleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// IsDuplicate checks if a message ID has already been processed.
//
// Returns true if the message exists and has not expired.
// This is a non-transactional check - for exactly-once guarantees,
// use IsDuplicateTx within a database transaction.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to check
//
// Returns:
//   - (true, nil): Message was already processed
//   - (false, nil): Message is new or expired
//   - (false, error): Database query failed
//
// Example:
//
//	isDuplicate, err := store.IsDuplicate(ctx, "order-123")
//	if err != nil {
//	    return fmt.Errorf("idempotency check: %w", err)
//	}
//	if isDuplicate {
//	    log.Info("skipping duplicate")
//	    return nil
//	}
func (s *PostgresStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM %s
			WHERE message_id = $1 AND expires_at > NOW()
		)
	`, s.table)

	var exists bool
	err := s.db.QueryRowContext(ctx, query, messageID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query idempotency: %w", err)
	}

	return exists, nil
}

// IsDuplicateTx checks if a message ID has already been processed within a transaction.
//
// This method provides exactly-once guarantees when used within the same
// transaction as your business logic. The check and subsequent mark are
// atomic within the transaction boundary.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - tx: The active SQL transaction
//   - messageID: The unique message identifier to check
//
// Returns:
//   - (true, nil): Message was already processed
//   - (false, nil): Message is new or expired
//   - (false, error): Query failed
//
// Example:
//
//	err := txManager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    isDup, err := store.IsDuplicateTx(ctx, sqlTx, msgID)
//	    if err != nil {
//	        return err
//	    }
//	    if isDup {
//	        return nil // Already processed
//	    }
//
//	    // Process message...
//
//	    return store.MarkProcessedTx(ctx, sqlTx, msgID)
//	})
func (s *PostgresStore) IsDuplicateTx(ctx context.Context, tx *sql.Tx, messageID string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM %s
			WHERE message_id = $1 AND expires_at > NOW()
		)
	`, s.table)

	var exists bool
	err := tx.QueryRowContext(ctx, query, messageID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query idempotency: %w", err)
	}

	return exists, nil
}

// MarkProcessed marks a message ID as processed using the default TTL.
//
// Uses upsert (INSERT ... ON CONFLICT) to handle concurrent calls safely.
// If the entry already exists, its expiration is updated.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//
// Returns nil on success, error if the database operation fails.
//
// Example:
//
//	if err := store.MarkProcessed(ctx, "order-123"); err != nil {
//	    log.Error("failed to mark processed", "error", err)
//	}
func (s *PostgresStore) MarkProcessed(ctx context.Context, messageID string) error {
	return s.MarkProcessedWithTTL(ctx, messageID, s.ttl)
}

// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
//
// Use this when different message types require different retention periods.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//   - ttl: How long to remember this message ID
//
// Returns nil on success, error if the database operation fails.
//
// Example:
//
//	// Keep payment records for 7 days
//	store.MarkProcessedWithTTL(ctx, "payment-xyz", 7*24*time.Hour)
//
//	// Keep notification records for 1 hour
//	store.MarkProcessedWithTTL(ctx, "notif-abc", time.Hour)
func (s *PostgresStore) MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, processed_at, expires_at)
		VALUES ($1, NOW(), NOW() + $2::interval)
		ON CONFLICT (message_id) DO UPDATE
		SET processed_at = NOW(), expires_at = NOW() + $2::interval
	`, s.table)

	_, err := s.db.ExecContext(ctx, query, messageID, ttl.String())
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}

	return nil
}

// MarkProcessedTx marks a message ID as processed within a transaction.
//
// This method should be called within the same transaction as your business
// logic to ensure atomicity. If the transaction rolls back, the idempotency
// marker is also rolled back.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - tx: The active SQL transaction
//   - messageID: The unique message identifier to mark
//
// Returns nil on success, error if the operation fails.
//
// Example:
//
//	err := txManager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    // Update business data
//	    _, err := sqlTx.ExecContext(ctx, "UPDATE orders SET status = $1", status)
//	    if err != nil {
//	        return err
//	    }
//
//	    // Mark as processed in same transaction
//	    return store.MarkProcessedTx(ctx, sqlTx, msgID)
//	})
func (s *PostgresStore) MarkProcessedTx(ctx context.Context, tx *sql.Tx, messageID string) error {
	return s.MarkProcessedWithTTLTx(ctx, tx, messageID, s.ttl)
}

// MarkProcessedWithTTLTx marks a message ID as processed with custom TTL within a transaction.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - tx: The active SQL transaction
//   - messageID: The unique message identifier to mark
//   - ttl: How long to remember this message ID
//
// Returns nil on success, error if the operation fails.
func (s *PostgresStore) MarkProcessedWithTTLTx(ctx context.Context, tx *sql.Tx, messageID string, ttl time.Duration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, processed_at, expires_at)
		VALUES ($1, NOW(), NOW() + $2::interval)
		ON CONFLICT (message_id) DO UPDATE
		SET processed_at = NOW(), expires_at = NOW() + $2::interval
	`, s.table)

	_, err := tx.ExecContext(ctx, query, messageID, ttl.String())
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}

	return nil
}

// Remove removes a message ID from the store.
//
// After removal, the message ID is no longer considered a duplicate and
// can be processed again if redelivered.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to remove
//
// Returns nil on success (including when the entry doesn't exist),
// error if the database operation fails.
//
// Example:
//
//	// Allow reprocessing after manual fix
//	if err := store.Remove(ctx, "order-failed-123"); err != nil {
//	    log.Error("failed to remove", "error", err)
//	}
func (s *PostgresStore) Remove(ctx context.Context, messageID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, s.table)

	_, err := s.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("remove idempotency: %w", err)
	}

	return nil
}

// Close stops the background cleanup goroutine.
//
// Call this when shutting down to cleanly stop the cleanup routine.
// After Close is called, the store can still be used but cleanup
// will no longer run automatically.
//
// Example:
//
//	store := idempotency.NewPostgresStore(db)
//	defer store.Close()
func (s *PostgresStore) Close() error {
	close(s.stopCleanup)
	return nil
}

// cleanupLoop runs the periodic cleanup of expired entries.
func (s *PostgresStore) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanup()
		case <-s.stopCleanup:
			return
		}
	}
}

// cleanup removes expired entries from the database.
func (s *PostgresStore) cleanup() {
	query := fmt.Sprintf(`DELETE FROM %s WHERE expires_at < NOW()`, s.table)
	_, _ = s.db.Exec(query)
}

// CreateTable creates the idempotency table if it doesn't exist.
//
// This is a convenience method for development and testing. In production,
// you should manage schema migrations separately.
//
// Example:
//
//	store := idempotency.NewPostgresStore(db)
//	if err := store.CreateTable(ctx); err != nil {
//	    log.Fatal("failed to create table:", err)
//	}
func (s *PostgresStore) CreateTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			message_id VARCHAR(255) PRIMARY KEY,
			processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_%s_expires ON %s(expires_at);
	`, s.table, s.table, s.table)

	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)
var _ TransactionalStore = (*PostgresStore)(nil)

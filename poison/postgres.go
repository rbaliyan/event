package poison

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// PostgresStore implements Store using PostgreSQL for poison message tracking.
//
// PostgresStore provides poison message detection with SQL database persistence.
// It tracks failure counts and quarantine status, suitable for deployments
// where Redis is not available or when you want to keep all data in PostgreSQL.
//
// Features:
//   - Atomic failure count increment with PostgreSQL UPSERT
//   - Automatic TTL-based expiration for failures and quarantine
//   - Background cleanup of expired entries
//   - Configurable table names for multi-tenant deployments
//
// Table Schema:
//
//	CREATE TABLE poison_failures (
//	    message_id VARCHAR(255) PRIMARY KEY,
//	    failure_count INTEGER NOT NULL DEFAULT 1,
//	    first_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
//	    last_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
//	    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
//	);
//	CREATE INDEX idx_poison_failures_expires ON poison_failures(expires_at);
//
//	CREATE TABLE poison_quarantine (
//	    message_id VARCHAR(255) PRIMARY KEY,
//	    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
//	    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
//	    reason TEXT
//	);
//	CREATE INDEX idx_poison_quarantine_expires ON poison_quarantine(expires_at);
//
// Example:
//
//	db, _ := sql.Open("postgres", connString)
//
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresFailureTTL(24 * time.Hour),
//	)
//
//	detector := poison.NewDetector(store,
//	    poison.WithThreshold(5),
//	    poison.WithQuarantineTime(time.Hour),
//	)
type PostgresStore struct {
	db               *sql.DB
	failuresTable    string
	quarantineTable  string
	failureTTL       time.Duration
	cleanupInterval  time.Duration
	stopCleanup      chan struct{}
}

// PostgresStoreOption configures a PostgresStore.
type PostgresStoreOption func(*PostgresStore)

// WithPostgresFailuresTable sets the table name for failure tracking.
//
// Default is "poison_failures".
//
// Example:
//
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresFailuresTable("orders_poison_failures"),
//	)
func WithPostgresFailuresTable(table string) PostgresStoreOption {
	return func(s *PostgresStore) {
		s.failuresTable = table
	}
}

// WithPostgresQuarantineTable sets the table name for quarantine tracking.
//
// Default is "poison_quarantine".
//
// Example:
//
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresQuarantineTable("orders_poison_quarantine"),
//	)
func WithPostgresQuarantineTable(table string) PostgresStoreOption {
	return func(s *PostgresStore) {
		s.quarantineTable = table
	}
}

// WithPostgresFailureTTL sets the TTL for failure count entries.
//
// Failure counts are automatically deleted after this duration. This prevents
// unbounded growth when messages fail occasionally but don't reach the
// quarantine threshold.
//
// Default: 24 hours
//
// Example:
//
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresFailureTTL(7 * 24 * time.Hour),
//	)
func WithPostgresFailureTTL(ttl time.Duration) PostgresStoreOption {
	return func(s *PostgresStore) {
		s.failureTTL = ttl
	}
}

// WithPostgresCleanupInterval sets how often expired entries are removed.
//
// Default is 1 minute. Set to 0 to disable automatic cleanup.
//
// Example:
//
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresCleanupInterval(5 * time.Minute),
//	)
func WithPostgresCleanupInterval(interval time.Duration) PostgresStoreOption {
	return func(s *PostgresStore) {
		s.cleanupInterval = interval
	}
}

// NewPostgresStore creates a new PostgreSQL-based poison store.
//
// The store requires tables with the schema described in the type documentation.
// Use CreateTables() to create them automatically in development.
//
// Parameters:
//   - db: An open PostgreSQL database connection
//   - opts: Optional configuration options
//
// Default configuration:
//   - Failures table: "poison_failures"
//   - Quarantine table: "poison_quarantine"
//   - Failure TTL: 24 hours
//   - Cleanup interval: 1 minute
//
// The store starts a background goroutine for cleanup. Call Close() to stop it.
//
// Example:
//
//	db, _ := sql.Open("postgres", "postgres://localhost/mydb")
//	store := poison.NewPostgresStore(db,
//	    poison.WithPostgresFailureTTL(24 * time.Hour),
//	)
//	defer store.Close()
//
//	detector := poison.NewDetector(store)
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) *PostgresStore {
	s := &PostgresStore{
		db:               db,
		failuresTable:    "poison_failures",
		quarantineTable:  "poison_quarantine",
		failureTTL:       24 * time.Hour,
		cleanupInterval:  time.Minute,
		stopCleanup:      make(chan struct{}),
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

// IncrementFailure atomically increments and returns the failure count.
//
// Uses PostgreSQL UPSERT (INSERT ... ON CONFLICT) for atomic increment.
// Also refreshes the expiration time on each failure.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//
// Returns:
//   - The new failure count after incrementing
//   - Error if the database operation fails
//
// Example:
//
//	count, err := store.IncrementFailure(ctx, "order-123")
//	if err != nil {
//	    return err
//	}
//	if count >= threshold {
//	    // Quarantine the message
//	}
func (s *PostgresStore) IncrementFailure(ctx context.Context, messageID string) (int, error) {
	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, failure_count, first_failure_at, last_failure_at, expires_at)
		VALUES ($1, 1, NOW(), NOW(), NOW() + $2::interval)
		ON CONFLICT (message_id) DO UPDATE
		SET failure_count = %s.failure_count + 1,
		    last_failure_at = NOW(),
		    expires_at = NOW() + $2::interval
		RETURNING failure_count
	`, s.failuresTable, s.failuresTable)

	var count int
	err := s.db.QueryRowContext(ctx, query, messageID, s.failureTTL.String()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("increment failure: %w", err)
	}

	return count, nil
}

// GetFailureCount returns the current failure count for a message.
//
// Returns 0 if the message has no recorded failures or if the entry has expired.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//
// Returns:
//   - The current failure count (0 if no failures recorded)
//   - Error if the database operation fails
func (s *PostgresStore) GetFailureCount(ctx context.Context, messageID string) (int, error) {
	query := fmt.Sprintf(`
		SELECT failure_count FROM %s
		WHERE message_id = $1 AND expires_at > NOW()
	`, s.failuresTable)

	var count int
	err := s.db.QueryRowContext(ctx, query, messageID).Scan(&count)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get failure count: %w", err)
	}

	return count, nil
}

// MarkPoison marks a message as quarantined for the given duration.
//
// After the TTL expires, the message is automatically released from quarantine.
// Uses UPSERT to handle multiple calls safely.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//   - ttl: How long to quarantine the message
//
// Returns nil on success, error if the database operation fails.
//
// Example:
//
//	// Quarantine for 1 hour
//	err := store.MarkPoison(ctx, "order-123", time.Hour)
func (s *PostgresStore) MarkPoison(ctx context.Context, messageID string, ttl time.Duration) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (message_id, quarantined_at, expires_at)
		VALUES ($1, NOW(), NOW() + $2::interval)
		ON CONFLICT (message_id) DO UPDATE
		SET quarantined_at = NOW(), expires_at = NOW() + $2::interval
	`, s.quarantineTable)

	_, err := s.db.ExecContext(ctx, query, messageID, ttl.String())
	if err != nil {
		return fmt.Errorf("mark poison: %w", err)
	}

	return nil
}

// IsPoison checks if a message is currently quarantined.
//
// Returns true if the message is quarantined and has not expired.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//
// Returns:
//   - (true, nil): Message is quarantined
//   - (false, nil): Message is not quarantined or quarantine expired
//   - (false, error): Database operation failed
func (s *PostgresStore) IsPoison(ctx context.Context, messageID string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS(
			SELECT 1 FROM %s
			WHERE message_id = $1 AND expires_at > NOW()
		)
	`, s.quarantineTable)

	var exists bool
	err := s.db.QueryRowContext(ctx, query, messageID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check poison: %w", err)
	}

	return exists, nil
}

// ClearPoison removes a message from quarantine immediately.
//
// Use this after investigating and fixing the issue that caused the
// message to be quarantined. The message can then be reprocessed.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//
// Returns nil on success (including when the message wasn't quarantined).
func (s *PostgresStore) ClearPoison(ctx context.Context, messageID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, s.quarantineTable)

	_, err := s.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("clear poison: %w", err)
	}

	return nil
}

// ClearFailures resets the failure count for a message.
//
// Typically called after successful processing to prevent failure history
// from affecting future attempts.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier
//
// Returns nil on success (including when there were no recorded failures).
func (s *PostgresStore) ClearFailures(ctx context.Context, messageID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE message_id = $1`, s.failuresTable)

	_, err := s.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("clear failures: %w", err)
	}

	return nil
}

// Close stops the background cleanup goroutine.
//
// Call this when shutting down to cleanly stop the cleanup routine.
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

// cleanup removes expired entries from both tables.
func (s *PostgresStore) cleanup() {
	failuresQuery := fmt.Sprintf(`DELETE FROM %s WHERE expires_at < NOW()`, s.failuresTable)
	quarantineQuery := fmt.Sprintf(`DELETE FROM %s WHERE expires_at < NOW()`, s.quarantineTable)

	_, _ = s.db.Exec(failuresQuery)
	_, _ = s.db.Exec(quarantineQuery)
}

// CreateTables creates the required tables if they don't exist.
//
// This is a convenience method for development and testing. In production,
// you should manage schema migrations separately.
//
// Example:
//
//	store := poison.NewPostgresStore(db)
//	if err := store.CreateTables(ctx); err != nil {
//	    log.Fatal("failed to create tables:", err)
//	}
func (s *PostgresStore) CreateTables(ctx context.Context) error {
	failuresQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			message_id VARCHAR(255) PRIMARY KEY,
			failure_count INTEGER NOT NULL DEFAULT 1,
			first_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			last_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_%s_expires ON %s(expires_at);
	`, s.failuresTable, s.failuresTable, s.failuresTable)

	quarantineQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			message_id VARCHAR(255) PRIMARY KEY,
			quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
			reason TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_%s_expires ON %s(expires_at);
	`, s.quarantineTable, s.quarantineTable, s.quarantineTable)

	if _, err := s.db.ExecContext(ctx, failuresQuery); err != nil {
		return fmt.Errorf("create failures table: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, quarantineQuery); err != nil {
		return fmt.Errorf("create quarantine table: %w", err)
	}

	return nil
}

// GetQuarantinedMessages returns a list of currently quarantined message IDs.
//
// This is useful for monitoring and administrative purposes.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - limit: Maximum number of messages to return (0 for no limit)
//
// Returns a slice of quarantined message IDs.
func (s *PostgresStore) GetQuarantinedMessages(ctx context.Context, limit int) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT message_id FROM %s
		WHERE expires_at > NOW()
		ORDER BY quarantined_at DESC
	`, s.quarantineTable)

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query quarantined: %w", err)
	}
	defer rows.Close()

	var messages []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan message id: %w", err)
		}
		messages = append(messages, id)
	}

	return messages, rows.Err()
}

// Compile-time check
var _ Store = (*PostgresStore)(nil)

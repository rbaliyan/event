// Package idempotency provides mechanisms to ensure exactly-once processing
// by tracking which messages have already been processed.
//
// Idempotency is critical in distributed systems where messages may be delivered
// more than once due to retries, network issues, or failovers. This package
// provides storage backends to track processed message IDs and prevent duplicate
// processing.
//
// # Overview
//
// The package provides:
//   - Store interface for idempotency tracking
//   - MemoryStore for single-instance deployments
//   - RedisStore for distributed deployments
//   - TransactionalStore for database transaction support
//
// # Basic Usage
//
// Using the in-memory store for development:
//
//	store := idempotency.NewMemoryStore(
//	    idempotency.WithDefaultTTL(time.Hour),
//	    idempotency.WithCleanupInterval(5 * time.Minute),
//	)
//	defer store.Close()
//
//	// Check if message was already processed
//	isDuplicate, err := store.IsDuplicate(ctx, messageID)
//	if err != nil {
//	    return err
//	}
//	if isDuplicate {
//	    log.Println("Skipping duplicate message:", messageID)
//	    return nil
//	}
//
//	// Process the message
//	if err := processMessage(message); err != nil {
//	    return err
//	}
//
//	// Mark as processed
//	return store.MarkProcessed(ctx, messageID)
//
// # Distributed Usage with Redis
//
// For multi-instance deployments, use Redis:
//
//	store := idempotency.NewRedisStore(redisClient,
//	    idempotency.WithRedisTTL(24 * time.Hour),
//	    idempotency.WithRedisPrefix("myapp:dedup:"),
//	)
//
//	// The Redis store uses SET NX for atomic check-and-set
//	isDuplicate, err := store.IsDuplicate(ctx, messageID)
//
// # Transactional Usage
//
// For exactly-once processing with database transactions:
//
//	txStore := idempotency.NewPostgresStore(db)
//
//	err := txManager.Execute(ctx, func(tx Transaction) error {
//	    // Check within transaction
//	    isDuplicate, err := txStore.IsDuplicateTx(ctx, tx.Tx(), messageID)
//	    if isDuplicate {
//	        return nil
//	    }
//
//	    // Process and update database
//	    if err := updateDatabase(tx, data); err != nil {
//	        return err
//	    }
//
//	    // Mark processed in same transaction
//	    return txStore.MarkProcessedTx(ctx, tx.Tx(), messageID)
//	})
//
// # Best Practices
//
//   - Use message IDs that are unique and deterministic (e.g., event ID + consumer group)
//   - Set appropriate TTLs based on your retry window
//   - For critical operations, use TransactionalStore with database transactions
//   - Clean up old entries periodically to prevent unbounded growth
package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrAlreadyProcessed is returned when attempting to process a message
// that has already been marked as processed. Handlers can check for this
// error to distinguish between duplicate processing and other errors.
//
// Example:
//
//	if errors.Is(err, idempotency.ErrAlreadyProcessed) {
//	    log.Info("skipping duplicate message")
//	    return nil
//	}
var ErrAlreadyProcessed = errors.New("message already processed")

// Store defines the interface for idempotency tracking.
//
// Implementations must be safe for concurrent use by multiple goroutines.
// The store tracks message IDs that have been processed, allowing handlers
// to skip duplicate messages.
//
// Example implementation usage:
//
//	func handleMessage(ctx context.Context, store idempotency.Store, msg Message) error {
//	    // Check for duplicate
//	    isDuplicate, err := store.IsDuplicate(ctx, msg.ID)
//	    if err != nil {
//	        return fmt.Errorf("idempotency check failed: %w", err)
//	    }
//	    if isDuplicate {
//	        return nil // Already processed, skip
//	    }
//
//	    // Process message
//	    if err := process(msg); err != nil {
//	        return err
//	    }
//
//	    // Mark as processed
//	    return store.MarkProcessed(ctx, msg.ID)
//	}
type Store interface {
	// IsDuplicate checks if a message ID has already been processed.
	//
	// Returns:
	//   - (true, nil): Message was already processed, skip it
	//   - (false, nil): Message is new, proceed with processing
	//   - (false, error): Check failed, handle error appropriately
	//
	// For atomic implementations (like Redis SET NX), this method may
	// also mark the message as "in progress" to prevent concurrent
	// processing of the same message.
	//
	// Example:
	//
	//	isDuplicate, err := store.IsDuplicate(ctx, "order-123-processed")
	//	if err != nil {
	//	    return err
	//	}
	//	if isDuplicate {
	//	    log.Debug("order-123 already processed")
	//	    return nil
	//	}
	IsDuplicate(ctx context.Context, messageID string) (bool, error)

	// MarkProcessed marks a message ID as successfully processed.
	//
	// This should be called after successful processing to ensure the
	// message won't be processed again. The implementation may use a
	// default TTL for automatic cleanup of old entries.
	//
	// Example:
	//
	//	if err := store.MarkProcessed(ctx, "order-123-processed"); err != nil {
	//	    log.Error("failed to mark as processed", "error", err)
	//	    // Message may be reprocessed on retry, but processing was successful
	//	}
	MarkProcessed(ctx context.Context, messageID string) error

	// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
	//
	// Use this when different message types require different retention periods.
	// After the TTL expires, the message ID is removed and the message could
	// theoretically be processed again if redelivered.
	//
	// Example:
	//
	//	// Keep payment dedup entries for 7 days
	//	err := store.MarkProcessedWithTTL(ctx, "payment-xyz", 7*24*time.Hour)
	//
	//	// Keep notification dedup entries for only 1 hour
	//	err := store.MarkProcessedWithTTL(ctx, "notification-abc", time.Hour)
	MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error

	// Remove removes a message ID from the store.
	//
	// This is primarily useful for:
	//   - Testing: Reset state between tests
	//   - Manual intervention: Allow reprocessing of a specific message
	//   - Cleanup: Remove entries that are no longer needed
	//
	// Example:
	//
	//	// Allow reprocessing of a failed message after manual fix
	//	if err := store.Remove(ctx, "order-123-processed"); err != nil {
	//	    log.Error("failed to remove idempotency key", "error", err)
	//	}
	Remove(ctx context.Context, messageID string) error
}

// TransactionalStore extends Store with database transaction support.
//
// This interface is essential for exactly-once processing guarantees when
// your message handler modifies a database. By checking and marking within
// the same transaction, you ensure atomicity: either both the business
// logic and the idempotency marker are committed, or neither is.
//
// Example with SQL transaction:
//
//	type PostgresIdempotencyStore struct {
//	    db *sql.DB
//	}
//
//	func (s *PostgresIdempotencyStore) IsDuplicateTx(ctx context.Context, tx any, messageID string) (bool, error) {
//	    sqlTx := tx.(*sql.Tx)
//	    var exists bool
//	    err := sqlTx.QueryRowContext(ctx,
//	        "SELECT EXISTS(SELECT 1 FROM processed_messages WHERE id = $1)",
//	        messageID,
//	    ).Scan(&exists)
//	    return exists, err
//	}
//
// Usage in handler:
//
//	err := txManager.Execute(ctx, func(tx Transaction) error {
//	    // Check idempotency within transaction
//	    isDup, err := store.IsDuplicateTx(ctx, tx.Tx(), msg.ID)
//	    if err != nil {
//	        return err
//	    }
//	    if isDup {
//	        return nil
//	    }
//
//	    // Update business data
//	    if _, err := tx.Tx().ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", status, orderID); err != nil {
//	        return err
//	    }
//
//	    // Mark as processed (same transaction)
//	    return store.MarkProcessedTx(ctx, tx.Tx(), msg.ID)
//	})
type TransactionalStore interface {
	Store

	// IsDuplicateTx checks for duplicate within a database transaction.
	//
	// The tx parameter should be a *sql.Tx for SQL databases or the
	// appropriate transaction type for other databases.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - tx: The active database transaction (e.g., *sql.Tx)
	//   - messageID: The unique message identifier to check
	//
	// Returns:
	//   - (true, nil): Message already processed
	//   - (false, nil): Message is new
	//   - (false, error): Check failed
	IsDuplicateTx(ctx context.Context, tx *sql.Tx, messageID string) (bool, error)

	// MarkProcessedTx marks a message as processed within a database transaction.
	//
	// This must be called within the same transaction as your business logic
	// to ensure atomicity. If the transaction rolls back, the idempotency
	// marker is also rolled back, allowing the message to be reprocessed.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - tx: The active database transaction (e.g., *sql.Tx)
	//   - messageID: The unique message identifier to mark
	MarkProcessedTx(ctx context.Context, tx *sql.Tx, messageID string) error
}

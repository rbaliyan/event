// Package outbox implements the transactional outbox pattern for reliable message publishing.
//
// The outbox pattern ensures that database writes and message publishing are atomic:
//  1. Store the message in an outbox table within the same transaction as your business data
//  2. A background relay polls the outbox and publishes messages to the transport
//  3. After successful publish, mark the message as published
//
// This guarantees that messages are never lost, even if the application crashes
// after committing the transaction but before publishing the message.
//
// # Overview
//
// The package provides:
//   - Store interface for outbox persistence
//   - PostgresStore for PostgreSQL databases
//   - RedisStore for Redis-based outbox (see redis.go)
//   - MongoStore for MongoDB (see mongodb.go)
//   - Publisher interface for storing messages
//   - Relay for background publishing
//
// # The Problem
//
// Without the outbox pattern, you face the "dual-write problem":
//
//	// UNSAFE: Not atomic!
//	if err := db.UpdateOrder(order); err != nil {
//	    return err
//	}
//	// If crash here, order is updated but event is lost
//	if err := bus.Publish(ctx, "order.updated", order); err != nil {
//	    return err  // Order updated, but event failed - inconsistent state
//	}
//
// # The Solution
//
// With the outbox pattern, writes are atomic:
//
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    if err := db.UpdateOrder(tx, order); err != nil {
//	        return err
//	    }
//	    // Store in outbox within same transaction
//	    return publisher.PublishInTransaction(ctx, tx, "order.updated", order, nil)
//	})
//	// Either both succeed or both fail - always consistent
//
// # SQL Schema
//
// For PostgreSQL:
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
//	    last_error   TEXT
//	);
//	CREATE INDEX idx_outbox_pending ON event_outbox(status, created_at) WHERE status = 'pending';
//
// # Complete Example
//
//	// Setup
//	db, _ := sql.Open("postgres", connString)
//	publisher := outbox.NewPostgresPublisher(db)
//	relay := outbox.NewRelay(publisher.Store(), transport).
//	    WithPollDelay(100 * time.Millisecond).
//	    WithBatchSize(100)
//
//	// Start relay in background
//	go relay.Start(ctx)
//
//	// In your handler
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    // Update business data
//	    if _, err := tx.Exec("UPDATE orders SET status = $1 WHERE id = $2", "shipped", orderID); err != nil {
//	        return err
//	    }
//
//	    // Store event in outbox (same transaction)
//	    return publisher.PublishInTransaction(ctx, tx, "order.shipped", order, nil)
//	})
//
// # Best Practices
//
//   - Run the relay as a separate process or goroutine
//   - Set appropriate batch sizes based on your throughput
//   - Configure cleanup to remove old published messages
//   - Monitor the pending message count for backlog detection
//   - Use idempotent handlers since messages may be delivered more than once
package outbox

import (
	"context"
	"database/sql"
	"time"
)

// Status represents the state of an outbox message.
//
// Messages progress through these states:
//   - StatusPending: Stored in outbox, waiting to be published
//   - StatusPublished: Successfully published to transport
//   - StatusFailed: Failed to publish (will be retried or moved to DLQ)
type Status string

const (
	// StatusPending indicates the message is waiting to be published.
	StatusPending Status = "pending"

	// StatusPublished indicates the message was successfully published.
	StatusPublished Status = "published"

	// StatusFailed indicates the message failed to publish.
	StatusFailed Status = "failed"
)

// Message represents a message in the outbox.
//
// Messages are stored in the outbox table within a database transaction
// and later published by the relay to the message transport.
//
// Fields:
//   - ID: Auto-generated database identifier
//   - EventName: The event topic/name for routing
//   - EventID: Unique identifier for the event (UUID)
//   - Payload: JSON-encoded event data
//   - Metadata: Optional key-value pairs for headers/context
//   - CreatedAt: When the message was stored
//   - PublishedAt: When the message was published (nil if pending)
//   - Status: Current state (pending, published, failed)
//   - RetryCount: Number of publish attempts
//   - LastError: Most recent error message if failed
type Message struct {
	ID          int64
	EventName   string
	EventID     string
	Payload     []byte
	Metadata    map[string]string
	CreatedAt   time.Time
	PublishedAt *time.Time
	Status      Status
	RetryCount  int
	LastError   string
}

// Store defines the interface for outbox storage.
//
// Implementations must be safe for concurrent use. The store is responsible
// for persisting messages and tracking their publish status.
//
// Implementations:
//   - PostgresStore: For PostgreSQL databases
//   - RedisStore: For Redis (see redis.go)
//   - MongoStore: For MongoDB (see mongodb.go)
//
// Example custom implementation:
//
//	type MySQLStore struct {
//	    db *sql.DB
//	}
//
//	func (s *MySQLStore) Insert(ctx context.Context, tx *sql.Tx, msg *Message) error {
//	    _, err := tx.ExecContext(ctx, "INSERT INTO outbox ...")
//	    return err
//	}
type Store interface {
	// Insert adds a message to the outbox within a transaction.
	//
	// The message is stored atomically with other database operations
	// in the same transaction. The msg.ID field is populated with the
	// generated identifier on success.
	//
	// Example:
	//
	//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
	//	    msg := &outbox.Message{
	//	        EventName: "order.created",
	//	        EventID:   uuid.New().String(),
	//	        Payload:   payload,
	//	    }
	//	    return store.Insert(ctx, tx, msg)
	//	})
	Insert(ctx context.Context, tx *sql.Tx, msg *Message) error

	// GetPending retrieves pending messages for publishing.
	//
	// Returns up to 'limit' messages ordered by creation time.
	// Uses SELECT FOR UPDATE SKIP LOCKED (or equivalent) to prevent
	// concurrent relays from processing the same messages.
	//
	// Example:
	//
	//	messages, err := store.GetPending(ctx, 100)
	//	for _, msg := range messages {
	//	    // Publish each message...
	//	}
	GetPending(ctx context.Context, limit int) ([]*Message, error)

	// MarkPublished marks a message as successfully published.
	//
	// Sets the status to StatusPublished and records the publish time.
	// Called by the relay after successfully publishing to the transport.
	MarkPublished(ctx context.Context, id int64) error

	// MarkFailed marks a message as failed with an error.
	//
	// Increments the retry count and stores the error message.
	// The relay may retry failed messages or move them to a DLQ.
	MarkFailed(ctx context.Context, id int64, err error) error

	// Delete removes old published messages.
	//
	// Deletes messages that were published more than 'olderThan' ago.
	// This is called periodically by the relay to prevent unbounded growth.
	//
	// Returns the number of deleted messages.
	//
	// Example:
	//
	//	// Delete messages published more than 7 days ago
	//	deleted, err := store.Delete(ctx, 7*24*time.Hour)
	Delete(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Publisher provides methods for publishing messages through the outbox.
//
// Publisher stores messages in the outbox table within a database transaction,
// ensuring atomicity with other database operations. The actual publishing
// to the message transport is done by the Relay.
//
// Example:
//
//	publisher := outbox.NewPostgresPublisher(db)
//
//	err := txManager.Execute(ctx, func(tx *sql.Tx) error {
//	    // Update order
//	    if _, err := tx.Exec("UPDATE orders ..."); err != nil {
//	        return err
//	    }
//
//	    // Store event in outbox
//	    return publisher.PublishInTransaction(ctx, tx, "order.updated", order, nil)
//	})
type Publisher interface {
	// PublishInTransaction stores a message in the outbox within the caller's transaction.
	//
	// The message will be published to the transport by the relay. This method
	// only stores the message - it does not actually publish to the transport.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - tx: The active database transaction
	//   - eventName: Event topic/name for routing
	//   - payload: The event data (will be JSON encoded)
	//   - metadata: Optional headers/context (can be nil)
	//
	// Example:
	//
	//	err := publisher.PublishInTransaction(ctx, tx,
	//	    "user.created",
	//	    user,
	//	    map[string]string{"source": "registration"},
	//	)
	PublishInTransaction(ctx context.Context, tx *sql.Tx, eventName string, payload any, metadata map[string]string) error
}

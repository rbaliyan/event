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
//   - Publisher interface for storing messages
//   - Relay for background publishing
//
// For MongoDB outbox support, use the event-mongodb module:
// https://github.com/rbaliyan/event-mongodb
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
//	    txCtx := event.WithOutboxTx(ctx, tx)
//	    return publisher.Publish(txCtx, "order.updated", order, nil)
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
//	    last_error   TEXT,
//	    priority     INT NOT NULL DEFAULT 0
//	);
//	CREATE INDEX idx_outbox_pending ON event_outbox(status, priority DESC, created_at) WHERE status IN ('pending', 'failed');
//
// # Complete Example
//
//	// Setup
//	db, _ := sql.Open("postgres", connString)
//	store, _ := outbox.NewPostgresStore(db)
//	publisher := outbox.NewPublisher(store)
//	relay := outbox.NewRelay(store, transport,
//	    outbox.WithPollDelay(100 * time.Millisecond),
//	    outbox.WithBatchSize(100),
//	)
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
//	    txCtx := event.WithOutboxTx(ctx, tx)
//	    return publisher.Publish(txCtx, "order.shipped", order, nil)
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
	"errors"
	"time"
)

// Status represents the state of an outbox message.
//
// Messages progress through these states:
//   - StatusPending: Stored in outbox, waiting to be published
//   - StatusProcessing: Claimed by a relay instance, being published (HA-safe)
//   - StatusPublished: Successfully published to transport
//   - StatusFailed: Failed to publish (will be retried or moved to DLQ)
type Status string

const (
	// StatusPending indicates the message is waiting to be published.
	StatusPending Status = "pending"

	// StatusProcessing indicates the message is claimed by a relay and being published.
	// Used in HA deployments to prevent duplicate processing.
	StatusProcessing Status = "processing"

	// StatusPublished indicates the message was successfully published.
	StatusPublished Status = "published"

	// StatusFailed indicates the message failed to publish.
	StatusFailed Status = "failed"
)

// errExhausted marks a message that has passed the max-retry limit.
var errExhausted = errors.New("outbox: exceeded max retries")

// Message is the backend-neutral outbox record. A Message obtained from
// Batch.Messages() carries an unexported backend token (row id / stream id /
// ObjectID) used by that Batch to resolve Ack/Fail. Callers never construct
// or read the token.
type Message struct {
	EventName  string
	EventID    string
	Payload    []byte
	Metadata   map[string]string
	CreatedAt  time.Time
	RetryCount int
	Priority   int

	// Introspection-only; populated by stores that track them. The engine
	// does not depend on these.
	Status      Status
	PublishedAt *time.Time
	LastError   string

	token any
}

// Store is the one backend contract: write, claim-for-read, and cleanup.
type Store interface {
	// Store persists an event within the tx/session bound to ctx
	// (event.WithOutboxTx). Identical signature to event.OutboxStore.Store,
	// so every Store satisfies event.OutboxStore.
	Store(ctx context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error
	// ClaimPending atomically claims up to limit pending/failed messages for
	// exclusive processing. Returns a non-nil, resource-free empty Batch when
	// nothing is pending.
	ClaimPending(ctx context.Context, limit int) (Batch, error)
	// Cleanup deletes published messages older than the cutoff.
	Cleanup(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Batch is a claimed set of messages plus the means to resolve each and
// release the claim scope.
type Batch interface {
	Messages() []Message
	Ack(ctx context.Context, msg Message) error
	Fail(ctx context.Context, msg Message, cause error) error
	Close(ctx context.Context) error
}

// StuckRecoverer re-queues messages left in 'processing' by a crashed relay.
// Implemented by claim-and-release backends (Redis, Mongo).
type StuckRecoverer interface {
	RecoverStuck(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Waker lets a backend push an early wakeup (PG NOTIFY, Mongo change stream).
type Waker interface {
	Notifications() <-chan struct{}
}

// Starter lets a backend perform one-time setup before the relay loop begins
// (Redis consumer-group creation, Mongo index creation). The engine calls
// EnsureReady once at the start of Start(). Stores that need no setup simply do
// not implement it.
type Starter interface {
	EnsureReady(ctx context.Context) error
}

// NewClaimedMessage builds a Message carrying the backend token, for stores in
// OTHER modules (e.g. event-mongodb) that cannot set the unexported field.
// Stores in this package set m.token directly. The engine never inspects token.
func NewClaimedMessage(m Message, token any) Message { m.token = token; return m }

// Token returns a claimed message's backend token (backend use only).
func Token(m Message) any { return m.token }

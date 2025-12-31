// Package dlq provides dead-letter queue management and reprocessing capabilities.
//
// Dead-letter queues store messages that failed processing after all retries.
// This package provides:
//   - Storage of failed messages with error details
//   - Filtering and listing of DLQ messages
//   - Replay functionality to reprocess messages
//   - Cleanup of old messages
//
// # Overview
//
// The DLQ pattern is essential for handling message processing failures:
//   - Prevent message loss when handlers fail permanently
//   - Enable manual intervention and debugging
//   - Support automated replay after fixing issues
//   - Track error patterns for monitoring
//
// The package provides:
//   - Store interface for DLQ persistence
//   - Manager for DLQ operations and replay
//   - Multiple store implementations (PostgreSQL, Redis, MongoDB, Memory)
//
// # Basic Usage
//
//	// Create store and manager
//	store := dlq.NewPostgresStore(db)
//	manager := dlq.NewManager(store, transport)
//
//	// Store a failed message (typically in handler error path)
//	if err := handler(ctx, msg); err != nil {
//	    if retryCount >= maxRetries {
//	        manager.Store(ctx, event.Name, msg.ID, payload, metadata, err, retryCount, "order-service")
//	    }
//	}
//
//	// Later: Replay failed messages after fixing the issue
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName: "orders.created",
//	    StartTime: time.Now().Add(-24 * time.Hour),
//	})
//
// # Monitoring
//
//	stats, err := manager.Stats(ctx)
//	fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
//	for event, count := range stats.MessagesByEvent {
//	    fmt.Printf("  %s: %d\n", event, count)
//	}
//
// # Cleanup
//
//	// Remove messages older than 30 days
//	deleted, err := manager.Cleanup(ctx, 30*24*time.Hour)
//
// # Best Practices
//
//   - Store messages before exhausting retries
//   - Add context (source, metadata) for debugging
//   - Monitor DLQ size and set up alerts
//   - Implement automated replay for recoverable errors
//   - Clean up old messages periodically
package dlq

import (
	"context"
	"time"
)

// Message represents a message in the dead-letter queue.
//
// A DLQ message contains the original message data along with metadata
// about the failure, enabling debugging and replay.
type Message struct {
	ID         string            // Unique DLQ message ID (generated)
	EventName  string            // Original event name/topic
	OriginalID string            // Original message ID for correlation
	Payload    []byte            // Original message payload
	Metadata   map[string]string // Original message metadata
	Error      string            // Error that caused the failure
	RetryCount int               // Number of retries attempted before DLQ
	CreatedAt  time.Time         // When the message was added to DLQ
	RetriedAt  *time.Time        // When the message was last replayed (nil if never)
	Source     string            // Source system/service that produced the error
}

// Filter specifies criteria for listing DLQ messages.
//
// All fields are optional. Empty filter returns all messages.
//
// Example:
//
//	// Find recent payment failures
//	filter := dlq.Filter{
//	    EventName:      "payment.process",
//	    StartTime:      time.Now().Add(-24 * time.Hour),
//	    ExcludeRetried: true,
//	    Limit:          100,
//	}
type Filter struct {
	EventName      string    // Filter by event name (empty = all events)
	StartTime      time.Time // Filter messages after this time (zero = no minimum)
	EndTime        time.Time // Filter messages before this time (zero = no maximum)
	Error          string    // Filter by error message (contains match)
	MaxRetries     int       // Filter by retry count (0 = no limit)
	Source         string    // Filter by source service (empty = all sources)
	ExcludeRetried bool      // Exclude already replayed messages
	Limit          int       // Maximum results (0 = no limit)
	Offset         int       // Offset for pagination
}

// Store defines the interface for DLQ storage.
//
// Implementations must be safe for concurrent use. The store persists
// failed messages and supports filtering, replay, and cleanup.
//
// Implementations:
//   - PostgresStore: For PostgreSQL databases
//   - RedisStore: For Redis (see redis.go)
//   - MongoStore: For MongoDB (see mongodb.go)
//   - MemoryStore: For testing (see memory.go)
type Store interface {
	// Store adds a message to the DLQ.
	// The message ID should be pre-generated.
	Store(ctx context.Context, msg *Message) error

	// Get retrieves a single message by ID.
	// Returns error if not found.
	Get(ctx context.Context, id string) (*Message, error)

	// List returns messages matching the filter.
	// Returns empty slice if no matches.
	List(ctx context.Context, filter Filter) ([]*Message, error)

	// Count returns the number of messages matching the filter.
	Count(ctx context.Context, filter Filter) (int64, error)

	// MarkRetried marks a message as replayed.
	// Sets RetriedAt to current time.
	MarkRetried(ctx context.Context, id string) error

	// Delete removes a message from the DLQ.
	Delete(ctx context.Context, id string) error

	// DeleteOlderThan removes messages older than the specified age.
	// Returns the number of deleted messages.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)

	// DeleteByFilter removes messages matching the filter.
	// Returns the number of deleted messages.
	DeleteByFilter(ctx context.Context, filter Filter) (int64, error)
}

// Stats provides DLQ statistics.
//
// Used for monitoring and alerting on DLQ health.
type Stats struct {
	TotalMessages   int64            // Total messages in DLQ
	MessagesByEvent map[string]int64 // Count per event type
	MessagesByError map[string]int64 // Count per error type
	OldestMessage   *time.Time       // Timestamp of oldest message
	NewestMessage   *time.Time       // Timestamp of newest message
	RetriedMessages int64            // Messages that have been replayed
	PendingMessages int64            // Messages awaiting replay
}

// StatsProvider is an optional interface for stores that support statistics.
//
// Stores implementing this interface provide detailed statistics about
// DLQ contents, useful for monitoring dashboards.
type StatsProvider interface {
	// Stats returns DLQ statistics.
	Stats(ctx context.Context) (*Stats, error)
}

package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// Manager handles DLQ operations including replay.
//
// The Manager provides a high-level API for:
//   - Storing failed messages in the DLQ
//   - Listing and filtering DLQ messages
//   - Replaying messages back to their original events
//   - Cleaning up old messages
//   - Getting DLQ statistics
//
// Example:
//
//	store := dlq.NewPostgresStore(db)
//	manager := dlq.NewManager(store, transport)
//
//	// Store a failed message
//	manager.Store(ctx, "order.process", msg.ID, payload, metadata, err, 3, "order-service")
//
//	// Replay failed messages
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName:      "order.process",
//	    ExcludeRetried: true,
//	})
//
//	// Get statistics
//	stats, _ := manager.Stats(ctx)
//	fmt.Printf("Pending: %d\n", stats.PendingMessages)
type Manager struct {
	store     Store
	transport transport.Transport
	logger    *slog.Logger
}

// NewManager creates a new DLQ manager.
//
// The manager requires a store for persistence and a transport for replaying
// messages back to their original events.
//
// Parameters:
//   - store: DLQ storage implementation
//   - t: Transport for publishing replayed messages
//
// Example:
//
//	store := dlq.NewRedisStore(redisClient)
//	manager := dlq.NewManager(store, transport)
func NewManager(store Store, t transport.Transport) *Manager {
	return &Manager{
		store:     store,
		transport: t,
		logger:    slog.Default().With("component", "dlq.manager"),
	}
}

// WithLogger sets a custom logger.
//
// The logger is used for info and error messages during DLQ operations.
func (m *Manager) WithLogger(l *slog.Logger) *Manager {
	m.logger = l
	return m
}

// Store adds a failed message to the DLQ.
//
// Call this when a message has exhausted all retries and needs to be
// preserved for later investigation or replay.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - eventName: The original event name/topic
//   - originalID: The original message ID for correlation
//   - payload: The message payload (typically JSON)
//   - metadata: Original message metadata
//   - err: The error that caused the failure
//   - retryCount: Number of retries attempted
//   - source: Source service identifier for debugging
//
// Example:
//
//	func handleWithDLQ(ctx context.Context, msg Message) error {
//	    for attempt := 0; attempt < maxRetries; attempt++ {
//	        if err := process(ctx, msg); err == nil {
//	            return nil
//	        }
//	    }
//	    return dlqManager.Store(ctx, event.Name, msg.ID, msg.Payload,
//	        msg.Metadata, err, maxRetries, "order-service")
//	}
func (m *Manager) Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error {
	msg := &Message{
		ID:         uuid.New().String(),
		EventName:  eventName,
		OriginalID: originalID,
		Payload:    payload,
		Metadata:   metadata,
		Error:      err.Error(),
		RetryCount: retryCount,
		CreatedAt:  time.Now(),
		Source:     source,
	}

	if storeErr := m.store.Store(ctx, msg); storeErr != nil {
		m.logger.Error("failed to store DLQ message",
			"event", eventName,
			"original_id", originalID,
			"error", storeErr)
		return fmt.Errorf("store dlq message: %w", storeErr)
	}

	m.logger.Info("stored message in DLQ",
		"id", msg.ID,
		"event", eventName,
		"original_id", originalID,
		"retry_count", retryCount)

	return nil
}

// Get retrieves a single DLQ message
func (m *Manager) Get(ctx context.Context, id string) (*Message, error) {
	return m.store.Get(ctx, id)
}

// List returns DLQ messages matching the filter
func (m *Manager) List(ctx context.Context, filter Filter) ([]*Message, error) {
	return m.store.List(ctx, filter)
}

// Count returns the number of messages matching the filter
func (m *Manager) Count(ctx context.Context, filter Filter) (int64, error) {
	return m.store.Count(ctx, filter)
}

// Replay replays all DLQ messages matching the filter back to their original events.
//
// Messages are republished to their original event topics with additional
// metadata indicating they are DLQ replays. Successfully replayed messages
// are marked as retried.
//
// The replay continues even if some messages fail to replay, logging errors
// for failed messages.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - filter: Criteria for selecting messages to replay
//
// Returns the number of successfully replayed messages.
//
// Example:
//
//	// Replay all unretried payment failures from the last day
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName:      "payment.process",
//	    StartTime:      time.Now().Add(-24 * time.Hour),
//	    ExcludeRetried: true,
//	})
//	log.Info("replayed messages", "count", replayed)
func (m *Manager) Replay(ctx context.Context, filter Filter) (int, error) {
	messages, err := m.store.List(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("list messages: %w", err)
	}

	replayed := 0
	for _, msg := range messages {
		if err := m.replayMessage(ctx, msg); err != nil {
			m.logger.Error("failed to replay message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			continue
		}

		if err := m.store.MarkRetried(ctx, msg.ID); err != nil {
			m.logger.Error("failed to mark message as retried",
				"id", msg.ID,
				"error", err)
		}

		replayed++
	}

	m.logger.Info("replayed DLQ messages",
		"total", len(messages),
		"replayed", replayed)

	return replayed, nil
}

// ReplaySingle replays a single DLQ message by ID
func (m *Manager) ReplaySingle(ctx context.Context, id string) error {
	msg, err := m.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get message: %w", err)
	}

	if err := m.replayMessage(ctx, msg); err != nil {
		return fmt.Errorf("replay message: %w", err)
	}

	if err := m.store.MarkRetried(ctx, id); err != nil {
		return fmt.Errorf("mark retried: %w", err)
	}

	m.logger.Info("replayed single DLQ message",
		"id", id,
		"event", msg.EventName,
		"original_id", msg.OriginalID)

	return nil
}

// replayMessage publishes a message back to its original event
func (m *Manager) replayMessage(ctx context.Context, msg *Message) error {
	// Add replay metadata
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["dlq_replay"] = "true"
	metadata["dlq_message_id"] = msg.ID
	metadata["dlq_original_error"] = msg.Error

	// Create transport message
	transportMsg := message.New(
		msg.OriginalID,
		"dlq-replay",
		msg.Payload, // Send raw payload
		metadata,
		trace.SpanContext{},
	)

	return m.transport.Publish(ctx, msg.EventName, transportMsg)
}

// Delete removes a message from the DLQ
func (m *Manager) Delete(ctx context.Context, id string) error {
	return m.store.Delete(ctx, id)
}

// DeleteByFilter removes messages matching the filter
func (m *Manager) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	return m.store.DeleteByFilter(ctx, filter)
}

// Cleanup removes messages older than the specified age
func (m *Manager) Cleanup(ctx context.Context, age time.Duration) (int64, error) {
	deleted, err := m.store.DeleteOlderThan(ctx, age)
	if err != nil {
		return 0, err
	}

	if deleted > 0 {
		m.logger.Info("cleaned up old DLQ messages",
			"deleted", deleted,
			"older_than", age)
	}

	return deleted, nil
}

// Stats returns DLQ statistics if the store supports it
func (m *Manager) Stats(ctx context.Context) (*Stats, error) {
	if sp, ok := m.store.(StatsProvider); ok {
		return sp.Stats(ctx)
	}

	// Fallback: compute basic stats
	total, err := m.store.Count(ctx, Filter{})
	if err != nil {
		return nil, err
	}

	pending, err := m.store.Count(ctx, Filter{ExcludeRetried: true})
	if err != nil {
		return nil, err
	}

	return &Stats{
		TotalMessages:   total,
		PendingMessages: pending,
		RetriedMessages: total - pending,
	}, nil
}

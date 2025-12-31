package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.opentelemetry.io/otel/trace"
)

// MongoRelay polls the MongoDB outbox and publishes messages to the transport
type MongoRelay struct {
	store      *MongoStore
	transport  transport.Transport
	pollDelay  time.Duration
	batchSize  int
	logger     *slog.Logger
	cleanupAge time.Duration
}

// NewMongoRelay creates a new MongoDB outbox relay
func NewMongoRelay(store *MongoStore, t transport.Transport) *MongoRelay {
	return &MongoRelay{
		store:      store,
		transport:  t,
		pollDelay:  100 * time.Millisecond,
		batchSize:  100,
		logger:     slog.Default().With("component", "outbox.mongo_relay"),
		cleanupAge: 24 * time.Hour,
	}
}

// WithPollDelay sets the polling interval
func (r *MongoRelay) WithPollDelay(d time.Duration) *MongoRelay {
	r.pollDelay = d
	return r
}

// WithBatchSize sets the number of messages to process per poll
func (r *MongoRelay) WithBatchSize(size int) *MongoRelay {
	r.batchSize = size
	return r
}

// WithLogger sets a custom logger
func (r *MongoRelay) WithLogger(l *slog.Logger) *MongoRelay {
	r.logger = l
	return r
}

// WithCleanupAge sets how old published messages should be before deletion
func (r *MongoRelay) WithCleanupAge(age time.Duration) *MongoRelay {
	r.cleanupAge = age
	return r
}

// Start begins polling the outbox and publishing messages.
// This method blocks until the context is cancelled.
func (r *MongoRelay) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.pollDelay)
	defer ticker.Stop()

	// Also start a cleanup ticker
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.publishPending(ctx)
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		}
	}
}

// publishPending fetches and publishes pending messages
func (r *MongoRelay) publishPending(ctx context.Context) {
	messages, err := r.store.GetPendingMongo(ctx, r.batchSize)
	if err != nil {
		r.logger.Error("failed to get pending messages", "error", err)
		return
	}

	for _, msg := range messages {
		if err := r.publishMessage(ctx, msg); err != nil {
			r.logger.Error("failed to publish message",
				"id", msg.ID.Hex(),
				"event", msg.EventName,
				"error", err)
			r.store.MarkFailed(ctx, msg.ID, err)
			continue
		}

		if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
			r.logger.Error("failed to mark message as published",
				"id", msg.ID.Hex(),
				"error", err)
		}

		r.logger.Debug("published outbox message",
			"id", msg.ID.Hex(),
			"event", msg.EventName,
			"event_id", msg.EventID)
	}
}

// publishMessage publishes a single message to the transport
func (r *MongoRelay) publishMessage(ctx context.Context, msg *MongoMessage) error {
	// Decode the payload (it was JSON encoded when stored)
	var payload any
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return err
	}

	// Create transport message
	transportMsg := message.New(
		msg.EventID,
		"outbox",
		payload,
		msg.Metadata,
		trace.SpanContext{},
	)

	return r.transport.Publish(ctx, msg.EventName, transportMsg)
}

// cleanup removes old published messages
func (r *MongoRelay) cleanup(ctx context.Context) {
	deleted, err := r.store.Delete(ctx, r.cleanupAge)
	if err != nil {
		r.logger.Error("failed to cleanup old messages", "error", err)
		return
	}

	if deleted > 0 {
		r.logger.Info("cleaned up old outbox messages", "count", deleted)
	}
}

// PublishOnce processes pending messages once (for testing or manual triggering)
func (r *MongoRelay) PublishOnce(ctx context.Context) error {
	r.publishPending(ctx)
	return nil
}

// MongoMessageWithID is a helper struct for operations that need both ObjectID and int64 ID
type MongoMessageWithID struct {
	ObjectID primitive.ObjectID
	Message  *Message
}

// GetPendingWithIDs returns pending messages with their MongoDB ObjectIDs
func (s *MongoStore) GetPendingWithIDs(ctx context.Context, limit int) ([]*MongoMessageWithID, error) {
	messages, err := s.GetPendingMongo(ctx, limit)
	if err != nil {
		return nil, err
	}

	result := make([]*MongoMessageWithID, len(messages))
	for i, msg := range messages {
		result[i] = &MongoMessageWithID{
			ObjectID: msg.ID,
			Message:  msg.ToMessage(),
		}
	}

	return result, nil
}

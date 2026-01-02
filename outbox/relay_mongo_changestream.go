package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/trace"
)

// ResumeTokenStore defines the interface for persisting Change Stream resume tokens.
// Implementing this interface allows the relay to resume from where it left off after a crash.
type ResumeTokenStore interface {
	// Save persists the resume token
	Save(ctx context.Context, token bson.Raw) error
	// Load retrieves the last saved resume token (returns nil if none exists)
	Load(ctx context.Context) (bson.Raw, error)
}

// MongoResumeTokenStore stores resume tokens in a MongoDB collection.
type MongoResumeTokenStore struct {
	collection *mongo.Collection
	relayID    string
}

// NewMongoResumeTokenStore creates a new MongoDB-based resume token store.
//
// Parameters:
//   - db: MongoDB database
//   - relayID: Unique identifier for this relay instance (allows multiple relays)
func NewMongoResumeTokenStore(db *mongo.Database, relayID string) *MongoResumeTokenStore {
	return &MongoResumeTokenStore{
		collection: db.Collection("outbox_resume_tokens"),
		relayID:    relayID,
	}
}

func (s *MongoResumeTokenStore) Save(ctx context.Context, token bson.Raw) error {
	filter := bson.M{"_id": s.relayID}
	update := bson.M{
		"$set": bson.M{
			"token":      token,
			"updated_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := s.collection.UpdateOne(ctx, filter, update, opts)
	return err
}

func (s *MongoResumeTokenStore) Load(ctx context.Context) (bson.Raw, error) {
	filter := bson.M{"_id": s.relayID}
	var result struct {
		Token bson.Raw `bson:"token"`
	}
	err := s.collection.FindOne(ctx, filter).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result.Token, nil
}

// ChangeStreamRelay uses MongoDB Change Streams to watch for new outbox messages.
// This is more efficient than polling as it receives real-time notifications.
//
// Requirements:
//   - MongoDB replica set or sharded cluster (Change Streams require oplog)
//
// Features:
//   - Real-time message publishing (no polling delay)
//   - Resume token persistence for crash recovery
//   - Automatic processing of existing pending messages on startup
//   - Stuck message recovery for HA deployments
//
// Example:
//
//	store := outbox.NewMongoStore(db)
//	tokenStore := outbox.NewMongoResumeTokenStore(db, "relay-1")
//	relay := outbox.NewChangeStreamRelay(store, transport).
//	    WithResumeTokenStore(tokenStore)
//
//	go relay.Start(ctx)
type ChangeStreamRelay struct {
	store            *MongoStore
	transport        transport.Transport
	logger           *slog.Logger
	cleanupAge       time.Duration
	stuckDuration    time.Duration
	batchSize        int
	resumeTokenStore ResumeTokenStore
	fullDocumentMode string // "updateLookup" or "whenAvailable" for MongoDB 6.0+

	mu          sync.Mutex
	resumeToken bson.Raw
}

// NewChangeStreamRelay creates a new Change Stream-based outbox relay.
//
// Parameters:
//   - store: The MongoDB outbox store to watch
//   - t: Transport for publishing messages
func NewChangeStreamRelay(store *MongoStore, t transport.Transport) *ChangeStreamRelay {
	return &ChangeStreamRelay{
		store:            store,
		transport:        t,
		logger:           slog.Default().With("component", "outbox.changestream_relay"),
		cleanupAge:       24 * time.Hour,
		stuckDuration:    5 * time.Minute,
		batchSize:        100,
		fullDocumentMode: "updateLookup",
	}
}

// WithLogger sets a custom logger.
func (r *ChangeStreamRelay) WithLogger(l *slog.Logger) *ChangeStreamRelay {
	r.logger = l
	return r
}

// WithCleanupAge sets how old published messages should be before deletion.
func (r *ChangeStreamRelay) WithCleanupAge(age time.Duration) *ChangeStreamRelay {
	r.cleanupAge = age
	return r
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
func (r *ChangeStreamRelay) WithStuckDuration(d time.Duration) *ChangeStreamRelay {
	r.stuckDuration = d
	return r
}

// WithBatchSize sets the number of messages to process per batch on startup.
func (r *ChangeStreamRelay) WithBatchSize(size int) *ChangeStreamRelay {
	r.batchSize = size
	return r
}

// WithResumeTokenStore sets the store for persisting resume tokens.
// If not set, the relay will start from the current time on restart.
func (r *ChangeStreamRelay) WithResumeTokenStore(store ResumeTokenStore) *ChangeStreamRelay {
	r.resumeTokenStore = store
	return r
}

// Start begins watching the outbox collection and publishing messages.
// This method blocks until the context is cancelled.
//
// On startup, it:
//  1. Recovers any stuck messages from crashed relays
//  2. Processes any existing pending messages
//  3. Starts watching for new inserts via Change Stream
func (r *ChangeStreamRelay) Start(ctx context.Context) error {
	// Recover stuck messages at startup
	r.recoverStuck(ctx)

	// Process any existing pending messages first
	r.processExistingPending(ctx)

	// Load resume token if available
	if r.resumeTokenStore != nil {
		token, err := r.resumeTokenStore.Load(ctx)
		if err != nil {
			r.logger.Warn("failed to load resume token, starting fresh", "error", err)
		} else if token != nil {
			r.resumeToken = token
			r.logger.Info("loaded resume token, resuming from last position")
		}
	}

	// Start background tasks
	go r.backgroundTasks(ctx)

	// Start watching for new messages
	return r.watchLoop(ctx)
}

// backgroundTasks runs cleanup and recovery in the background.
func (r *ChangeStreamRelay) backgroundTasks(ctx context.Context) {
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		case <-recoveryTicker.C:
			r.recoverStuck(ctx)
		}
	}
}

// watchLoop watches the collection for new inserts using Change Streams.
func (r *ChangeStreamRelay) watchLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := r.watch(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("change stream error, reconnecting", "error", err)
			time.Sleep(time.Second) // Brief pause before reconnecting
		}
	}
}

// watch creates a change stream and processes events.
func (r *ChangeStreamRelay) watch(ctx context.Context) error {
	// Watch for insert operations only
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"operationType": "insert",
		}}},
	}

	opts := options.ChangeStream().
		SetFullDocument(options.FullDocument(r.fullDocumentMode))

	// Resume from last position if available
	r.mu.Lock()
	if r.resumeToken != nil {
		opts.SetResumeAfter(r.resumeToken)
	}
	r.mu.Unlock()

	stream, err := r.store.collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}
	defer stream.Close(ctx)

	r.logger.Info("change stream started")

	for stream.Next(ctx) {
		var event changeEvent
		if err := stream.Decode(&event); err != nil {
			r.logger.Error("failed to decode change event", "error", err)
			continue
		}

		// Process the inserted document
		if event.FullDocument != nil {
			r.processDocument(ctx, event.FullDocument)
		}

		// Save resume token
		r.saveResumeToken(ctx, stream.ResumeToken())
	}

	if err := stream.Err(); err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	return nil
}

// changeEvent represents a MongoDB change stream event.
type changeEvent struct {
	OperationType string        `bson:"operationType"`
	FullDocument  *MongoMessage `bson:"fullDocument"`
	DocumentKey   struct {
		ID primitive.ObjectID `bson:"_id"`
	} `bson:"documentKey"`
}

// processDocument handles a single inserted document.
func (r *ChangeStreamRelay) processDocument(ctx context.Context, msg *MongoMessage) {
	// Only process pending messages
	if msg.Status != StatusPending {
		return
	}

	// Try to claim the message atomically
	claimed, err := r.claimMessage(ctx, msg.ID)
	if err != nil {
		r.logger.Error("failed to claim message", "id", msg.ID.Hex(), "error", err)
		return
	}
	if !claimed {
		// Another relay already claimed it
		return
	}

	// Publish the message
	if err := r.publishMessage(ctx, msg); err != nil {
		r.logger.Error("failed to publish message",
			"id", msg.ID.Hex(),
			"event", msg.EventName,
			"error", err)
		r.store.MarkFailed(ctx, msg.ID, err)
		return
	}

	// Mark as published
	if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
		r.logger.Error("failed to mark message as published",
			"id", msg.ID.Hex(),
			"error", err)
		return
	}

	r.logger.Debug("published outbox message",
		"id", msg.ID.Hex(),
		"event", msg.EventName,
		"event_id", msg.EventID)
}

// claimMessage atomically claims a message for processing.
func (r *ChangeStreamRelay) claimMessage(ctx context.Context, id primitive.ObjectID) (bool, error) {
	filter := bson.M{
		"_id":    id,
		"status": StatusPending,
	}
	update := bson.M{
		"$set": bson.M{
			"status":     StatusProcessing,
			"claimed_at": time.Now(),
		},
	}

	result, err := r.store.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, err
	}

	return result.ModifiedCount > 0, nil
}

// publishMessage publishes a single message to the transport.
func (r *ChangeStreamRelay) publishMessage(ctx context.Context, msg *MongoMessage) error {
	var payload any
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return err
	}

	transportMsg := message.New(
		msg.EventID,
		"outbox",
		payload,
		msg.Metadata,
		trace.SpanContext{},
	)

	return r.transport.Publish(ctx, msg.EventName, transportMsg)
}

// saveResumeToken persists the resume token for crash recovery.
func (r *ChangeStreamRelay) saveResumeToken(ctx context.Context, token bson.Raw) {
	r.mu.Lock()
	r.resumeToken = token
	r.mu.Unlock()

	if r.resumeTokenStore != nil {
		if err := r.resumeTokenStore.Save(ctx, token); err != nil {
			r.logger.Error("failed to save resume token", "error", err)
		}
	}
}

// processExistingPending processes any messages that were pending before startup.
func (r *ChangeStreamRelay) processExistingPending(ctx context.Context) {
	r.logger.Info("processing existing pending messages")

	for {
		messages, err := r.store.GetPendingMongo(ctx, r.batchSize)
		if err != nil {
			r.logger.Error("failed to get pending messages", "error", err)
			return
		}

		if len(messages) == 0 {
			break
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
		}
	}

	r.logger.Info("finished processing existing pending messages")
}

// cleanup removes old published messages.
func (r *ChangeStreamRelay) cleanup(ctx context.Context) {
	deleted, err := r.store.Delete(ctx, r.cleanupAge)
	if err != nil {
		r.logger.Error("failed to cleanup old messages", "error", err)
		return
	}

	if deleted > 0 {
		r.logger.Info("cleaned up old outbox messages", "count", deleted)
	}
}

// recoverStuck moves messages stuck in "processing" back to "pending".
func (r *ChangeStreamRelay) recoverStuck(ctx context.Context) {
	recovered, err := r.store.RecoverStuck(ctx, r.stuckDuration)
	if err != nil {
		r.logger.Error("failed to recover stuck messages", "error", err)
		return
	}

	if recovered > 0 {
		r.logger.Warn("recovered stuck messages", "count", recovered, "stuck_duration", r.stuckDuration)
	}
}

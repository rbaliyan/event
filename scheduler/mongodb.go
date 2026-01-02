package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel/trace"
)

/*
MongoDB Schema:

Collection: scheduled_messages

Document structure:
{
    "_id": string (message ID),
    "event_name": string,
    "payload": Binary,
    "metadata": object,
    "scheduled_at": ISODate,
    "created_at": ISODate,
    "status": string (pending/processing),
    "claimed_at": ISODate (optional, set when processing)
}

Indexes:
db.scheduled_messages.createIndex({ "scheduled_at": 1 })
db.scheduled_messages.createIndex({ "event_name": 1 })
db.scheduled_messages.createIndex({ "status": 1, "scheduled_at": 1 })
*/

// SchedulerStatus represents the state of a scheduled message
type SchedulerStatus string

const (
	// SchedulerStatusPending indicates the message is waiting to be delivered
	SchedulerStatusPending SchedulerStatus = "pending"

	// SchedulerStatusProcessing indicates the message is claimed by a scheduler
	SchedulerStatusProcessing SchedulerStatus = "processing"
)

// MongoMessage represents a scheduled message document in MongoDB
type MongoMessage struct {
	ID          string            `bson:"_id"`
	EventName   string            `bson:"event_name"`
	Payload     []byte            `bson:"payload"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
	ScheduledAt time.Time         `bson:"scheduled_at"`
	CreatedAt   time.Time         `bson:"created_at"`
	Status      SchedulerStatus   `bson:"status,omitempty"`
	ClaimedAt   *time.Time        `bson:"claimed_at,omitempty"`
}

// ToMessage converts MongoMessage to Message
func (m *MongoMessage) ToMessage() *Message {
	return &Message{
		ID:          m.ID,
		EventName:   m.EventName,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		ScheduledAt: m.ScheduledAt,
		CreatedAt:   m.CreatedAt,
	}
}

// FromMessage creates a MongoMessage from Message
func FromSchedulerMessage(m *Message) *MongoMessage {
	return &MongoMessage{
		ID:          m.ID,
		EventName:   m.EventName,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		ScheduledAt: m.ScheduledAt,
		CreatedAt:   m.CreatedAt,
	}
}

// MongoScheduler uses MongoDB for scheduling
type MongoScheduler struct {
	collection    *mongo.Collection
	transport     transport.Transport
	opts          *Options
	logger        *slog.Logger
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	stuckDuration time.Duration // How long before a processing message is considered stuck
}

// NewMongoScheduler creates a new MongoDB-based scheduler
func NewMongoScheduler(db *mongo.Database, t transport.Transport, opts ...Option) *MongoScheduler {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &MongoScheduler{
		collection:    db.Collection("scheduled_messages"),
		transport:     t,
		opts:          o,
		logger:        slog.Default().With("component", "scheduler.mongodb"),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		stuckDuration: 5 * time.Minute, // Recover messages stuck in processing after 5 min
	}
}

// WithCollection sets a custom collection name
func (s *MongoScheduler) WithCollection(name string) *MongoScheduler {
	s.collection = s.collection.Database().Collection(name)
	return s
}

// WithLogger sets a custom logger
func (s *MongoScheduler) WithLogger(l *slog.Logger) *MongoScheduler {
	s.logger = l
	return s
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
// Messages stuck in processing longer than this duration are moved back to pending.
// This handles scheduler crashes where messages were claimed but never published.
// Default: 5 minutes
func (s *MongoScheduler) WithStuckDuration(d time.Duration) *MongoScheduler {
	s.stuckDuration = d
	return s
}

// Collection returns the underlying MongoDB collection
func (s *MongoScheduler) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required indexes for the scheduler collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := scheduler.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoScheduler) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "scheduled_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "event_name", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "scheduled_at", Value: 1},
			},
		},
	}
}

// EnsureIndexes creates the required indexes for the scheduler collection
func (s *MongoScheduler) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Schedule adds a message for future delivery
func (s *MongoScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	mongoMsg := FromSchedulerMessage(&msg)
	mongoMsg.Status = SchedulerStatusPending

	_, err := s.collection.InsertOne(ctx, mongoMsg)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("message already exists: %s", msg.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}

	s.logger.Debug("scheduled message",
		"id", msg.ID,
		"event", msg.EventName,
		"scheduled_at", msg.ScheduledAt)

	return nil
}

// ScheduleAt schedules a message for a specific time
func (s *MongoScheduler) ScheduleAt(ctx context.Context, eventName string, payload []byte, metadata map[string]string, at time.Time) (string, error) {
	msg := Message{
		ID:          uuid.New().String(),
		EventName:   eventName,
		Payload:     payload,
		Metadata:    metadata,
		ScheduledAt: at,
		CreatedAt:   time.Now(),
	}

	if err := s.Schedule(ctx, msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

// ScheduleAfter schedules a message after a delay
func (s *MongoScheduler) ScheduleAfter(ctx context.Context, eventName string, payload []byte, metadata map[string]string, delay time.Duration) (string, error) {
	return s.ScheduleAt(ctx, eventName, payload, metadata, time.Now().Add(delay))
}

// Cancel cancels a scheduled message
func (s *MongoScheduler) Cancel(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	s.logger.Debug("cancelled scheduled message", "id", id)
	return nil
}

// Get retrieves a scheduled message by ID
func (s *MongoScheduler) Get(ctx context.Context, id string) (*Message, error) {
	filter := bson.M{"_id": id}

	var mongoMsg MongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("message not found: %s", id)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.ToMessage(), nil
}

// List returns scheduled messages
func (s *MongoScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	mongoFilter := bson.M{}

	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}

	if !filter.After.IsZero() {
		if mongoFilter["scheduled_at"] == nil {
			mongoFilter["scheduled_at"] = bson.M{}
		}
		mongoFilter["scheduled_at"].(bson.M)["$gte"] = filter.After
	}

	if !filter.Before.IsZero() {
		if mongoFilter["scheduled_at"] == nil {
			mongoFilter["scheduled_at"] = bson.M{}
		}
		mongoFilter["scheduled_at"].(bson.M)["$lte"] = filter.Before
	}

	opts := options.Find().SetSort(bson.D{{Key: "scheduled_at", Value: 1}})
	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []*Message
	for cursor.Next(ctx) {
		var mongoMsg MongoMessage
		if err := cursor.Decode(&mongoMsg); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		messages = append(messages, mongoMsg.ToMessage())
	}

	return messages, cursor.Err()
}

// Start begins the scheduler polling loop.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
func (s *MongoScheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()

	// Recovery ticker for stuck messages (check every minute)
	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	s.logger.Info("scheduler started",
		"poll_interval", s.opts.PollInterval,
		"batch_size", s.opts.BatchSize)

	// Recover any stuck messages at startup
	s.recoverStuck(ctx)

	for {
		select {
		case <-ctx.Done():
			close(s.stoppedCh)
			return ctx.Err()
		case <-s.stopCh:
			close(s.stoppedCh)
			return nil
		case <-ticker.C:
			s.processDue(ctx)
		case <-recoveryTicker.C:
			s.recoverStuck(ctx)
		}
	}
}

// Stop gracefully stops the scheduler
func (s *MongoScheduler) Stop(ctx context.Context) error {
	close(s.stopCh)

	select {
	case <-s.stoppedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processDue processes messages that are due for delivery.
// Uses a 2-phase approach for crash safety:
// 1. Atomically claim message (status: pending -> processing)
// 2. Publish to transport
// 3. Delete the message
// If the scheduler crashes after claiming, recoverStuck will move it back to pending.
func (s *MongoScheduler) processDue(ctx context.Context) {
	now := time.Now()

	for i := 0; i < s.opts.BatchSize; i++ {
		msg, err := s.claimDue(ctx, now)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break // No more due messages
			}
			s.logger.Error("failed to claim due message", "error", err)
			break
		}

		// Publish to transport
		if err := s.publishMessage(ctx, msg); err != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			// Message stays in processing, will be recovered by recoverStuck
			continue
		}

		// Delete the message after successful publish
		if err := s.deleteClaimed(ctx, msg.ID); err != nil {
			s.logger.Error("failed to delete published message",
				"id", msg.ID,
				"error", err)
		}

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
	}
}

// claimDue atomically claims a due message for processing.
// Uses FindOneAndUpdate to prevent race conditions in HA deployments.
func (s *MongoScheduler) claimDue(ctx context.Context, now time.Time) (*Message, error) {
	filter := bson.M{
		"scheduled_at": bson.M{"$lte": now},
		"$or": []bson.M{
			{"status": SchedulerStatusPending},
			{"status": bson.M{"$exists": false}}, // Backward compat: no status = pending
		},
	}
	claimedAt := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":     SchedulerStatusProcessing,
			"claimed_at": claimedAt,
		},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "scheduled_at", Value: 1}}).
		SetReturnDocument(options.After)

	var mongoMsg MongoMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&mongoMsg)
	if err != nil {
		return nil, err
	}

	return mongoMsg.ToMessage(), nil
}

// deleteClaimed deletes a message that was successfully published
func (s *MongoScheduler) deleteClaimed(ctx context.Context, id string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles scheduler crashes where messages were claimed but never published.
func (s *MongoScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.stuckDuration)
	filter := bson.M{
		"status":     SchedulerStatusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	update := bson.M{
		"$set": bson.M{
			"status": SchedulerStatusPending,
		},
		"$unset": bson.M{
			"claimed_at": "",
		},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		s.logger.Error("failed to recover stuck messages", "error", err)
		return
	}

	if result.ModifiedCount > 0 {
		s.logger.Warn("recovered stuck scheduled messages",
			"count", result.ModifiedCount,
			"stuck_duration", s.stuckDuration)
	}
}

// publishMessage publishes a scheduled message to the transport
func (s *MongoScheduler) publishMessage(ctx context.Context, msg *Message) error {
	// Add scheduler metadata
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["scheduled_message_id"] = msg.ID
	metadata["scheduled_at"] = msg.ScheduledAt.Format(time.RFC3339)

	// Create transport message
	transportMsg := message.New(
		msg.ID,
		"scheduler",
		msg.Payload,
		metadata,
		trace.SpanContext{},
	)

	return s.transport.Publish(ctx, msg.EventName, transportMsg)
}

// Count returns the number of scheduled messages
func (s *MongoScheduler) Count(ctx context.Context) (int64, error) {
	return s.collection.CountDocuments(ctx, bson.M{})
}

// CountByEvent returns the number of scheduled messages for a specific event
func (s *MongoScheduler) CountByEvent(ctx context.Context, eventName string) (int64, error) {
	filter := bson.M{"event_name": eventName}
	return s.collection.CountDocuments(ctx, filter)
}

// CountDue returns the number of messages due for delivery
func (s *MongoScheduler) CountDue(ctx context.Context) (int64, error) {
	filter := bson.M{
		"scheduled_at": bson.M{"$lte": time.Now()},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// DeleteOlderThan removes messages older than the specified age (already delivered would be deleted)
func (s *MongoScheduler) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	filter := bson.M{
		"created_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// Compile-time check
var _ Scheduler = (*MongoScheduler)(nil)

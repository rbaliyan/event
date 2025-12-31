package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport/codec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoDB Schema:

Collection: event_outbox

Document structure:
{
    "_id": ObjectId,
    "event_name": string,
    "event_id": string,
    "payload": Binary,
    "metadata": object,
    "created_at": ISODate,
    "published_at": ISODate (optional),
    "status": string,
    "retry_count": int,
    "last_error": string (optional)
}

Indexes:
- { "status": 1, "created_at": 1 } for pending message queries
- { "published_at": 1 } for cleanup operations

Create indexes with:
db.event_outbox.createIndex({ "status": 1, "created_at": 1 })
db.event_outbox.createIndex({ "published_at": 1 }, { sparse: true })
*/

// MongoMessage represents a message document in MongoDB
type MongoMessage struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	EventName   string             `bson:"event_name"`
	EventID     string             `bson:"event_id"`
	Payload     []byte             `bson:"payload"`
	Metadata    map[string]string  `bson:"metadata,omitempty"`
	CreatedAt   time.Time          `bson:"created_at"`
	PublishedAt *time.Time         `bson:"published_at,omitempty"`
	Status      Status             `bson:"status"`
	RetryCount  int                `bson:"retry_count"`
	LastError   string             `bson:"last_error,omitempty"`
}

// ToMessage converts MongoMessage to Message
func (m *MongoMessage) ToMessage() *Message {
	return &Message{
		ID:          m.ID.Timestamp().Unix(), // Use timestamp as int64 ID for compatibility
		EventName:   m.EventName,
		EventID:     m.EventID,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		CreatedAt:   m.CreatedAt,
		PublishedAt: m.PublishedAt,
		Status:      m.Status,
		RetryCount:  m.RetryCount,
		LastError:   m.LastError,
	}
}

// MongoStore defines the interface for MongoDB outbox storage
type MongoStore struct {
	collection *mongo.Collection
}

// NewMongoStore creates a new MongoDB outbox store
func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		collection: db.Collection("event_outbox"),
	}
}

// WithCollection sets a custom collection name
func (s *MongoStore) WithCollection(name string) *MongoStore {
	s.collection = s.collection.Database().Collection(name)
	return s
}

// Collection returns the underlying MongoDB collection
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// EnsureIndexes creates the required indexes for the outbox collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
		{
			Keys: bson.D{
				{Key: "published_at", Value: 1},
			},
			Options: options.Index().SetSparse(true),
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// InsertInSession adds a message to the outbox within a MongoDB session/transaction
func (s *MongoStore) InsertInSession(ctx context.Context, sess mongo.SessionContext, msg *MongoMessage) error {
	if msg.ID.IsZero() {
		msg.ID = primitive.NewObjectID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Status = StatusPending

	_, err := s.collection.InsertOne(sess, msg)
	return err
}

// Insert adds a message to the outbox (without transaction)
func (s *MongoStore) Insert(ctx context.Context, msg *MongoMessage) error {
	if msg.ID.IsZero() {
		msg.ID = primitive.NewObjectID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	msg.Status = StatusPending

	_, err := s.collection.InsertOne(ctx, msg)
	return err
}

// GetPending retrieves pending messages for publishing
func (s *MongoStore) GetPending(ctx context.Context, limit int) ([]*Message, error) {
	filter := bson.M{"status": StatusPending}
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: 1}}).
		SetLimit(int64(limit))

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find pending: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []*Message
	for cursor.Next(ctx) {
		var mongoMsg MongoMessage
		if err := cursor.Decode(&mongoMsg); err != nil {
			return nil, fmt.Errorf("decode message: %w", err)
		}
		messages = append(messages, mongoMsg.ToMessage())
	}

	return messages, cursor.Err()
}

// GetPendingMongo retrieves pending messages as MongoMessage for internal use
func (s *MongoStore) GetPendingMongo(ctx context.Context, limit int) ([]*MongoMessage, error) {
	filter := bson.M{"status": StatusPending}
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: 1}}).
		SetLimit(int64(limit))

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find pending: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []*MongoMessage
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, fmt.Errorf("decode messages: %w", err)
	}

	return messages, nil
}

// MarkPublished marks a message as successfully published
func (s *MongoStore) MarkPublished(ctx context.Context, id primitive.ObjectID) error {
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":       StatusPublished,
			"published_at": now,
		},
	}

	result, err := s.collection.UpdateByID(ctx, id, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id.Hex())
	}

	return nil
}

// MarkPublishedByEventID marks a message as published using the event ID
func (s *MongoStore) MarkPublishedByEventID(ctx context.Context, eventID string) error {
	now := time.Now()
	filter := bson.M{"event_id": eventID}
	update := bson.M{
		"$set": bson.M{
			"status":       StatusPublished,
			"published_at": now,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", eventID)
	}

	return nil
}

// MarkFailed marks a message as failed with an error
func (s *MongoStore) MarkFailed(ctx context.Context, id primitive.ObjectID, err error) error {
	update := bson.M{
		"$set": bson.M{
			"status":     StatusFailed,
			"last_error": err.Error(),
		},
		"$inc": bson.M{
			"retry_count": 1,
		},
	}

	result, updateErr := s.collection.UpdateByID(ctx, id, update)
	if updateErr != nil {
		return fmt.Errorf("update: %w", updateErr)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id.Hex())
	}

	return nil
}

// MarkFailedByEventID marks a message as failed using the event ID
func (s *MongoStore) MarkFailedByEventID(ctx context.Context, eventID string, err error) error {
	filter := bson.M{"event_id": eventID}
	update := bson.M{
		"$set": bson.M{
			"status":     StatusFailed,
			"last_error": err.Error(),
		},
		"$inc": bson.M{
			"retry_count": 1,
		},
	}

	result, updateErr := s.collection.UpdateOne(ctx, filter, update)
	if updateErr != nil {
		return fmt.Errorf("update: %w", updateErr)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", eventID)
	}

	return nil
}

// Delete removes old published messages
func (s *MongoStore) Delete(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	filter := bson.M{
		"status":       StatusPublished,
		"published_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// RetryFailed moves failed messages back to pending status
func (s *MongoStore) RetryFailed(ctx context.Context, maxRetries int) (int64, error) {
	filter := bson.M{
		"status":      StatusFailed,
		"retry_count": bson.M{"$lt": maxRetries},
	}
	update := bson.M{
		"$set": bson.M{
			"status": StatusPending,
		},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("update: %w", err)
	}

	return result.ModifiedCount, nil
}

// Count returns the count of messages by status
func (s *MongoStore) Count(ctx context.Context, status Status) (int64, error) {
	filter := bson.M{"status": status}
	return s.collection.CountDocuments(ctx, filter)
}

// MongoPublisher provides methods for publishing messages through the MongoDB outbox
type MongoPublisher struct {
	store  *MongoStore
	client *mongo.Client
	codec  codec.Codec
}

// NewMongoPublisher creates a new MongoDB outbox publisher
func NewMongoPublisher(client *mongo.Client, db *mongo.Database) *MongoPublisher {
	return &MongoPublisher{
		store:  NewMongoStore(db),
		client: client,
		codec:  codec.Default(),
	}
}

// WithCodec sets a custom codec for encoding payloads
func (p *MongoPublisher) WithCodec(c codec.Codec) *MongoPublisher {
	p.codec = c
	return p
}

// WithCollection sets a custom collection name
func (p *MongoPublisher) WithCollection(name string) *MongoPublisher {
	p.store = p.store.WithCollection(name)
	return p
}

// Store returns the underlying MongoStore
func (p *MongoPublisher) Store() *MongoStore {
	return p.store
}

// PublishInSession stores a message in the outbox within a MongoDB session/transaction
func (p *MongoPublisher) PublishInSession(
	ctx context.Context,
	sess mongo.SessionContext,
	eventName string,
	payload any,
	metadata map[string]string,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := &MongoMessage{
		EventName: eventName,
		EventID:   uuid.New().String(),
		Payload:   encoded,
		Metadata:  metadata,
	}

	return p.store.InsertInSession(ctx, sess, msg)
}

// PublishWithTransaction stores a message in the outbox and executes fn within a transaction
func (p *MongoPublisher) PublishWithTransaction(
	ctx context.Context,
	eventName string,
	payload any,
	metadata map[string]string,
	fn func(mongo.SessionContext) error,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := &MongoMessage{
		EventName: eventName,
		EventID:   uuid.New().String(),
		Payload:   encoded,
		Metadata:  metadata,
	}

	sess, err := p.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		// Execute user's business logic
		if fn != nil {
			if err := fn(sessCtx); err != nil {
				return nil, err
			}
		}

		// Insert outbox message
		if err := p.store.InsertInSession(ctx, sessCtx, msg); err != nil {
			return nil, fmt.Errorf("insert outbox: %w", err)
		}

		return nil, nil
	})

	return err
}

// Publish stores a message in the outbox (without transaction)
func (p *MongoPublisher) Publish(
	ctx context.Context,
	eventName string,
	payload any,
	metadata map[string]string,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := &MongoMessage{
		EventName: eventName,
		EventID:   uuid.New().String(),
		Payload:   encoded,
		Metadata:  metadata,
	}

	return p.store.Insert(ctx, msg)
}

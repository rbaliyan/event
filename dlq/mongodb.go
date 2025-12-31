package dlq

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoDB Schema:

Collection: event_dlq

Document structure:
{
    "_id": string (DLQ message ID),
    "event_name": string,
    "original_id": string,
    "payload": Binary,
    "metadata": object,
    "error": string,
    "retry_count": int,
    "source": string (optional),
    "created_at": ISODate,
    "retried_at": ISODate (optional)
}

Indexes:
db.event_dlq.createIndex({ "event_name": 1 })
db.event_dlq.createIndex({ "created_at": 1 })
db.event_dlq.createIndex({ "retried_at": 1 }, { sparse: true })
db.event_dlq.createIndex({ "event_name": 1, "created_at": 1 })
*/

// MongoMessage represents a DLQ message document in MongoDB
type MongoMessage struct {
	ID         string            `bson:"_id"`
	EventName  string            `bson:"event_name"`
	OriginalID string            `bson:"original_id"`
	Payload    []byte            `bson:"payload"`
	Metadata   map[string]string `bson:"metadata,omitempty"`
	Error      string            `bson:"error"`
	RetryCount int               `bson:"retry_count"`
	Source     string            `bson:"source,omitempty"`
	CreatedAt  time.Time         `bson:"created_at"`
	RetriedAt  *time.Time        `bson:"retried_at,omitempty"`
}

// ToMessage converts MongoMessage to Message
func (m *MongoMessage) ToMessage() *Message {
	return &Message{
		ID:         m.ID,
		EventName:  m.EventName,
		OriginalID: m.OriginalID,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		Error:      m.Error,
		RetryCount: m.RetryCount,
		Source:     m.Source,
		CreatedAt:  m.CreatedAt,
		RetriedAt:  m.RetriedAt,
	}
}

// FromMessage creates a MongoMessage from Message
func FromMessage(m *Message) *MongoMessage {
	return &MongoMessage{
		ID:         m.ID,
		EventName:  m.EventName,
		OriginalID: m.OriginalID,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		Error:      m.Error,
		RetryCount: m.RetryCount,
		Source:     m.Source,
		CreatedAt:  m.CreatedAt,
		RetriedAt:  m.RetriedAt,
	}
}

// MongoStore is a MongoDB-based DLQ store
type MongoStore struct {
	collection *mongo.Collection
}

// NewMongoStore creates a new MongoDB DLQ store
func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		collection: db.Collection("event_dlq"),
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

// EnsureIndexes creates the required indexes for the DLQ collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "event_name", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "created_at", Value: 1}},
		},
		{
			Keys:    bson.D{{Key: "retried_at", Value: 1}},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys: bson.D{
				{Key: "event_name", Value: 1},
				{Key: "created_at", Value: 1},
			},
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Store adds a message to the DLQ
func (s *MongoStore) Store(ctx context.Context, msg *Message) error {
	mongoMsg := FromMessage(msg)

	_, err := s.collection.InsertOne(ctx, mongoMsg)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("message already exists: %s", msg.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves a single message by ID
func (s *MongoStore) Get(ctx context.Context, id string) (*Message, error) {
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

// List returns messages matching the filter
func (s *MongoStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	mongoFilter := s.buildFilter(filter)

	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}
	if filter.Offset > 0 {
		opts.SetSkip(int64(filter.Offset))
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

// Count returns the number of messages matching the filter
func (s *MongoStore) Count(ctx context.Context, filter Filter) (int64, error) {
	mongoFilter := s.buildFilter(filter)
	return s.collection.CountDocuments(ctx, mongoFilter)
}

// buildFilter creates a MongoDB filter from DLQ Filter
func (s *MongoStore) buildFilter(filter Filter) bson.M {
	mongoFilter := bson.M{}

	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}

	if !filter.StartTime.IsZero() {
		if mongoFilter["created_at"] == nil {
			mongoFilter["created_at"] = bson.M{}
		}
		mongoFilter["created_at"].(bson.M)["$gte"] = filter.StartTime
	}

	if !filter.EndTime.IsZero() {
		if mongoFilter["created_at"] == nil {
			mongoFilter["created_at"] = bson.M{}
		}
		mongoFilter["created_at"].(bson.M)["$lte"] = filter.EndTime
	}

	if filter.Error != "" {
		mongoFilter["error"] = primitive.Regex{Pattern: filter.Error, Options: "i"}
	}

	if filter.MaxRetries > 0 {
		mongoFilter["retry_count"] = bson.M{"$lte": filter.MaxRetries}
	}

	if filter.Source != "" {
		mongoFilter["source"] = filter.Source
	}

	if filter.ExcludeRetried {
		mongoFilter["retried_at"] = nil
	}

	return mongoFilter
}

// MarkRetried marks a message as replayed
func (s *MongoStore) MarkRetried(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}
	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"retried_at": now,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// Delete removes a message from the DLQ
func (s *MongoStore) Delete(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *MongoStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
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

// DeleteByFilter removes messages matching the filter
func (s *MongoStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	mongoFilter := s.buildFilter(filter)

	result, err := s.collection.DeleteMany(ctx, mongoFilter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// Stats returns DLQ statistics
func (s *MongoStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count
	total, err := s.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}
	stats.TotalMessages = total

	// Pending count (not retried)
	pending, err := s.collection.CountDocuments(ctx, bson.M{"retried_at": nil})
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}
	stats.PendingMessages = pending
	stats.RetriedMessages = total - pending

	// Count by event using aggregation
	eventPipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":   "$event_name",
			"count": bson.M{"$sum": 1},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, eventPipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate by event: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var result struct {
			EventName string `bson:"_id"`
			Count     int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		stats.MessagesByEvent[result.EventName] = result.Count
	}

	// Find oldest and newest
	oldestOpts := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: 1}})
	var oldest MongoMessage
	err = s.collection.FindOne(ctx, bson.M{}, oldestOpts).Decode(&oldest)
	if err == nil {
		stats.OldestMessage = &oldest.CreatedAt
	}

	newestOpts := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: -1}})
	var newest MongoMessage
	err = s.collection.FindOne(ctx, bson.M{}, newestOpts).Decode(&newest)
	if err == nil {
		stats.NewestMessage = &newest.CreatedAt
	}

	return stats, nil
}

// GetByOriginalID retrieves a message by its original message ID
func (s *MongoStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	filter := bson.M{"original_id": originalID}

	var mongoMsg MongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("message not found with original_id: %s", originalID)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.ToMessage(), nil
}

// GetByEventName retrieves all messages for a specific event
func (s *MongoStore) GetByEventName(ctx context.Context, eventName string, limit int) ([]*Message, error) {
	return s.List(ctx, Filter{
		EventName: eventName,
		Limit:     limit,
	})
}

// IncrementRetryCount increments the retry count for a message
func (s *MongoStore) IncrementRetryCount(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}
	update := bson.M{
		"$inc": bson.M{"retry_count": 1},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("message not found: %s", id)
	}

	return nil
}

// Compile-time checks
var _ Store = (*MongoStore)(nil)
var _ StatsProvider = (*MongoStore)(nil)

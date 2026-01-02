package dlq

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// isNamespaceNotFoundError checks if the error is a MongoDB namespace not found error.
// This occurs when querying collection stats for a non-existent collection.
func isNamespaceNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "ns not found") ||
		strings.Contains(errStr, "NamespaceNotFound") ||
		strings.Contains(errStr, "Collection") && strings.Contains(errStr, "not found")
}

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

// CappedInfo contains information about a capped collection
type CappedInfo struct {
	Capped   bool  // Whether the collection is capped
	Size     int64 // Maximum size in bytes
	MaxDocs  int64 // Maximum number of documents (0 = unlimited)
	StorSize int64 // Current storage size in bytes
	Count    int64 // Current document count
}

// MongoStore is a MongoDB-based DLQ store
type MongoStore struct {
	collection *mongo.Collection
	cappedInfo *CappedInfo // Cached capped info (nil = not checked yet)
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

// Indexes returns the required indexes for the DLQ collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
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
}

// EnsureIndexes creates the required indexes for the DLQ collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// IsCapped returns whether the collection is a capped collection.
// The result is cached after the first call.
func (s *MongoStore) IsCapped(ctx context.Context) (bool, error) {
	info, err := s.GetCappedInfo(ctx)
	if err != nil {
		return false, err
	}
	return info.Capped, nil
}

// GetCappedInfo returns detailed information about the collection's capped status.
// The result is cached after the first call.
func (s *MongoStore) GetCappedInfo(ctx context.Context) (*CappedInfo, error) {
	if s.cappedInfo != nil {
		return s.cappedInfo, nil
	}

	info, err := s.fetchCappedInfo(ctx)
	if err != nil {
		return nil, err
	}

	s.cappedInfo = info
	return info, nil
}

// RefreshCappedInfo forces a refresh of the cached capped collection info.
func (s *MongoStore) RefreshCappedInfo(ctx context.Context) (*CappedInfo, error) {
	s.cappedInfo = nil
	return s.GetCappedInfo(ctx)
}

// fetchCappedInfo queries MongoDB for collection stats to determine if capped.
func (s *MongoStore) fetchCappedInfo(ctx context.Context) (*CappedInfo, error) {
	var result bson.M
	err := s.collection.Database().RunCommand(ctx, bson.D{
		{Key: "collStats", Value: s.collection.Name()},
	}).Decode(&result)

	if err != nil {
		// Collection might not exist yet - treat as non-capped
		// MongoDB returns "ns not found" or similar for missing collections
		if isNamespaceNotFoundError(err) {
			return &CappedInfo{Capped: false}, nil
		}
		return nil, fmt.Errorf("collStats: %w", err)
	}

	info := &CappedInfo{}

	if capped, ok := result["capped"].(bool); ok {
		info.Capped = capped
	}
	if size, ok := result["maxSize"].(int64); ok {
		info.Size = size
	} else if size, ok := result["maxSize"].(int32); ok {
		info.Size = int64(size)
	}
	if maxDocs, ok := result["max"].(int64); ok {
		info.MaxDocs = maxDocs
	} else if maxDocs, ok := result["max"].(int32); ok {
		info.MaxDocs = int64(maxDocs)
	}
	if storSize, ok := result["storageSize"].(int64); ok {
		info.StorSize = storSize
	} else if storSize, ok := result["storageSize"].(int32); ok {
		info.StorSize = int64(storSize)
	}
	if count, ok := result["count"].(int64); ok {
		info.Count = count
	} else if count, ok := result["count"].(int32); ok {
		info.Count = int64(count)
	}

	return info, nil
}

// CreateCapped creates the collection as a capped collection.
// This must be called before any documents are inserted.
// Returns an error if the collection already exists.
//
// Parameters:
//   - sizeBytes: Maximum size of the collection in bytes (required, minimum 4096)
//   - maxDocs: Maximum number of documents (0 = no limit, only size matters)
//
// Example:
//
//	// Create 100MB capped collection for DLQ
//	err := store.CreateCapped(ctx, 100*1024*1024, 0)
//
//	// Create capped collection with max 50000 DLQ messages
//	err := store.CreateCapped(ctx, 100*1024*1024, 50000)
func (s *MongoStore) CreateCapped(ctx context.Context, sizeBytes int64, maxDocs int64) error {
	opts := options.CreateCollection().SetCapped(true).SetSizeInBytes(sizeBytes)
	if maxDocs > 0 {
		opts.SetMaxDocuments(maxDocs)
	}

	err := s.collection.Database().CreateCollection(ctx, s.collection.Name(), opts)
	if err != nil {
		return fmt.Errorf("create capped collection: %w", err)
	}

	// Refresh cached info
	s.cappedInfo = nil

	return nil
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

// Delete removes a message from the DLQ.
// For capped collections, this returns an error since deletion is not supported.
func (s *MongoStore) Delete(ctx context.Context, id string) error {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return fmt.Errorf("check capped: %w", err)
	}
	if capped {
		return fmt.Errorf("delete not supported on capped collection")
	}

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

// DeleteOlderThan removes messages older than the specified age.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
func (s *MongoStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

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

// DeleteByFilter removes messages matching the filter.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
func (s *MongoStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

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

package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/transport/codec"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Compile-time check that MongoStore implements event.OutboxStore
var _ event.OutboxStore = (*MongoStore)(nil)

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
	ClaimedAt   *time.Time         `bson:"claimed_at,omitempty"` // When claimed for processing (HA)
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

// CappedInfo contains information about a capped collection
type CappedInfo struct {
	Capped   bool  // Whether the collection is capped
	Size     int64 // Maximum size in bytes
	MaxDocs  int64 // Maximum number of documents (0 = unlimited)
	StorSize int64 // Current storage size in bytes
	Count    int64 // Current document count
}

// MongoStore defines the interface for MongoDB outbox storage
type MongoStore struct {
	collection *mongo.Collection
	cappedInfo *CappedInfo // Cached capped info (nil = not checked yet)
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

// Indexes returns the required indexes for the outbox collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
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
}

// EnsureIndexes creates the required indexes for the outbox collection
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
//	// Create 100MB capped collection
//	err := store.CreateCapped(ctx, 100*1024*1024, 0)
//
//	// Create capped collection with max 10000 documents
//	err := store.CreateCapped(ctx, 100*1024*1024, 10000)
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

// Store implements event.OutboxStore interface.
// It stores a message in the outbox within the active transaction.
// The transaction session is extracted from the context using event.OutboxTx().
//
// If the context contains a mongo.SessionContext (via event.WithOutboxTx),
// the message is stored within that session's transaction.
// Otherwise, it stores without a transaction (for testing or non-transactional use).
//
// Example:
//
//	sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
//	    ctx := event.WithOutboxTx(sessCtx, sessCtx)
//	    // ... business logic ...
//	    return nil, orderEvent.Publish(ctx, order)  // Routed to Store()
//	})
func (s *MongoStore) Store(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	msg := &MongoMessage{
		ID:        primitive.NewObjectID(),
		EventName: eventName,
		EventID:   eventID,
		Payload:   payload,
		Metadata:  metadata,
		CreatedAt: time.Now(),
		Status:    StatusPending,
	}

	// Check if we're inside a transaction
	if session := event.OutboxTx(ctx); session != nil {
		if sessCtx, ok := session.(mongo.SessionContext); ok {
			_, err := s.collection.InsertOne(sessCtx, msg)
			return err
		}
	}

	// Fallback to non-transactional insert
	_, err := s.collection.InsertOne(ctx, msg)
	return err
}

// GetPending retrieves pending messages for publishing with atomic claiming.
// Uses FindOneAndUpdate to atomically claim messages, preventing duplicate
// processing by multiple relay instances in HA deployments.
func (s *MongoStore) GetPending(ctx context.Context, limit int) ([]*Message, error) {
	var messages []*Message

	for i := 0; i < limit; i++ {
		msg, err := s.claimNextPending(ctx)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break // No more pending messages
			}
			return messages, fmt.Errorf("claim pending: %w", err)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// claimNextPending atomically claims a single pending message for processing.
// Uses FindOneAndUpdate to prevent race conditions in HA deployments.
func (s *MongoStore) claimNextPending(ctx context.Context) (*Message, error) {
	filter := bson.M{"status": StatusPending}
	update := bson.M{
		"$set": bson.M{
			"status":     StatusProcessing,
			"claimed_at": time.Now(),
		},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "created_at", Value: 1}}).
		SetReturnDocument(options.After)

	var mongoMsg MongoMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&mongoMsg)
	if err != nil {
		return nil, err
	}

	return mongoMsg.ToMessage(), nil
}

// GetPendingMongo retrieves pending messages as MongoMessage with atomic claiming.
// Uses FindOneAndUpdate to atomically claim messages, preventing duplicate
// processing by multiple relay instances in HA deployments.
func (s *MongoStore) GetPendingMongo(ctx context.Context, limit int) ([]*MongoMessage, error) {
	var messages []*MongoMessage

	for i := 0; i < limit; i++ {
		msg, err := s.claimNextPendingMongo(ctx)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break // No more pending messages
			}
			return messages, fmt.Errorf("claim pending: %w", err)
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// claimNextPendingMongo atomically claims a single pending message for processing.
func (s *MongoStore) claimNextPendingMongo(ctx context.Context) (*MongoMessage, error) {
	filter := bson.M{"status": StatusPending}
	update := bson.M{
		"$set": bson.M{
			"status":     StatusProcessing,
			"claimed_at": time.Now(),
		},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "created_at", Value: 1}}).
		SetReturnDocument(options.After)

	var mongoMsg MongoMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&mongoMsg)
	if err != nil {
		return nil, err
	}

	return &mongoMsg, nil
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

// Delete removes old published messages.
// For capped collections, this is a no-op since MongoDB handles cleanup automatically.
// Returns 0 for capped collections without error.
func (s *MongoStore) Delete(ctx context.Context, olderThan time.Duration) (int64, error) {
	// Check if capped - deletion not allowed on capped collections
	capped, err := s.IsCapped(ctx)
	if err != nil {
		return 0, fmt.Errorf("check capped: %w", err)
	}
	if capped {
		// Capped collections auto-cleanup, deletion not needed
		return 0, nil
	}

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

// RecoverStuck moves messages stuck in "processing" status back to pending.
// This handles relay crashes where a message was claimed but never published.
// Should be called periodically (e.g., every minute) or at relay startup.
//
// Parameters:
//   - ctx: Context for cancellation
//   - stuckDuration: How long a message must be in processing to be considered stuck
//
// Returns the number of recovered messages.
func (s *MongoStore) RecoverStuck(ctx context.Context, stuckDuration time.Duration) (int64, error) {
	cutoff := time.Now().Add(-stuckDuration)
	filter := bson.M{
		"status":     StatusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	update := bson.M{
		"$set": bson.M{
			"status": StatusPending,
		},
		"$unset": bson.M{
			"claimed_at": "",
		},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, fmt.Errorf("recover stuck: %w", err)
	}

	return result.ModifiedCount, nil
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

// Transaction executes the given function within a MongoDB transaction.
// The context passed to fn contains the transaction session via event.WithOutboxTx,
// so any event.Publish() calls within fn will automatically route to the outbox.
//
// If fn returns an error, the transaction is rolled back.
// If fn returns nil, the transaction is committed.
//
// Example:
//
//	store := outbox.NewMongoStore(db)
//	bus, _ := event.NewBus("mybus", event.WithTransport(t), event.WithOutbox(store))
//	orderEvent := event.New[Order]("order.created")
//	event.Register(ctx, bus, orderEvent)
//
//	err := outbox.Transaction(ctx, mongoClient, func(ctx context.Context) error {
//	    // Business logic - uses the transaction context
//	    _, err := ordersCol.InsertOne(ctx, order)
//	    if err != nil {
//	        return err
//	    }
//
//	    // This automatically goes to the outbox (same transaction)
//	    return orderEvent.Publish(ctx, order)
//	})
func Transaction(ctx context.Context, client *mongo.Client, fn func(ctx context.Context) error) error {
	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		// Wrap the session context with outbox transaction marker
		txCtx := event.WithOutboxTx(sessCtx, sessCtx)
		return nil, fn(txCtx)
	})

	return err
}

// TransactionWithOptions executes the given function within a MongoDB transaction
// with custom transaction options.
//
// Example:
//
//	opts := options.Transaction().SetReadConcern(readconcern.Snapshot())
//	err := outbox.TransactionWithOptions(ctx, mongoClient, opts, func(ctx context.Context) error {
//	    // Business logic...
//	    return orderEvent.Publish(ctx, order)
//	})
func TransactionWithOptions(ctx context.Context, client *mongo.Client, opts *options.TransactionOptions, fn func(ctx context.Context) error) error {
	sess, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer sess.EndSession(ctx)

	_, err = sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		// Wrap the session context with outbox transaction marker
		txCtx := event.WithOutboxTx(sessCtx, sessCtx)
		return nil, fn(txCtx)
	}, opts)

	return err
}

package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	// defaultStateCollection is the default MongoDB collection name for state storage.
	defaultStateCollection = "_message_state"

	// MongoDB document status values.
	statusProcessing = "processing"
	statusCompleted  = "completed"
	statusReleased   = "released"
)

// stateDocument represents a state record in MongoDB.
type stateDocument struct {
	ID        string            `bson:"_id"`
	Status    string            `bson:"status"`
	WorkerID  string            `bson:"worker_id,omitempty"`
	ExpiresAt time.Time         `bson:"expires_at"`
	CreatedAt time.Time         `bson:"created_at"`
	UpdatedAt time.Time         `bson:"updated_at"`
	Payload   []byte            `bson:"payload,omitempty"`
	Metadata  map[string]string `bson:"metadata,omitempty"`
	EventName string            `bson:"event_name,omitempty"`
}

// MongoStateManager implements Coordinator and PayloadStore using MongoDB for distributed deployments.
//
// MongoStateManager uses MongoDB's atomic findOneAndUpdate with conditional filters
// for race-condition-free message state management. This is ideal when you're already
// using MongoDB (e.g., with the MongoDB Change Streams transport) and don't
// want to introduce Redis as an additional dependency.
//
// Design Philosophy:
//
// This implementation uses database atomic operations, not distributed locks:
//   - findOneAndUpdate with upsert provides atomic state transitions
//   - TTL indexes provide automatic cleanup of expired states
//   - No separate lock acquisition/release - state IS the coordination mechanism
//
// Features:
//   - Atomic state acquisition using findOneAndUpdate with upsert
//   - Automatic expiration using MongoDB TTL indexes
//   - Configurable collection for multi-tenant deployments
//   - Optional capped collection for high-throughput scenarios
//   - Supports MongoDB replica sets and sharded clusters
//
// MongoDB Collection:
//
// States are stored in a collection (default: "_message_state") with documents:
//
//	{
//	    "_id": "msg-123",           // Message ID
//	    "status": "processing",     // "processing" or "completed"
//	    "worker_id": "a1b2c3...",   // Unique per Acquire call
//	    "expires_at": ISODate(...), // TTL expiration
//	    "created_at": ISODate(...),
//	    "updated_at": ISODate(...)
//	}
//
// TTL Index:
//
// For automatic cleanup, create a TTL index on the collection:
//
//	db.collection.createIndex({"expires_at": 1}, {expireAfterSeconds: 0})
//
// Or call EnsureIndexes() after creating the state manager.
//
// Capped Collection Mode:
//
// For high-throughput scenarios, you can use a capped collection:
//
//	sm := distributed.NewMongoStateManager(db,
//	    distributed.WithCollection("state_buffer"),
//	    distributed.WithCapped(100*1024*1024, 100000), // 100MB, 100k docs max
//	)
//	sm.CreateCollection(ctx) // Creates capped collection
//
// IMPORTANT: Capped collections have limitations:
//   - Reset() is a no-op (MongoDB doesn't allow deletes in capped collections)
//   - No TTL index support (cleanup is by size/count, not time)
//   - Failed states must wait for natural expiration or size-based removal
//
// Example:
//
//	// Basic setup - uses "_message_state" collection
//	sm := distributed.NewMongoStateManager(db)
//	sm.EnsureIndexes(ctx)
//
//	// With custom collection
//	sm := distributed.NewMongoStateManager(db,
//	    distributed.WithCollection("my_states"),
//	)
//
//	// With capped collection for high throughput
//	sm := distributed.NewMongoStateManager(db,
//	    distributed.WithCollection("state_buffer"),
//	    distributed.WithCapped(100*1024*1024, 0), // 100MB, unlimited docs
//	)
//	sm.CreateCollection(ctx)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, 5*time.Minute),
//	    ),
//	)
type MongoStateManager struct {
	collection    *mongo.Collection
	completionTTL time.Duration
	capped        bool
	cappedSize    int64 // Size in bytes
	cappedMaxDocs int64 // Max documents (0 = unlimited)
}

// NewMongoStateManager creates a new MongoDB-based state manager.
//
// The state manager uses MongoDB's findOneAndUpdate for atomic state acquisition,
// which prevents race conditions between workers.
//
// Parameters:
//   - db: A connected MongoDB database
//   - opts: Optional configuration (WithCollection, WithCapped, WithCompletedTTL)
//
// Returns a configured MongoStateManager ready for use.
//
// Example:
//
//	// Simple setup - uses "_message_state" collection
//	sm := distributed.NewMongoStateManager(db)
//
//	// With custom collection
//	sm := distributed.NewMongoStateManager(db,
//	    distributed.WithCollection("worker_state"),
//	)
//
//	// Don't forget to create indexes for TTL cleanup
//	sm.EnsureIndexes(ctx)
func NewMongoStateManager(db *mongo.Database, opts ...Option) *MongoStateManager {
	o := defaultStateOptions()
	for _, opt := range opts {
		opt(o)
	}

	collName := defaultStateCollection
	if o.collectionName != "" {
		collName = o.collectionName
	}

	return &MongoStateManager{
		collection:    db.Collection(collName),
		completionTTL: o.completionTTL,
		capped:        o.capped,
		cappedSize:    o.cappedSize,
		cappedMaxDocs: o.cappedMaxDocs,
	}
}

// CreateCollection creates the state collection.
//
// For capped collections, this creates a capped collection with the
// configured size and max documents. For regular collections, this
// is a no-op (MongoDB creates collections automatically on first write).
//
// This method should be called once during application startup.
//
// Example:
//
//	sm := distributed.NewMongoStateManager(db,
//	    distributed.WithCapped(100*1024*1024, 0),
//	)
//	if err := sm.CreateCollection(ctx); err != nil {
//	    log.Fatal("failed to create collection:", err)
//	}
func (s *MongoStateManager) CreateCollection(ctx context.Context) error {
	if !s.capped {
		// Regular collections are created automatically
		return nil
	}

	// Create capped collection
	opts := options.CreateCollection().
		SetCapped(true).
		SetSizeInBytes(s.cappedSize)

	if s.cappedMaxDocs > 0 {
		opts.SetMaxDocuments(s.cappedMaxDocs)
	}

	err := s.collection.Database().CreateCollection(ctx, s.collection.Name(), opts)
	if err != nil {
		// Ignore "collection already exists" error
		if !mongo.IsDuplicateKeyError(err) && !isNamespaceExistsError(err) {
			return fmt.Errorf("create capped collection: %w", err)
		}
	}

	return nil
}

// isNamespaceExistsError checks if the error indicates collection already exists.
func isNamespaceExistsError(err error) bool {
	if err == nil {
		return false
	}
	// MongoDB error code 48 = NamespaceExists
	if cmdErr, ok := err.(mongo.CommandError); ok {
		return cmdErr.Code == 48
	}
	return false
}

// generateWorkerID creates a unique identifier for an Acquire call.
func generateWorkerID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Acquire atomically transitions a message to "processing" state using MongoDB findOneAndUpdate.
//
// The transition is atomic: the update only succeeds if:
//   - The document doesn't exist (new state), OR
//   - The existing state has expired (TTL passed)
//
// Each Acquire call generates a unique worker_id stored in the document.
// After the atomic update, the returned document's worker_id is compared
// against the caller's to confirm ownership (deterministic, no timing races).
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to acquire
//   - ttl: How long to hold the state
//
// Returns:
//   - (true, nil): Acquisition succeeded, process the message
//   - (false, nil): Already acquired (active state exists), skip the message
//   - (false, error): MongoDB error occurred
func (s *MongoStateManager) Acquire(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(ttl)
	workerID := generateWorkerID()

	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}},
			{"status": bson.M{"$exists": false}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"status":     statusProcessing,
			"worker_id":  workerID,
			"expires_at": expiresAt,
			"updated_at": now,
		},
		"$setOnInsert": bson.M{
			"created_at": now,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result stateDocument
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		if errors.Is(err, mongo.ErrNoDocuments) {
			// ErrNoDocuments with upsert=true means the filter didn't match
			// (active non-expired state exists) AND a concurrent insert on
			// the _id unique index prevented the upsert. Verify the existing
			// state is still active, otherwise retry with a direct insert.
			var existing stateDocument
			findErr := s.collection.FindOne(ctx, bson.M{"_id": messageID}).Decode(&existing)
			if findErr == nil && existing.ExpiresAt.After(now) {
				return false, nil
			}
			return s.tryInsert(ctx, messageID, ttl, workerID)
		}
		return false, fmt.Errorf("mongodb find and update: %w", err)
	}

	// Check worker_id to confirm we are the one who acquired the state
	return result.WorkerID == workerID, nil
}

// MarkProcessed transitions a message to "completed" state.
//
// Updates the state status to "completed" and extends the expiry to completionTTL.
// This prevents the message from being reprocessed if delivered again
// within the completion window.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message that was successfully processed
//
// Returns nil on success, error if MongoDB operation fails.
func (s *MongoStateManager) MarkProcessed(ctx context.Context, messageID string) error {
	now := time.Now()

	filter := bson.M{"_id": messageID}
	update := bson.M{
		"$set": bson.M{
			"status":     statusCompleted,
			"expires_at": now.Add(s.completionTTL),
			"updated_at": now,
		},
	}

	_, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongodb update: %w", err)
	}

	return nil
}

// Reset removes the message state to allow immediate reacquisition.
//
// For regular collections: Deletes the state document so another worker
// can acquire the message immediately instead of waiting for TTL expiration.
//
// For capped collections: This is a no-op because MongoDB doesn't allow
// deletes in capped collections. The state will remain until it expires
// naturally or is removed by size-based cleanup.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to reset
//
// Returns nil on success (including when document doesn't exist), error if MongoDB fails.
func (s *MongoStateManager) Reset(ctx context.Context, messageID string) error {
	// Capped collections don't support deletes
	if s.capped {
		// Update status to "released" so Acquire can reacquire immediately
		// (by treating "released" as expired)
		now := time.Now()
		filter := bson.M{"_id": messageID}
		update := bson.M{
			"$set": bson.M{
				"status":     statusReleased,
				"expires_at": now, // Set to now so it's immediately reacquirable
				"updated_at": now,
			},
		}
		_, err := s.collection.UpdateOne(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("mongodb update (reset): %w", err)
		}
		return nil
	}

	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": messageID})
	if err != nil {
		return fmt.Errorf("mongodb delete: %w", err)
	}

	return nil
}

// ListStale returns message IDs of states that have been processing
// for longer than staleTimeout.
//
// This enables active recovery: detecting crashed workers faster than
// waiting for TTL expiration.
//
// MongoDB query:
//
//	find({status: "processing", updated_at: {$lt: now - staleTimeout}})
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to return (0 = no limit)
//
// Returns list of message IDs that are stale.
func (s *MongoStateManager) ListStale(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	cutoff := time.Now().Add(-staleTimeout)

	filter := bson.M{
		"status":     statusProcessing,
		"updated_at": bson.M{"$lt": cutoff},
	}

	opts := options.Find().SetProjection(bson.M{"_id": 1})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb find stale: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var stale []string
	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		stale = append(stale, doc.ID)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb cursor: %w", err)
	}

	return stale, nil
}

// ResetStale resets all stale states, allowing them to be reacquired.
//
// This is a convenience method that combines ListStale and Reset.
// It's useful for batch cleanup of stale states.
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to reset (0 = no limit)
//
// Returns the number of states reset.
func (s *MongoStateManager) ResetStale(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	// Use ListStale to get the bounded set of stale IDs, then reset them.
	// This ensures the limit is always respected (MongoDB's DeleteMany and
	// UpdateMany do not natively support a limit clause).
	stale, err := s.ListStale(ctx, staleTimeout, limit)
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}

	filter := bson.M{"_id": bson.M{"$in": stale}}

	if s.capped {
		// Capped collections don't support deletes, update status instead
		now := time.Now()
		update := bson.M{
			"$set": bson.M{
				"status":     statusReleased,
				"expires_at": now,
				"updated_at": now,
			},
		}
		result, err := s.collection.UpdateMany(ctx, filter, update)
		if err != nil {
			return 0, fmt.Errorf("mongodb update stale: %w", err)
		}
		return result.ModifiedCount, nil
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("mongodb delete stale: %w", err)
	}
	return result.DeletedCount, nil
}

// tryInsert attempts a direct insert when findOneAndUpdate fails.
func (s *MongoStateManager) tryInsert(ctx context.Context, messageID string, ttl time.Duration, workerID string) (bool, error) {
	now := time.Now()

	doc := stateDocument{
		ID:        messageID,
		Status:    statusProcessing,
		WorkerID:  workerID,
		ExpiresAt: now.Add(ttl),
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err := s.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}
		return false, fmt.Errorf("mongodb insert: %w", err)
	}

	return true, nil
}

// StorePayload persists message payload alongside state for recovery re-publishing.
// Uses $set to atomically update the same document that Acquire created.
func (s *MongoStateManager) StorePayload(ctx context.Context, messageID string, data *MessageData) error {
	if data == nil || len(data.Payload) == 0 {
		return nil
	}

	setFields := bson.M{
		"payload": data.Payload,
	}
	if len(data.Metadata) > 0 {
		setFields["metadata"] = data.Metadata
	}
	if data.EventName != "" {
		setFields["event_name"] = data.EventName
	}

	_, err := s.collection.UpdateOne(ctx, bson.M{"_id": messageID}, bson.M{"$set": setFields})
	if err != nil {
		return fmt.Errorf("mongodb store payload: %w", err)
	}
	return nil
}

// LoadStalePayloads returns stale messages that have stored payload.
func (s *MongoStateManager) LoadStalePayloads(ctx context.Context, staleTimeout time.Duration, limit int) ([]*StaleMessage, error) {
	cutoff := time.Now().Add(-staleTimeout)

	// Only return entries that have payload stored
	filter := bson.M{
		"status":     statusProcessing,
		"updated_at": bson.M{"$lt": cutoff},
		"payload":    bson.M{"$exists": true},
	}

	opts := options.Find()
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb find stale payloads: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var results []*StaleMessage
	for cursor.Next(ctx) {
		var doc stateDocument
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		results = append(results, &StaleMessage{
			MessageID: doc.ID,
			Data: MessageData{
				Payload:   doc.Payload,
				Metadata:  doc.Metadata,
				EventName: doc.EventName,
			},
			CreatedAt: doc.CreatedAt,
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb cursor: %w", err)
	}

	return results, nil
}

// ClearPayload removes stored payload for a message.
func (s *MongoStateManager) ClearPayload(ctx context.Context, messageID string) error {
	_, err := s.collection.UpdateOne(ctx, bson.M{"_id": messageID}, bson.M{
		"$unset": bson.M{
			"payload":    "",
			"metadata":   "",
			"event_name": "",
		},
	})
	if err != nil {
		return fmt.Errorf("mongodb clear payload: %w", err)
	}
	return nil
}

// EnsureIndexes creates the necessary indexes for the state collection.
//
// For regular collections: Creates a TTL index on expires_at for automatic
// cleanup of expired states.
//
// For capped collections: TTL indexes are not supported, so this only creates
// a regular index on expires_at for query performance. Cleanup is handled by
// the capped collection's size-based removal.
//
// This should be called once during application startup.
//
// Example:
//
//	sm := distributed.NewMongoStateManager(db)
//	if err := sm.EnsureIndexes(ctx); err != nil {
//	    log.Fatal("failed to create indexes:", err)
//	}
func (s *MongoStateManager) EnsureIndexes(ctx context.Context) error {
	indexes := s.Indexes()
	if len(indexes) == 0 {
		return nil
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Indexes returns the index models for the state collection.
//
// For regular collections: Returns a TTL index for automatic expiration.
// For capped collections: Returns a regular index (no TTL support in capped).
//
// Use this if you prefer to create indexes yourself or need to inspect them.
func (s *MongoStateManager) Indexes() []mongo.IndexModel {
	if s.capped {
		// Capped collections don't support TTL indexes
		// Return a regular index for query performance
		return []mongo.IndexModel{
			{
				Keys: bson.D{{Key: "expires_at", Value: 1}},
			},
			{
				Keys: bson.D{{Key: "status", Value: 1}},
			},
		}
	}

	// Regular collection with TTL index and compound index for stale detection
	return []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "updated_at", Value: 1},
			},
		},
	}
}

// Compile-time interface checks
var (
	_ Coordinator   = (*MongoStateManager)(nil)
	_ PayloadStore  = (*MongoStateManager)(nil)
	_ StaleResetter = (*MongoStateManager)(nil)
)

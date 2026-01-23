package distributed

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Default collection name for state storage
const DefaultStateCollection = "_message_state"

// stateDocument represents a state record in MongoDB.
type stateDocument struct {
	ID        string    `bson:"_id"`
	Status    string    `bson:"status"`
	ExpiresAt time.Time `bson:"expires_at"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// MongoStateManager implements StateManager using MongoDB for distributed deployments.
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
//   - Configurable database and collection for multi-tenant deployments
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
//	sm := distributed.NewMongoStateManager(db).
//	    WithCapped(100*1024*1024, 100000) // 100MB, 100k docs max
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
//	sm := distributed.NewMongoStateManager(db).
//	    WithCollection("my_states")
//
//	// With custom database and collection
//	sm := distributed.NewMongoStateManager(db).
//	    WithDatabase(client.Database("other_db")).
//	    WithCollection("my_states")
//
//	// With capped collection for high throughput
//	sm := distributed.NewMongoStateManager(db).
//	    WithCollection("state_buffer").
//	    WithCapped(100*1024*1024, 0) // 100MB, unlimited docs
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
//
// Returns a configured MongoStateManager ready for use.
// Use WithDatabase() and WithCollection() to customize storage location.
//
// Example:
//
//	// Simple setup - uses "_message_state" collection
//	sm := distributed.NewMongoStateManager(db)
//
//	// With custom collection
//	sm := distributed.NewMongoStateManager(db).
//	    WithCollection("worker_state")
//
//	// Don't forget to create indexes for TTL cleanup
//	sm.EnsureIndexes(ctx)
func NewMongoStateManager(db *mongo.Database) *MongoStateManager {
	opts := defaultStateOptions()

	return &MongoStateManager{
		collection:    db.Collection(DefaultStateCollection),
		completionTTL: opts.completionTTL,
	}
}

// WithDatabase sets a different database for state storage.
//
// Use this when you want to store states in a different database than
// the one used for your main application data.
//
// Example:
//
//	sm := distributed.NewMongoStateManager(appDB).
//	    WithDatabase(client.Database("state_db"))
func (s *MongoStateManager) WithDatabase(db *mongo.Database) *MongoStateManager {
	s.collection = db.Collection(s.collection.Name())
	return s
}

// WithCollection sets a custom collection name for state storage.
//
// Default: "_message_state"
//
// Example:
//
//	sm := distributed.NewMongoStateManager(db).
//	    WithCollection("worker_state")
func (s *MongoStateManager) WithCollection(name string) *MongoStateManager {
	s.collection = s.collection.Database().Collection(name)
	return s
}

// WithCompletedTTL sets how long to remember completed messages.
//
// After a message is completed, its ID is remembered for this duration
// to prevent reprocessing if the same message is delivered again.
//
// Default: 24 hours
func (s *MongoStateManager) WithCompletedTTL(ttl time.Duration) *MongoStateManager {
	if ttl > 0 {
		s.completionTTL = ttl
	}
	return s
}

// Collection returns the underlying MongoDB collection.
func (s *MongoStateManager) Collection() *mongo.Collection {
	return s.collection
}

// WithCapped enables capped collection mode for high-throughput scenarios.
//
// Capped collections are fixed-size collections that automatically remove
// the oldest documents when the size limit is reached. This provides:
//   - Very high write throughput
//   - Automatic cleanup without TTL indexes
//   - Predictable storage size
//
// Parameters:
//   - sizeBytes: Maximum collection size in bytes (required, minimum 4096)
//   - maxDocs: Maximum number of documents (0 = unlimited, size-based only)
//
// IMPORTANT LIMITATIONS:
//   - Reset() becomes a no-op (MongoDB doesn't allow deletes in capped collections)
//   - No TTL index support (EnsureIndexes skips TTL index for capped collections)
//   - Failed states wait for size-based removal, not time-based expiration
//   - Updates cannot increase document size
//
// After calling WithCapped(), you must call CreateCollection() to create
// the capped collection before using the state manager.
//
// Example:
//
//	sm := distributed.NewMongoStateManager(db).
//	    WithCollection("state_buffer").
//	    WithCapped(100*1024*1024, 100000) // 100MB, max 100k docs
//	sm.CreateCollection(ctx) // Creates the capped collection
func (s *MongoStateManager) WithCapped(sizeBytes int64, maxDocs int64) *MongoStateManager {
	s.capped = true
	s.cappedSize = sizeBytes
	s.cappedMaxDocs = maxDocs
	return s
}

// IsCapped returns true if capped collection mode is enabled.
func (s *MongoStateManager) IsCapped() bool {
	return s.capped
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
//	sm := distributed.NewMongoStateManager(db).
//	    WithCapped(100*1024*1024, 0)
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

// Acquire atomically transitions a message to "processing" state using MongoDB findOneAndUpdate.
//
// The transition is atomic: the update only succeeds if:
//   - The document doesn't exist (new state), OR
//   - The existing state has expired (TTL passed)
//
// MongoDB query:
//
//	findOneAndUpdate(
//	    {$or: [{_id: msgID, expires_at: {$lt: now}}, {_id: msgID, status: {$exists: false}}]},
//	    {$set: {status: "processing", expires_at: now+ttl, ...}},
//	    {upsert: true}
//	)
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

	// Atomic upsert: only succeeds if document doesn't exist OR has expired
	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}}, // Expired state
			{"status": bson.M{"$exists": false}}, // New document (shouldn't happen with upsert, but safe)
		},
	}

	update := bson.M{
		"$set": bson.M{
			"status":     "processing",
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
			// Document exists with active state - another worker acquired it
			return false, nil
		}
		if err == mongo.ErrNoDocuments {
			// This can happen if the filter didn't match (active state exists)
			// Check if document exists with active status
			var existing stateDocument
			findErr := s.collection.FindOne(ctx, bson.M{"_id": messageID}).Decode(&existing)
			if findErr == nil && existing.ExpiresAt.After(now) {
				// Active state exists
				return false, nil
			}
			// No document or expired - try insert
			return s.tryInsert(ctx, messageID, ttl)
		}
		return false, fmt.Errorf("mongodb find and update: %w", err)
	}

	// Check if we actually got the state (status is processing and our expiry)
	if result.Status == "processing" && result.ExpiresAt.Equal(expiresAt) {
		return true, nil
	}

	// Someone else has the state
	return false, nil
}

// tryInsert attempts a direct insert when findOneAndUpdate fails.
func (s *MongoStateManager) tryInsert(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	now := time.Now()

	doc := stateDocument{
		ID:        messageID,
		Status:    "processing",
		ExpiresAt: now.Add(ttl),
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err := s.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Another worker acquired it
			return false, nil
		}
		return false, fmt.Errorf("mongodb insert: %w", err)
	}

	return true, nil
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
			"status":     "completed",
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
				"status":     "released",
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
		"status":     "processing",
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
	defer cursor.Close(ctx)

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
	cutoff := time.Now().Add(-staleTimeout)

	filter := bson.M{
		"status":     "processing",
		"updated_at": bson.M{"$lt": cutoff},
	}

	if s.capped {
		// Capped collections don't support deletes, update status instead
		now := time.Now()
		update := bson.M{
			"$set": bson.M{
				"status":     "released",
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

	// Regular collection with TTL index
	return []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0),
		},
	}
}

// Compile-time interface check
var _ StateManager = (*MongoStateManager)(nil)

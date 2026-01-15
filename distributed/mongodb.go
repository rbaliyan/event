package distributed

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Default collection name for claim storage
const DefaultClaimCollection = "_event_claims"

// claimDocument represents a claim record in MongoDB.
type claimDocument struct {
	ID        string    `bson:"_id"`
	State     string    `bson:"state"`
	ExpiresAt time.Time `bson:"expires_at"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// MongoClaimer implements MessageClaimer using MongoDB for distributed deployments.
//
// MongoClaimer uses MongoDB's atomic findOneAndUpdate with conditional filters
// for race-condition-free message claiming. This is ideal when you're already
// using MongoDB (e.g., with the MongoDB Change Streams transport) and don't
// want to introduce Redis as an additional dependency.
//
// Features:
//   - Atomic claim acquisition using findOneAndUpdate with upsert
//   - Automatic expiration using MongoDB TTL indexes
//   - Configurable database and collection for multi-tenant deployments
//   - Optional capped collection for high-throughput scenarios
//   - Supports MongoDB replica sets and sharded clusters
//
// MongoDB Collection:
//
// Claims are stored in a collection (default: "_event_claims") with documents:
//
//	{
//	    "_id": "msg-123",           // Message ID
//	    "state": "pending",         // "pending" or "completed"
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
// Or call EnsureIndexes() after creating the claimer.
//
// Capped Collection Mode:
//
// For high-throughput scenarios, you can use a capped collection:
//
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCapped(100*1024*1024, 100000) // 100MB, 100k docs max
//	claimer.CreateCollection(ctx) // Creates capped collection
//
// IMPORTANT: Capped collections have limitations:
//   - Release() is a no-op (MongoDB doesn't allow deletes in capped collections)
//   - No TTL index support (cleanup is by size/count, not time)
//   - Failed claims must wait for natural expiration or size-based removal
//
// Example:
//
//	// Basic setup - uses "_event_claims" collection
//	claimer := distributed.NewMongoClaimer(db)
//	claimer.EnsureIndexes(ctx)
//
//	// With custom collection
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCollection("my_claims")
//
//	// With custom database and collection
//	claimer := distributed.NewMongoClaimer(db).
//	    WithDatabase(client.Database("other_db")).
//	    WithCollection("my_claims")
//
//	// With capped collection for high throughput
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCollection("claim_buffer").
//	    WithCapped(100*1024*1024, 0) // 100MB, unlimited docs
//	claimer.CreateCollection(ctx)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
//	    ),
//	)
type MongoClaimer struct {
	collection    *mongo.Collection
	completionTTL time.Duration
	capped        bool
	cappedSize    int64 // Size in bytes
	cappedMaxDocs int64 // Max documents (0 = unlimited)
}

// NewMongoClaimer creates a new MongoDB-based message claimer.
//
// The claimer uses MongoDB's findOneAndUpdate for atomic claim acquisition,
// which prevents race conditions between workers.
//
// Parameters:
//   - db: A connected MongoDB database
//
// Returns a configured MongoClaimer ready for use.
// Use WithDatabase() and WithCollection() to customize storage location.
//
// Example:
//
//	// Simple setup - uses "_event_claims" collection
//	claimer := distributed.NewMongoClaimer(db)
//
//	// With custom collection
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCollection("worker_claims")
//
//	// Don't forget to create indexes for TTL cleanup
//	claimer.EnsureIndexes(ctx)
func NewMongoClaimer(db *mongo.Database) *MongoClaimer {
	opts := defaultClaimerOptions()

	return &MongoClaimer{
		collection:    db.Collection(DefaultClaimCollection),
		completionTTL: opts.completionTTL,
	}
}

// WithDatabase sets a different database for claim storage.
//
// Use this when you want to store claims in a different database than
// the one used for your main application data.
//
// Example:
//
//	claimer := distributed.NewMongoClaimer(appDB).
//	    WithDatabase(client.Database("claims_db"))
func (c *MongoClaimer) WithDatabase(db *mongo.Database) *MongoClaimer {
	c.collection = db.Collection(c.collection.Name())
	return c
}

// WithCollection sets a custom collection name for claim storage.
//
// Default: "_event_claims"
//
// Example:
//
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCollection("worker_claims")
func (c *MongoClaimer) WithCollection(name string) *MongoClaimer {
	c.collection = c.collection.Database().Collection(name)
	return c
}

// WithCompletionTTL sets how long to remember completed messages.
//
// After a message is completed, its ID is remembered for this duration
// to prevent reprocessing if the same message is delivered again.
//
// Default: 24 hours
func (c *MongoClaimer) WithCompletionTTL(ttl time.Duration) *MongoClaimer {
	if ttl > 0 {
		c.completionTTL = ttl
	}
	return c
}

// Collection returns the underlying MongoDB collection.
func (c *MongoClaimer) Collection() *mongo.Collection {
	return c.collection
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
//   - Release() becomes a no-op (MongoDB doesn't allow deletes in capped collections)
//   - No TTL index support (EnsureIndexes skips TTL index for capped collections)
//   - Failed claims wait for size-based removal, not time-based expiration
//   - Updates cannot increase document size
//
// After calling WithCapped(), you must call CreateCollection() to create
// the capped collection before using the claimer.
//
// Example:
//
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCollection("claim_buffer").
//	    WithCapped(100*1024*1024, 100000) // 100MB, max 100k docs
//	claimer.CreateCollection(ctx) // Creates the capped collection
func (c *MongoClaimer) WithCapped(sizeBytes int64, maxDocs int64) *MongoClaimer {
	c.capped = true
	c.cappedSize = sizeBytes
	c.cappedMaxDocs = maxDocs
	return c
}

// IsCapped returns true if capped collection mode is enabled.
func (c *MongoClaimer) IsCapped() bool {
	return c.capped
}

// CreateCollection creates the claims collection.
//
// For capped collections, this creates a capped collection with the
// configured size and max documents. For regular collections, this
// is a no-op (MongoDB creates collections automatically on first write).
//
// This method should be called once during application startup.
//
// Example:
//
//	claimer := distributed.NewMongoClaimer(db).
//	    WithCapped(100*1024*1024, 0)
//	if err := claimer.CreateCollection(ctx); err != nil {
//	    log.Fatal("failed to create collection:", err)
//	}
func (c *MongoClaimer) CreateCollection(ctx context.Context) error {
	if !c.capped {
		// Regular collections are created automatically
		return nil
	}

	// Create capped collection
	opts := options.CreateCollection().
		SetCapped(true).
		SetSizeInBytes(c.cappedSize)

	if c.cappedMaxDocs > 0 {
		opts.SetMaxDocuments(c.cappedMaxDocs)
	}

	err := c.collection.Database().CreateCollection(ctx, c.collection.Name(), opts)
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

// TryClaim attempts to claim a message using MongoDB findOneAndUpdate.
//
// The claim is atomic: the update only succeeds if:
//   - The document doesn't exist (new claim), OR
//   - The existing claim has expired (TTL passed)
//
// MongoDB query:
//
//	findOneAndUpdate(
//	    {$or: [{_id: msgID, expires_at: {$lt: now}}, {_id: msgID, state: {$exists: false}}]},
//	    {$set: {state: "pending", expires_at: now+ttl, ...}},
//	    {upsert: true}
//	)
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to claim
//   - ttl: How long to hold the claim
//
// Returns:
//   - (true, nil): Claim succeeded, process the message
//   - (false, nil): Already claimed (active claim exists), skip the message
//   - (false, error): MongoDB error occurred
func (c *MongoClaimer) TryClaim(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(ttl)

	// Atomic upsert: only succeeds if document doesn't exist OR has expired
	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}}, // Expired claim
			{"state": bson.M{"$exists": false}}, // New document (shouldn't happen with upsert, but safe)
		},
	}

	update := bson.M{
		"$set": bson.M{
			"state":      "pending",
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

	var result claimDocument
	err := c.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Document exists with active claim - another worker claimed it
			return false, nil
		}
		if err == mongo.ErrNoDocuments {
			// This can happen if the filter didn't match (active claim exists)
			// Check if document exists with active state
			var existing claimDocument
			findErr := c.collection.FindOne(ctx, bson.M{"_id": messageID}).Decode(&existing)
			if findErr == nil && existing.ExpiresAt.After(now) {
				// Active claim exists
				return false, nil
			}
			// No document or expired - try insert
			return c.tryInsert(ctx, messageID, ttl)
		}
		return false, fmt.Errorf("mongodb find and update: %w", err)
	}

	// Check if we actually got the claim (state is pending and our expiry)
	if result.State == "pending" && result.ExpiresAt.Equal(expiresAt) {
		return true, nil
	}

	// Someone else has the claim
	return false, nil
}

// tryInsert attempts a direct insert when findOneAndUpdate fails.
func (c *MongoClaimer) tryInsert(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	now := time.Now()

	doc := claimDocument{
		ID:        messageID,
		State:     "pending",
		ExpiresAt: now.Add(ttl),
		CreatedAt: now,
		UpdatedAt: now,
	}

	_, err := c.collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Another worker claimed it
			return false, nil
		}
		return false, fmt.Errorf("mongodb insert: %w", err)
	}

	return true, nil
}

// Complete marks a message as successfully processed.
//
// Updates the claim state to "completed" and extends the expiry to completionTTL.
// This prevents the message from being reprocessed if delivered again
// within the completion window.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message that was successfully processed
//
// Returns nil on success, error if MongoDB operation fails.
func (c *MongoClaimer) Complete(ctx context.Context, messageID string) error {
	now := time.Now()

	filter := bson.M{"_id": messageID}
	update := bson.M{
		"$set": bson.M{
			"state":      "completed",
			"expires_at": now.Add(c.completionTTL),
			"updated_at": now,
		},
	}

	_, err := c.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("mongodb update: %w", err)
	}

	return nil
}

// Release removes the claim to allow immediate retry by another worker.
//
// For regular collections: Deletes the claim document so another worker
// can claim the message immediately instead of waiting for TTL expiration.
//
// For capped collections: This is a no-op because MongoDB doesn't allow
// deletes in capped collections. The claim will remain until it expires
// naturally or is removed by size-based cleanup.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to release
//
// Returns nil on success (including when document doesn't exist), error if MongoDB fails.
func (c *MongoClaimer) Release(ctx context.Context, messageID string) error {
	// Capped collections don't support deletes
	if c.capped {
		// Update state to "released" so TryClaim can reclaim immediately
		// (by treating "released" as expired)
		now := time.Now()
		filter := bson.M{"_id": messageID}
		update := bson.M{
			"$set": bson.M{
				"state":      "released",
				"expires_at": now, // Set to now so it's immediately reclaimable
				"updated_at": now,
			},
		}
		_, err := c.collection.UpdateOne(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("mongodb update (release): %w", err)
		}
		return nil
	}

	_, err := c.collection.DeleteOne(ctx, bson.M{"_id": messageID})
	if err != nil {
		return fmt.Errorf("mongodb delete: %w", err)
	}

	return nil
}

// EnsureIndexes creates the necessary indexes for the claims collection.
//
// For regular collections: Creates a TTL index on expires_at for automatic
// cleanup of expired claims.
//
// For capped collections: TTL indexes are not supported, so this only creates
// a regular index on expires_at for query performance. Cleanup is handled by
// the capped collection's size-based removal.
//
// This should be called once during application startup.
//
// Example:
//
//	claimer := distributed.NewMongoClaimer(db)
//	if err := claimer.EnsureIndexes(ctx); err != nil {
//	    log.Fatal("failed to create indexes:", err)
//	}
func (c *MongoClaimer) EnsureIndexes(ctx context.Context) error {
	indexes := c.Indexes()
	if len(indexes) == 0 {
		return nil
	}

	_, err := c.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Indexes returns the index models for the claims collection.
//
// For regular collections: Returns a TTL index for automatic expiration.
// For capped collections: Returns a regular index (no TTL support in capped).
//
// Use this if you prefer to create indexes yourself or need to inspect them.
func (c *MongoClaimer) Indexes() []mongo.IndexModel {
	if c.capped {
		// Capped collections don't support TTL indexes
		// Return a regular index for query performance
		return []mongo.IndexModel{
			{
				Keys: bson.D{{Key: "expires_at", Value: 1}},
			},
			{
				Keys: bson.D{{Key: "state", Value: 1}},
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
var _ MessageClaimer = (*MongoClaimer)(nil)

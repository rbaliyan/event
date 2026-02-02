package idempotency

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/rbaliyan/event/v3/store/base"
)

// MongoStore implements Store using MongoDB for distributed idempotency.
//
// MongoStore is the recommended store for production deployments with:
//   - Multiple application instances sharing idempotency state
//   - Need for exactly-once processing with database transactions
//   - Persistence and durability requirements
//
// Features:
//   - Atomic check-and-set using MongoDB findOneAndUpdate
//   - Automatic TTL-based cleanup via MongoDB TTL index
//   - Configurable collection name for multi-tenant deployments
//   - Transactional support for exactly-once guarantees
//
// Collection Schema:
//
//	{
//	    _id: "message-id",           // Primary key
//	    processed_at: ISODate(...),  // When message was processed
//	    expires_at: ISODate(...)     // When entry can be removed
//	}
//
// Required Index (created automatically by EnsureIndexes):
//
//	db.event_idempotency.createIndex({ "expires_at": 1 }, { expireAfterSeconds: 0 })
//
// Example:
//
//	// Create MongoDB client
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost"))
//	db := client.Database("mydb")
//
//	// Create idempotency store with 24-hour TTL
//	store := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoTTL(24*time.Hour),
//	)
//
//	// Use in message handler
//	func handlePayment(ctx context.Context, payment Payment) error {
//	    key := fmt.Sprintf("payment:%s", payment.ID)
//
//	    isDuplicate, err := store.IsDuplicate(ctx, key)
//	    if err != nil {
//	        return fmt.Errorf("idempotency check: %w", err)
//	    }
//	    if isDuplicate {
//	        log.Info("duplicate payment, skipping", "payment_id", payment.ID)
//	        return nil
//	    }
//
//	    // Process payment...
//	    return processPayment(payment)
//	}
//
// Transactional Usage:
//
//	sess, _ := client.StartSession()
//	defer sess.EndSession(ctx)
//
//	_, err := sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
//	    // Check within transaction
//	    isDup, err := store.IsDuplicateTx(sessCtx, msgID)
//	    if isDup {
//	        return nil, nil
//	    }
//
//	    // Process and update database...
//
//	    // Mark as processed in same transaction
//	    return nil, store.MarkProcessedTx(sessCtx, msgID)
//	})
type MongoStore struct {
	collection      *mongo.Collection
	collectionName  string // Used during construction only
	ttl             time.Duration
	cleanupInterval time.Duration
	stopCleanup     chan struct{}
}

// MongoOption configures a MongoStore.
type MongoOption func(*MongoStore)

// WithMongoTTL sets the default TTL for idempotency entries.
//
// Entries older than this duration are considered expired and will be
// cleaned up by MongoDB's TTL index. Default is 24 hours.
//
// Example:
//
//	store := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoTTL(7*24*time.Hour), // 7 days
//	)
func WithMongoTTL(ttl time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.ttl = ttl
	}
}

// WithMongoCollection sets a custom collection name.
//
// Default is "event_idempotency". Use this for multi-tenant deployments
// or when you need multiple idempotency stores.
//
// Example:
//
//	store := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoCollection("payments_idempotency"),
//	)
func WithMongoCollection(name string) MongoOption {
	return func(s *MongoStore) {
		if name != "" {
			s.collectionName = name
		}
	}
}

// WithMongoCleanupInterval sets how often to run manual cleanup.
//
// MongoDB TTL indexes handle automatic cleanup, but this provides a
// backup mechanism. Set to 0 to disable manual cleanup.
// Default is 1 hour.
//
// Example:
//
//	store := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoCleanupInterval(0), // Disable manual cleanup
//	)
func WithMongoCleanupInterval(interval time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.cleanupInterval = interval
	}
}

// idempotencyEntry represents a stored idempotency entry in MongoDB.
type idempotencyEntry struct {
	ID          string    `bson:"_id"`
	ProcessedAt time.Time `bson:"processed_at"`
	ExpiresAt   time.Time `bson:"expires_at"`
}

// NewMongoStore creates a new MongoDB-based idempotency store.
//
// The store uses MongoDB's findOneAndUpdate with upsert for atomic duplicate
// detection, and TTL indexes for automatic cleanup.
//
// Parameters:
//   - db: A connected MongoDB database
//   - opts: Optional configuration options
//
// Example:
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost"))
//	db := client.Database("mydb")
//
//	store := idempotency.NewMongoStore(db,
//	    idempotency.WithMongoTTL(24*time.Hour),
//	)
//	defer store.Close()
//
//	// Call EnsureIndexes once during startup
//	if err := store.EnsureIndexes(ctx); err != nil {
//	    log.Fatal("failed to create indexes:", err)
//	}
func NewMongoStore(db *mongo.Database, opts ...MongoOption) *MongoStore {
	s := &MongoStore{
		collectionName:  "event_idempotency",
		ttl:             24 * time.Hour,
		cleanupInterval: time.Hour,
		stopCleanup:     make(chan struct{}),
	}

	// Apply options (may override collectionName)
	for _, opt := range opts {
		opt(s)
	}

	// Set the collection using the final collection name
	s.collection = db.Collection(s.collectionName)

	// Start background cleanup (backup for TTL index)
	if s.cleanupInterval > 0 {
		go base.SimpleCleanupLoop(s.cleanupInterval, s.stopCleanup, s.cleanup)
	}

	return s
}

// NewMongoStoreWithCollection creates a new MongoDB-based idempotency store
// with a custom collection name.
//
// Parameters:
//   - db: A connected MongoDB database
//   - collectionName: Name of the collection to use
//   - opts: Optional configuration options
//
// Example:
//
//	store := idempotency.NewMongoStoreWithCollection(db, "payment_idempotency",
//	    idempotency.WithMongoTTL(7*24*time.Hour),
//	)
func NewMongoStoreWithCollection(db *mongo.Database, collectionName string, opts ...MongoOption) *MongoStore {
	s := &MongoStore{
		collection:      db.Collection(collectionName),
		ttl:             24 * time.Hour,
		cleanupInterval: time.Hour,
		stopCleanup:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	// Start background cleanup (backup for TTL index)
	if s.cleanupInterval > 0 {
		go base.SimpleCleanupLoop(s.cleanupInterval, s.stopCleanup, s.cleanup)
	}

	return s
}

// IsDuplicate checks if a message ID has already been processed.
//
// This method performs an atomic check-and-set operation using MongoDB's
// findOneAndUpdate with upsert. This means:
//   - If the document doesn't exist: creates it with TTL and returns false
//   - If the document exists and is not expired: returns true (is duplicate)
//   - If the document exists but is expired: updates it and returns false
//
// The atomic nature prevents race conditions where two instances might both
// think they're the first to process a message.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to check
//
// Returns:
//   - (true, nil): Message was already processed or is being processed
//   - (false, nil): Message is new, caller should process it
//   - (false, error): MongoDB operation failed
//
// Example:
//
//	isDuplicate, err := store.IsDuplicate(ctx, "order-123")
//	if err != nil {
//	    return fmt.Errorf("idempotency check failed: %w", err)
//	}
//	if isDuplicate {
//	    log.Debug("skipping duplicate message", "id", "order-123")
//	    return nil
//	}
//	// Process message - we're guaranteed to be the only processor
func (s *MongoStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	return s.isDuplicateWithCollection(ctx, s.collection, messageID)
}

// IsDuplicateTx checks for duplicate within a MongoDB session/transaction.
//
// Use this for exactly-once processing when you need the idempotency check
// and business logic to be in the same transaction.
//
// Parameters:
//   - ctx: Must be a mongo.SessionContext from WithTransaction callback
//   - messageID: The unique message identifier to check
//
// Returns:
//   - (true, nil): Message already processed
//   - (false, nil): Message is new
//   - (false, error): Check failed
//
// Example:
//
//	sess, _ := client.StartSession()
//	defer sess.EndSession(ctx)
//
//	_, err := sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
//	    isDup, err := store.IsDuplicateTx(sessCtx, msgID)
//	    if err != nil {
//	        return nil, err
//	    }
//	    if isDup {
//	        return nil, nil // Already processed
//	    }
//
//	    // Process message within transaction...
//
//	    return nil, store.MarkProcessedTx(sessCtx, msgID)
//	})
func (s *MongoStore) IsDuplicateTx(ctx context.Context, messageID string) (bool, error) {
	// The context should be a mongo.SessionContext for transaction support
	// Operations on the collection will automatically use the session
	return s.isDuplicateWithCollection(ctx, s.collection, messageID)
}

// isDuplicateWithCollection performs the actual atomic check-and-set.
func (s *MongoStore) isDuplicateWithCollection(ctx context.Context, coll *mongo.Collection, messageID string) (bool, error) {
	now := time.Now()
	expiresAt := now.Add(s.ttl)

	// Atomic upsert: only succeeds if document doesn't exist OR has expired
	filter := bson.M{
		"_id": messageID,
		"$or": []bson.M{
			{"expires_at": bson.M{"$lt": now}}, // Expired
		},
	}

	update := bson.M{
		"$set": bson.M{
			"processed_at": now,
			"expires_at":   expiresAt,
		},
	}

	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result idempotencyEntry
	err := coll.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Document exists and is not expired - this is a duplicate
			return true, nil
		}

		// Check for duplicate key error (another instance beat us to it)
		if mongo.IsDuplicateKeyError(err) {
			return true, nil
		}

		return false, fmt.Errorf("mongo findOneAndUpdate: %w", err)
	}

	// Successfully created or updated - not a duplicate
	return false, nil
}

// MarkProcessed marks a message ID as processed using the default TTL.
//
// For MongoStore, IsDuplicate already marks the message when it returns false,
// so this method primarily serves to refresh the TTL after successful processing.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//
// Returns nil on success, error if the MongoDB operation fails.
func (s *MongoStore) MarkProcessed(ctx context.Context, messageID string) error {
	return s.MarkProcessedWithTTL(ctx, messageID, s.ttl)
}

// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
//
// Use this when different message types require different retention periods.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//   - ttl: Custom time-to-live for this entry
//
// Returns nil on success, error if the MongoDB operation fails.
func (s *MongoStore) MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, ttl)
}

// MarkProcessedTx marks a message ID as processed within a MongoDB transaction.
//
// Parameters:
//   - ctx: Must be a mongo.SessionContext from WithTransaction callback
//   - messageID: The unique message identifier to mark
//
// Returns nil on success, error if the operation fails.
func (s *MongoStore) MarkProcessedTx(ctx context.Context, messageID string) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, s.ttl)
}

// MarkProcessedWithTTLTx marks a message ID as processed with custom TTL within a transaction.
//
// Parameters:
//   - ctx: Must be a mongo.SessionContext from WithTransaction callback
//   - messageID: The unique message identifier to mark
//   - ttl: How long to remember this message ID
//
// Returns nil on success, error if the operation fails.
func (s *MongoStore) MarkProcessedWithTTLTx(ctx context.Context, messageID string, ttl time.Duration) error {
	return s.markProcessedWithCollection(ctx, s.collection, messageID, ttl)
}

// markProcessedWithCollection performs the actual upsert operation.
func (s *MongoStore) markProcessedWithCollection(ctx context.Context, coll *mongo.Collection, messageID string, ttl time.Duration) error {
	now := time.Now()
	expiresAt := now.Add(ttl)

	entry := idempotencyEntry{
		ID:          messageID,
		ProcessedAt: now,
		ExpiresAt:   expiresAt,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := coll.ReplaceOne(ctx, bson.M{"_id": messageID}, entry, opts)
	if err != nil {
		return fmt.Errorf("mongo replace: %w", err)
	}

	return nil
}

// Remove removes a message ID from the store.
//
// After removal, the message ID is no longer considered a duplicate and
// can be processed again if redelivered.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to remove
//
// Returns nil on success (including when the entry doesn't exist),
// error if the MongoDB operation fails.
func (s *MongoStore) Remove(ctx context.Context, messageID string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": messageID})
	if err != nil {
		return fmt.Errorf("mongo delete: %w", err)
	}
	return nil
}

// Close stops the background cleanup goroutine.
//
// Call this when shutting down to cleanly stop the cleanup routine.
//
// Example:
//
//	store := idempotency.NewMongoStore(db)
//	defer store.Close()
func (s *MongoStore) Close() error {
	select {
	case <-s.stopCleanup:
		// Already closed
	default:
		close(s.stopCleanup)
	}
	return nil
}

// cleanup removes expired entries from the collection.
// This is a backup mechanism - MongoDB TTL indexes should handle most cleanup.
func (s *MongoStore) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, _ = s.collection.DeleteMany(ctx, bson.M{
		"expires_at": bson.M{"$lt": time.Now()},
	})
}

// EnsureIndexes creates indexes for efficient queries.
//
// This creates:
//   - TTL index on expires_at for automatic cleanup
//
// Call this once during application startup.
//
// Example:
//
//	store := idempotency.NewMongoStore(db)
//	if err := store.EnsureIndexes(ctx); err != nil {
//	    log.Fatal("failed to create indexes:", err)
//	}
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "expires_at", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(0), // TTL index - delete when expires_at is reached
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create indexes: %w", err)
	}

	return nil
}

// Collection returns the underlying MongoDB collection for advanced usage.
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// Compile-time check
var _ Store = (*MongoStore)(nil)

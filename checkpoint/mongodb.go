package checkpoint

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoStore implements CheckpointStore using MongoDB.
// Checkpoints are stored as documents with subscriber ID as the key.
//
// Document structure:
//
//	{
//	    "_id": "subscriber-id",
//	    "position": ISODate("2024-01-15T10:30:00Z"),
//	    "updated_at": ISODate("2024-01-15T10:30:00Z")
//	}
//
// Example:
//
//	store := checkpoint.NewMongoStore(collection)
//	ev.Subscribe(ctx, handler,
//	    event.WithCheckpoint[Order](store, "order-processor"),
//	)
type MongoStore struct {
	collection *mongo.Collection
	ttl        time.Duration
}

// MongoOption configures the MongoDB checkpoint store
type MongoOption func(*MongoStore)

// WithMongoTTL sets a TTL for checkpoint documents.
// MongoDB will automatically remove expired documents.
// This creates a TTL index on the "updated_at" field.
// Default is 0 (no expiration).
func WithMongoTTL(ttl time.Duration) MongoOption {
	return func(s *MongoStore) {
		s.ttl = ttl
	}
}

// checkpointDoc represents the MongoDB document structure
type checkpointDoc struct {
	ID        string    `bson:"_id"`
	Position  time.Time `bson:"position"`
	UpdatedAt time.Time `bson:"updated_at"`
}

// NewMongoStore creates a new MongoDB-backed checkpoint store.
//
// Parameters:
//   - collection: MongoDB collection to store checkpoints
//
// Example:
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//	collection := client.Database("myapp").Collection("checkpoints")
//	store := checkpoint.NewMongoStore(collection)
//
//	// With TTL (checkpoints expire after 7 days)
//	store := checkpoint.NewMongoStore(collection,
//	    checkpoint.WithMongoTTL(7*24*time.Hour))
func NewMongoStore(collection *mongo.Collection, opts ...MongoOption) *MongoStore {
	s := &MongoStore{
		collection: collection,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Indexes returns the index models for the checkpoint collection.
// Use this to create indexes manually or with a migration tool.
//
// Returns:
//   - TTL index on "updated_at" field (if TTL is configured)
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	var indexes []mongo.IndexModel

	if s.ttl > 0 {
		indexes = append(indexes, mongo.IndexModel{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
			Options: options.Index().
				SetExpireAfterSeconds(int32(s.ttl.Seconds())).
				SetName("checkpoint_ttl"),
		})
	}

	return indexes
}

// EnsureIndexes creates the required indexes for the checkpoint collection.
// Call this once during application startup.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := s.Indexes()
	if len(indexes) == 0 {
		return nil
	}
	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Save persists the checkpoint position for a subscriber.
func (s *MongoStore) Save(ctx context.Context, subscriberID string, position time.Time) error {
	now := time.Now()
	doc := checkpointDoc{
		ID:        subscriberID,
		Position:  position,
		UpdatedAt: now,
	}

	opts := options.Replace().SetUpsert(true)
	_, err := s.collection.ReplaceOne(
		ctx,
		bson.M{"_id": subscriberID},
		doc,
		opts,
	)
	return err
}

// Load retrieves the last saved checkpoint for a subscriber.
// Returns zero time and nil error if no checkpoint exists.
func (s *MongoStore) Load(ctx context.Context, subscriberID string) (time.Time, error) {
	var doc checkpointDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": subscriberID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		// No checkpoint exists - this is not an error
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return doc.Position, nil
}

// Delete removes a checkpoint for a subscriber.
func (s *MongoStore) Delete(ctx context.Context, subscriberID string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": subscriberID})
	return err
}

// DeleteAll removes all checkpoints (useful for testing or cleanup).
func (s *MongoStore) DeleteAll(ctx context.Context) error {
	_, err := s.collection.DeleteMany(ctx, bson.M{})
	return err
}

// List returns all subscriber IDs with checkpoints.
func (s *MongoStore) List(ctx context.Context) ([]string, error) {
	cursor, err := s.collection.Find(ctx, bson.M{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []string
	for cursor.Next(ctx) {
		var doc struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		ids = append(ids, doc.ID)
	}
	return ids, cursor.Err()
}

// GetAll returns all checkpoints as a map.
func (s *MongoStore) GetAll(ctx context.Context) (map[string]time.Time, error) {
	cursor, err := s.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	checkpoints := make(map[string]time.Time)
	for cursor.Next(ctx) {
		var doc checkpointDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		checkpoints[doc.ID] = doc.Position
	}
	return checkpoints, cursor.Err()
}

// GetCheckpointInfo returns detailed checkpoint information for a subscriber.
func (s *MongoStore) GetCheckpointInfo(ctx context.Context, subscriberID string) (*CheckpointInfo, error) {
	var doc checkpointDoc
	err := s.collection.FindOne(ctx, bson.M{"_id": subscriberID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &CheckpointInfo{
		SubscriberID: doc.ID,
		Position:     doc.Position,
		UpdatedAt:    doc.UpdatedAt,
	}, nil
}


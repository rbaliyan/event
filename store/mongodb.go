package store

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoDocument represents a document that can be stored in MongoDB.
// Items stored in MongoStore must implement this interface.
type MongoDocument interface {
	Identifier
	// GetCreatedAt returns when the document was created.
	GetCreatedAt() time.Time
}

// MongoStore provides a MongoDB implementation of CoreStore.
// It handles common CRUD operations, pagination, and time-based queries.
//
// Domain-specific stores can embed MongoStore and add specialized methods:
//
//	type SagaMongoStore struct {
//	    *store.MongoStore[*State]
//	}
//
//	func (s *SagaMongoStore) ListByStatus(ctx context.Context, statuses []Status) ([]*State, error) {
//	    // Use s.Collection() for custom queries
//	}
type MongoStore[T MongoDocument] struct {
	collection   *mongo.Collection
	defaultLimit int

	// Field names for queries (customizable)
	idField        string
	createdAtField string
}

// MongoStoreOption configures a MongoStore.
type MongoStoreOption[T MongoDocument] func(*MongoStore[T])

// WithMongoDefaultLimit sets the default page size.
func WithMongoDefaultLimit[T MongoDocument](limit int) MongoStoreOption[T] {
	return func(s *MongoStore[T]) {
		if limit > 0 {
			s.defaultLimit = limit
		}
	}
}

// WithMongoIDField sets the field name used for document IDs.
// Default is "_id".
func WithMongoIDField[T MongoDocument](field string) MongoStoreOption[T] {
	return func(s *MongoStore[T]) {
		if field != "" {
			s.idField = field
		}
	}
}

// WithMongoCreatedAtField sets the field name used for creation timestamp.
// Default is "created_at".
func WithMongoCreatedAtField[T MongoDocument](field string) MongoStoreOption[T] {
	return func(s *MongoStore[T]) {
		if field != "" {
			s.createdAtField = field
		}
	}
}

// NewMongoStore creates a new MongoDB store.
func NewMongoStore[T MongoDocument](collection *mongo.Collection, opts ...MongoStoreOption[T]) *MongoStore[T] {
	s := &MongoStore[T]{
		collection:     collection,
		defaultLimit:   100,
		idField:        "_id",
		createdAtField: "created_at",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Collection returns the underlying MongoDB collection for custom queries.
func (s *MongoStore[T]) Collection() *mongo.Collection {
	return s.collection
}

// Create stores a new document.
func (s *MongoStore[T]) Create(ctx context.Context, item T) error {
	id := item.GetID()
	if id == "" {
		return ErrInvalidID
	}

	_, err := s.collection.InsertOne(ctx, item)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// Get retrieves a document by ID.
func (s *MongoStore[T]) Get(ctx context.Context, id string) (T, error) {
	var zero T
	if id == "" {
		return zero, ErrInvalidID
	}

	var item T
	err := s.collection.FindOne(ctx, bson.M{s.idField: id}).Decode(&item)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return zero, ErrNotFound
		}
		return zero, err
	}
	return item, nil
}

// Update modifies an existing document.
func (s *MongoStore[T]) Update(ctx context.Context, item T) error {
	id := item.GetID()
	if id == "" {
		return ErrInvalidID
	}

	result, err := s.collection.ReplaceOne(ctx, bson.M{s.idField: id}, item)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return ErrNotFound
	}
	return nil
}

// Delete removes a document by ID.
func (s *MongoStore[T]) Delete(ctx context.Context, id string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{s.idField: id})
	return err
}

// List retrieves documents matching the filter with pagination.
func (s *MongoStore[T]) List(ctx context.Context, filter Filter) (*Page[T], error) {
	// Build query
	query := s.buildTimeQuery(filter)

	// Apply cursor
	cursor := DecodeCursor(filter.Cursor)
	if !cursor.IsZero() {
		cursorQuery := s.buildCursorQuery(cursor, filter.OrderDesc)
		query = bson.M{"$and": []bson.M{query, cursorQuery}}
	}

	// Determine sort direction
	sortDir := 1
	if filter.OrderDesc {
		sortDir = -1
	}

	// Apply limit
	limit := filter.Limit
	if limit <= 0 {
		limit = s.defaultLimit
	}

	// Execute query
	opts := options.Find().
		SetSort(bson.D{{Key: s.createdAtField, Value: sortDir}, {Key: s.idField, Value: sortDir}}).
		SetLimit(int64(limit + 1)) // Fetch one extra to detect if there are more

	cur, err := s.collection.Find(ctx, query, opts)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var items []T
	if err := cur.All(ctx, &items); err != nil {
		return nil, err
	}

	page := &Page[T]{}

	// Check if there are more items
	if len(items) > limit {
		items = items[:limit]
		lastItem := items[len(items)-1]
		page.NextCursor = EncodeCursor(NewCursor(lastItem.GetCreatedAt(), lastItem.GetID()))
	}

	page.Items = items
	return page, nil
}

// Count returns the number of documents matching the filter.
func (s *MongoStore[T]) Count(ctx context.Context, filter Filter) (int64, error) {
	query := s.buildTimeQuery(filter)
	return s.collection.CountDocuments(ctx, query)
}

// DeleteOlderThan removes documents older than the given age.
func (s *MongoStore[T]) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	result, err := s.collection.DeleteMany(ctx, bson.M{
		s.createdAtField: bson.M{"$lt": cutoff},
	})
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}

// buildTimeQuery builds a query for time-based filtering.
func (s *MongoStore[T]) buildTimeQuery(filter Filter) bson.M {
	query := bson.M{}

	if !filter.StartTime.IsZero() || !filter.EndTime.IsZero() {
		timeQuery := bson.M{}
		if !filter.StartTime.IsZero() {
			timeQuery["$gte"] = filter.StartTime
		}
		if !filter.EndTime.IsZero() {
			timeQuery["$lt"] = filter.EndTime
		}
		query[s.createdAtField] = timeQuery
	}

	return query
}

// buildCursorQuery builds a query for cursor-based pagination.
func (s *MongoStore[T]) buildCursorQuery(cursor Cursor, orderDesc bool) bson.M {
	if orderDesc {
		// For descending: items before cursor (older or same time with smaller ID)
		return bson.M{
			"$or": []bson.M{
				{s.createdAtField: bson.M{"$lt": cursor.Timestamp}},
				{
					s.createdAtField: cursor.Timestamp,
					s.idField:        bson.M{"$lt": cursor.ID},
				},
			},
		}
	}
	// For ascending: items after cursor (newer or same time with larger ID)
	return bson.M{
		"$or": []bson.M{
			{s.createdAtField: bson.M{"$gt": cursor.Timestamp}},
			{
				s.createdAtField: cursor.Timestamp,
				s.idField:        bson.M{"$gt": cursor.ID},
			},
		},
	}
}

// EnsureIndexes creates indexes for efficient queries.
// Call this once during application startup.
func (s *MongoStore[T]) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: s.createdAtField, Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: s.createdAtField, Value: 1},
				{Key: s.idField, Value: 1},
			},
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Upsert creates or updates a document.
func (s *MongoStore[T]) Upsert(ctx context.Context, item T) error {
	id := item.GetID()
	if id == "" {
		return ErrInvalidID
	}

	_, err := s.collection.ReplaceOne(
		ctx,
		bson.M{s.idField: id},
		item,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Compile-time interface checks.
var _ CoreStore[MongoDocument] = (*MongoStore[MongoDocument])(nil)
var _ CleanableStore[MongoDocument] = (*MongoStore[MongoDocument])(nil)

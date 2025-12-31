package saga

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoDB Schema:

Collection: sagas

Document structure:
{
    "_id": string (saga ID),
    "name": string,
    "status": string,
    "current_step": int,
    "completed_steps": [string],
    "data": any (BSON document),
    "error": string (optional),
    "started_at": ISODate,
    "completed_at": ISODate (optional),
    "last_updated_at": ISODate
}

Indexes:
db.sagas.createIndex({ "name": 1 })
db.sagas.createIndex({ "status": 1 })
db.sagas.createIndex({ "started_at": 1 })
db.sagas.createIndex({ "name": 1, "status": 1 })
*/

// MongoState represents the saga state document in MongoDB
type MongoState struct {
	ID             string     `bson:"_id"`
	Name           string     `bson:"name"`
	Status         Status     `bson:"status"`
	CurrentStep    int        `bson:"current_step"`
	CompletedSteps []string   `bson:"completed_steps,omitempty"`
	Data           any        `bson:"data,omitempty"`
	Error          string     `bson:"error,omitempty"`
	StartedAt      time.Time  `bson:"started_at"`
	CompletedAt    *time.Time `bson:"completed_at,omitempty"`
	LastUpdatedAt  time.Time  `bson:"last_updated_at"`
}

// ToState converts MongoState to State
func (m *MongoState) ToState() *State {
	return &State{
		ID:             m.ID,
		Name:           m.Name,
		Status:         m.Status,
		CurrentStep:    m.CurrentStep,
		CompletedSteps: m.CompletedSteps,
		Data:           m.Data,
		Error:          m.Error,
		StartedAt:      m.StartedAt,
		CompletedAt:    m.CompletedAt,
		LastUpdatedAt:  m.LastUpdatedAt,
	}
}

// FromState creates a MongoState from State
func FromState(s *State) *MongoState {
	return &MongoState{
		ID:             s.ID,
		Name:           s.Name,
		Status:         s.Status,
		CurrentStep:    s.CurrentStep,
		CompletedSteps: s.CompletedSteps,
		Data:           s.Data,
		Error:          s.Error,
		StartedAt:      s.StartedAt,
		CompletedAt:    s.CompletedAt,
		LastUpdatedAt:  s.LastUpdatedAt,
	}
}

// MongoStore is a MongoDB-based saga store
type MongoStore struct {
	collection *mongo.Collection
}

// NewMongoStore creates a new MongoDB saga store
func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		collection: db.Collection("sagas"),
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

// EnsureIndexes creates the required indexes for the saga collection
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "name", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "started_at", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "name", Value: 1},
				{Key: "status", Value: 1},
			},
		},
	}

	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

// Create creates a new saga instance
func (s *MongoStore) Create(ctx context.Context, state *State) error {
	mongoState := FromState(state)

	_, err := s.collection.InsertOne(ctx, mongoState)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("saga already exists: %s", state.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves saga state by ID
func (s *MongoStore) Get(ctx context.Context, id string) (*State, error) {
	filter := bson.M{"_id": id}

	var mongoState MongoState
	err := s.collection.FindOne(ctx, filter).Decode(&mongoState)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("saga not found: %s", id)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoState.ToState(), nil
}

// Update updates saga state
func (s *MongoStore) Update(ctx context.Context, state *State) error {
	filter := bson.M{"_id": state.ID}
	update := bson.M{
		"$set": bson.M{
			"status":          state.Status,
			"current_step":    state.CurrentStep,
			"completed_steps": state.CompletedSteps,
			"data":            state.Data,
			"error":           state.Error,
			"completed_at":    state.CompletedAt,
			"last_updated_at": state.LastUpdatedAt,
		},
	}

	result, err := s.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	if result.MatchedCount == 0 {
		return fmt.Errorf("saga not found: %s", state.ID)
	}

	return nil
}

// List lists sagas matching the filter
func (s *MongoStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	mongoFilter := bson.M{}

	if filter.Name != "" {
		mongoFilter["name"] = filter.Name
	}

	if len(filter.Status) > 0 {
		mongoFilter["status"] = bson.M{"$in": filter.Status}
	}

	opts := options.Find().SetSort(bson.D{{Key: "started_at", Value: -1}})
	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer cursor.Close(ctx)

	var results []*State
	for cursor.Next(ctx) {
		var mongoState MongoState
		if err := cursor.Decode(&mongoState); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		results = append(results, mongoState.ToState())
	}

	return results, cursor.Err()
}

// Delete removes a saga by ID
func (s *MongoStore) Delete(ctx context.Context, id string) error {
	filter := bson.M{"_id": id}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("saga not found: %s", id)
	}

	return nil
}

// DeleteOlderThan removes sagas older than the specified age
func (s *MongoStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	filter := bson.M{
		"started_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// DeleteCompleted removes completed sagas older than the specified age
func (s *MongoStore) DeleteCompleted(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	filter := bson.M{
		"status":       bson.M{"$in": []Status{StatusCompleted, StatusCompensated}},
		"completed_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// Count returns the count of sagas by status
func (s *MongoStore) Count(ctx context.Context, status Status) (int64, error) {
	filter := bson.M{"status": status}
	return s.collection.CountDocuments(ctx, filter)
}

// CountByName returns the count of sagas by name and optional status
func (s *MongoStore) CountByName(ctx context.Context, name string, status *Status) (int64, error) {
	filter := bson.M{"name": name}
	if status != nil {
		filter["status"] = *status
	}
	return s.collection.CountDocuments(ctx, filter)
}

// GetFailed returns all failed sagas for a given saga name
func (s *MongoStore) GetFailed(ctx context.Context, name string, limit int) ([]*State, error) {
	return s.List(ctx, StoreFilter{
		Name:   name,
		Status: []Status{StatusFailed},
		Limit:  limit,
	})
}

// GetPending returns all pending/running sagas (useful for recovery after restart)
func (s *MongoStore) GetPending(ctx context.Context, limit int) ([]*State, error) {
	return s.List(ctx, StoreFilter{
		Status: []Status{StatusPending, StatusRunning, StatusCompensating},
		Limit:  limit,
	})
}

// Stats returns saga statistics
type Stats struct {
	Total        int64            `json:"total"`
	ByStatus     map[Status]int64 `json:"by_status"`
	ByName       map[string]int64 `json:"by_name"`
	OldestActive *time.Time       `json:"oldest_active,omitempty"`
}

// GetStats returns saga statistics
func (s *MongoStore) GetStats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		ByStatus: make(map[Status]int64),
		ByName:   make(map[string]int64),
	}

	// Total count
	total, err := s.collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}
	stats.Total = total

	// Count by status
	statuses := []Status{StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusCompensating, StatusCompensated}
	for _, status := range statuses {
		count, err := s.collection.CountDocuments(ctx, bson.M{"status": status})
		if err != nil {
			return nil, fmt.Errorf("count status %s: %w", status, err)
		}
		stats.ByStatus[status] = count
	}

	// Count by name using aggregation
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.M{
			"_id":   "$name",
			"count": bson.M{"$sum": 1},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate by name: %w", err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var result struct {
			Name  string `bson:"_id"`
			Count int64  `bson:"count"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		stats.ByName[result.Name] = result.Count
	}

	// Find oldest active saga
	activeFilter := bson.M{
		"status": bson.M{"$in": []Status{StatusPending, StatusRunning, StatusCompensating}},
	}
	opts := options.FindOne().SetSort(bson.D{{Key: "started_at", Value: 1}})

	var oldest MongoState
	err = s.collection.FindOne(ctx, activeFilter, opts).Decode(&oldest)
	if err == nil {
		stats.OldestActive = &oldest.StartedAt
	}

	return stats, nil
}

// Compile-time check
var _ Store = (*MongoStore)(nil)

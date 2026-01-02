package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoDB Schema:

Collection: monitor_entries

Document structure:
{
    "_id": ObjectId,
    "event_id": string,
    "subscription_id": string,
    "event_name": string,
    "bus_id": string,
    "delivery_mode": string,
    "metadata": object,
    "status": string,
    "error": string,
    "retry_count": int,
    "started_at": ISODate,
    "completed_at": ISODate,
    "duration_ms": int64,
    "trace_id": string,
    "span_id": string
}

Indexes:
db.monitor_entries.createIndex({"event_id": 1, "subscription_id": 1}, {unique: true})
db.monitor_entries.createIndex({"event_name": 1})
db.monitor_entries.createIndex({"status": 1})
db.monitor_entries.createIndex({"started_at": 1})
db.monitor_entries.createIndex({"delivery_mode": 1})
*/

// MongoEntry represents a monitor entry document in MongoDB.
type MongoEntry struct {
	EventID        string            `bson:"event_id"`
	SubscriptionID string            `bson:"subscription_id"`
	EventName      string            `bson:"event_name"`
	BusID          string            `bson:"bus_id"`
	DeliveryMode   string            `bson:"delivery_mode"`
	Metadata       map[string]string `bson:"metadata,omitempty"`
	Status         string            `bson:"status"`
	Error          string            `bson:"error,omitempty"`
	RetryCount     int               `bson:"retry_count"`
	StartedAt      time.Time         `bson:"started_at"`
	CompletedAt    *time.Time        `bson:"completed_at,omitempty"`
	DurationMs     *int64            `bson:"duration_ms,omitempty"`
	TraceID        string            `bson:"trace_id,omitempty"`
	SpanID         string            `bson:"span_id,omitempty"`
}

// ToEntry converts MongoEntry to Entry.
func (m *MongoEntry) ToEntry() *Entry {
	entry := &Entry{
		EventID:        m.EventID,
		SubscriptionID: m.SubscriptionID,
		EventName:      m.EventName,
		BusID:          m.BusID,
		DeliveryMode:   ParseDeliveryMode(m.DeliveryMode),
		Metadata:       m.Metadata,
		Status:         Status(m.Status),
		Error:          m.Error,
		RetryCount:     m.RetryCount,
		StartedAt:      m.StartedAt,
		CompletedAt:    m.CompletedAt,
		TraceID:        m.TraceID,
		SpanID:         m.SpanID,
	}
	if m.DurationMs != nil {
		entry.Duration = time.Duration(*m.DurationMs) * time.Millisecond
	}
	return entry
}

// FromEntry creates a MongoEntry from Entry.
func FromEntry(e *Entry) *MongoEntry {
	var durationMs *int64
	if e.Duration > 0 {
		ms := e.Duration.Milliseconds()
		durationMs = &ms
	}

	subscriptionID := e.SubscriptionID
	if e.DeliveryMode == WorkerPool {
		subscriptionID = ""
	}

	return &MongoEntry{
		EventID:        e.EventID,
		SubscriptionID: subscriptionID,
		EventName:      e.EventName,
		BusID:          e.BusID,
		DeliveryMode:   e.DeliveryMode.String(),
		Metadata:       e.Metadata,
		Status:         string(e.Status),
		Error:          e.Error,
		RetryCount:     e.RetryCount,
		StartedAt:      e.StartedAt,
		CompletedAt:    e.CompletedAt,
		DurationMs:     durationMs,
		TraceID:        e.TraceID,
		SpanID:         e.SpanID,
	}
}

// MongoStore is a MongoDB-based monitor store.
type MongoStore struct {
	collection *mongo.Collection
}

// NewMongoStore creates a new MongoDB monitor store.
//
// Example:
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//	db := client.Database("mydb")
//	store := monitor.NewMongoStore(db)
func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		collection: db.Collection("monitor_entries"),
	}
}

// WithCollection sets a custom collection name.
func (s *MongoStore) WithCollection(name string) *MongoStore {
	s.collection = s.collection.Database().Collection(name)
	return s
}

// Collection returns the underlying MongoDB collection.
func (s *MongoStore) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required indexes for the monitor collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := store.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoStore) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "event_id", Value: 1}, {Key: "subscription_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "event_name", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "started_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "delivery_mode", Value: 1}},
		},
	}
}

// EnsureIndexes creates the required indexes for the monitor collection.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Record creates or updates a monitor entry.
func (s *MongoStore) Record(ctx context.Context, entry *Entry) error {
	mongoEntry := FromEntry(entry)

	filter := bson.M{
		"event_id":        mongoEntry.EventID,
		"subscription_id": mongoEntry.SubscriptionID,
	}

	update := bson.M{
		"$set": bson.M{
			"event_name":    mongoEntry.EventName,
			"bus_id":        mongoEntry.BusID,
			"delivery_mode": mongoEntry.DeliveryMode,
			"metadata":      mongoEntry.Metadata,
			"status":        mongoEntry.Status,
			"error":         mongoEntry.Error,
			"retry_count":   mongoEntry.RetryCount,
			"started_at":    mongoEntry.StartedAt,
			"completed_at":  mongoEntry.CompletedAt,
			"duration_ms":   mongoEntry.DurationMs,
			"trace_id":      mongoEntry.TraceID,
			"span_id":       mongoEntry.SpanID,
		},
		"$setOnInsert": bson.M{
			"event_id":        mongoEntry.EventID,
			"subscription_id": mongoEntry.SubscriptionID,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := s.collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return fmt.Errorf("record monitor: %w", err)
	}

	return nil
}

// Get retrieves a monitor entry by its composite key.
func (s *MongoStore) Get(ctx context.Context, eventID, subscriptionID string) (*Entry, error) {
	filter := bson.M{
		"event_id":        eventID,
		"subscription_id": subscriptionID,
	}

	var doc MongoEntry
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor: %w", err)
	}

	return doc.ToEntry(), nil
}

// GetByEventID returns all entries for an event ID.
func (s *MongoStore) GetByEventID(ctx context.Context, eventID string) ([]*Entry, error) {
	filter := bson.M{"event_id": eventID}
	opts := options.Find().SetSort(bson.D{{Key: "started_at", Value: 1}})

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("get by event id: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []*Entry
	for cursor.Next(ctx) {
		var doc MongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode entry: %w", err)
		}
		entries = append(entries, doc.ToEntry())
	}

	return entries, cursor.Err()
}

// mongoCursor represents the pagination cursor state for MongoDB.
type mongoCursor struct {
	StartedAt time.Time `json:"s"`
	EventID   string    `json:"e"`
	SubID     string    `json:"u"`
}

// List returns a page of entries matching the filter.
func (s *MongoStore) List(ctx context.Context, filter Filter) (*Page, error) {
	mongoFilter := s.buildFilter(filter)

	// Apply cursor for pagination
	if filter.Cursor != "" {
		cur, err := decodeMongoCursor(filter.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		var cursorFilter bson.M
		if filter.OrderDesc {
			cursorFilter = bson.M{
				"$or": []bson.M{
					{"started_at": bson.M{"$lt": cur.StartedAt}},
					{
						"started_at": cur.StartedAt,
						"event_id":   bson.M{"$lt": cur.EventID},
					},
					{
						"started_at":      cur.StartedAt,
						"event_id":        cur.EventID,
						"subscription_id": bson.M{"$lt": cur.SubID},
					},
				},
			}
		} else {
			cursorFilter = bson.M{
				"$or": []bson.M{
					{"started_at": bson.M{"$gt": cur.StartedAt}},
					{
						"started_at": cur.StartedAt,
						"event_id":   bson.M{"$gt": cur.EventID},
					},
					{
						"started_at":      cur.StartedAt,
						"event_id":        cur.EventID,
						"subscription_id": bson.M{"$gt": cur.SubID},
					},
				},
			}
		}

		// Merge cursor filter with existing filters
		if len(mongoFilter) > 0 {
			mongoFilter = bson.M{"$and": []bson.M{mongoFilter, cursorFilter}}
		} else {
			mongoFilter = cursorFilter
		}
	}

	// Build sort
	sortOrder := 1
	if filter.OrderDesc {
		sortOrder = -1
	}
	sort := bson.D{
		{Key: "started_at", Value: sortOrder},
		{Key: "event_id", Value: sortOrder},
		{Key: "subscription_id", Value: sortOrder},
	}

	// Query one extra row to check for more pages
	limit := int64(filter.EffectiveLimit() + 1)

	findOpts := options.Find().
		SetSort(sort).
		SetLimit(limit)

	cursor, err := s.collection.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("list monitor: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []*Entry
	for cursor.Next(ctx) {
		var doc MongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode entry: %w", err)
		}
		entries = append(entries, doc.ToEntry())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	// Check if there are more pages
	hasMore := len(entries) > filter.EffectiveLimit()
	if hasMore {
		entries = entries[:filter.EffectiveLimit()]
	}

	// Create next cursor
	var nextCursor string
	if hasMore && len(entries) > 0 {
		lastEntry := entries[len(entries)-1]
		nextCursor = encodeMongoCursor(mongoCursor{
			StartedAt: lastEntry.StartedAt,
			EventID:   lastEntry.EventID,
			SubID:     lastEntry.SubscriptionID,
		})
	}

	return &Page{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// buildFilter creates a MongoDB filter from monitor Filter.
func (s *MongoStore) buildFilter(filter Filter) bson.M {
	mongoFilter := bson.M{}

	if filter.EventID != "" {
		mongoFilter["event_id"] = filter.EventID
	}
	if filter.SubscriptionID != "" {
		mongoFilter["subscription_id"] = filter.SubscriptionID
	}
	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}
	if filter.BusID != "" {
		mongoFilter["bus_id"] = filter.BusID
	}
	if filter.DeliveryMode != nil {
		mongoFilter["delivery_mode"] = filter.DeliveryMode.String()
	}
	if len(filter.Status) > 0 {
		statuses := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			statuses[i] = string(status)
		}
		mongoFilter["status"] = bson.M{"$in": statuses}
	}
	if filter.HasError != nil {
		if *filter.HasError {
			mongoFilter["error"] = bson.M{"$ne": ""}
		} else {
			mongoFilter["$or"] = []bson.M{
				{"error": ""},
				{"error": bson.M{"$exists": false}},
			}
		}
	}
	if !filter.StartTime.IsZero() {
		if mongoFilter["started_at"] == nil {
			mongoFilter["started_at"] = bson.M{}
		}
		mongoFilter["started_at"].(bson.M)["$gte"] = filter.StartTime
	}
	if !filter.EndTime.IsZero() {
		if mongoFilter["started_at"] == nil {
			mongoFilter["started_at"] = bson.M{}
		}
		mongoFilter["started_at"].(bson.M)["$lt"] = filter.EndTime
	}
	if filter.MinDuration > 0 {
		mongoFilter["duration_ms"] = bson.M{"$gte": filter.MinDuration.Milliseconds()}
	}
	if filter.MinRetries > 0 {
		mongoFilter["retry_count"] = bson.M{"$gte": filter.MinRetries}
	}

	return mongoFilter
}

// Count returns the number of entries matching the filter.
func (s *MongoStore) Count(ctx context.Context, filter Filter) (int64, error) {
	mongoFilter := s.buildFilter(filter)
	return s.collection.CountDocuments(ctx, mongoFilter)
}

// UpdateStatus updates the status and related fields of an existing entry.
func (s *MongoStore) UpdateStatus(ctx context.Context, eventID, subscriptionID string, status Status, err error, duration time.Duration) error {
	filter := bson.M{
		"event_id":        eventID,
		"subscription_id": subscriptionID,
	}

	now := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":       string(status),
			"duration_ms":  duration.Milliseconds(),
			"completed_at": now,
		},
	}

	if err != nil {
		update["$set"].(bson.M)["error"] = err.Error()
	}

	_, updateErr := s.collection.UpdateOne(ctx, filter, update)
	if updateErr != nil {
		return fmt.Errorf("update status: %w", updateErr)
	}

	return nil
}

// DeleteOlderThan removes entries older than the specified age.
func (s *MongoStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	filter := bson.M{"started_at": bson.M{"$lt": cutoff}}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete old entries: %w", err)
	}

	return result.DeletedCount, nil
}

// encodeMongoCursor encodes a cursor to a string.
func encodeMongoCursor(c mongoCursor) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

// decodeMongoCursor decodes a cursor from a string.
func decodeMongoCursor(str string) (mongoCursor, error) {
	var c mongoCursor
	if str == "" {
		return c, nil
	}
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// RecordStart records when event processing begins.
// Implements event.MonitorStore interface.
func (s *MongoStore) RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string,
	workerPool bool, metadata map[string]string, traceID, spanID string) error {

	mode := Broadcast
	if workerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:        eventID,
		SubscriptionID: subscriptionID,
		EventName:      eventName,
		BusID:          busID,
		DeliveryMode:   mode,
		Metadata:       metadata,
		Status:         StatusPending,
		StartedAt:      time.Now(),
		TraceID:        traceID,
		SpanID:         spanID,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *MongoStore) RecordComplete(ctx context.Context, eventID, subscriptionID, status string,
	handlerErr error, duration time.Duration) error {

	return s.UpdateStatus(ctx, eventID, subscriptionID, Status(status), handlerErr, duration)
}

// Compile-time check that MongoStore implements Store.
var _ Store = (*MongoStore)(nil)

package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	event "github.com/rbaliyan/event/v3"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

/*
MongoDB Schema:

Collection: monitor_entries

Document structure:
{
    "_id": ObjectId,
    "event_id": string,
    "subscription_id": string,
    "subscriber_name": string,
    "subscriber_description": string,
    "event_name": string,
    "bus_id": string,
    "instance_id": string,
    "delivery_mode": string,
    "metadata": object,
    "status": string,
    "error": string,
    "retry_count": int,
    "started_at": ISODate,
    "completed_at": ISODate,
    "duration_ms": int64,
    "trace_id": string,
    "span_id": string,
    "worker_group": string
}

Indexes:
db.monitor_entries.createIndex({"event_id": 1, "subscription_id": 1}, {unique: true})
db.monitor_entries.createIndex({"event_name": 1, "started_at": 1})
db.monitor_entries.createIndex({"status": 1, "started_at": 1})
db.monitor_entries.createIndex({"bus_id": 1, "started_at": 1})
db.monitor_entries.createIndex({"subscriber_name": 1, "started_at": 1})
db.monitor_entries.createIndex({"started_at": 1, "event_id": 1, "subscription_id": 1})
*/

// mongoEntry represents a monitor entry document in MongoDB.
type mongoEntry struct {
	EventID               string            `bson:"event_id"`
	SubscriptionID        string            `bson:"subscription_id"`
	SubscriberName        string            `bson:"subscriber_name,omitempty"`
	SubscriberDescription string            `bson:"subscriber_description,omitempty"`
	EventName             string            `bson:"event_name"`
	BusID                 string            `bson:"bus_id"`
	InstanceID            string            `bson:"instance_id,omitempty"`
	DeliveryMode          string            `bson:"delivery_mode"`
	Metadata              map[string]string `bson:"metadata,omitempty"`
	Status                string            `bson:"status"`
	Error                 string            `bson:"error,omitempty"`
	RetryCount            int               `bson:"retry_count"`
	StartedAt             time.Time         `bson:"started_at"`
	CompletedAt           *time.Time        `bson:"completed_at,omitempty"`
	DurationMs            *int64            `bson:"duration_ms,omitempty"`
	TraceID               string            `bson:"trace_id,omitempty"`
	SpanID                string            `bson:"span_id,omitempty"`
	WorkerGroup           string            `bson:"worker_group,omitempty"`
}

// toEntry converts mongoEntry to Entry.
func (m *mongoEntry) toEntry() *Entry {
	entry := &Entry{
		EventID:               m.EventID,
		SubscriptionID:        m.SubscriptionID,
		SubscriberName:        m.SubscriberName,
		SubscriberDescription: m.SubscriberDescription,
		EventName:             m.EventName,
		BusID:                 m.BusID,
		InstanceID:            m.InstanceID,
		DeliveryMode:          ParseDeliveryMode(m.DeliveryMode),
		Metadata:              m.Metadata,
		Status:                Status(m.Status),
		Error:                 m.Error,
		RetryCount:            m.RetryCount,
		StartedAt:             m.StartedAt,
		CompletedAt:           m.CompletedAt,
		TraceID:               m.TraceID,
		SpanID:                m.SpanID,
		WorkerGroup:           m.WorkerGroup,
	}
	if m.DurationMs != nil {
		entry.Duration = time.Duration(*m.DurationMs) * time.Millisecond
	}
	return entry
}

// fromEntry creates a mongoEntry from Entry.
func fromEntry(e *Entry) *mongoEntry {
	var durationMs *int64
	if e.Duration > 0 {
		ms := e.Duration.Milliseconds()
		durationMs = &ms
	}

	subscriptionID := e.SubscriptionID
	if e.DeliveryMode == WorkerPool {
		subscriptionID = ""
	}

	return &mongoEntry{
		EventID:               e.EventID,
		SubscriptionID:        subscriptionID,
		SubscriberName:        e.SubscriberName,
		SubscriberDescription: e.SubscriberDescription,
		EventName:             e.EventName,
		BusID:                 e.BusID,
		InstanceID:            e.InstanceID,
		DeliveryMode:          e.DeliveryMode.String(),
		Metadata:              e.Metadata,
		Status:                string(e.Status),
		Error:                 e.Error,
		RetryCount:            e.RetryCount,
		StartedAt:             e.StartedAt,
		CompletedAt:           e.CompletedAt,
		DurationMs:            durationMs,
		TraceID:               e.TraceID,
		SpanID:                e.SpanID,
		WorkerGroup:           e.WorkerGroup,
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
func NewMongoStore(db *mongo.Database) (*MongoStore, error) {
	if db == nil {
		return nil, errors.New("mongodb: database is required")
	}

	return &MongoStore{
		collection: db.Collection("monitor_entries"),
	}, nil
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
		// Unique constraint: primary key for upserts, point lookups, and status updates.
		{
			Keys:    bson.D{{Key: "event_id", Value: 1}, {Key: "subscription_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		// event_name + started_at: supports filtering by event with time-range queries
		// and eliminates in-memory sorts on the result set.
		{
			Keys: bson.D{{Key: "event_name", Value: 1}, {Key: "started_at", Value: 1}},
		},
		// status + started_at: supports "show all failed/pending events in last N hours"
		// queries without in-memory sorts.
		{
			Keys: bson.D{{Key: "status", Value: 1}, {Key: "started_at", Value: 1}},
		},
		// bus_id + started_at: supports per-bus filtering in multi-store deployments.
		{
			Keys: bson.D{{Key: "bus_id", Value: 1}, {Key: "started_at", Value: 1}},
		},
		// subscriber_name + started_at: supports per-subscriber filtering with time range.
		{
			Keys: bson.D{{Key: "subscriber_name", Value: 1}, {Key: "started_at", Value: 1}},
		},
		// started_at + event_id + subscription_id: covers DeleteOlderThan (range on
		// started_at), unfiltered List queries, and the full cursor sort key
		// (started_at, event_id, subscription_id) used in cursor-based pagination.
		{
			Keys: bson.D{
				{Key: "started_at", Value: 1},
				{Key: "event_id", Value: 1},
				{Key: "subscription_id", Value: 1},
			},
		},
	}
}

// EnsureIndexes creates the required indexes for the monitor collection.
func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Record creates or updates a monitor entry.
//
// In WorkerPool mode, fields are written with $setOnInsert so that only the
// first pod to call Record() (the one most likely to win acquisition) populates
// the entry. Subsequent pods that lose the worker pool race will not overwrite
// instance_id, started_at, trace_id, or span_id of the winning pod.
func (s *MongoStore) Record(ctx context.Context, entry *Entry) error {
	mongoEntry := fromEntry(entry)

	filter := bson.M{
		"event_id":        mongoEntry.EventID,
		"subscription_id": mongoEntry.SubscriptionID,
	}

	var update bson.M

	if entry.DeliveryMode == WorkerPool {
		// WorkerPool mode: use $setOnInsert for all fields so only the first
		// writer populates the entry. This prevents losing pods from overwriting
		// the winning pod's instance_id, started_at, and trace context.
		update = bson.M{
			"$setOnInsert": bson.M{
				"event_id":               mongoEntry.EventID,
				"subscription_id":        mongoEntry.SubscriptionID,
				"subscriber_name":        mongoEntry.SubscriberName,
				"subscriber_description": mongoEntry.SubscriberDescription,
				"event_name":             mongoEntry.EventName,
				"bus_id":                 mongoEntry.BusID,
				"instance_id":            mongoEntry.InstanceID,
				"delivery_mode":          mongoEntry.DeliveryMode,
				"metadata":               mongoEntry.Metadata,
				"status":                 mongoEntry.Status,
				"error":                  mongoEntry.Error,
				"retry_count":            mongoEntry.RetryCount,
				"started_at":             mongoEntry.StartedAt,
				"completed_at":           mongoEntry.CompletedAt,
				"duration_ms":            mongoEntry.DurationMs,
				"trace_id":               mongoEntry.TraceID,
				"span_id":                mongoEntry.SpanID,
				"worker_group":           mongoEntry.WorkerGroup,
			},
		}
	} else {
		// Broadcast mode: each subscription has its own entry, so $set is safe.
		update = bson.M{
			"$set": bson.M{
				"subscriber_name":        mongoEntry.SubscriberName,
				"subscriber_description": mongoEntry.SubscriberDescription,
				"event_name":             mongoEntry.EventName,
				"bus_id":                 mongoEntry.BusID,
				"instance_id":            mongoEntry.InstanceID,
				"delivery_mode":          mongoEntry.DeliveryMode,
				"metadata":               mongoEntry.Metadata,
				"status":                 mongoEntry.Status,
				"error":                  mongoEntry.Error,
				"retry_count":            mongoEntry.RetryCount,
				"started_at":             mongoEntry.StartedAt,
				"completed_at":           mongoEntry.CompletedAt,
				"duration_ms":            mongoEntry.DurationMs,
				"trace_id":               mongoEntry.TraceID,
				"span_id":                mongoEntry.SpanID,
				"worker_group":           mongoEntry.WorkerGroup,
			},
			"$setOnInsert": bson.M{
				"event_id":        mongoEntry.EventID,
				"subscription_id": mongoEntry.SubscriptionID,
			},
		}
	}

	opts := options.UpdateOne().SetUpsert(true)
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

	var doc mongoEntry
	err := s.collection.FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor: %w", err)
	}

	return doc.toEntry(), nil
}

// GetByEventID returns all entries for an event ID.
func (s *MongoStore) GetByEventID(ctx context.Context, eventID string) ([]*Entry, error) {
	filter := bson.M{"event_id": eventID}
	opts := options.Find().SetSort(bson.D{{Key: "started_at", Value: 1}})

	cursor, err := s.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("get by event id: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*Entry
	for cursor.Next(ctx) {
		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode entry: %w", err)
		}
		entries = append(entries, doc.toEntry())
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
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*Entry
	for cursor.Next(ctx) {
		var doc mongoEntry
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode entry: %w", err)
		}
		entries = append(entries, doc.toEntry())
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
	if filter.SubscriberName != "" {
		mongoFilter["subscriber_name"] = filter.SubscriberName
	}
	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}
	if filter.BusID != "" {
		mongoFilter["bus_id"] = filter.BusID
	}
	if filter.InstanceID != "" {
		mongoFilter["instance_id"] = filter.InstanceID
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
	if filter.WorkerGroup != "" {
		mongoFilter["worker_group"] = filter.WorkerGroup
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
func (s *MongoStore) RecordStart(ctx context.Context, params event.RecordStartParams) error {
	mode := Broadcast
	if params.WorkerPool {
		mode = WorkerPool
	}

	entry := &Entry{
		EventID:               params.EventID,
		SubscriptionID:        params.SubscriptionID,
		SubscriberName:        params.SubscriberName,
		SubscriberDescription: params.SubscriberDescription,
		EventName:             params.EventName,
		BusID:                 params.BusID,
		DeliveryMode:          mode,
		Metadata:              params.Metadata,
		Status:                StatusPending,
		StartedAt:             time.Now(),
		TraceID:               params.TraceID,
		SpanID:                params.SpanID,
		WorkerGroup:           params.WorkerGroup,
	}

	return s.Record(ctx, entry)
}

// RecordComplete updates the entry with the final result.
// Implements event.MonitorStore interface.
func (s *MongoStore) RecordComplete(ctx context.Context, params event.RecordCompleteParams) error {
	return s.UpdateStatus(ctx, params.EventID, params.SubscriptionID, Status(params.Status), params.Error, params.Duration)
}

// Summary returns aggregated statistics using a MongoDB $facet aggregation pipeline.
func (s *MongoStore) Summary(ctx context.Context, filter Filter) (*Summary, error) {
	matchStage := s.buildFilter(filter)

	// Default to last 24h if no time range specified to avoid full collection scan
	if filter.StartTime.IsZero() && filter.EndTime.IsZero() {
		if existing, ok := matchStage["started_at"].(bson.M); ok {
			existing["$gte"] = time.Now().Add(-24 * time.Hour)
		} else {
			matchStage["started_at"] = bson.M{"$gte": time.Now().Add(-24 * time.Hour)}
		}
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchStage}},
		{{Key: "$facet", Value: bson.M{
			"by_status": mongo.Pipeline{
				{{Key: "$group", Value: bson.M{
					"_id":   "$status",
					"count": bson.M{"$sum": 1},
				}}},
			},
			"by_event": mongo.Pipeline{
				{{Key: "$group", Value: bson.M{
					"_id":       "$event_name",
					"total":     bson.M{"$sum": 1},
					"completed": bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$status", "completed"}}, 1, 0}}},
					"failed":    bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$status", "failed"}}, 1, 0}}},
					"retrying":  bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$status", "retrying"}}, 1, 0}}},
					"pending":   bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$status", "pending"}}, 1, 0}}},
					"avg_dur":   bson.M{"$avg": "$duration_ms"},
				}}},
			},
			"by_instance": mongo.Pipeline{
				{{Key: "$match", Value: bson.M{"instance_id": bson.M{"$ne": ""}}}},
				{{Key: "$group", Value: bson.M{
					"_id":   "$instance_id",
					"count": bson.M{"$sum": 1},
				}}},
			},
			"global": mongo.Pipeline{
				{{Key: "$group", Value: bson.M{
					"_id":     nil,
					"total":   bson.M{"$sum": 1},
					"avg_dur": bson.M{"$avg": "$duration_ms"},
					"failed":  bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$status", "failed"}}, 1, 0}}},
				}}},
			},
			"oldest": mongo.Pipeline{
				{{Key: "$sort", Value: bson.M{"started_at": 1}}},
				{{Key: "$limit", Value: 1}},
				{{Key: "$project", Value: bson.M{"started_at": 1}}},
			},
			"newest": mongo.Pipeline{
				{{Key: "$sort", Value: bson.M{"started_at": -1}}},
				{{Key: "$limit", Value: 1}},
				{{Key: "$project", Value: bson.M{"started_at": 1}}},
			},
		}}},
	}

	cursor, err := s.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("summary aggregate: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var r struct {
		ByStatus []struct {
			ID    string `bson:"_id"`
			Count int64  `bson:"count"`
		} `bson:"by_status"`
		ByEvent []struct {
			ID        string   `bson:"_id"`
			Total     int64    `bson:"total"`
			Completed int64    `bson:"completed"`
			Failed    int64    `bson:"failed"`
			Retrying  int64    `bson:"retrying"`
			Pending   int64    `bson:"pending"`
			AvgDur    *float64 `bson:"avg_dur"`
		} `bson:"by_event"`
		ByInstance []struct {
			ID    string `bson:"_id"`
			Count int64  `bson:"count"`
		} `bson:"by_instance"`
		Global []struct {
			Total  int64    `bson:"total"`
			AvgDur *float64 `bson:"avg_dur"`
			Failed int64    `bson:"failed"`
		} `bson:"global"`
		Oldest []struct {
			StartedAt time.Time `bson:"started_at"`
		} `bson:"oldest"`
		Newest []struct {
			StartedAt time.Time `bson:"started_at"`
		} `bson:"newest"`
	}

	if !cursor.Next(ctx) {
		return &Summary{
			ByStatus:    make(map[Status]int64),
			ByEventName: make(map[string]*EventStats),
		}, nil
	}
	if err := cursor.Decode(&r); err != nil {
		return nil, fmt.Errorf("decode summary: %w", err)
	}
	summary := &Summary{
		ByStatus:    make(map[Status]int64, len(r.ByStatus)),
		ByEventName: make(map[string]*EventStats, len(r.ByEvent)),
	}

	for _, s := range r.ByStatus {
		summary.ByStatus[Status(s.ID)] = s.Count
	}

	for _, e := range r.ByEvent {
		es := &EventStats{
			Total:     e.Total,
			Completed: e.Completed,
			Failed:    e.Failed,
			Retrying:  e.Retrying,
			Pending:   e.Pending,
		}
		if e.AvgDur != nil {
			es.AvgDurationMs = int64(*e.AvgDur)
		}
		if es.Total > 0 {
			es.ErrorRate = float64(es.Failed) / float64(es.Total)
		}
		summary.ByEventName[e.ID] = es
	}

	if len(r.ByInstance) > 0 {
		summary.ByInstance = make(map[string]int64, len(r.ByInstance))
		for _, inst := range r.ByInstance {
			summary.ByInstance[inst.ID] = inst.Count
		}
	}

	if len(r.Global) > 0 {
		g := r.Global[0]
		summary.TotalEntries = g.Total
		if g.AvgDur != nil {
			summary.AvgDurationMs = int64(*g.AvgDur)
		}
		if g.Total > 0 {
			summary.ErrorRate = float64(g.Failed) / float64(g.Total)
		}
	}

	if len(r.Oldest) > 0 {
		t := r.Oldest[0].StartedAt
		summary.TimeRange.Oldest = &t
	}
	if len(r.Newest) > 0 {
		t := r.Newest[0].StartedAt
		summary.TimeRange.Newest = &t
	}

	return summary, nil
}

// Compile-time check that MongoStore implements Store.
var _ Store = (*MongoStore)(nil)
var _ SummaryProvider = (*MongoStore)(nil)

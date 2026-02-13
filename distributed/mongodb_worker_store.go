package distributed

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// workerCursor represents the pagination cursor state for worker queries.
type workerCursor struct {
	UpdatedAt time.Time `json:"u"`
	ID        string    `json:"i"`
}

// ListWorkers returns a paginated list of worker entries matching the filter.
func (s *MongoStateManager) ListWorkers(ctx context.Context, filter WorkerFilter) (*WorkerPage, error) {
	mongoFilter := s.buildWorkerFilter(filter)

	// Apply cursor for pagination
	if filter.Cursor != "" {
		cur, err := decodeWorkerCursor(filter.Cursor)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}

		var cursorFilter bson.M
		if filter.OrderDesc {
			cursorFilter = bson.M{
				"$or": []bson.M{
					{"updated_at": bson.M{"$lt": cur.UpdatedAt}},
					{
						"updated_at": cur.UpdatedAt,
						"_id":        bson.M{"$lt": cur.ID},
					},
				},
			}
		} else {
			cursorFilter = bson.M{
				"$or": []bson.M{
					{"updated_at": bson.M{"$gt": cur.UpdatedAt}},
					{
						"updated_at": cur.UpdatedAt,
						"_id":        bson.M{"$gt": cur.ID},
					},
				},
			}
		}

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
		{Key: "updated_at", Value: sortOrder},
		{Key: "_id", Value: sortOrder},
	}

	// Query one extra to detect next page
	limit := int64(filter.EffectiveLimit() + 1)

	findOpts := options.Find().
		SetSort(sort).
		SetLimit(limit)

	cursor, err := s.collection.Find(ctx, mongoFilter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var entries []*WorkerEntry
	for cursor.Next(ctx) {
		var doc stateDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode worker: %w", err)
		}
		entries = append(entries, stateDocToWorkerEntry(&doc))
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	hasMore := len(entries) > filter.EffectiveLimit()
	if hasMore {
		entries = entries[:filter.EffectiveLimit()]
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = encodeWorkerCursor(workerCursor{
			UpdatedAt: last.UpdatedAt,
			ID:        last.MessageID,
		})
	}

	return &WorkerPage{
		Entries:    entries,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// CountWorkers returns the number of worker entries matching the filter.
func (s *MongoStateManager) CountWorkers(ctx context.Context, filter WorkerFilter) (int64, error) {
	mongoFilter := s.buildWorkerFilter(filter)
	count, err := s.collection.CountDocuments(ctx, mongoFilter)
	if err != nil {
		return 0, fmt.Errorf("count workers: %w", err)
	}
	return count, nil
}

// GetWorker returns a single worker entry by message ID.
func (s *MongoStateManager) GetWorker(ctx context.Context, messageID string) (*WorkerEntry, error) {
	var doc stateDocument
	err := s.collection.FindOne(ctx, bson.M{"_id": messageID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get worker: %w", err)
	}
	return stateDocToWorkerEntry(&doc), nil
}

// buildWorkerFilter creates a MongoDB filter from WorkerFilter.
func (s *MongoStateManager) buildWorkerFilter(filter WorkerFilter) bson.M {
	m := bson.M{}

	if filter.StaleTimeout > 0 {
		// StaleTimeout implies processing status with stale updated_at.
		// This is mutually exclusive with the Status filter.
		cutoff := time.Now().Add(-filter.StaleTimeout)
		m["status"] = statusProcessing
		m["updated_at"] = bson.M{"$lt": cutoff}
	} else if len(filter.Status) > 0 {
		statuses := make([]string, len(filter.Status))
		for i, st := range filter.Status {
			statuses[i] = string(st)
		}
		m["status"] = bson.M{"$in": statuses}
	}
	if filter.EventName != "" {
		m["event_name"] = filter.EventName
	}
	if filter.WorkerID != "" {
		m["worker_id"] = filter.WorkerID
	}
	if !filter.CreatedAfter.IsZero() || !filter.CreatedBefore.IsZero() {
		createdAt := bson.M{}
		if !filter.CreatedAfter.IsZero() {
			createdAt["$gte"] = filter.CreatedAfter
		}
		if !filter.CreatedBefore.IsZero() {
			createdAt["$lt"] = filter.CreatedBefore
		}
		m["created_at"] = createdAt
	}

	return m
}

// stateDocToWorkerEntry converts a stateDocument to a WorkerEntry.
func stateDocToWorkerEntry(doc *stateDocument) *WorkerEntry {
	return &WorkerEntry{
		MessageID: doc.ID,
		Status:    WorkerState(doc.Status),
		WorkerID:  doc.WorkerID,
		EventName: doc.EventName,
		Metadata:  doc.Metadata,
		ExpiresAt: doc.ExpiresAt,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
	}
}

func encodeWorkerCursor(c workerCursor) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

func decodeWorkerCursor(str string) (workerCursor, error) {
	var c workerCursor
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// Compile-time check that MongoStateManager implements WorkerStore.
var _ WorkerStore = (*MongoStateManager)(nil)

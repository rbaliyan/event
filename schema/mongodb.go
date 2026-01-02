package schema

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoDB Schema:

Collection: event_schemas

Document structure:
{
    "_id": string,  // event name as document ID
    "version": int,
    "description": string,
    "sub_timeout_ms": int64,
    "max_retries": int,
    "retry_backoff_ms": int64,
    "enable_monitor": bool,
    "enable_idempotency": bool,
    "enable_poison": bool,
    "metadata": object,
    "created_at": ISODate,
    "updated_at": ISODate
}

Indexes:
db.event_schemas.createIndex({"updated_at": 1})
*/

// mongoSchema represents a schema document in MongoDB.
type mongoSchema struct {
	Name              string            `bson:"_id"`
	Version           int               `bson:"version"`
	Description       string            `bson:"description,omitempty"`
	SubTimeoutMs      *int64            `bson:"sub_timeout_ms,omitempty"`
	MaxRetries        *int              `bson:"max_retries,omitempty"`
	RetryBackoffMs    *int64            `bson:"retry_backoff_ms,omitempty"`
	EnableMonitor     bool              `bson:"enable_monitor"`
	EnableIdempotency bool              `bson:"enable_idempotency"`
	EnablePoison      bool              `bson:"enable_poison"`
	Metadata          map[string]string `bson:"metadata,omitempty"`
	CreatedAt         time.Time         `bson:"created_at"`
	UpdatedAt         time.Time         `bson:"updated_at"`
}

func (m *mongoSchema) toEventSchema() *EventSchema {
	schema := &EventSchema{
		Name:              m.Name,
		Version:           m.Version,
		Description:       m.Description,
		EnableMonitor:     m.EnableMonitor,
		EnableIdempotency: m.EnableIdempotency,
		EnablePoison:      m.EnablePoison,
		Metadata:          m.Metadata,
		CreatedAt:         m.CreatedAt,
		UpdatedAt:         m.UpdatedAt,
	}
	if m.SubTimeoutMs != nil {
		schema.SubTimeout = time.Duration(*m.SubTimeoutMs) * time.Millisecond
	}
	if m.MaxRetries != nil {
		schema.MaxRetries = *m.MaxRetries
	}
	if m.RetryBackoffMs != nil {
		schema.RetryBackoff = time.Duration(*m.RetryBackoffMs) * time.Millisecond
	}
	return schema
}

func fromEventSchema(s *EventSchema) *mongoSchema {
	m := &mongoSchema{
		Name:              s.Name,
		Version:           s.Version,
		Description:       s.Description,
		EnableMonitor:     s.EnableMonitor,
		EnableIdempotency: s.EnableIdempotency,
		EnablePoison:      s.EnablePoison,
		Metadata:          s.Metadata,
		CreatedAt:         s.CreatedAt,
		UpdatedAt:         s.UpdatedAt,
	}
	if s.SubTimeout > 0 {
		ms := s.SubTimeout.Milliseconds()
		m.SubTimeoutMs = &ms
	}
	if s.MaxRetries > 0 {
		m.MaxRetries = &s.MaxRetries
	}
	if s.RetryBackoff > 0 {
		ms := s.RetryBackoff.Milliseconds()
		m.RetryBackoffMs = &ms
	}
	return m
}

// MongoProvider implements SchemaProvider using MongoDB.
//
// Example:
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//	db := client.Database("mydb")
//	provider := schema.NewMongoProvider(db, publishFunc)
//	defer provider.Close()
type MongoProvider struct {
	collection *mongo.Collection
	publisher  func(context.Context, SchemaChangeEvent) error
	watchers   []chan SchemaChangeEvent
	mu         sync.RWMutex
	closed     bool
	closeChan  chan struct{}
}

// NewMongoProvider creates a new MongoDB-based schema provider.
//
// The publisher callback is called when a schema is set, to notify
// subscribers via the transport. It can be nil if Watch is not needed.
func NewMongoProvider(db *mongo.Database, publisher func(context.Context, SchemaChangeEvent) error) *MongoProvider {
	return &MongoProvider{
		collection: db.Collection("event_schemas"),
		publisher:  publisher,
		closeChan:  make(chan struct{}),
	}
}

// WithCollection sets a custom collection name.
func (p *MongoProvider) WithCollection(name string) *MongoProvider {
	p.collection = p.collection.Database().Collection(name)
	return p
}

// Collection returns the underlying MongoDB collection.
func (p *MongoProvider) Collection() *mongo.Collection {
	return p.collection
}

// Indexes returns the required indexes.
func (p *MongoProvider) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "updated_at", Value: 1}},
		},
	}
}

// EnsureIndexes creates the required indexes.
func (p *MongoProvider) EnsureIndexes(ctx context.Context) error {
	_, err := p.collection.Indexes().CreateMany(ctx, p.Indexes())
	return err
}

// Get retrieves a schema by event name.
func (p *MongoProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	var m mongoSchema
	err := p.collection.FindOne(ctx, bson.M{"_id": eventName}).Decode(&m)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	return m.toEventSchema(), nil
}

// Set stores a schema and notifies subscribers.
func (p *MongoProvider) Set(ctx context.Context, schema *EventSchema) error {
	if err := schema.Validate(); err != nil {
		return err
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	// Check version
	existing, err := p.Get(ctx, schema.Name)
	if err != nil {
		return err
	}
	if existing != nil && schema.Version < existing.Version {
		return fmt.Errorf("%w: %d < %d", ErrVersionDowngrade, schema.Version, existing.Version)
	}

	now := time.Now()
	m := fromEventSchema(schema)
	m.UpdatedAt = now
	if existing == nil {
		m.CreatedAt = now
	} else {
		m.CreatedAt = existing.CreatedAt
	}

	opts := options.Replace().SetUpsert(true)
	_, err = p.collection.ReplaceOne(ctx, bson.M{"_id": schema.Name}, m, opts)
	if err != nil {
		return fmt.Errorf("set schema: %w", err)
	}

	// Notify watchers and publisher
	change := SchemaChangeEvent{
		EventName: schema.Name,
		Version:   schema.Version,
		UpdatedAt: now,
	}

	p.mu.RLock()
	for _, watcher := range p.watchers {
		select {
		case watcher <- change:
		default:
			// Watcher buffer full, skip
		}
	}
	p.mu.RUnlock()

	if p.publisher != nil {
		if err := p.publisher(ctx, change); err != nil {
			// Log but don't fail - schema is stored
			return nil
		}
	}

	return nil
}

// Delete removes a schema.
func (p *MongoProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	_, err := p.collection.DeleteOne(ctx, bson.M{"_id": eventName})
	if err != nil {
		return fmt.Errorf("delete schema: %w", err)
	}
	return nil
}

// Watch returns a channel that receives schema change notifications.
func (p *MongoProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrProviderClosed
	}

	ch := make(chan SchemaChangeEvent, 100)
	p.watchers = append(p.watchers, ch)

	// Close channel when context is done or provider is closed
	go func() {
		select {
		case <-ctx.Done():
		case <-p.closeChan:
		}
		p.mu.Lock()
		defer p.mu.Unlock()

		// Remove watcher
		for i, w := range p.watchers {
			if w == ch {
				p.watchers = append(p.watchers[:i], p.watchers[i+1:]...)
				break
			}
		}
		close(ch)
	}()

	return ch, nil
}

// List returns all schemas.
func (p *MongoProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	cursor, err := p.collection.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	defer cursor.Close(ctx)

	var schemas []*EventSchema
	for cursor.Next(ctx) {
		var m mongoSchema
		if err := cursor.Decode(&m); err != nil {
			return nil, fmt.Errorf("decode schema: %w", err)
		}
		schemas = append(schemas, m.toEventSchema())
	}

	return schemas, cursor.Err()
}

// Close closes the provider.
func (p *MongoProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.closeChan)
	return nil
}

// Compile-time check that MongoProvider implements SchemaProvider.
var _ SchemaProvider = (*MongoProvider)(nil)

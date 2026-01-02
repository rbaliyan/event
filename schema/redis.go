package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisProvider implements SchemaProvider using Redis Hash.
//
// Schemas are stored in a Redis Hash where:
//   - Key: configurable (default: "event:schemas")
//   - Field: event name
//   - Value: JSON-encoded EventSchema
//
// Example:
//
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	provider := schema.NewRedisProvider(client, publishFunc)
//	defer provider.Close()
type RedisProvider struct {
	client    redis.UniversalClient
	key       string
	publisher func(context.Context, SchemaChangeEvent) error
	watchers  []chan SchemaChangeEvent
	mu        sync.RWMutex
	closed    bool
	closeChan chan struct{}
}

// RedisOption configures RedisProvider.
type RedisOption func(*RedisProvider)

// WithKey sets a custom hash key (default: "event:schemas").
func WithKey(key string) RedisOption {
	return func(p *RedisProvider) {
		p.key = key
	}
}

// NewRedisProvider creates a new Redis-based schema provider.
//
// The publisher callback is called when a schema is set, to notify
// subscribers via the transport. It can be nil if Watch is not needed.
func NewRedisProvider(client redis.UniversalClient, publisher func(context.Context, SchemaChangeEvent) error, opts ...RedisOption) *RedisProvider {
	p := &RedisProvider{
		client:    client,
		key:       "event:schemas",
		publisher: publisher,
		closeChan: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Get retrieves a schema by event name.
func (p *RedisProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	data, err := p.client.HGet(ctx, p.key, eventName).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}

	var schema EventSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("unmarshal schema: %w", err)
	}

	return &schema, nil
}

// Set stores a schema and notifies subscribers.
func (p *RedisProvider) Set(ctx context.Context, schema *EventSchema) error {
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
	schemaCopy := schema.Clone()
	if schemaCopy.CreatedAt.IsZero() {
		if existing != nil {
			schemaCopy.CreatedAt = existing.CreatedAt
		} else {
			schemaCopy.CreatedAt = now
		}
	}
	schemaCopy.UpdatedAt = now

	data, err := json.Marshal(schemaCopy)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	if err := p.client.HSet(ctx, p.key, schema.Name, data).Err(); err != nil {
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
func (p *RedisProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProviderClosed
	}
	p.mu.RUnlock()

	if err := p.client.HDel(ctx, p.key, eventName).Err(); err != nil {
		return fmt.Errorf("delete schema: %w", err)
	}
	return nil
}

// Watch returns a channel that receives schema change notifications.
func (p *RedisProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
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
func (p *RedisProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrProviderClosed
	}
	p.mu.RUnlock()

	result, err := p.client.HGetAll(ctx, p.key).Result()
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}

	schemas := make([]*EventSchema, 0, len(result))
	for _, data := range result {
		var schema EventSchema
		if err := json.Unmarshal([]byte(data), &schema); err != nil {
			return nil, fmt.Errorf("unmarshal schema: %w", err)
		}
		schemas = append(schemas, &schema)
	}

	return schemas, nil
}

// Close closes the provider.
func (p *RedisProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.closeChan)
	return nil
}

// Compile-time check that RedisProvider implements SchemaProvider.
var _ SchemaProvider = (*RedisProvider)(nil)

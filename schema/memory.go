package schema

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryProvider implements SchemaProvider using in-memory storage.
// Primarily intended for testing and development.
// Data is lost on restart.
//
// Example:
//
//	provider := schema.NewMemoryProvider()
//	defer provider.Close()
//
//	bus := event.NewBus(
//	    event.WithTransport(transport),
//	    event.WithSchemaProvider(provider),
//	)
type MemoryProvider struct {
	mu        sync.RWMutex
	schemas   map[string]*EventSchema
	watchers  []chan SchemaChangeEvent
	closed    bool
	closeChan chan struct{}
}

// NewMemoryProvider creates a new in-memory schema provider.
func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{
		schemas:   make(map[string]*EventSchema),
		closeChan: make(chan struct{}),
	}
}

// Get retrieves a schema by event name.
func (p *MemoryProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrProviderClosed
	}

	schema, ok := p.schemas[eventName]
	if !ok {
		return nil, nil
	}
	return schema.Clone(), nil
}

// Set stores a schema and notifies subscribers.
func (p *MemoryProvider) Set(ctx context.Context, schema *EventSchema) error {
	if err := schema.Validate(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrProviderClosed
	}

	// Check version
	if existing, ok := p.schemas[schema.Name]; ok {
		if schema.Version < existing.Version {
			return fmt.Errorf("%w: %d < %d", ErrVersionDowngrade, schema.Version, existing.Version)
		}
	}

	// Store copy
	now := time.Now()
	schemaCopy := schema.Clone()
	if schemaCopy.CreatedAt.IsZero() {
		schemaCopy.CreatedAt = now
	}
	schemaCopy.UpdatedAt = now
	p.schemas[schema.Name] = schemaCopy

	// Notify watchers
	change := SchemaChangeEvent{
		EventName: schema.Name,
		Version:   schema.Version,
		UpdatedAt: now,
	}
	for _, watcher := range p.watchers {
		select {
		case watcher <- change:
		default:
			// Watcher buffer full, skip
		}
	}

	return nil
}

// Delete removes a schema.
func (p *MemoryProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrProviderClosed
	}

	delete(p.schemas, eventName)
	return nil
}

// Watch returns a channel that receives schema change notifications.
func (p *MemoryProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
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
func (p *MemoryProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return nil, ErrProviderClosed
	}

	result := make([]*EventSchema, 0, len(p.schemas))
	for _, schema := range p.schemas {
		result = append(result, schema.Clone())
	}
	return result, nil
}

// Close closes the provider.
func (p *MemoryProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.closeChan)
	p.schemas = nil
	return nil
}

// Len returns the number of schemas (for testing).
func (p *MemoryProvider) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.schemas)
}

// Compile-time check that MemoryProvider implements SchemaProvider.
var _ SchemaProvider = (*MemoryProvider)(nil)

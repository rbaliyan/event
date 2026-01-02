// Package schema provides event configuration registry with transport-first
// storage and database fallback support.
//
// Publishers define event processing configuration (timeouts, retries, feature flags)
// in the schema registry. Subscribers automatically load and apply this configuration
// when events are registered, ensuring all workers processing the same event have
// consistent settings.
//
// Schema providers support two strategies:
//   - Transport-based: Uses transport's KV/retention if available (NATS KV, Kafka compacted)
//   - Database fallback: PostgreSQL, MongoDB, or Redis for transports without retention
//
// Example usage:
//
//	// Publisher registers schema
//	err := schema.Register(ctx, bus, &schema.EventSchema{
//	    Name:              "order.created",
//	    Version:           1,
//	    SubTimeout:        30 * time.Second,
//	    MaxRetries:        3,
//	    EnableMonitor:     true,
//	    EnableIdempotency: true,
//	})
//
//	// Subscriber auto-loads schema on Register()
//	orderEvent := event.New[Order]("order.created")
//	event.Register(ctx, bus, orderEvent)
package schema

import (
	"context"
	"time"
)

// EventSchema defines processing configuration for an event.
// Publishers register schemas; subscribers auto-load them.
// This ensures all workers processing the same event have consistent settings.
type EventSchema struct {
	// Identity
	Name        string `json:"name" bson:"_id"`
	Version     int    `json:"version" bson:"version"`
	Description string `json:"description,omitempty" bson:"description,omitempty"`

	// Processing behavior (applied to all subscribers)
	SubTimeout   time.Duration `json:"sub_timeout,omitempty" bson:"sub_timeout,omitempty"`
	MaxRetries   int           `json:"max_retries,omitempty" bson:"max_retries,omitempty"`
	RetryBackoff time.Duration `json:"retry_backoff,omitempty" bson:"retry_backoff,omitempty"`

	// Feature flags (require corresponding bus config to take effect)
	EnableMonitor     bool `json:"enable_monitor" bson:"enable_monitor"`
	EnableIdempotency bool `json:"enable_idempotency" bson:"enable_idempotency"`
	EnablePoison      bool `json:"enable_poison" bson:"enable_poison"`

	// Metadata
	Metadata  map[string]string `json:"metadata,omitempty" bson:"metadata,omitempty"`
	CreatedAt time.Time         `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time         `json:"updated_at" bson:"updated_at"`
}

// SchemaChangeEvent is published when a schema is updated.
// All buses with schema registry auto-subscribe to this event.
type SchemaChangeEvent struct {
	EventName string    `json:"event_name"`
	Version   int       `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SchemaChangedEventName is the internal event name for schema change notifications.
const SchemaChangedEventName = "__schema.changed"

// SchemaProvider abstracts schema storage.
// Implemented by transports (with retention) or database stores.
type SchemaProvider interface {
	// Get retrieves a schema by event name.
	// Returns nil, nil if not found.
	Get(ctx context.Context, eventName string) (*EventSchema, error)

	// Set stores a schema and notifies subscribers.
	// Version must be >= existing version (no downgrades).
	Set(ctx context.Context, schema *EventSchema) error

	// Delete removes a schema.
	Delete(ctx context.Context, eventName string) error

	// Watch returns a channel that receives schema change notifications.
	// The channel is closed when the context is cancelled.
	Watch(ctx context.Context) (<-chan SchemaChangeEvent, error)

	// List returns all schemas (for startup sync).
	List(ctx context.Context) ([]*EventSchema, error)

	// Close releases resources.
	Close() error
}

// Validate validates the schema fields.
func (s *EventSchema) Validate() error {
	if s.Name == "" {
		return ErrEmptyName
	}
	if s.Version < 1 {
		return ErrInvalidVersion
	}
	return nil
}

// Clone creates a deep copy of the schema.
func (s *EventSchema) Clone() *EventSchema {
	clone := *s
	if s.Metadata != nil {
		clone.Metadata = make(map[string]string, len(s.Metadata))
		for k, v := range s.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

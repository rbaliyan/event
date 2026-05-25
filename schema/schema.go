// Package schema provides two related-but-distinct features for event
// definitions: an EventSchema configuration registry and a payload-schema
// evolution mechanism.
//
// # 1. EventSchema configuration registry
//
// Publishers define event processing configuration (timeouts, retries,
// feature flags) in the schema registry. Subscribers automatically load and
// apply this configuration when events are registered, ensuring all workers
// processing the same event have consistent settings.
//
// SchemaProvider implementations support two storage strategies:
//   - Transport-based: uses transport KV/retention if available (NATS KV,
//     Kafka compacted topics).
//   - Database fallback: PostgreSQL or Redis for transports without
//     retention. For MongoDB use the event-mongodb module
//     (https://github.com/rbaliyan/event-mongodb).
//
//	// Publisher stores the schema in a provider...
//	provider := schema.NewMemoryProvider()
//	_ = provider.Set(ctx, &schema.EventSchema{
//	    Name:              "order.created",
//	    Version:           1,
//	    SubTimeout:        30 * time.Second,
//	    MaxRetries:        3,
//	    EnableMonitor:     true,
//	    EnableIdempotency: true,
//	})
//
//	// ...and wires the provider into the bus.
//	bus, _ := event.NewBus("orders",
//	    event.WithTransport(transport),
//	    event.WithSchemaProvider(provider),
//	)
//
//	// Subscribers auto-load the schema when the event is registered with
//	// the bus; no extra call is needed.
//	orderEvent := event.New[Order]("order.created")
//	_ = event.Register(ctx, bus, orderEvent)
//
// # 2. Payload-schema evolution
//
// Payload schema evolution enables safe versioning of message payloads:
//   - Schema registration and validation (JSONSchema)
//   - Version upcasting (transforming old payload versions to new)
//   - Backward-compatibility checks
//   - Envelope-based wire format that carries version metadata
//
// The lifecycle is: register the new schema version, add an upcaster from
// the previous version, then consumers auto-upcast old messages to the
// latest schema before invoking the typed handler.
//
//	registry := schema.NewMemoryRegistry()
//	v1 := schema.NewJSONSchema("orders.created", 1).
//	    WithRequired("order_id", "customer_id").
//	    WithProperty("order_id", "string").
//	    WithProperty("customer_id", "string")
//	registry.Register(ctx, "orders.created", v1)
//
//	v2 := schema.NewJSONSchema("orders.created", 2).
//	    WithRequired("order_id", "customer_id", "email").
//	    WithProperty("order_id", "string").
//	    WithProperty("customer_id", "string").
//	    WithProperty("email", "string")
//	registry.Register(ctx, "orders.created", v2)
//
//	registry.AddUpcaster("orders.created",
//	    schema.NewFieldMapper(1, 2).
//	        AddDefault("email", "unknown@example.com"))
//
//	codec := schema.NewVersionedCodec(registry)
//	data, _ := codec.Encode(ctx, "orders.created", order) // emits latest version
//	var order Order
//	_ = codec.Decode(ctx, "orders.created", data, &order) // auto-upcasts older payloads
//
// Best practices:
//   - Add new payload fields as optional or with defaults.
//   - Never remove required fields without a matching upcaster.
//   - Test upcasters against production payload samples.
//   - Monitor failed validations in production.
package schema

import (
	"context"
	"time"
)

// EventSchema defines processing configuration for an event.
// Publishers register schemas; subscribers auto-load them.
// This ensures all workers processing the same event have consistent settings.
//
// EventSchema serves two purposes:
//   - Configuration: Processing behavior (timeouts, retries, feature flags)
//   - Identity: Event name, version, and description metadata
//
// These are deliberately combined into a single type because they are always
// stored, transmitted, and applied together. Splitting would add indirection
// without practical benefit since subscribers need all fields on registration.
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

// SchemaReader provides read-only access to event schemas.
// The Bus only needs this interface to load schemas during event registration.
// Use this when you only need to read schemas (e.g., subscriber-side).
type SchemaReader interface {
	// Get retrieves a schema by event name.
	// Returns nil, nil if not found.
	Get(ctx context.Context, eventName string) (*EventSchema, error)
}

// SchemaProvider abstracts schema storage with full read/write capabilities.
// Implemented by transports (with retention) or database stores.
// Extends SchemaReader with write, watch, and lifecycle operations.
type SchemaProvider interface {
	SchemaReader

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

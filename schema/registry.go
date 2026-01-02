// Package schema provides schema evolution support for event payloads.
//
// Schema evolution enables safe payload versioning with:
//   - Schema registration and validation
//   - Version upcasting (transforming old versions to new)
//   - Backward compatibility checks
//   - Envelope-based versioning for wire format
//
// # Overview
//
// The package provides:
//   - Registry interface for schema storage
//   - MemoryRegistry for in-memory storage
//   - Schema interface for validation
//   - JSONSchema for simple JSON validation
//   - Upcaster interface for version transformations
//   - FieldMapper for common field transformations
//   - VersionedCodec for encoding/decoding with versioning
//   - Envelope for wire format with version metadata
//
// # Schema Evolution Pattern
//
// When event payloads change over time:
//  1. Register new schema version
//  2. Add upcaster from old to new version
//  3. Consumers auto-upcast old messages to latest
//
// This allows:
//   - Producers to emit new version immediately
//   - Consumers to handle both old and new versions
//   - Gradual migration without downtime
//
// # Basic Usage
//
//	registry := schema.NewMemoryRegistry()
//
//	// Register schema v1
//	v1 := schema.NewJSONSchema("orders.created", 1).
//	    WithRequired("order_id", "customer_id").
//	    WithProperty("order_id", "string").
//	    WithProperty("customer_id", "string")
//	registry.Register(ctx, "orders.created", v1)
//
//	// Register schema v2 (added "email" field)
//	v2 := schema.NewJSONSchema("orders.created", 2).
//	    WithRequired("order_id", "customer_id", "email").
//	    WithProperty("order_id", "string").
//	    WithProperty("customer_id", "string").
//	    WithProperty("email", "string")
//	registry.Register(ctx, "orders.created", v2)
//
//	// Add upcaster v1 -> v2
//	registry.AddUpcaster("orders.created",
//	    schema.NewFieldMapper(1, 2).
//	        AddDefault("email", "unknown@example.com"))
//
// # Encoding/Decoding with Versioning
//
//	codec := schema.NewVersionedCodec(registry)
//
//	// Encode (uses latest schema version)
//	data, err := codec.Encode(ctx, "orders.created", order)
//
//	// Decode (auto-upcasts old versions)
//	var order Order
//	err := codec.Decode(ctx, "orders.created", data, &order)
//
// # Best Practices
//
//   - Always add new fields as optional or with defaults
//   - Never remove required fields without an upcaster
//   - Test upcasters with production data samples
//   - Monitor failed validations in production
package schema

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrSchemaNotFound indicates a schema was not found.
	ErrSchemaNotFound = errors.New("schema not found")

	// ErrInvalidPayload indicates the payload doesn't match the schema.
	ErrInvalidPayload = errors.New("invalid payload")

	// ErrNoUpcaster indicates no upcaster is available for the version transition.
	ErrNoUpcaster = errors.New("no upcaster available")
)

// Schema represents a message schema.
//
// Schemas define the structure and validation rules for event payloads.
// Each schema has a version number for evolution tracking.
//
// Implementations:
//   - JSONSchema: Simple JSON structure validation
type Schema interface {
	// Version returns the schema version number.
	// Versions should be positive integers, incrementing with each change.
	Version() int

	// Validate validates the data against this schema.
	// Returns ErrInvalidPayload if validation fails.
	Validate(data []byte) error

	// Name returns the schema name/type for identification.
	Name() string
}

// Registry manages schemas for events.
//
// A Registry stores multiple versions of schemas for each event and
// provides upcasting capabilities for version transformations.
//
// Implementations:
//   - MemoryRegistry: In-memory storage
type Registry interface {
	// Register registers a schema for an event.
	// Returns the registered version number.
	Register(ctx context.Context, eventName string, schema Schema) (int, error)

	// GetSchema retrieves a specific schema version.
	// Returns ErrSchemaNotFound if not found.
	GetSchema(ctx context.Context, eventName string, version int) (Schema, error)

	// GetLatestSchema retrieves the latest (highest version) schema.
	// Returns ErrSchemaNotFound if no schemas registered.
	GetLatestSchema(ctx context.Context, eventName string) (Schema, int, error)

	// ListVersions lists all registered schema versions for an event.
	// Returns empty slice if no schemas registered.
	ListVersions(ctx context.Context, eventName string) ([]int, error)

	// AddUpcaster adds an upcaster for transforming between versions.
	AddUpcaster(eventName string, upcaster Upcaster)
}

// Upcaster transforms data from one schema version to another.
//
// Upcasters enable schema evolution by converting old message formats
// to new ones. They should be registered in sequence (v1->v2, v2->v3, etc.)
// to allow chained upgrades.
//
// Implementations:
//   - FieldMapper: Common field transformations (rename, add, remove)
//
// Example:
//
//	type OrderV1ToV2Upcaster struct{}
//
//	func (u *OrderV1ToV2Upcaster) FromVersion() int { return 1 }
//	func (u *OrderV1ToV2Upcaster) ToVersion() int   { return 2 }
//	func (u *OrderV1ToV2Upcaster) Upcast(ctx context.Context, data []byte) ([]byte, error) {
//	    // Add new "email" field with default value
//	    var m map[string]any
//	    json.Unmarshal(data, &m)
//	    m["email"] = "unknown@example.com"
//	    return json.Marshal(m)
//	}
type Upcaster interface {
	// FromVersion returns the source version this upcaster handles.
	FromVersion() int

	// ToVersion returns the target version this upcaster produces.
	ToVersion() int

	// Upcast transforms the data from source to target version.
	// Returns the transformed data or error on failure.
	Upcast(ctx context.Context, data []byte) ([]byte, error)
}

// MemoryRegistry is an in-memory schema registry.
//
// MemoryRegistry stores schemas and upcasters in memory. Suitable for
// development and testing, or when schemas are loaded at startup.
// Data is not persisted across restarts.
//
// MemoryRegistry is safe for concurrent use.
//
// Example:
//
//	registry := schema.NewMemoryRegistry()
//	registry.Register(ctx, "orders.created", orderSchemaV1)
//	registry.Register(ctx, "orders.created", orderSchemaV2)
//	registry.AddUpcaster("orders.created", orderV1ToV2Upcaster)
type MemoryRegistry struct {
	mu        sync.RWMutex
	schemas   map[string]map[int]Schema // eventName -> version -> schema
	upcasters map[string][]Upcaster     // eventName -> upcasters
}

// NewMemoryRegistry creates a new in-memory schema registry.
func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		schemas:   make(map[string]map[int]Schema),
		upcasters: make(map[string][]Upcaster),
	}
}

// Register registers a schema for an event.
// Returns the registered version number.
func (r *MemoryRegistry) Register(ctx context.Context, eventName string, schema Schema) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.schemas[eventName] == nil {
		r.schemas[eventName] = make(map[int]Schema)
	}

	version := schema.Version()
	r.schemas[eventName][version] = schema

	return version, nil
}

// GetSchema retrieves a specific schema version.
// Returns ErrSchemaNotFound if the event or version is not found.
func (r *MemoryRegistry) GetSchema(ctx context.Context, eventName string, version int) (Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[eventName]
	if !ok {
		return nil, fmt.Errorf("%w: event %s", ErrSchemaNotFound, eventName)
	}

	schema, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf("%w: version %d for event %s", ErrSchemaNotFound, version, eventName)
	}

	return schema, nil
}

// GetLatestSchema retrieves the schema with the highest version number.
// Returns ErrSchemaNotFound if no schemas are registered for the event.
func (r *MemoryRegistry) GetLatestSchema(ctx context.Context, eventName string) (Schema, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[eventName]
	if !ok {
		return nil, 0, fmt.Errorf("%w: event %s", ErrSchemaNotFound, eventName)
	}

	maxVersion := 0
	var latestSchema Schema

	for v, s := range versions {
		if v > maxVersion {
			maxVersion = v
			latestSchema = s
		}
	}

	return latestSchema, maxVersion, nil
}

// ListVersions lists all registered schema versions for an event.
// Returns nil if no schemas are registered.
func (r *MemoryRegistry) ListVersions(ctx context.Context, eventName string) ([]int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.schemas[eventName]
	if !ok {
		return nil, nil
	}

	result := make([]int, 0, len(versions))
	for v := range versions {
		result = append(result, v)
	}

	return result, nil
}

// AddUpcaster adds an upcaster for transforming between versions.
// Register upcasters in sequence (v1->v2, v2->v3, etc.) for chained upgrades.
func (r *MemoryRegistry) AddUpcaster(eventName string, upcaster Upcaster) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.upcasters[eventName] = append(r.upcasters[eventName], upcaster)
}

// GetUpcaster finds an upcaster for a specific version transition.
// Returns ErrNoUpcaster if no matching upcaster is found.
func (r *MemoryRegistry) GetUpcaster(eventName string, fromVersion, toVersion int) (Upcaster, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	upcasters := r.upcasters[eventName]
	for _, u := range upcasters {
		if u.FromVersion() == fromVersion && u.ToVersion() == toVersion {
			return u, nil
		}
	}

	return nil, fmt.Errorf("%w: %s from v%d to v%d", ErrNoUpcaster, eventName, fromVersion, toVersion)
}

// UpcastToLatest upcasts data from a given version to the latest version.
//
// Applies upcasters in sequence (v1->v2->v3->...) until the latest version
// is reached. Returns ErrNoUpcaster if any required upcaster is missing.
func (r *MemoryRegistry) UpcastToLatest(ctx context.Context, eventName string, data []byte, fromVersion int) ([]byte, int, error) {
	_, latestVersion, err := r.GetLatestSchema(ctx, eventName)
	if err != nil {
		return nil, 0, err
	}

	result := data
	for v := fromVersion; v < latestVersion; v++ {
		upcaster, err := r.GetUpcaster(eventName, v, v+1)
		if err != nil {
			return nil, v, err
		}

		result, err = upcaster.Upcast(ctx, result)
		if err != nil {
			return nil, v, fmt.Errorf("upcast from v%d to v%d: %w", v, v+1, err)
		}
	}

	return result, latestVersion, nil
}

// Compile-time check
var _ Registry = (*MemoryRegistry)(nil)

// Envelope wraps a payload with version information.
//
// The Envelope provides a standard wire format that includes:
//   - Schema version for the payload
//   - Event name for routing
//   - Raw payload bytes
//   - Optional metadata
//
// Use Envelope when you need explicit version tracking in the message format.
//
// Example:
//
//	// Create envelope
//	env := schema.NewEnvelope("orders.created", 2, orderJSON)
//	data, _ := env.Encode()
//
//	// Decode envelope
//	env, _ := schema.DecodeEnvelope(data)
//	fmt.Printf("Event: %s, Version: %d\n", env.Event, env.Version)
type Envelope struct {
	// Version is the schema version of the payload.
	Version int `json:"version"`

	// Event is the event name/type.
	Event string `json:"event"`

	// Payload is the raw message data.
	Payload json.RawMessage `json:"payload"`

	// Metadata contains optional key-value pairs.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// NewEnvelope creates a new envelope with the given event, version, and payload.
func NewEnvelope(eventName string, version int, payload []byte) *Envelope {
	return &Envelope{
		Version: version,
		Event:   eventName,
		Payload: payload,
	}
}

// Encode encodes the envelope to JSON.
func (e *Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// DecodeEnvelope decodes an envelope from JSON.
// Returns error if the data is not a valid envelope.
func DecodeEnvelope(data []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	return &env, nil
}

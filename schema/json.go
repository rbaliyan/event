package schema

import (
	"context"
	"encoding/json"
	"fmt"
)

// JSONSchema provides simple JSON structure validation for event payloads.
//
// JSONSchema validates that JSON documents contain required fields and that
// fields have the expected types. It supports basic JSON types: string, number,
// boolean, array, and object.
//
// Use JSONSchema for straightforward validation needs. For complex schemas
// (nested validation, patterns, enums), consider integrating a full JSON Schema
// library like github.com/xeipuuv/gojsonschema.
//
// JSONSchema uses a fluent builder pattern for configuration.
//
// Supported types:
//   - "string": JSON string values
//   - "number": JSON number values (integers and floats)
//   - "boolean": JSON true/false values
//   - "array": JSON arrays
//   - "object": JSON objects (nested documents)
//
// Example:
//
//	// Create schema for order events
//	schema := schema.NewJSONSchema("orders.created", 1).
//	    WithRequired("order_id", "customer_id", "total").
//	    WithProperty("order_id", "string").
//	    WithProperty("customer_id", "string").
//	    WithProperty("total", "number").
//	    WithProperty("items", "array")
//
//	// Validate payload
//	payload := []byte(`{"order_id": "123", "customer_id": "456", "total": 99.99}`)
//	if err := schema.Validate(payload); err != nil {
//	    // Handle validation error
//	}
type JSONSchema struct {
	version    int
	name       string
	required   []string          // Required fields
	properties map[string]string // Field name -> type (string, number, boolean, array, object)
}

// NewJSONSchema creates a new JSON schema with the given name and version.
//
// Parameters:
//   - name: Schema name, typically matching the event name (e.g., "orders.created")
//   - version: Schema version number (positive integer)
//
// Use the fluent WithRequired() and WithProperty() methods to configure
// the schema after creation.
//
// Example:
//
//	schema := schema.NewJSONSchema("orders.created", 2).
//	    WithRequired("order_id", "email").
//	    WithProperty("order_id", "string").
//	    WithProperty("email", "string")
func NewJSONSchema(name string, version int) *JSONSchema {
	return &JSONSchema{
		name:       name,
		version:    version,
		properties: make(map[string]string),
	}
}

// Version returns the schema version number.
//
// Implements the Schema interface.
func (s *JSONSchema) Version() int {
	return s.version
}

// Name returns the schema name/type identifier.
//
// Implements the Schema interface.
func (s *JSONSchema) Name() string {
	return s.name
}

// WithRequired sets the required fields for validation.
//
// Validation will fail if any required field is missing from the payload.
// Calling WithRequired replaces any previously set required fields.
// Returns the schema for method chaining.
//
// Example:
//
//	schema.WithRequired("order_id", "customer_id", "total")
func (s *JSONSchema) WithRequired(fields ...string) *JSONSchema {
	s.required = fields
	return s
}

// WithProperty adds a property type constraint to the schema.
//
// Parameters:
//   - name: Field name in the JSON document
//   - typ: Expected type ("string", "number", "boolean", "array", "object")
//
// If a field is present in the document, its type must match.
// Missing fields are ignored unless they are required.
// Returns the schema for method chaining.
//
// Example:
//
//	schema.
//	    WithProperty("order_id", "string").
//	    WithProperty("total", "number").
//	    WithProperty("items", "array")
func (s *JSONSchema) WithProperty(name, typ string) *JSONSchema {
	s.properties[name] = typ
	return s
}

// Validate validates JSON data against the schema.
//
// Validation checks:
//  1. Data must be valid JSON that can be unmarshaled to a map
//  2. All required fields must be present
//  3. Fields with defined properties must have the correct type
//
// Returns ErrInvalidPayload wrapped with details if validation fails.
// Returns nil if validation passes.
//
// Example:
//
//	if err := schema.Validate(payload); err != nil {
//	    if errors.Is(err, schema.ErrInvalidPayload) {
//	        log.Printf("Validation failed: %v", err)
//	    }
//	}
func (s *JSONSchema) Validate(data []byte) error {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	// Check required fields
	for _, field := range s.required {
		if _, ok := m[field]; !ok {
			return fmt.Errorf("%w: missing required field %s", ErrInvalidPayload, field)
		}
	}

	// Check property types
	for name, expectedType := range s.properties {
		value, ok := m[name]
		if !ok {
			continue // Not required, skip
		}

		if !s.checkType(value, expectedType) {
			return fmt.Errorf("%w: field %s should be %s", ErrInvalidPayload, name, expectedType)
		}
	}

	return nil
}

// checkType checks if a value matches the expected JSON type.
//
// Supported types:
//   - "string": Go string
//   - "number": Go float64 (JSON numbers unmarshal to float64)
//   - "boolean": Go bool
//   - "array": Go []any
//   - "object": Go map[string]any
//
// Returns true for unknown types (permissive by default).
func (s *JSONSchema) checkType(value any, expectedType string) bool {
	switch expectedType {
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	default:
		return true // Unknown type, accept
	}
}

// Compile-time check
var _ Schema = (*JSONSchema)(nil)

// FieldMapper transforms payloads between schema versions using field operations.
//
// FieldMapper is an Upcaster implementation that handles common schema
// evolution scenarios:
//   - Renaming fields (e.g., "customer_name" -> "customerName")
//   - Adding fields with default values
//   - Removing deprecated fields
//
// Operations are applied in order: renames, then defaults, then removals.
// This ordering ensures that renamed fields can have defaults applied
// and that removals happen last.
//
// FieldMapper uses a fluent builder pattern for configuration.
//
// Example:
//
//	// Upcast from v1 to v2:
//	// - Rename "customer_name" to "customerName"
//	// - Add "email" with default value
//	// - Remove deprecated "legacy_id"
//	upcaster := schema.NewFieldMapper(1, 2).
//	    RenameField("customer_name", "customerName").
//	    AddDefault("email", "unknown@example.com").
//	    RemoveField("legacy_id")
//
//	registry.AddUpcaster("orders.created", upcaster)
type FieldMapper struct {
	from        int
	to          int
	fieldMap    map[string]string // old name -> new name
	defaults    map[string]any    // new field -> default value
	removeFields []string         // fields to remove
}

// NewFieldMapper creates a new field mapper for the specified version transition.
//
// Parameters:
//   - from: Source schema version (input payloads must be this version)
//   - to: Target schema version (output payloads will be this version)
//
// Use the fluent builder methods to configure field transformations.
//
// Example:
//
//	// Create upcaster from version 1 to version 2
//	upcaster := schema.NewFieldMapper(1, 2).
//	    AddDefault("created_at", time.Now().Format(time.RFC3339))
func NewFieldMapper(from, to int) *FieldMapper {
	return &FieldMapper{
		from:     from,
		to:       to,
		fieldMap: make(map[string]string),
		defaults: make(map[string]any),
	}
}

// FromVersion returns the source version this upcaster handles.
//
// Implements the Upcaster interface.
func (f *FieldMapper) FromVersion() int {
	return f.from
}

// ToVersion returns the target version this upcaster produces.
//
// Implements the Upcaster interface.
func (f *FieldMapper) ToVersion() int {
	return f.to
}

// RenameField adds a field rename transformation.
//
// During upcasting, the value from oldName is moved to newName
// and oldName is deleted. If oldName doesn't exist, no action is taken.
// Returns the mapper for method chaining.
//
// Example:
//
//	// Rename "customer_name" to "customerName" (snake_case to camelCase)
//	mapper.RenameField("customer_name", "customerName")
func (f *FieldMapper) RenameField(oldName, newName string) *FieldMapper {
	f.fieldMap[oldName] = newName
	return f
}

// AddDefault sets a default value for a field that may not exist.
//
// During upcasting, if the field is not present in the payload,
// it is added with the specified default value. If the field exists,
// its current value is preserved.
// Returns the mapper for method chaining.
//
// Example:
//
//	// Add default email for payloads that don't have one
//	mapper.AddDefault("email", "unknown@example.com")
//
//	// Add a structured default
//	mapper.AddDefault("metadata", map[string]any{"source": "migration"})
func (f *FieldMapper) AddDefault(field string, value any) *FieldMapper {
	f.defaults[field] = value
	return f
}

// RemoveField marks a field for removal during upcasting.
//
// During upcasting, the specified field is deleted from the payload.
// Useful for removing deprecated fields in newer schema versions.
// Returns the mapper for method chaining.
//
// Example:
//
//	// Remove deprecated fields
//	mapper.RemoveField("legacy_id").RemoveField("old_status")
func (f *FieldMapper) RemoveField(field string) *FieldMapper {
	f.removeFields = append(f.removeFields, field)
	return f
}

// Upcast transforms the data from the source to target version.
//
// The transformation applies operations in this order:
//  1. Field renames (move values from old names to new names)
//  2. Default values (add missing fields with defaults)
//  3. Field removals (delete deprecated fields)
//
// Returns the transformed JSON data and any error that occurred.
// Implements the Upcaster interface.
//
// Example:
//
//	// v1 payload: {"customer_name": "John", "legacy_id": "old123"}
//	// After upcast to v2: {"customerName": "John", "email": "unknown@example.com"}
func (f *FieldMapper) Upcast(ctx context.Context, data []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	// Apply field renames
	for oldName, newName := range f.fieldMap {
		if value, ok := m[oldName]; ok {
			m[newName] = value
			delete(m, oldName)
		}
	}

	// Add defaults for new fields
	for field, value := range f.defaults {
		if _, ok := m[field]; !ok {
			m[field] = value
		}
	}

	// Remove deprecated fields
	for _, field := range f.removeFields {
		delete(m, field)
	}

	return json.Marshal(m)
}

// Compile-time check
var _ Upcaster = (*FieldMapper)(nil)

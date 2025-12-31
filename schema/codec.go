package schema

import (
	"context"
	"encoding/json"
	"fmt"
)

// VersionedCodec handles encoding/decoding with schema versioning.
//
// VersionedCodec wraps payloads in an Envelope with version information
// and automatically upcasts old versions to the latest when decoding.
//
// Encoding:
//   - Marshals data to JSON
//   - Wraps in Envelope with latest schema version
//   - Returns JSON-encoded envelope
//
// Decoding:
//   - Decodes envelope to get version
//   - Upcasts payload to latest version if needed
//   - Validates against latest schema
//   - Unmarshals to target type
//
// Example:
//
//	codec := schema.NewVersionedCodec(registry)
//
//	// Encode with version envelope
//	data, err := codec.Encode(ctx, "orders.created", order)
//
//	// Decode with auto-upcasting
//	var order Order
//	err := codec.Decode(ctx, "orders.created", data, &order)
type VersionedCodec struct {
	registry Registry
}

// NewVersionedCodec creates a new versioned codec.
//
// Parameters:
//   - registry: Schema registry for version lookups and upcasting
func NewVersionedCodec(registry Registry) *VersionedCodec {
	return &VersionedCodec{registry: registry}
}

// Encode encodes data with version information in an Envelope.
//
// The data is wrapped in an Envelope with the latest schema version.
// If no schema is registered, the data is encoded directly without version.
func (c *VersionedCodec) Encode(ctx context.Context, eventName string, data any) ([]byte, error) {
	// Get latest schema version
	_, version, err := c.registry.GetLatestSchema(ctx, eventName)
	if err != nil {
		// No schema registered, encode without version
		return json.Marshal(data)
	}

	// Encode payload
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}

	// Wrap in envelope
	env := NewEnvelope(eventName, version, payload)
	return env.Encode()
}

// Decode decodes data, upcasting if necessary
func (c *VersionedCodec) Decode(ctx context.Context, eventName string, data []byte, target any) error {
	// Try to decode as envelope
	env, err := DecodeEnvelope(data)
	if err != nil {
		// Not an envelope, decode directly
		return json.Unmarshal(data, target)
	}

	// Get latest schema version
	_, latestVersion, err := c.registry.GetLatestSchema(ctx, eventName)
	if err != nil {
		// No schema registered, decode payload directly
		return json.Unmarshal(env.Payload, target)
	}

	payload := []byte(env.Payload)

	// Upcast if needed
	if env.Version < latestVersion {
		if memReg, ok := c.registry.(*MemoryRegistry); ok {
			payload, _, err = memReg.UpcastToLatest(ctx, eventName, payload, env.Version)
			if err != nil {
				return fmt.Errorf("upcast: %w", err)
			}
		}
	}

	// Validate against latest schema
	schema, err := c.registry.GetSchema(ctx, eventName, latestVersion)
	if err == nil {
		if err := schema.Validate(payload); err != nil {
			return fmt.Errorf("validate: %w", err)
		}
	}

	return json.Unmarshal(payload, target)
}

// DecodeRaw decodes data and returns the raw payload and version
func (c *VersionedCodec) DecodeRaw(ctx context.Context, eventName string, data []byte) ([]byte, int, error) {
	// Try to decode as envelope
	env, err := DecodeEnvelope(data)
	if err != nil {
		// Not an envelope, return as-is with version 0
		return data, 0, nil
	}

	// Get latest schema version
	_, latestVersion, err := c.registry.GetLatestSchema(ctx, eventName)
	if err != nil {
		// No schema registered
		return env.Payload, env.Version, nil
	}

	payload := []byte(env.Payload)

	// Upcast if needed
	if env.Version < latestVersion {
		if memReg, ok := c.registry.(*MemoryRegistry); ok {
			payload, latestVersion, err = memReg.UpcastToLatest(ctx, eventName, payload, env.Version)
			if err != nil {
				return nil, env.Version, fmt.Errorf("upcast: %w", err)
			}
		}
	}

	return payload, latestVersion, nil
}

// ValidateAndUpcast validates data and upcasts to latest version
func (c *VersionedCodec) ValidateAndUpcast(ctx context.Context, eventName string, data []byte, version int) ([]byte, int, error) {
	// Validate against source schema
	schema, err := c.registry.GetSchema(ctx, eventName, version)
	if err == nil {
		if err := schema.Validate(data); err != nil {
			return nil, version, fmt.Errorf("validate source: %w", err)
		}
	}

	// Get latest version
	_, latestVersion, err := c.registry.GetLatestSchema(ctx, eventName)
	if err != nil {
		return data, version, nil
	}

	// No upcast needed
	if version >= latestVersion {
		return data, version, nil
	}

	// Upcast
	if memReg, ok := c.registry.(*MemoryRegistry); ok {
		upcast, newVersion, err := memReg.UpcastToLatest(ctx, eventName, data, version)
		if err != nil {
			return nil, version, err
		}
		return upcast, newVersion, nil
	}

	return data, version, nil
}

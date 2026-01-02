package schema

import (
	"context"
	"encoding/json"
	"testing"
)

func TestJSONSchema(t *testing.T) {
	t.Run("NewJSONSchema creates schema", func(t *testing.T) {
		s := NewJSONSchema("orders.created", 1)

		if s.Name() != "orders.created" {
			t.Errorf("expected name orders.created, got %s", s.Name())
		}
		if s.Version() != 1 {
			t.Errorf("expected version 1, got %d", s.Version())
		}
	})

	t.Run("WithRequired sets required fields", func(t *testing.T) {
		s := NewJSONSchema("orders.created", 1).
			WithRequired("order_id", "customer_id")

		// Should pass with required fields
		valid := []byte(`{"order_id": "123", "customer_id": "456"}`)
		if err := s.Validate(valid); err != nil {
			t.Errorf("expected valid, got %v", err)
		}

		// Should fail with missing required field
		invalid := []byte(`{"order_id": "123"}`)
		if err := s.Validate(invalid); err == nil {
			t.Error("expected error for missing required field")
		}
	})

	t.Run("WithProperty validates types", func(t *testing.T) {
		s := NewJSONSchema("orders.created", 1).
			WithProperty("order_id", "string").
			WithProperty("total", "number").
			WithProperty("confirmed", "boolean").
			WithProperty("items", "array").
			WithProperty("metadata", "object")

		// Valid types
		valid := []byte(`{
			"order_id": "123",
			"total": 99.99,
			"confirmed": true,
			"items": [1, 2, 3],
			"metadata": {"key": "value"}
		}`)
		if err := s.Validate(valid); err != nil {
			t.Errorf("expected valid, got %v", err)
		}

		// Invalid string type
		invalidString := []byte(`{"order_id": 123}`)
		if err := s.Validate(invalidString); err == nil {
			t.Error("expected error for wrong string type")
		}

		// Invalid number type
		invalidNumber := []byte(`{"total": "not a number"}`)
		if err := s.Validate(invalidNumber); err == nil {
			t.Error("expected error for wrong number type")
		}

		// Invalid boolean type
		invalidBool := []byte(`{"confirmed": "yes"}`)
		if err := s.Validate(invalidBool); err == nil {
			t.Error("expected error for wrong boolean type")
		}
	})

	t.Run("Validate returns error for invalid JSON", func(t *testing.T) {
		s := NewJSONSchema("test", 1)

		invalid := []byte(`not valid json`)
		if err := s.Validate(invalid); err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("Unknown property types are permissive", func(t *testing.T) {
		s := NewJSONSchema("test", 1).
			WithProperty("field", "unknown_type")

		// Should not fail for unknown type
		valid := []byte(`{"field": "any value"}`)
		if err := s.Validate(valid); err != nil {
			t.Errorf("expected valid for unknown type, got %v", err)
		}
	})
}

func TestMemoryRegistry(t *testing.T) {
	ctx := context.Background()

	t.Run("Register and GetSchema", func(t *testing.T) {
		registry := NewMemoryRegistry()

		s := NewJSONSchema("orders.created", 1)
		version, err := registry.Register(ctx, "orders.created", s)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}
		if version != 1 {
			t.Errorf("expected version 1, got %d", version)
		}

		retrieved, err := registry.GetSchema(ctx, "orders.created", 1)
		if err != nil {
			t.Fatalf("GetSchema failed: %v", err)
		}
		if retrieved.Version() != 1 {
			t.Errorf("expected version 1, got %d", retrieved.Version())
		}
	})

	t.Run("GetSchema returns error for non-existent", func(t *testing.T) {
		registry := NewMemoryRegistry()

		_, err := registry.GetSchema(ctx, "non-existent", 1)
		if err == nil {
			t.Error("expected error for non-existent event")
		}

		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 1))
		_, err = registry.GetSchema(ctx, "orders.created", 99)
		if err == nil {
			t.Error("expected error for non-existent version")
		}
	})

	t.Run("GetLatestSchema returns highest version", func(t *testing.T) {
		registry := NewMemoryRegistry()

		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 1))
		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 3))
		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 2))

		s, version, err := registry.GetLatestSchema(ctx, "orders.created")
		if err != nil {
			t.Fatalf("GetLatestSchema failed: %v", err)
		}
		if version != 3 {
			t.Errorf("expected version 3, got %d", version)
		}
		if s.Version() != 3 {
			t.Errorf("expected schema version 3, got %d", s.Version())
		}
	})

	t.Run("GetLatestSchema returns error for non-existent", func(t *testing.T) {
		registry := NewMemoryRegistry()

		_, _, err := registry.GetLatestSchema(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent event")
		}
	})

	t.Run("ListVersions returns all versions", func(t *testing.T) {
		registry := NewMemoryRegistry()

		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 1))
		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 2))
		registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 3))

		versions, err := registry.ListVersions(ctx, "orders.created")
		if err != nil {
			t.Fatalf("ListVersions failed: %v", err)
		}
		if len(versions) != 3 {
			t.Errorf("expected 3 versions, got %d", len(versions))
		}
	})

	t.Run("ListVersions returns nil for non-existent", func(t *testing.T) {
		registry := NewMemoryRegistry()

		versions, err := registry.ListVersions(ctx, "non-existent")
		if err != nil {
			t.Fatalf("ListVersions failed: %v", err)
		}
		if versions != nil {
			t.Errorf("expected nil, got %v", versions)
		}
	})
}

func TestFieldMapper(t *testing.T) {
	ctx := context.Background()

	t.Run("FromVersion and ToVersion", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2)

		if mapper.FromVersion() != 1 {
			t.Errorf("expected from version 1, got %d", mapper.FromVersion())
		}
		if mapper.ToVersion() != 2 {
			t.Errorf("expected to version 2, got %d", mapper.ToVersion())
		}
	})

	t.Run("RenameField", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2).
			RenameField("old_name", "newName")

		data := []byte(`{"old_name": "value", "other": "unchanged"}`)
		result, err := mapper.Upcast(ctx, data)
		if err != nil {
			t.Fatalf("Upcast failed: %v", err)
		}

		// Check result contains renamed field
		// Due to map ordering, we need to unmarshal and check
		var m map[string]any
		if err := unmarshalJSON(result, &m); err != nil {
			t.Fatalf("unmarshal result failed: %v", err)
		}

		if m["newName"] != "value" {
			t.Errorf("expected newName=value, got %v", m["newName"])
		}
		if _, ok := m["old_name"]; ok {
			t.Error("old_name should be removed")
		}
		if m["other"] != "unchanged" {
			t.Errorf("other should be unchanged")
		}
	})

	t.Run("AddDefault", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2).
			AddDefault("email", "default@example.com")

		// Missing field gets default
		data := []byte(`{"name": "John"}`)
		result, err := mapper.Upcast(ctx, data)
		if err != nil {
			t.Fatalf("Upcast failed: %v", err)
		}

		var m map[string]any
		unmarshalJSON(result, &m)

		if m["email"] != "default@example.com" {
			t.Errorf("expected default email, got %v", m["email"])
		}
		if m["name"] != "John" {
			t.Error("name should be preserved")
		}

		// Existing field is not overwritten
		dataWithEmail := []byte(`{"name": "John", "email": "john@example.com"}`)
		result, _ = mapper.Upcast(ctx, dataWithEmail)
		unmarshalJSON(result, &m)

		if m["email"] != "john@example.com" {
			t.Errorf("existing email should be preserved, got %v", m["email"])
		}
	})

	t.Run("RemoveField", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2).
			RemoveField("deprecated")

		data := []byte(`{"name": "John", "deprecated": "old value"}`)
		result, err := mapper.Upcast(ctx, data)
		if err != nil {
			t.Fatalf("Upcast failed: %v", err)
		}

		var m map[string]any
		unmarshalJSON(result, &m)

		if _, ok := m["deprecated"]; ok {
			t.Error("deprecated field should be removed")
		}
		if m["name"] != "John" {
			t.Error("name should be preserved")
		}
	})

	t.Run("Combined operations", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2).
			RenameField("customer_name", "customerName").
			AddDefault("email", "unknown@example.com").
			RemoveField("legacy_id")

		data := []byte(`{"customer_name": "John", "legacy_id": "old123"}`)
		result, err := mapper.Upcast(ctx, data)
		if err != nil {
			t.Fatalf("Upcast failed: %v", err)
		}

		var m map[string]any
		unmarshalJSON(result, &m)

		if m["customerName"] != "John" {
			t.Errorf("customerName should be John, got %v", m["customerName"])
		}
		if m["email"] != "unknown@example.com" {
			t.Errorf("email should have default, got %v", m["email"])
		}
		if _, ok := m["customer_name"]; ok {
			t.Error("customer_name should be removed after rename")
		}
		if _, ok := m["legacy_id"]; ok {
			t.Error("legacy_id should be removed")
		}
	})

	t.Run("Upcast returns error for invalid JSON", func(t *testing.T) {
		mapper := NewFieldMapper(1, 2)

		_, err := mapper.Upcast(ctx, []byte(`not valid json`))
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})
}

func TestUpcasterChain(t *testing.T) {
	ctx := context.Background()

	registry := NewMemoryRegistry()

	// Register schemas v1, v2, v3
	registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 1))
	registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 2))
	registry.Register(ctx, "orders.created", NewJSONSchema("orders.created", 3))

	// Add upcasters
	registry.AddUpcaster("orders.created",
		NewFieldMapper(1, 2).AddDefault("email", "unknown@example.com"))
	registry.AddUpcaster("orders.created",
		NewFieldMapper(2, 3).RenameField("name", "fullName"))

	t.Run("UpcastToLatest chains upcasters", func(t *testing.T) {
		data := []byte(`{"name": "John", "order_id": "123"}`)

		result, version, err := registry.UpcastToLatest(ctx, "orders.created", data, 1)
		if err != nil {
			t.Fatalf("UpcastToLatest failed: %v", err)
		}

		if version != 3 {
			t.Errorf("expected version 3, got %d", version)
		}

		var m map[string]any
		unmarshalJSON(result, &m)

		if m["fullName"] != "John" {
			t.Errorf("fullName should be John, got %v", m["fullName"])
		}
		if m["email"] != "unknown@example.com" {
			t.Errorf("email should have default, got %v", m["email"])
		}
	})

	t.Run("GetUpcaster returns specific upcaster", func(t *testing.T) {
		upcaster, err := registry.GetUpcaster("orders.created", 1, 2)
		if err != nil {
			t.Fatalf("GetUpcaster failed: %v", err)
		}

		if upcaster.FromVersion() != 1 || upcaster.ToVersion() != 2 {
			t.Error("wrong upcaster returned")
		}
	})

	t.Run("GetUpcaster returns error for missing upcaster", func(t *testing.T) {
		_, err := registry.GetUpcaster("orders.created", 1, 3)
		if err == nil {
			t.Error("expected error for missing upcaster")
		}
	})
}

func TestEnvelope(t *testing.T) {
	t.Run("NewEnvelope creates envelope", func(t *testing.T) {
		payload := []byte(`{"order_id": "123"}`)
		env := NewEnvelope("orders.created", 2, payload)

		if env.Event != "orders.created" {
			t.Errorf("expected event orders.created, got %s", env.Event)
		}
		if env.Version != 2 {
			t.Errorf("expected version 2, got %d", env.Version)
		}
		if string(env.Payload) != `{"order_id": "123"}` {
			t.Errorf("unexpected payload: %s", env.Payload)
		}
	})

	t.Run("Encode and Decode roundtrip", func(t *testing.T) {
		payload := []byte(`{"order_id": "123"}`)
		env := NewEnvelope("orders.created", 2, payload)
		env.Metadata = map[string]any{"source": "test"}

		data, err := env.Encode()
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := DecodeEnvelope(data)
		if err != nil {
			t.Fatalf("DecodeEnvelope failed: %v", err)
		}

		if decoded.Event != "orders.created" {
			t.Errorf("expected event orders.created, got %s", decoded.Event)
		}
		if decoded.Version != 2 {
			t.Errorf("expected version 2, got %d", decoded.Version)
		}
		if decoded.Metadata["source"] != "test" {
			t.Errorf("expected metadata source=test, got %v", decoded.Metadata["source"])
		}
	})

	t.Run("DecodeEnvelope returns error for invalid JSON", func(t *testing.T) {
		_, err := DecodeEnvelope([]byte(`not valid json`))
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})
}

func TestErrors(t *testing.T) {
	if ErrSchemaNotFound.Error() != "schema not found" {
		t.Errorf("unexpected error message: %s", ErrSchemaNotFound.Error())
	}
	if ErrInvalidPayload.Error() != "invalid payload" {
		t.Errorf("unexpected error message: %s", ErrInvalidPayload.Error())
	}
	if ErrNoUpcaster.Error() != "no upcaster available" {
		t.Errorf("unexpected error message: %s", ErrNoUpcaster.Error())
	}
}

// Helper function for unmarshaling JSON
func unmarshalJSON(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

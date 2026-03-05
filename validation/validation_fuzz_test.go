package validation

import (
	"encoding/json"
	"testing"
)

func FuzzNewJSONSchemaValidator(f *testing.F) {
	f.Add(`{"type": "object"}`)
	f.Add(`{"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}`)
	f.Add(`{"type": "array", "items": {"type": "number"}}`)
	f.Add(`{}`)
	f.Add(``)
	f.Add(`{{{`)
	f.Add(`{"type": "invalid"}`)
	f.Add(`null`)

	f.Fuzz(func(t *testing.T, schema string) {
		_, _ = NewJSONSchemaValidator(schema)
	})
}

func FuzzJSONSchemaValidate(f *testing.F) {
	f.Add(`{"name": "test", "age": 30}`)
	f.Add(`{}`)
	f.Add(`{"name": 123}`)
	f.Add(`null`)
	f.Add(`[]`)
	f.Add(`"string"`)
	f.Add(``)
	f.Add(`{{{`)

	schema := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer", "minimum": 0}
		},
		"required": ["name"]
	}`

	validator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		f.Fatalf("failed to create validator: %v", err)
	}

	f.Fuzz(func(t *testing.T, payload string) {
		_ = validator.Validate(json.RawMessage(payload))
	})
}

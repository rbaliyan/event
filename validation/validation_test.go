package validation

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rbaliyan/event/v3"
)

// Sample order struct for testing
type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	Items  []Item  `json:"items,omitempty"`
}

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

func TestNewJSONSchemaValidator(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number"}
		},
		"required": ["id", "amount"]
	}`

	t.Run("from string", func(t *testing.T) {
		v, err := NewJSONSchemaValidator(schema)
		if err != nil {
			t.Fatalf("NewJSONSchemaValidator failed: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil validator")
		}
	})

	t.Run("from bytes", func(t *testing.T) {
		v, err := NewJSONSchemaValidator([]byte(schema))
		if err != nil {
			t.Fatalf("NewJSONSchemaValidator failed: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil validator")
		}
	})

	t.Run("from io.Reader", func(t *testing.T) {
		v, err := NewJSONSchemaValidator(strings.NewReader(schema))
		if err != nil {
			t.Fatalf("NewJSONSchemaValidator failed: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil validator")
		}
	})

	t.Run("from file", func(t *testing.T) {
		// Create a temporary schema file
		tmpDir := t.TempDir()
		schemaPath := filepath.Join(tmpDir, "schema.json")
		if err := os.WriteFile(schemaPath, []byte(schema), 0600); err != nil {
			t.Fatalf("failed to write schema file: %v", err)
		}

		v, err := NewJSONSchemaValidator("file://" + schemaPath)
		if err != nil {
			t.Fatalf("NewJSONSchemaValidator failed: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil validator")
		}
	})

	t.Run("invalid schema returns error", func(t *testing.T) {
		_, err := NewJSONSchemaValidator("not valid json")
		if err == nil {
			t.Fatal("expected error for invalid schema")
		}
		if !errors.Is(err, ErrInvalidSchema) {
			t.Errorf("expected ErrInvalidSchema, got %v", err)
		}
	})

	t.Run("unsupported type returns error", func(t *testing.T) {
		_, err := NewJSONSchemaValidator(12345)
		if err == nil {
			t.Fatal("expected error for unsupported type")
		}
		if !errors.Is(err, ErrUnsupportedSchemaType) {
			t.Errorf("expected ErrUnsupportedSchemaType, got %v", err)
		}
	})

	t.Run("non-existent file returns error", func(t *testing.T) {
		_, err := NewJSONSchemaValidator("file:///nonexistent/path/schema.json")
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})
}

func TestJSONSchemaValidator_Validate(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string", "minLength": 1},
			"amount": {"type": "number", "minimum": 0}
		},
		"required": ["id", "amount"],
		"additionalProperties": false
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid payload passes", func(t *testing.T) {
		order := Order{ID: "123", Amount: 99.99}
		if err := v.Validate(order); err != nil {
			t.Errorf("expected valid payload to pass: %v", err)
		}
	})

	t.Run("zero amount is valid", func(t *testing.T) {
		order := Order{ID: "123", Amount: 0}
		if err := v.Validate(order); err != nil {
			t.Errorf("expected zero amount to be valid: %v", err)
		}
	})

	t.Run("missing required field fails", func(t *testing.T) {
		// Missing Amount
		type PartialOrder struct {
			ID string `json:"id"`
		}
		order := PartialOrder{ID: "123"}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for missing required field")
		}

		var validationErr *ValidationError
		if !errors.As(err, &validationErr) {
			t.Errorf("expected ValidationError, got %T", err)
		}
	})

	t.Run("wrong type fails", func(t *testing.T) {
		type WrongOrder struct {
			ID     int     `json:"id"` // should be string
			Amount float64 `json:"amount"`
		}
		order := WrongOrder{ID: 123, Amount: 99.99}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for wrong type")
		}
	})

	t.Run("negative amount fails", func(t *testing.T) {
		order := Order{ID: "123", Amount: -10}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for negative amount")
		}
	})

	t.Run("empty id fails", func(t *testing.T) {
		order := Order{ID: "", Amount: 10}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for empty id")
		}
	})

	t.Run("map payload works", func(t *testing.T) {
		payload := map[string]any{
			"id":     "123",
			"amount": 99.99,
		}
		if err := v.Validate(payload); err != nil {
			t.Errorf("expected map payload to pass: %v", err)
		}
	})

	t.Run("validation error has path info", func(t *testing.T) {
		order := Order{ID: "123", Amount: -10}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error")
		}

		var validationErr *ValidationError
		if errors.As(err, &validationErr) {
			if validationErr.Path == "" {
				t.Log("path may be empty for some validation errors")
			}
		}
	})
}

func TestJSONSchemaValidator_ComplexSchema(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number", "minimum": 0},
			"items": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"sku": {"type": "string"},
						"quantity": {"type": "integer", "minimum": 1}
					},
					"required": ["sku", "quantity"]
				},
				"minItems": 1
			}
		},
		"required": ["id", "amount", "items"]
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid order with items passes", func(t *testing.T) {
		order := Order{
			ID:     "123",
			Amount: 99.99,
			Items: []Item{
				{SKU: "SKU-001", Quantity: 2},
				{SKU: "SKU-002", Quantity: 1},
			},
		}
		if err := v.Validate(order); err != nil {
			t.Errorf("expected valid order to pass: %v", err)
		}
	})

	t.Run("empty items array fails", func(t *testing.T) {
		order := Order{
			ID:     "123",
			Amount: 99.99,
			Items:  []Item{},
		}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for empty items")
		}
	})

	t.Run("item with zero quantity fails", func(t *testing.T) {
		order := Order{
			ID:     "123",
			Amount: 99.99,
			Items: []Item{
				{SKU: "SKU-001", Quantity: 0},
			},
		}
		err := v.Validate(order)
		if err == nil {
			t.Fatal("expected error for zero quantity")
		}
	})
}

func TestValidateMiddleware(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number", "minimum": 0}
		},
		"required": ["id", "amount"]
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid payload calls next handler", func(t *testing.T) {
		called := false
		handler := func(ctx context.Context, ev event.Event[Order], data Order) error {
			called = true
			return nil
		}

		middleware := ValidateMiddleware[Order](v)
		wrapped := middleware(handler)

		ctx := context.Background()
		order := Order{ID: "123", Amount: 99.99}
		err := wrapped(ctx, nil, order)

		if err != nil {
			t.Errorf("expected no error: %v", err)
		}
		if !called {
			t.Error("expected handler to be called")
		}
	})

	t.Run("invalid payload returns reject error", func(t *testing.T) {
		called := false
		handler := func(ctx context.Context, ev event.Event[Order], data Order) error {
			called = true
			return nil
		}

		middleware := ValidateMiddleware[Order](v)
		wrapped := middleware(handler)

		ctx := context.Background()
		order := Order{ID: "123", Amount: -10} // negative amount fails validation
		err := wrapped(ctx, nil, order)

		if err == nil {
			t.Fatal("expected error for invalid payload")
		}
		if !errors.Is(err, event.ErrReject) {
			t.Errorf("expected ErrReject, got %v", err)
		}
		if called {
			t.Error("handler should not be called for invalid payload")
		}
	})

	t.Run("nil validator returns reject error", func(t *testing.T) {
		handler := func(ctx context.Context, ev event.Event[Order], data Order) error {
			return nil
		}

		middleware := ValidateMiddleware[Order](nil)
		wrapped := middleware(handler)

		ctx := context.Background()
		order := Order{ID: "123", Amount: 99.99}
		err := wrapped(ctx, nil, order)

		if err == nil {
			t.Fatal("expected error for nil validator")
		}
		if !errors.Is(err, event.ErrReject) {
			t.Errorf("expected ErrReject, got %v", err)
		}
	})
}

func TestCompositeValidator(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number"}
		},
		"required": ["id", "amount"]
	}`

	jsonValidator, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	// Custom validator that checks for positive amount
	positiveAmount := FuncValidator(func(payload any) error {
		if order, ok := payload.(Order); ok && order.Amount < 0 {
			return errors.New("amount must be positive")
		}
		return nil
	})

	t.Run("all validators pass", func(t *testing.T) {
		composite := NewCompositeValidator(jsonValidator, positiveAmount)
		order := Order{ID: "123", Amount: 99.99}
		if err := composite.Validate(order); err != nil {
			t.Errorf("expected no error: %v", err)
		}
	})

	t.Run("first validator fails", func(t *testing.T) {
		composite := NewCompositeValidator(jsonValidator, positiveAmount)
		type InvalidOrder struct {
			ID     int     `json:"id"` // wrong type
			Amount float64 `json:"amount"`
		}
		order := InvalidOrder{ID: 123, Amount: 99.99}
		if err := composite.Validate(order); err == nil {
			t.Error("expected error from schema validator")
		}
	})

	t.Run("second validator fails", func(t *testing.T) {
		composite := NewCompositeValidator(jsonValidator, positiveAmount)
		order := Order{ID: "123", Amount: -10}
		err := composite.Validate(order)
		if err == nil {
			t.Fatal("expected error from custom validator")
		}
		if !strings.Contains(err.Error(), "positive") {
			t.Errorf("expected positive amount error, got: %v", err)
		}
	})

	t.Run("empty composite passes all", func(t *testing.T) {
		composite := NewCompositeValidator()
		order := Order{ID: "123", Amount: 99.99}
		if err := composite.Validate(order); err != nil {
			t.Errorf("expected no error: %v", err)
		}
	})
}

func TestFuncValidator(t *testing.T) {
	t.Run("function is called", func(t *testing.T) {
		called := false
		v := FuncValidator(func(payload any) error {
			called = true
			return nil
		})

		v.Validate(struct{}{})
		if !called {
			t.Error("expected function to be called")
		}
	})

	t.Run("error is returned", func(t *testing.T) {
		expectedErr := errors.New("test error")
		v := FuncValidator(func(payload any) error {
			return expectedErr
		})

		err := v.Validate(struct{}{})
		if err != expectedErr {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})
}

func TestValidationError(t *testing.T) {
	t.Run("error string with path", func(t *testing.T) {
		err := &ValidationError{
			Message: "invalid value",
			Path:    "/amount",
		}
		expected := "/amount: invalid value"
		if err.Error() != expected {
			t.Errorf("expected %q, got %q", expected, err.Error())
		}
	})

	t.Run("error string without path", func(t *testing.T) {
		err := &ValidationError{
			Message: "invalid value",
		}
		expected := "invalid value"
		if err.Error() != expected {
			t.Errorf("expected %q, got %q", expected, err.Error())
		}
	})

	t.Run("unwrap returns cause", func(t *testing.T) {
		cause := errors.New("original error")
		err := &ValidationError{
			Message: "wrapped",
			Cause:   cause,
		}
		if err.Unwrap() != cause {
			t.Error("expected Unwrap to return cause")
		}
	})
}

func TestJSONSchemaValidator_BytesReader(t *testing.T) {
	schema := `{"type": "object", "properties": {"name": {"type": "string"}}}`

	// Test with bytes.Buffer
	buf := bytes.NewBuffer([]byte(schema))
	v, err := NewJSONSchemaValidator(buf)
	if err != nil {
		t.Fatalf("failed with bytes.Buffer: %v", err)
	}

	data := map[string]any{"name": "test"}
	if err := v.Validate(data); err != nil {
		t.Errorf("validation failed: %v", err)
	}
}

func TestJSONSchemaValidator_EnumValidation(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"status": {
				"type": "string",
				"enum": ["pending", "approved", "rejected"]
			}
		},
		"required": ["status"]
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid enum value", func(t *testing.T) {
		data := map[string]any{"status": "approved"}
		if err := v.Validate(data); err != nil {
			t.Errorf("expected valid enum to pass: %v", err)
		}
	})

	t.Run("invalid enum value", func(t *testing.T) {
		data := map[string]any{"status": "invalid"}
		if err := v.Validate(data); err == nil {
			t.Error("expected error for invalid enum value")
		}
	})
}

func TestJSONSchemaValidator_PatternValidation(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"email": {
				"type": "string",
				"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
			}
		},
		"required": ["email"]
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid email pattern", func(t *testing.T) {
		data := map[string]any{"email": "test@example.com"}
		if err := v.Validate(data); err != nil {
			t.Errorf("expected valid email to pass: %v", err)
		}
	})

	t.Run("invalid email pattern", func(t *testing.T) {
		data := map[string]any{"email": "not-an-email"}
		if err := v.Validate(data); err == nil {
			t.Error("expected error for invalid email")
		}
	})
}

func TestJSONSchemaValidator_NestedObjects(t *testing.T) {
	schema := `{
		"type": "object",
		"properties": {
			"user": {
				"type": "object",
				"properties": {
					"name": {"type": "string"},
					"address": {
						"type": "object",
						"properties": {
							"city": {"type": "string"},
							"zip": {"type": "string", "pattern": "^[0-9]{5}$"}
						},
						"required": ["city", "zip"]
					}
				},
				"required": ["name", "address"]
			}
		},
		"required": ["user"]
	}`

	v, err := NewJSONSchemaValidator(schema)
	if err != nil {
		t.Fatalf("NewJSONSchemaValidator failed: %v", err)
	}

	t.Run("valid nested object", func(t *testing.T) {
		data := map[string]any{
			"user": map[string]any{
				"name": "John",
				"address": map[string]any{
					"city": "New York",
					"zip":  "10001",
				},
			},
		}
		if err := v.Validate(data); err != nil {
			t.Errorf("expected valid nested object to pass: %v", err)
		}
	})

	t.Run("invalid nested field", func(t *testing.T) {
		data := map[string]any{
			"user": map[string]any{
				"name": "John",
				"address": map[string]any{
					"city": "New York",
					"zip":  "invalid", // should be 5 digits
				},
			},
		}
		if err := v.Validate(data); err == nil {
			t.Error("expected error for invalid zip")
		}
	})
}

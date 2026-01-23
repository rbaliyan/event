package validation_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/validation"
)

// Order represents an order payload
type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

// mockEvent implements a minimal event interface for examples
type mockEvent[T any] struct {
	name string
}

func (e mockEvent[T]) Name() string { return e.name }
func (e mockEvent[T]) Publish(ctx context.Context, data T) error {
	return nil
}
func (e mockEvent[T]) Subscribe(ctx context.Context, handler event.Handler[T], opts ...event.SubscribeOption[T]) error {
	return nil
}

func ExampleNewJSONSchemaValidator() {
	// Define a JSON Schema
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string", "minLength": 1},
			"amount": {"type": "number", "minimum": 0}
		},
		"required": ["id", "amount"]
	}`

	// Create validator from string
	validator, err := validation.NewJSONSchemaValidator(schema)
	if err != nil {
		log.Fatal(err)
	}

	// Validate a valid order
	order := Order{ID: "ORD-123", Amount: 99.99}
	if err := validator.Validate(order); err != nil {
		log.Printf("Validation failed: %v", err)
	} else {
		fmt.Println("Order is valid")
	}

	// Validate an invalid order (negative amount)
	invalidOrder := Order{ID: "ORD-456", Amount: -10}
	if err := validator.Validate(invalidOrder); err != nil {
		fmt.Println("Invalid order rejected")
	}

	// Output:
	// Order is valid
	// Invalid order rejected
}

func ExampleNewJSONSchemaValidator_fromBytes() {
	schemaBytes := []byte(`{"type": "string", "minLength": 1}`)

	validator, err := validation.NewJSONSchemaValidator(schemaBytes)
	if err != nil {
		log.Fatal(err)
	}

	if err := validator.Validate("hello"); err != nil {
		fmt.Printf("Validation failed: %v\n", err)
	} else {
		fmt.Println("String is valid")
	}

	if err := validator.Validate(""); err != nil {
		fmt.Println("Empty string rejected")
	}

	// Output:
	// String is valid
	// Empty string rejected
}

func ExampleValidateMiddleware() {
	// Define schema
	schema := `{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number", "minimum": 0}
		},
		"required": ["id", "amount"]
	}`

	validator, _ := validation.NewJSONSchemaValidator(schema)

	// Create a handler
	handler := func(ctx context.Context, ev event.Event[Order], order Order) error {
		fmt.Printf("Processing order: %s\n", order.ID)
		return nil
	}

	// Wrap with validation middleware
	middleware := validation.ValidateMiddleware[Order](validator)
	wrapped := middleware(handler)

	// Valid order passes through
	ctx := context.Background()
	ev := mockEvent[Order]{name: "orders"}
	validOrder := Order{ID: "ORD-001", Amount: 50.00}
	if err := wrapped(ctx, ev, validOrder); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Invalid order is rejected
	invalidOrder := Order{ID: "ORD-002", Amount: -10}
	if err := wrapped(ctx, ev, invalidOrder); err != nil {
		if errors.Is(err, event.ErrReject) {
			fmt.Println("Invalid order rejected (sent to DLQ)")
		}
	}

	// Output:
	// Processing order: ORD-001
	// Invalid order rejected (sent to DLQ)
}

func ExampleCompositeValidator() {
	// JSON Schema validator for structure
	schemaValidator, _ := validation.NewJSONSchemaValidator(`{
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"amount": {"type": "number"},
			"status": {"type": "string"}
		},
		"required": ["id", "amount", "status"]
	}`)

	// Custom business logic validator
	businessValidator := validation.FuncValidator(func(payload any) error {
		order, ok := payload.(Order)
		if !ok {
			return nil // Skip if not an Order
		}

		// Business rule: approved orders must have positive amount
		if order.Status == "approved" && order.Amount <= 0 {
			return errors.New("approved orders must have positive amount")
		}

		return nil
	})

	// Combine validators
	composite := validation.NewCompositeValidator(schemaValidator, businessValidator)

	// Valid order
	validOrder := Order{ID: "ORD-001", Amount: 100, Status: "approved"}
	if err := composite.Validate(validOrder); err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("Order validated successfully")
	}

	// Invalid: approved with zero amount
	invalidOrder := Order{ID: "ORD-002", Amount: 0, Status: "approved"}
	if err := composite.Validate(invalidOrder); err != nil {
		fmt.Println("Business rule violation detected")
	}

	// Output:
	// Order validated successfully
	// Business rule violation detected
}

func ExampleFuncValidator() {
	// Simple custom validator
	positiveAmount := validation.FuncValidator(func(payload any) error {
		if order, ok := payload.(Order); ok {
			if order.Amount < 0 {
				return fmt.Errorf("amount must be positive, got %v", order.Amount)
			}
		}
		return nil
	})

	order := Order{ID: "ORD-001", Amount: -50}
	if err := positiveAmount.Validate(order); err != nil {
		fmt.Println("Validation failed:", err)
	}

	// Output:
	// Validation failed: amount must be positive, got -50
}

func ExampleValidationError() {
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

	validator, _ := validation.NewJSONSchemaValidator(schema)

	data := map[string]any{"email": "invalid-email"}
	err := validator.Validate(data)

	if err != nil {
		var validationErr *validation.ValidationError
		if errors.As(err, &validationErr) {
			fmt.Printf("Validation error: %s\n", validationErr.Message)
			if validationErr.Path != "" {
				fmt.Printf("At path: %s\n", validationErr.Path)
			}
		}
	}

	// Output:
	// Validation error: does not match pattern '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
	// At path: /email
}

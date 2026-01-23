// Package validation provides event payload validation with JSON Schema support.
//
// This package enables validation of event payloads before processing,
// helping to catch malformed or invalid data early in the processing pipeline.
// It provides a generic Validator interface and a JSON Schema implementation.
//
// The validation middleware can be applied to event subscriptions to automatically
// validate incoming payloads against a schema before they reach the handler.
//
// Example usage:
//
//	// Define a JSON Schema
//	schema := `{
//	    "type": "object",
//	    "properties": {
//	        "id": {"type": "string"},
//	        "amount": {"type": "number", "minimum": 0}
//	    },
//	    "required": ["id", "amount"]
//	}`
//
//	// Create validator
//	validator, err := validation.NewJSONSchemaValidator(schema)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use with middleware
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        validation.ValidateMiddleware[Order](validator),
//	    ),
//	)
package validation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/rbaliyan/event/v3"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Common validation errors.
var (
	// ErrInvalidPayload is returned when validation fails.
	ErrInvalidPayload = errors.New("payload validation failed")

	// ErrNilValidator is returned when a nil validator is provided.
	ErrNilValidator = errors.New("validator cannot be nil")

	// ErrInvalidSchema is returned when the schema is invalid or cannot be parsed.
	ErrInvalidSchema = errors.New("invalid schema")

	// ErrUnsupportedSchemaType is returned when the schema input type is not supported.
	ErrUnsupportedSchemaType = errors.New("unsupported schema type")
)

// Validator validates event payloads before processing.
//
// Implementations should be safe for concurrent use.
type Validator interface {
	// Validate checks if the payload is valid.
	// Returns nil if valid, error describing the issue if invalid.
	Validate(payload any) error
}

// ValidationError provides detailed information about validation failures.
type ValidationError struct {
	// Message describes the validation failure.
	Message string

	// Path indicates which field failed validation (JSON pointer format).
	Path string

	// Cause is the underlying error if any.
	Cause error
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Path != "" {
		return fmt.Sprintf("%s: %s", e.Path, e.Message)
	}
	return e.Message
}

// Unwrap returns the underlying cause.
func (e *ValidationError) Unwrap() error {
	return e.Cause
}

// JSONSchemaValidator validates payloads against a JSON Schema.
//
// JSONSchemaValidator is safe for concurrent use. The compiled schema is
// immutable after creation and validation is thread-safe.
//
// Supports JSON Schema draft-07 and draft-2019-09 (default).
type JSONSchemaValidator struct {
	schema *jsonschema.Schema
}

// NewJSONSchemaValidator creates a validator from a JSON Schema.
//
// The schema can be provided as:
//   - string: JSON schema as a string
//   - []byte: JSON schema as bytes
//   - io.Reader: Reader containing the JSON schema
//   - file path: Path starting with "file://" (e.g., "file:///path/to/schema.json")
//
// Returns an error if the schema is invalid or cannot be parsed.
//
// Example:
//
//	// From string
//	validator, err := NewJSONSchemaValidator(`{"type": "object"}`)
//
//	// From file
//	validator, err := NewJSONSchemaValidator("file:///path/to/schema.json")
//
//	// From bytes
//	validator, err := NewJSONSchemaValidator(schemaBytes)
//
//	// From reader
//	validator, err := NewJSONSchemaValidator(bytes.NewReader(schemaBytes))
func NewJSONSchemaValidator(schema any) (*JSONSchemaValidator, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2019

	var compiled *jsonschema.Schema
	var err error

	switch s := schema.(type) {
	case string:
		if strings.HasPrefix(s, "file://") {
			path := strings.TrimPrefix(s, "file://")
			compiled, err = compileFromFile(compiler, path)
		} else {
			compiled, err = compileFromReader(compiler, strings.NewReader(s))
		}
	case []byte:
		compiled, err = compileFromReader(compiler, bytes.NewReader(s))
	case io.Reader:
		compiled, err = compileFromReader(compiler, s)
	default:
		return nil, fmt.Errorf("%w: expected string, []byte, or io.Reader, got %T", ErrUnsupportedSchemaType, schema)
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidSchema, err)
	}

	return &JSONSchemaValidator{schema: compiled}, nil
}

// compileFromReader compiles a schema from an io.Reader.
func compileFromReader(compiler *jsonschema.Compiler, r io.Reader) (*jsonschema.Schema, error) {
	// Read all content
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	// Add the schema to the compiler with a virtual URL
	if err := compiler.AddResource("schema.json", bytes.NewReader(data)); err != nil {
		return nil, fmt.Errorf("failed to add schema resource: %w", err)
	}

	return compiler.Compile("schema.json")
}

// compileFromFile compiles a schema from a file path.
func compileFromFile(compiler *jsonschema.Compiler, path string) (*jsonschema.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open schema file: %w", err)
	}
	defer f.Close()

	return compileFromReader(compiler, f)
}

// Validate validates the payload against the JSON schema.
//
// The payload is first marshaled to JSON, then validated against the schema.
// Returns nil if the payload is valid, or a ValidationError if validation fails.
//
// Example:
//
//	type Order struct {
//	    ID     string  `json:"id"`
//	    Amount float64 `json:"amount"`
//	}
//
//	order := Order{ID: "123", Amount: 99.99}
//	if err := validator.Validate(order); err != nil {
//	    log.Printf("Validation failed: %v", err)
//	}
func (v *JSONSchemaValidator) Validate(payload any) error {
	// Marshal the payload to JSON
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return &ValidationError{
			Message: fmt.Sprintf("failed to marshal payload: %v", err),
			Cause:   err,
		}
	}

	// Unmarshal to interface{} for schema validation
	var data any
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return &ValidationError{
			Message: fmt.Sprintf("failed to unmarshal payload: %v", err),
			Cause:   err,
		}
	}

	// Validate against schema
	if err := v.schema.Validate(data); err != nil {
		var validationErr *jsonschema.ValidationError
		if errors.As(err, &validationErr) {
			return convertValidationError(validationErr)
		}
		return &ValidationError{
			Message: err.Error(),
			Cause:   err,
		}
	}

	return nil
}

// convertValidationError converts a jsonschema.ValidationError to our ValidationError.
func convertValidationError(err *jsonschema.ValidationError) *ValidationError {
	// Get the most specific error message
	if len(err.Causes) > 0 {
		// Use the first cause for the most specific error
		return convertValidationError(err.Causes[0])
	}

	return &ValidationError{
		Message: err.Message,
		Path:    err.InstanceLocation,
		Cause:   err,
	}
}

// ValidateMiddleware creates middleware that validates payloads before processing.
//
// When validation fails, the middleware returns an error that wraps ErrReject,
// causing the message to be sent to the dead letter queue (if configured)
// rather than being retried.
//
// Example:
//
//	validator, _ := validation.NewJSONSchemaValidator(orderSchema)
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        validation.ValidateMiddleware[Order](validator),
//	    ),
//	)
func ValidateMiddleware[T any](validator Validator) event.Middleware[T] {
	if validator == nil {
		// Return a middleware that always fails if validator is nil
		return func(next event.Handler[T]) event.Handler[T] {
			return func(ctx context.Context, ev event.Event[T], data T) error {
				return fmt.Errorf("%w: %v", event.ErrReject, ErrNilValidator)
			}
		}
	}

	return func(next event.Handler[T]) event.Handler[T] {
		return func(ctx context.Context, ev event.Event[T], data T) error {
			if err := validator.Validate(data); err != nil {
				// Validation errors are permanent - reject the message
				return fmt.Errorf("%w: %v", event.ErrReject, err)
			}
			return next(ctx, ev, data)
		}
	}
}

// CompositeValidator combines multiple validators into one.
//
// All validators are executed in order. If any validator fails,
// the first error is returned immediately.
type CompositeValidator struct {
	validators []Validator
}

// NewCompositeValidator creates a validator that runs multiple validators.
//
// Validators are executed in order. The first failure stops execution
// and returns the error.
//
// Example:
//
//	schemaValidator, _ := validation.NewJSONSchemaValidator(schema)
//	customValidator := &MyCustomValidator{}
//
//	composite := validation.NewCompositeValidator(schemaValidator, customValidator)
//	orderEvent.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        validation.ValidateMiddleware[Order](composite),
//	    ),
//	)
func NewCompositeValidator(validators ...Validator) *CompositeValidator {
	return &CompositeValidator{validators: validators}
}

// Validate runs all validators in order.
func (c *CompositeValidator) Validate(payload any) error {
	for _, v := range c.validators {
		if err := v.Validate(payload); err != nil {
			return err
		}
	}
	return nil
}

// FuncValidator adapts a function to the Validator interface.
//
// This is useful for simple, one-off validation logic.
//
// Example:
//
//	positiveAmount := validation.FuncValidator(func(payload any) error {
//	    if order, ok := payload.(Order); ok && order.Amount < 0 {
//	        return errors.New("amount must be positive")
//	    }
//	    return nil
//	})
type FuncValidator func(payload any) error

// Validate calls the underlying function.
func (f FuncValidator) Validate(payload any) error {
	return f(payload)
}

// Compile-time interface checks
var (
	_ Validator = (*JSONSchemaValidator)(nil)
	_ Validator = (*CompositeValidator)(nil)
	_ Validator = (FuncValidator)(nil)
)

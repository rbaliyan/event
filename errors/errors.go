// Package errors provides common error types shared across the event ecosystem.
//
// These errors provide consistent error semantics for:
//   - event-dlq: Dead Letter Queue storage
//   - event-scheduler: Scheduled message delivery
//   - event-extras: Saga orchestration and rate limiting
//
// Usage:
//
//	import "github.com/rbaliyan/event/v3/errors"
//
//	if errors.Is(err, errors.ErrNotFound) {
//	    // Handle not found
//	}
package errors

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Common sentinel errors for the event ecosystem.
// Use errors.Is() to check for these errors as they may be wrapped.
var (
	// ErrNotFound indicates the requested resource was not found.
	// Used by:
	//   - event-scheduler: scheduled message not found
	//   - event-dlq: DLQ message not found
	//   - event-extras/saga: saga state not found
	ErrNotFound = errors.New("not found")

	// ErrVersionConflict indicates an optimistic locking conflict.
	// The resource was modified by another process between read and update.
	// Used by:
	//   - event-extras/saga: saga state was modified during execution
	ErrVersionConflict = errors.New("version conflict: resource was modified by another process")

	// ErrAlreadyExists indicates the resource already exists.
	// Used when creating a resource with a duplicate key or ID.
	ErrAlreadyExists = errors.New("already exists")

	// ErrClosed indicates the component has been closed.
	// Operations cannot proceed on a closed component.
	ErrClosed = errors.New("closed")

	// ErrTimeout indicates an operation timed out.
	ErrTimeout = errors.New("timeout")

	// ErrInvalidArgument indicates an invalid argument was provided.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrMaxRetriesExceeded indicates all retry attempts have been exhausted.
	// Used by:
	//   - event-scheduler: message delivery failed after max retries
	//   - event-dlq: replay failed after max retries
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")

	// ErrStorageUnavailable indicates the storage backend is not available.
	// Used when database connections fail or storage is temporarily unavailable.
	ErrStorageUnavailable = errors.New("storage unavailable")

	// ErrScheduledInPast indicates a message was scheduled for a time in the past.
	// Used by event-scheduler when ScheduledAt is before the current time.
	ErrScheduledInPast = errors.New("scheduled time is in the past")

	// ErrTransportUnavailable indicates the transport is not available.
	// Used when message publishing fails due to transport issues.
	ErrTransportUnavailable = errors.New("transport unavailable")

	// ErrCompensationFailed indicates saga compensation failed.
	// Used by event-extras/saga when rollback operations fail.
	ErrCompensationFailed = errors.New("compensation failed")
)

// RequestContext contains contextual information about a request for debugging.
// Embed this in error types to provide rich context for logging and monitoring.
type RequestContext struct {
	// EventID is the unique identifier of the event being processed.
	EventID string `json:"event_id,omitempty"`

	// EventName is the name of the event type.
	EventName string `json:"event_name,omitempty"`

	// TraceID is the distributed tracing ID (OpenTelemetry).
	TraceID string `json:"trace_id,omitempty"`

	// SpanID is the current span ID (OpenTelemetry).
	SpanID string `json:"span_id,omitempty"`

	// BusID is the identifier of the event bus.
	BusID string `json:"bus_id,omitempty"`

	// SubscriptionID identifies the subscriber processing the event.
	SubscriptionID string `json:"subscription_id,omitempty"`

	// Timestamp is when the error occurred.
	Timestamp time.Time `json:"timestamp,omitempty"`

	// Extra holds additional context-specific key-value pairs.
	Extra map[string]string `json:"extra,omitempty"`
}

// String returns a formatted string representation of the context.
func (c RequestContext) String() string {
	if c.IsEmpty() {
		return ""
	}

	var parts []string
	if c.EventID != "" {
		parts = append(parts, "event_id="+c.EventID)
	}
	if c.EventName != "" {
		parts = append(parts, "event="+c.EventName)
	}
	if c.TraceID != "" {
		parts = append(parts, "trace_id="+c.TraceID)
	}
	if c.BusID != "" {
		parts = append(parts, "bus="+c.BusID)
	}
	if c.SubscriptionID != "" {
		parts = append(parts, "subscription="+c.SubscriptionID)
	}
	for k, v := range c.Extra {
		parts = append(parts, k+"="+v)
	}

	return "[" + strings.Join(parts, ", ") + "]"
}

// IsEmpty returns true if no context fields are set.
func (c RequestContext) IsEmpty() bool {
	return c.EventID == "" && c.EventName == "" && c.TraceID == "" &&
		c.SpanID == "" && c.BusID == "" && c.SubscriptionID == "" && len(c.Extra) == 0
}

// WithExtra returns a copy of the context with an additional key-value pair.
func (c RequestContext) WithExtra(key, value string) RequestContext {
	copy := c
	if copy.Extra == nil {
		copy.Extra = make(map[string]string)
	} else {
		// Deep copy the map
		newExtra := make(map[string]string, len(c.Extra)+1)
		for k, v := range c.Extra {
			newExtra[k] = v
		}
		copy.Extra = newExtra
	}
	copy.Extra[key] = value
	return copy
}

// ContextualError is an interface for errors that carry request context.
type ContextualError interface {
	error
	// Context returns the request context associated with this error.
	Context() RequestContext
}

// NotFoundError wraps ErrNotFound with additional context.
type NotFoundError struct {
	Resource string // e.g., "scheduled message", "saga state", "DLQ message"
	ID       string
	Ctx      RequestContext
}

func (e *NotFoundError) Error() string {
	msg := e.Resource + " not found"
	if e.ID != "" {
		msg = fmt.Sprintf("%s not found: %s", e.Resource, e.ID)
	}
	if ctx := e.Ctx.String(); ctx != "" {
		msg += " " + ctx
	}
	return msg
}

func (e *NotFoundError) Unwrap() error {
	return ErrNotFound
}

// Context returns the request context.
func (e *NotFoundError) Context() RequestContext {
	return e.Ctx
}

// NewNotFoundError creates a NotFoundError for the given resource and ID.
func NewNotFoundError(resource, id string) error {
	return &NotFoundError{Resource: resource, ID: id}
}

// NewNotFoundErrorWithContext creates a NotFoundError with request context.
func NewNotFoundErrorWithContext(resource, id string, ctx RequestContext) error {
	return &NotFoundError{Resource: resource, ID: id, Ctx: ctx}
}

// IsNotFound checks if an error indicates a not found condition.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// VersionConflictError wraps ErrVersionConflict with additional context.
type VersionConflictError struct {
	Resource        string
	ID              string
	ExpectedVersion int64
	ActualVersion   int64
	Ctx             RequestContext
}

func (e *VersionConflictError) Error() string {
	var msg string
	if e.ID != "" {
		msg = fmt.Sprintf("%s %s version conflict: expected %d, got %d",
			e.Resource, e.ID, e.ExpectedVersion, e.ActualVersion)
	} else {
		msg = fmt.Sprintf("%s version conflict: expected %d, got %d",
			e.Resource, e.ExpectedVersion, e.ActualVersion)
	}
	if ctx := e.Ctx.String(); ctx != "" {
		msg += " " + ctx
	}
	return msg
}

func (e *VersionConflictError) Unwrap() error {
	return ErrVersionConflict
}

// Context returns the request context.
func (e *VersionConflictError) Context() RequestContext {
	return e.Ctx
}

// NewVersionConflictError creates a VersionConflictError.
func NewVersionConflictError(resource, id string, expected, actual int64) error {
	return &VersionConflictError{
		Resource:        resource,
		ID:              id,
		ExpectedVersion: expected,
		ActualVersion:   actual,
	}
}

// NewVersionConflictErrorWithContext creates a VersionConflictError with request context.
func NewVersionConflictErrorWithContext(resource, id string, expected, actual int64, ctx RequestContext) error {
	return &VersionConflictError{
		Resource:        resource,
		ID:              id,
		ExpectedVersion: expected,
		ActualVersion:   actual,
		Ctx:             ctx,
	}
}

// IsVersionConflict checks if an error indicates a version conflict.
func IsVersionConflict(err error) bool {
	return errors.Is(err, ErrVersionConflict)
}

// ValidationError wraps ErrInvalidArgument with field-level details.
type ValidationError struct {
	Field   string
	Message string
	Ctx     RequestContext
}

func (e *ValidationError) Error() string {
	var msg string
	if e.Field != "" {
		msg = fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
	} else {
		msg = fmt.Sprintf("validation error: %s", e.Message)
	}
	if ctx := e.Ctx.String(); ctx != "" {
		msg += " " + ctx
	}
	return msg
}

func (e *ValidationError) Unwrap() error {
	return ErrInvalidArgument
}

// Context returns the request context.
func (e *ValidationError) Context() RequestContext {
	return e.Ctx
}

// NewValidationError creates a ValidationError for the given field.
func NewValidationError(field, message string) error {
	return &ValidationError{Field: field, Message: message}
}

// NewValidationErrorWithContext creates a ValidationError with request context.
func NewValidationErrorWithContext(field, message string, ctx RequestContext) error {
	return &ValidationError{Field: field, Message: message, Ctx: ctx}
}

// IsInvalidArgument checks if an error indicates an invalid argument.
func IsInvalidArgument(err error) bool {
	return errors.Is(err, ErrInvalidArgument)
}

// IsAlreadyExists checks if an error indicates a resource already exists.
func IsAlreadyExists(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}

// IsClosed checks if an error indicates a closed component.
func IsClosed(err error) bool {
	return errors.Is(err, ErrClosed)
}

// IsTimeout checks if an error indicates a timeout.
func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// RequireNotNil panics if the value is nil with a descriptive message.
// Use this in constructors for required parameters that must not be nil.
// Handles both untyped nil and typed nil (e.g., var db *sql.DB = nil).
//
// Example:
//
//	func NewStore(db *sql.DB) *Store {
//	    errors.RequireNotNil(db, "db")
//	    return &Store{db: db}
//	}
func RequireNotNil(value any, name string) {
	if value == nil {
		panic(fmt.Sprintf("%s must not be nil", name))
	}
	// Check for typed nils using reflection
	// This handles cases like var db *sql.DB = nil
	v := reflect.ValueOf(value)
	kind := v.Kind()
	if (kind == reflect.Ptr || kind == reflect.Interface ||
		kind == reflect.Map || kind == reflect.Slice ||
		kind == reflect.Chan || kind == reflect.Func) && v.IsNil() {
		panic(fmt.Sprintf("%s must not be nil", name))
	}
}

// RequireNotEmpty panics if the string is empty with a descriptive message.
// Use this in constructors for required string parameters.
//
// Example:
//
//	func NewSaga(name string) *Saga {
//	    errors.RequireNotEmpty(name, "name")
//	    return &Saga{name: name}
//	}
func RequireNotEmpty(value string, name string) {
	if value == "" {
		panic(fmt.Sprintf("%s must not be empty", name))
	}
}

// RequirePositive panics if the integer is not positive (> 0).
// Use this in constructors for required positive integer parameters.
func RequirePositive(value int, name string) {
	if value <= 0 {
		panic(fmt.Sprintf("%s must be positive, got %d", name, value))
	}
}

// RequireNonNegative panics if the integer is negative.
// Use this in constructors for parameters that must be >= 0.
func RequireNonNegative(value int, name string) {
	if value < 0 {
		panic(fmt.Sprintf("%s must be non-negative, got %d", name, value))
	}
}

// IsMaxRetriesExceeded checks if an error indicates max retries exceeded.
func IsMaxRetriesExceeded(err error) bool {
	return errors.Is(err, ErrMaxRetriesExceeded)
}

// IsStorageUnavailable checks if an error indicates storage unavailability.
func IsStorageUnavailable(err error) bool {
	return errors.Is(err, ErrStorageUnavailable)
}

// IsScheduledInPast checks if an error indicates scheduling in the past.
func IsScheduledInPast(err error) bool {
	return errors.Is(err, ErrScheduledInPast)
}

// IsTransportUnavailable checks if an error indicates transport unavailability.
func IsTransportUnavailable(err error) bool {
	return errors.Is(err, ErrTransportUnavailable)
}

// IsCompensationFailed checks if an error indicates compensation failure.
func IsCompensationFailed(err error) bool {
	return errors.Is(err, ErrCompensationFailed)
}

// MaxRetriesError wraps ErrMaxRetriesExceeded with attempt details.
type MaxRetriesError struct {
	Attempts int
	LastErr  error
	Ctx      RequestContext
}

func (e *MaxRetriesError) Error() string {
	var msg string
	if e.LastErr != nil {
		msg = fmt.Sprintf("max retries exceeded after %d attempts: %v", e.Attempts, e.LastErr)
	} else {
		msg = fmt.Sprintf("max retries exceeded after %d attempts", e.Attempts)
	}
	if ctx := e.Ctx.String(); ctx != "" {
		msg += " " + ctx
	}
	return msg
}

func (e *MaxRetriesError) Unwrap() error {
	return ErrMaxRetriesExceeded
}

// Context returns the request context.
func (e *MaxRetriesError) Context() RequestContext {
	return e.Ctx
}

// NewMaxRetriesError creates a MaxRetriesError.
func NewMaxRetriesError(attempts int, lastErr error) error {
	return &MaxRetriesError{Attempts: attempts, LastErr: lastErr}
}

// NewMaxRetriesErrorWithContext creates a MaxRetriesError with request context.
func NewMaxRetriesErrorWithContext(attempts int, lastErr error, ctx RequestContext) error {
	return &MaxRetriesError{Attempts: attempts, LastErr: lastErr, Ctx: ctx}
}

// StorageError wraps ErrStorageUnavailable with details.
type StorageError struct {
	Operation string // e.g., "save", "load", "delete"
	Cause     error
	Ctx       RequestContext
}

func (e *StorageError) Error() string {
	var msg string
	if e.Cause != nil {
		msg = fmt.Sprintf("storage unavailable during %s: %v", e.Operation, e.Cause)
	} else {
		msg = fmt.Sprintf("storage unavailable during %s", e.Operation)
	}
	if ctx := e.Ctx.String(); ctx != "" {
		msg += " " + ctx
	}
	return msg
}

func (e *StorageError) Unwrap() error {
	return ErrStorageUnavailable
}

// Context returns the request context.
func (e *StorageError) Context() RequestContext {
	return e.Ctx
}

// NewStorageError creates a StorageError.
func NewStorageError(operation string, cause error) error {
	return &StorageError{Operation: operation, Cause: cause}
}

// NewStorageErrorWithContext creates a StorageError with request context.
func NewStorageErrorWithContext(operation string, cause error, ctx RequestContext) error {
	return &StorageError{Operation: operation, Cause: cause, Ctx: ctx}
}

// Compile-time checks
var (
	_ ContextualError = (*NotFoundError)(nil)
	_ ContextualError = (*VersionConflictError)(nil)
	_ ContextualError = (*ValidationError)(nil)
	_ ContextualError = (*MaxRetriesError)(nil)
	_ ContextualError = (*StorageError)(nil)
)

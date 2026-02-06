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

// NotFoundError wraps ErrNotFound with additional context.
type NotFoundError struct {
	Resource string // e.g., "scheduled message", "saga state", "DLQ message"
	ID       string
}

func (e *NotFoundError) Error() string {
	if e.ID != "" {
		return fmt.Sprintf("%s not found: %s", e.Resource, e.ID)
	}
	return fmt.Sprintf("%s not found", e.Resource)
}

func (e *NotFoundError) Unwrap() error {
	return ErrNotFound
}

// NewNotFoundError creates a NotFoundError for the given resource and ID.
func NewNotFoundError(resource, id string) error {
	return &NotFoundError{Resource: resource, ID: id}
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
}

func (e *VersionConflictError) Error() string {
	if e.ID != "" {
		return fmt.Sprintf("%s %s version conflict: expected %d, got %d",
			e.Resource, e.ID, e.ExpectedVersion, e.ActualVersion)
	}
	return fmt.Sprintf("%s version conflict: expected %d, got %d",
		e.Resource, e.ExpectedVersion, e.ActualVersion)
}

func (e *VersionConflictError) Unwrap() error {
	return ErrVersionConflict
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

// IsVersionConflict checks if an error indicates a version conflict.
func IsVersionConflict(err error) bool {
	return errors.Is(err, ErrVersionConflict)
}

// ValidationError wraps ErrInvalidArgument with field-level details.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

func (e *ValidationError) Unwrap() error {
	return ErrInvalidArgument
}

// NewValidationError creates a ValidationError for the given field.
func NewValidationError(field, message string) error {
	return &ValidationError{Field: field, Message: message}
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
}

func (e *MaxRetriesError) Error() string {
	if e.LastErr != nil {
		return fmt.Sprintf("max retries exceeded after %d attempts: %v", e.Attempts, e.LastErr)
	}
	return fmt.Sprintf("max retries exceeded after %d attempts", e.Attempts)
}

func (e *MaxRetriesError) Unwrap() error {
	return ErrMaxRetriesExceeded
}

// NewMaxRetriesError creates a MaxRetriesError.
func NewMaxRetriesError(attempts int, lastErr error) error {
	return &MaxRetriesError{Attempts: attempts, LastErr: lastErr}
}

// StorageError wraps ErrStorageUnavailable with details.
type StorageError struct {
	Operation string // e.g., "save", "load", "delete"
	Cause     error
}

func (e *StorageError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("storage unavailable during %s: %v", e.Operation, e.Cause)
	}
	return fmt.Sprintf("storage unavailable during %s", e.Operation)
}

func (e *StorageError) Unwrap() error {
	return ErrStorageUnavailable
}

// NewStorageError creates a StorageError.
func NewStorageError(operation string, cause error) error {
	return &StorageError{Operation: operation, Cause: cause}
}

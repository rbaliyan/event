package store

import "errors"

// Common store errors.
var (
	// ErrNotFound is returned when an item is not found.
	ErrNotFound = errors.New("store: item not found")

	// ErrAlreadyExists is returned when creating an item with an existing ID.
	ErrAlreadyExists = errors.New("store: item already exists")

	// ErrInvalidID is returned when an ID is empty or invalid.
	ErrInvalidID = errors.New("store: invalid ID")

	// ErrStoreClosed is returned when operating on a closed store.
	ErrStoreClosed = errors.New("store: store is closed")
)

// IsNotFound returns true if the error indicates an item was not found.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsAlreadyExists returns true if the error indicates a duplicate item.
func IsAlreadyExists(err error) bool {
	return errors.Is(err, ErrAlreadyExists)
}

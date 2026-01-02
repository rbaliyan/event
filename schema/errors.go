package schema

import "errors"

var (
	// ErrEmptyName is returned when schema name is empty.
	ErrEmptyName = errors.New("schema name cannot be empty")

	// ErrInvalidVersion is returned when schema version is less than 1.
	ErrInvalidVersion = errors.New("schema version must be >= 1")

	// ErrVersionDowngrade is returned when trying to set a lower version.
	ErrVersionDowngrade = errors.New("cannot downgrade schema version")

	// ErrNotFound is returned when schema is not found.
	ErrNotFound = errors.New("schema not found")

	// ErrProviderClosed is returned when operating on a closed provider.
	ErrProviderClosed = errors.New("schema provider is closed")
)

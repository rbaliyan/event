// Package store provides common store interfaces and utilities for persistence.
//
// This package defines a core set of interfaces that can be implemented by
// different backends (PostgreSQL, Redis, in-memory, or MongoDB via event-mongodb)
// while allowing domain-specific extensions.
//
// # Architecture
//
// The store package uses a three-tier architecture:
//
//  1. Core Interfaces: Basic CRUD operations common to all stores
//  2. Domain Extensions: Specialized interfaces for specific use cases
//  3. Backend Implementations: PostgreSQL, Redis, memory stores (MongoDB via event-mongodb)
//
// # Usage
//
// Domain packages define their own store interfaces that embed CoreStore:
//
//	type SagaStore interface {
//	    store.CoreStore[*State]
//	    ListByStatus(ctx context.Context, statuses []Status) ([]*State, error)
//	}
//
// Backend implementations satisfy both the core and domain interfaces.
// For MongoDB implementations, use the event-mongodb module (https://github.com/rbaliyan/event-mongodb).
package store

import (
	"context"
	"time"
)

// Identifier is implemented by types that have a unique ID.
// All items stored via CoreStore must implement this interface.
type Identifier interface {
	// GetID returns the unique identifier for this item.
	GetID() string
}

// CoreStore defines the basic CRUD operations common to all stores.
// Domain-specific stores should embed this interface and add specialized methods.
//
// Type parameter T must implement Identifier to provide consistent ID handling.
type CoreStore[T Identifier] interface {
	// Create stores a new item. Returns ErrAlreadyExists if ID already exists.
	Create(ctx context.Context, item T) error

	// Get retrieves an item by ID. Returns ErrNotFound if not found.
	Get(ctx context.Context, id string) (T, error)

	// Update modifies an existing item. Returns ErrNotFound if not found.
	Update(ctx context.Context, item T) error

	// Delete removes an item by ID. Returns nil if not found (idempotent).
	Delete(ctx context.Context, id string) error

	// List retrieves items matching the filter with pagination.
	List(ctx context.Context, filter Filter) (*Page[T], error)

	// Count returns the number of items matching the filter.
	Count(ctx context.Context, filter Filter) (int64, error)
}

// CleanableStore extends CoreStore with cleanup operations.
// Stores that support TTL or age-based deletion should implement this.
type CleanableStore[T Identifier] interface {
	CoreStore[T]

	// DeleteOlderThan removes items older than the given age.
	// Returns the number of deleted items.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

// Filter specifies criteria for listing and counting items.
// Domain-specific stores may embed this and add additional fields.
type Filter struct {
	// Cursor is an opaque pagination token from a previous Page response.
	// Empty string starts from the beginning.
	Cursor string

	// Limit is the maximum number of items to return.
	// Zero means use the store's default limit.
	Limit int

	// OrderDesc reverses the default sort order.
	// Default is ascending by creation time.
	OrderDesc bool

	// StartTime filters items created at or after this time.
	// Zero value means no lower bound.
	StartTime time.Time

	// EndTime filters items created before this time.
	// Zero value means no upper bound.
	EndTime time.Time
}

// Page represents a paginated response.
type Page[T any] struct {
	// Items contains the results for this page.
	Items []T

	// NextCursor is the pagination token for the next page.
	// Empty string means no more pages.
	NextCursor string

	// Total is the total count of items matching the filter (optional).
	// Zero means the count was not computed.
	Total int64
}

// HasMore returns true if there are more pages available.
func (p *Page[T]) HasMore() bool {
	return p.NextCursor != ""
}

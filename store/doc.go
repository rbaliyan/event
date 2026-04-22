// Package store provides core store interfaces and implementations.
//
// This package defines the CoreStore interface that provides common CRUD
// operations for all store implementations. Domain-specific stores extend
// this interface with specialized methods.
//
// # Package Structure
//
// The store packages are organized as follows:
//
//	store/           - Core interfaces (CoreStore, Filter, Page) and implementations
//	store/base/      - Low-level utilities for building stores (cursor, SQL helpers)
//
// # Core Interfaces
//
// CoreStore[T] provides basic CRUD operations:
//   - Create, Get, Update, Delete
//   - List with pagination
//   - Count
//
// CleanableStore[T] extends CoreStore with TTL support:
//   - DeleteOlderThan
//
// # Implementations
//
// MemoryStore[T] - Thread-safe in-memory store for testing and development.
//
// For MongoDB store implementations, use the event-mongodb module:
// https://github.com/rbaliyan/event-mongodb
//
// # Usage
//
// Domain packages define their own store interfaces that embed CoreStore:
//
//	package saga
//
//	type Store interface {
//	    store.CoreStore[*State]
//
//	    // Domain-specific methods
//	    ListByStatus(ctx context.Context, statuses []Status) ([]*State, error)
//	    TransitionState(ctx context.Context, id string, from, to Status) error
//	}
//
// Implementations satisfy both core and domain interfaces. For example,
// using the MongoDB store from the event-mongodb module:
//
//	// See github.com/rbaliyan/event-mongodb/store for MongoDB implementation
//	type MyMongoStore struct {
//	    *mongostore.MongoStore[*State]
//	}
//
//	func (s *MyMongoStore) ListByStatus(ctx context.Context, statuses []Status) ([]*State, error) {
//	    return s.Collection().Find(ctx, bson.M{"status": bson.M{"$in": statuses}})
//	}
//
// # Pagination
//
// All stores use cursor-based pagination for consistent, stable results.
// The Cursor type encodes a (timestamp, id) pair that uniquely identifies
// a position in the result set.
//
// Example pagination:
//
//	filter := store.Filter{Limit: 10}
//	for {
//	    page, err := myStore.List(ctx, filter)
//	    if err != nil { return err }
//
//	    for _, item := range page.Items {
//	        process(item)
//	    }
//
//	    if !page.HasMore() {
//	        break
//	    }
//	    filter.Cursor = page.NextCursor
//	}
//
// # Error Handling
//
// Common errors are defined in errors.go:
//   - ErrNotFound: item does not exist
//   - ErrAlreadyExists: duplicate ID on create
//   - ErrInvalidID: empty or invalid ID
//
// Use IsNotFound() and IsAlreadyExists() helper functions:
//
//	item, err := myStore.Get(ctx, id)
//	if store.IsNotFound(err) {
//	    // Handle missing item
//	}
//
// # Relationship with store/base
//
// The store/base package provides lower-level utilities for building
// custom stores:
//   - Generic cursor encoding/decoding for any cursor type
//   - SQL query builders for PostgreSQL stores
//   - Cleanup manager for background cleanup goroutines
//
// Use store/base when you need custom cursor types or SQL-specific helpers.
// Use store when you want pre-built interfaces and standard implementations.
package store

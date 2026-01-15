// Package base provides shared utilities for store implementations.
//
// This package contains common patterns used across PostgreSQL, MongoDB,
// and other store implementations to reduce code duplication.
//
// Key components:
//
//   - Cursor encoding/decoding: Generic base64+JSON cursor pagination
//   - QueryBuilder: Dynamic SQL query construction with parameter numbering
//   - CleanupManager: Background cleanup goroutines with graceful shutdown
//   - Metadata helpers: JSON marshaling for metadata maps
//   - Null helpers: SQL null type conversion utilities
//
// Example - Cursor-based pagination:
//
//	type cursor struct {
//	    LastID    string    `json:"id"`
//	    CreatedAt time.Time `json:"ts"`
//	}
//
//	// Encode cursor for client
//	encoded := base.EncodeCursor(cursor{LastID: "123", CreatedAt: time.Now()})
//
//	// Decode cursor from client
//	decoded, err := base.DecodeCursor[cursor](encoded)
//
//	// Paginate results (using limit+1 pattern)
//	result := base.Paginate(items, limit, func(item Item) cursor {
//	    return cursor{LastID: item.ID, CreatedAt: item.CreatedAt}
//	})
//
// Example - QueryBuilder:
//
//	qb := base.NewQueryBuilder()
//	qb.AddIfNotEmpty("name = $%d", filter.Name)
//	qb.AddIfNotZero("created_at >= $%d", filter.StartTime)
//	qb.AddIn("status", filter.Statuses)
//
//	query, args := qb.Build("SELECT * FROM users %s ORDER BY id")
package base

import (
	"encoding/base64"
	"encoding/json"
)

// EncodeCursor encodes any cursor struct to a base64 string.
// The cursor struct should be JSON-serializable.
func EncodeCursor[T any](c T) string {
	data, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeCursor decodes a base64 cursor string into the provided type.
// Returns the zero value and nil error for empty strings.
func DecodeCursor[T any](str string) (T, error) {
	var c T
	if str == "" {
		return c, nil
	}
	data, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(data, &c)
	return c, err
}

// PageResult represents a paginated result with cursor-based navigation.
type PageResult[T any] struct {
	Items      []T    // The items for this page
	NextCursor string // Cursor for the next page, empty if no more pages
	HasMore    bool   // Whether there are more pages
}

// Paginate processes a slice that was fetched with limit+1 to determine
// if there are more pages and creates the appropriate PageResult.
// The cursorFn extracts cursor data from the last item if there are more pages.
func Paginate[T any, C any](items []T, limit int, cursorFn func(T) C) PageResult[T] {
	hasMore := len(items) > limit
	if hasMore {
		items = items[:limit]
	}

	var nextCursor string
	if hasMore && len(items) > 0 {
		lastItem := items[len(items)-1]
		nextCursor = EncodeCursor(cursorFn(lastItem))
	}

	return PageResult[T]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}
}

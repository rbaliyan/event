package monitor

import (
	"context"
	"time"
)

// Store defines the interface for monitor storage.
// Implementations must be safe for concurrent use.
//
// The store uses a composite key that behaves differently based on delivery mode:
//   - Broadcast: Primary key = (event_id, subscription_id)
//   - WorkerPool: Primary key = (event_id), subscription_id stored but not key
type Store interface {
	// Record creates or updates a monitor entry.
	// Key behavior depends on entry.DeliveryMode:
	//   - Broadcast: key is (EventID, SubscriptionID)
	//   - WorkerPool: key is EventID only
	Record(ctx context.Context, entry *Entry) error

	// Get retrieves a monitor entry by its composite key.
	// For WorkerPool mode, subscriptionID can be empty.
	Get(ctx context.Context, eventID, subscriptionID string) (*Entry, error)

	// GetByEventID returns all entries for an event ID.
	// For Broadcast: returns N entries (one per subscriber)
	// For WorkerPool: returns 1 entry
	GetByEventID(ctx context.Context, eventID string) ([]*Entry, error)

	// List returns a page of entries matching the filter.
	// Uses cursor-based pagination for efficient large dataset traversal.
	List(ctx context.Context, filter Filter) (*Page, error)

	// Count returns the number of entries matching the filter.
	Count(ctx context.Context, filter Filter) (int64, error)

	// UpdateStatus updates the status and related fields of an existing entry.
	// For WorkerPool mode, subscriptionID can be empty.
	UpdateStatus(ctx context.Context, eventID, subscriptionID string, status Status, err error, duration time.Duration) error

	// DeleteOlderThan removes entries older than the specified age.
	// Returns the number of entries deleted.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)
}

// Filter specifies criteria for listing monitor entries.
// All fields are optional. Empty filter returns all entries.
type Filter struct {
	// Identity filters
	EventID        string // Exact match on event ID
	SubscriptionID string // Exact match on subscription ID (only meaningful for Broadcast)
	EventName      string // Filter by event name
	BusID          string // Filter by bus ID

	// Mode filter
	DeliveryMode *DeliveryMode // Filter by delivery mode (nil = all modes)

	// Status filters
	Status   []Status // Filter by status (empty = all statuses)
	HasError *bool    // Filter by error presence (nil = ignore, true = has error, false = no error)

	// Time filters
	StartTime time.Time // Entries started after this time (inclusive)
	EndTime   time.Time // Entries started before this time (exclusive)

	// Performance filters
	MinDuration time.Duration // Entries with duration >= this value

	// Retry filters
	MinRetries int // Entries with retry count >= this value

	// Cursor-based pagination
	Cursor    string // Opaque cursor from previous page (empty for first page)
	Limit     int    // Max results per page (0 = default limit)
	OrderDesc bool   // Order by started_at descending (default: ascending)
}

// Page represents a page of monitor entries with cursor-based pagination.
type Page struct {
	// Entries contains the monitor entries for this page.
	Entries []*Entry `json:"entries"`

	// NextCursor is the opaque cursor for the next page.
	// Empty if there are no more pages.
	NextCursor string `json:"next_cursor,omitempty"`

	// HasMore indicates whether there are more pages available.
	HasMore bool `json:"has_more"`
}

// DefaultLimit is the default page size when Limit is 0.
const DefaultLimit = 100

// MaxLimit is the maximum allowed page size.
const MaxLimit = 1000

// EffectiveLimit returns the effective limit, applying defaults and bounds.
func (f *Filter) EffectiveLimit() int {
	if f.Limit <= 0 {
		return DefaultLimit
	}
	if f.Limit > MaxLimit {
		return MaxLimit
	}
	return f.Limit
}

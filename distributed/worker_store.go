package distributed

import (
	"context"
	"time"
)

// WorkerState represents the state of a worker entry.
type WorkerState string

const (
	WorkerStateProcessing WorkerState = "processing"
	WorkerStateCompleted  WorkerState = "completed"
	WorkerStateReleased   WorkerState = "released"
)

// WorkerEntry represents a worker state entry for API responses.
type WorkerEntry struct {
	MessageID string            `json:"message_id"`
	Status    WorkerState       `json:"status"`
	WorkerID  string            `json:"worker_id,omitempty"`
	EventName string            `json:"event_name,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// WorkerFilter specifies criteria for querying worker entries.
type WorkerFilter struct {
	Status        []WorkerState
	EventName     string
	WorkerID      string
	StaleTimeout  time.Duration
	CreatedAfter  time.Time
	CreatedBefore time.Time
	Cursor        string
	Limit         int
	OrderDesc     bool
}

// EffectiveLimit returns the limit to use for queries, defaulting to 50.
func (f WorkerFilter) EffectiveLimit() int {
	if f.Limit <= 0 || f.Limit > 1000 {
		return 50
	}
	return f.Limit
}

// WorkerPage represents a paginated list of worker entries.
type WorkerPage struct {
	Entries    []*WorkerEntry `json:"entries"`
	NextCursor string         `json:"next_cursor,omitempty"`
	HasMore    bool           `json:"has_more"`
}

// WorkerStore provides read-only access to worker pool state for observability.
type WorkerStore interface {
	ListWorkers(ctx context.Context, filter WorkerFilter) (*WorkerPage, error)
	CountWorkers(ctx context.Context, filter WorkerFilter) (int64, error)
	GetWorker(ctx context.Context, messageID string) (*WorkerEntry, error)
}

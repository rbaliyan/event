package http

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/monitor"
)

// DLQProvider supplies DLQ summary and health.
// Satisfied by event-dlq Manager.
type DLQProvider interface {
	DLQStats(ctx context.Context) (*DLQStats, error)
	health.Checker
}

// DLQStats contains summary statistics for the dead letter queue.
type DLQStats struct {
	TotalMessages   int64            `json:"total_messages"`
	PendingMessages int64            `json:"pending_messages"`
	MessagesByEvent map[string]int64 `json:"messages_by_event,omitempty"`
	MessagesByError map[string]int64 `json:"messages_by_error,omitempty"`
	OldestMessage   *time.Time       `json:"oldest_message,omitempty"`
	NewestMessage   *time.Time       `json:"newest_message,omitempty"`
}

// SchedulerProvider supplies scheduler summary and health.
// Satisfied by scheduler implementations.
type SchedulerProvider interface {
	SchedulerStats(ctx context.Context) (*SchedulerStats, error)
	health.Checker
}

// SchedulerStats contains summary statistics for the scheduler.
type SchedulerStats struct {
	PendingMessages int64      `json:"pending_messages"`
	StuckMessages   int64      `json:"stuck_messages,omitempty"`
	NextDue         *time.Time `json:"next_due,omitempty"`
}

// StuckPendingStats contains statistics about monitor entries stuck in pending state.
// A stuck entry has status=pending and started_at older than the configured threshold,
// indicating the pod that claimed the message likely crashed before completing.
type StuckPendingStats struct {
	Count     int64            `json:"count"`
	Threshold time.Duration    `json:"threshold"`
	OldestAt  *time.Time       `json:"oldest_at,omitempty"`
	Samples   []*monitor.Entry `json:"samples,omitempty"`
}

// StuckPendingProvider supplies stuck-pending detection for /v1/system.
// Implementations should cache results externally; this is called on every
// system view refresh (typically every 10–30 seconds).
type StuckPendingProvider interface {
	StuckPendingStats(ctx context.Context) (*StuckPendingStats, error)
}

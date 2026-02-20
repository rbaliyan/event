package http

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/health"
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

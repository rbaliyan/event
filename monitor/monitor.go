// Package monitor provides event processing monitoring and observability.
//
// The monitor package tracks event processing with mode-aware tracking granularity:
//   - Broadcast (Pub/Sub): Track per (EventID, SubscriptionID) - each subscriber's processing is separate
//   - WorkerPool (Queue): Track per EventID only - one worker processes each event
//
// Example usage:
//
//	// Create a PostgreSQL monitor store
//	store := monitor.NewPostgresStore(db)
//	defer store.Close()
//
//	// Add monitor middleware to subscription
//	orderEvent.Subscribe(ctx, handler,
//	    event.AsWorker[Order](),
//	    event.WithMiddleware(monitor.Middleware[Order](store)),
//	)
//
//	// Query monitor entries
//	page, err := store.List(ctx, monitor.Filter{
//	    Status:    []monitor.Status{monitor.StatusFailed},
//	    StartTime: time.Now().Add(-time.Hour),
//	    Limit:     100,
//	})
package monitor

import (
	"time"
)

// DeliveryMode determines how messages are distributed to subscribers.
// This mirrors transport.DeliveryMode for monitoring purposes.
type DeliveryMode int

const (
	// Broadcast delivers message to ALL subscribers (pub/sub fan-out).
	// Each subscriber's processing is tracked separately with key (EventID, SubscriptionID).
	Broadcast DeliveryMode = iota

	// WorkerPool delivers message to ONE subscriber (load balancing across workers).
	// Processing is tracked by EventID only since only one worker processes each message.
	WorkerPool
)

// String returns the string representation of the delivery mode.
func (m DeliveryMode) String() string {
	switch m {
	case Broadcast:
		return "broadcast"
	case WorkerPool:
		return "worker_pool"
	default:
		return "unknown"
	}
}

// ParseDeliveryMode parses a string into a DeliveryMode.
// Returns Broadcast for unknown values.
func ParseDeliveryMode(s string) DeliveryMode {
	switch s {
	case "worker_pool":
		return WorkerPool
	default:
		return Broadcast
	}
}

// Status represents the processing status of a monitor entry.
type Status string

const (
	// StatusPending indicates the handler has started but not completed.
	StatusPending Status = "pending"

	// StatusCompleted indicates the handler succeeded.
	StatusCompleted Status = "completed"

	// StatusFailed indicates the handler returned an error that caused rejection.
	// The message was sent to DLQ (if configured) and will not be retried.
	StatusFailed Status = "failed"

	// StatusRetrying indicates the handler returned an error that will be retried.
	// This includes both immediate retries (nack) and backoff retries (defer).
	StatusRetrying Status = "retrying"
)

// Entry represents a single monitor record for event processing.
//
// For Broadcast mode, each subscriber gets a separate Entry with the same EventID
// but different SubscriptionID. For WorkerPool mode, there is only one Entry per
// EventID since only one worker processes each message.
type Entry struct {
	// Primary identifiers - key depends on DeliveryMode
	// For Broadcast: (EventID, SubscriptionID) is the unique key
	// For WorkerPool: EventID is the unique key
	EventID        string `json:"event_id"`
	SubscriptionID string `json:"subscription_id"`

	// Event context
	EventName    string            `json:"event_name"`
	BusID        string            `json:"bus_id"`
	DeliveryMode DeliveryMode      `json:"delivery_mode"`
	Metadata     map[string]string `json:"metadata,omitempty"`

	// Processing status
	Status     Status `json:"status"`
	Error      string `json:"error,omitempty"`
	RetryCount int    `json:"retry_count"`

	// Timing
	StartedAt   time.Time     `json:"started_at"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	Duration    time.Duration `json:"duration,omitempty"`

	// Tracing correlation (OpenTelemetry)
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
}

// IsComplete returns true if the entry represents a completed processing attempt.
func (e *Entry) IsComplete() bool {
	return e.Status == StatusCompleted || e.Status == StatusFailed
}

// HasError returns true if the entry has an error recorded.
func (e *Entry) HasError() bool {
	return e.Error != ""
}

package event

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/scheduler"
)

// EventScheduler provides typed event scheduling for future delivery.
//
// EventScheduler wraps a low-level scheduler (Redis, MongoDB, PostgreSQL) and
// provides type-safe methods for scheduling events. When the scheduled time
// arrives, the event is automatically published to the event bus.
//
// Example:
//
//	// Create scheduler with Redis backend
//	redisScheduler := scheduler.NewRedisScheduler(redisClient, bus.Transport())
//	eventScheduler := event.NewEventScheduler(redisScheduler)
//
//	// Start the scheduler (runs in background)
//	go eventScheduler.Start(ctx)
//
//	// Schedule a typed event
//	id, err := event.ScheduleAt(eventScheduler, orderEvent, order, time.Now().Add(time.Hour))
//
//	// Or schedule after a delay
//	id, err := event.ScheduleAfter(eventScheduler, orderEvent, order, 30*time.Minute)
type EventScheduler struct {
	scheduler scheduler.Scheduler
}

// NewEventScheduler creates a new typed event scheduler.
//
// Parameters:
//   - s: The underlying scheduler implementation (Redis, MongoDB, or PostgreSQL)
//
// Example:
//
//	// Using Redis scheduler
//	redisScheduler := scheduler.NewRedisScheduler(redisClient, bus.Transport())
//	eventScheduler := event.NewEventScheduler(redisScheduler)
//
//	// Using MongoDB scheduler
//	mongoScheduler := scheduler.NewMongoScheduler(db, bus.Transport())
//	eventScheduler := event.NewEventScheduler(mongoScheduler)
func NewEventScheduler(s scheduler.Scheduler) *EventScheduler {
	return &EventScheduler{
		scheduler: s,
	}
}

// Scheduler returns the underlying scheduler for advanced operations.
func (es *EventScheduler) Scheduler() scheduler.Scheduler {
	return es.scheduler
}

// Start begins the scheduler polling loop.
// This method blocks until the context is cancelled or Stop is called.
func (es *EventScheduler) Start(ctx context.Context) error {
	return es.scheduler.Start(ctx)
}

// Stop gracefully stops the scheduler.
func (es *EventScheduler) Stop(ctx context.Context) error {
	return es.scheduler.Stop(ctx)
}

// Cancel cancels a scheduled event by its ID.
func (es *EventScheduler) Cancel(ctx context.Context, id string) error {
	return es.scheduler.Cancel(ctx, id)
}

// Get retrieves a scheduled message by ID.
func (es *EventScheduler) Get(ctx context.Context, id string) (*scheduler.Message, error) {
	return es.scheduler.Get(ctx, id)
}

// List returns scheduled messages matching the filter.
func (es *EventScheduler) List(ctx context.Context, filter scheduler.Filter) ([]*scheduler.Message, error) {
	return es.scheduler.List(ctx, filter)
}

// ScheduleAt schedules a typed event for delivery at a specific time.
//
// The event data is JSON-encoded and stored until the scheduled time,
// then automatically published to the event's topic.
//
// Parameters:
//   - ctx: Context for the operation
//   - es: The event scheduler
//   - ev: The event to schedule (must be bound to a bus)
//   - data: The typed payload
//   - at: When to deliver the event
//   - metadata: Optional metadata (can be nil)
//
// Returns the scheduled message ID for cancellation.
//
// Example:
//
//	// Schedule order reminder for tomorrow
//	id, err := event.ScheduleAt(scheduler, orderEvent, order, time.Now().Add(24*time.Hour), nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Cancel if needed
//	scheduler.Cancel(ctx, id)
func ScheduleAt[T any](ctx context.Context, es *EventScheduler, ev Event[T], data T, at time.Time, metadata map[string]string) (string, error) {
	// Encode payload as JSON
	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("encode payload: %w", err)
	}

	// Get event name
	eventName := ev.Name()

	return es.scheduler.ScheduleAt(ctx, eventName, payload, metadata, at)
}

// ScheduleAfter schedules a typed event for delivery after a delay.
//
// This is a convenience wrapper around ScheduleAt that calculates
// the delivery time based on the current time plus the delay.
//
// Parameters:
//   - ctx: Context for the operation
//   - es: The event scheduler
//   - ev: The event to schedule (must be bound to a bus)
//   - data: The typed payload
//   - delay: How long to wait before delivery
//   - metadata: Optional metadata (can be nil)
//
// Returns the scheduled message ID for cancellation.
//
// Example:
//
//	// Send reminder in 30 minutes
//	id, err := event.ScheduleAfter(scheduler, reminderEvent, reminder, 30*time.Minute, nil)
func ScheduleAfter[T any](ctx context.Context, es *EventScheduler, ev Event[T], data T, delay time.Duration, metadata map[string]string) (string, error) {
	return ScheduleAt(ctx, es, ev, data, time.Now().Add(delay), metadata)
}

// ScheduleAtWithID schedules a typed event with a custom ID.
//
// Use this when you need a specific ID for the scheduled message,
// for example to correlate with business entities or for idempotency.
//
// Parameters:
//   - ctx: Context for the operation
//   - es: The event scheduler
//   - ev: The event to schedule
//   - id: Custom message ID
//   - data: The typed payload
//   - at: When to deliver the event
//   - metadata: Optional metadata (can be nil)
//
// Example:
//
//	// Schedule with order ID for easy cancellation
//	err := event.ScheduleAtWithID(scheduler, reminderEvent, order.ID, reminder, deliveryTime, nil)
//
//	// Later, cancel by order ID
//	scheduler.Cancel(ctx, order.ID)
func ScheduleAtWithID[T any](ctx context.Context, es *EventScheduler, ev Event[T], id string, data T, at time.Time, metadata map[string]string) error {
	// Encode payload as JSON
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := scheduler.Message{
		ID:          id,
		EventName:   ev.Name(),
		Payload:     payload,
		Metadata:    metadata,
		ScheduledAt: at,
		CreatedAt:   time.Now(),
	}

	return es.scheduler.Schedule(ctx, msg)
}

// ScheduleAfterWithID schedules a typed event with a custom ID after a delay.
//
// Example:
//
//	// Schedule reminder with order ID
//	err := event.ScheduleAfterWithID(scheduler, reminderEvent, order.ID, reminder, time.Hour, nil)
func ScheduleAfterWithID[T any](ctx context.Context, es *EventScheduler, ev Event[T], id string, data T, delay time.Duration, metadata map[string]string) error {
	return ScheduleAtWithID(ctx, es, ev, id, data, time.Now().Add(delay), metadata)
}

// ScheduledEventInfo contains information about a scheduled event.
type ScheduledEventInfo struct {
	// ID is the unique identifier for the scheduled message
	ID string

	// EventName is the event topic
	EventName string

	// ScheduledAt is when the event will be delivered
	ScheduledAt time.Time

	// CreatedAt is when the event was scheduled
	CreatedAt time.Time

	// Metadata contains additional key-value pairs
	Metadata map[string]string
}

// ListScheduled returns scheduled events for a specific event type.
//
// Example:
//
//	// List all scheduled order reminders
//	scheduled, err := event.ListScheduled(scheduler, orderReminderEvent, scheduler.Filter{
//	    Before: time.Now().Add(24 * time.Hour),
//	    Limit:  100,
//	})
func ListScheduled[T any](ctx context.Context, es *EventScheduler, ev Event[T], filter scheduler.Filter) ([]ScheduledEventInfo, error) {
	// Set event name filter
	filter.EventName = ev.Name()

	messages, err := es.scheduler.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	result := make([]ScheduledEventInfo, len(messages))
	for i, msg := range messages {
		result[i] = ScheduledEventInfo{
			ID:          msg.ID,
			EventName:   msg.EventName,
			ScheduledAt: msg.ScheduledAt,
			CreatedAt:   msg.CreatedAt,
			Metadata:    msg.Metadata,
		}
	}

	return result, nil
}

// GetScheduledPayload retrieves and decodes a scheduled event's payload.
//
// Example:
//
//	order, err := event.GetScheduledPayload[Order](scheduler, messageID)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println("Scheduled order:", order.ID)
func GetScheduledPayload[T any](ctx context.Context, es *EventScheduler, id string) (T, error) {
	var zero T

	msg, err := es.scheduler.Get(ctx, id)
	if err != nil {
		return zero, err
	}

	var data T
	if err := json.Unmarshal(msg.Payload, &data); err != nil {
		return zero, fmt.Errorf("decode payload: %w", err)
	}

	return data, nil
}

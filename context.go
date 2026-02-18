package event

import (
	"context"
	"log/slog"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	eventcontextKey contextKey = iota
)

type eventContextData struct {
	name                  string
	source                string
	eventID               string
	subID                 string
	metadata              map[string]string
	rawPayload            []byte
	messageTime           time.Time
	logger                *slog.Logger
	bus                   *Bus
	deliveryMode          DeliveryMode
	subscriberName        string
	subscriberDescription string
	coalescedCount        int
}

// contextKey
type contextKey int

// ContextEventID get event id stored in context
func ContextEventID(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.eventID
	}
	return ""
}

// ContextName get event name stored in context
func ContextName(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.name
	}
	return ""
}

// ContextSource get event source stored in context
func ContextSource(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.source
	}
	return ""
}

// ContextMetadata get event metadata stored in context.
// Returns a copy of the metadata map to prevent mutation of internal state.
func ContextMetadata(ctx context.Context) map[string]string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if !ok || s.metadata == nil {
		return nil
	}
	copied := make(map[string]string, len(s.metadata))
	for k, v := range s.metadata {
		copied[k] = v
	}
	return copied
}

// ContextLogger get event Logger stored in context
func ContextLogger(ctx context.Context) *slog.Logger {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.logger
	}
	return nil
}

// ContextBus get event bus stored in context
func ContextBus(ctx context.Context) *Bus {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.bus
	}
	return nil
}

// ContextSubscriptionID get event subscriber id stored in context
func ContextSubscriptionID(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.subID
	}
	return ""
}

// ContextDeliveryMode get delivery mode stored in context.
// Returns Broadcast (0) if not set.
func ContextDeliveryMode(ctx context.Context) DeliveryMode {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.deliveryMode
	}
	return Broadcast
}

// ContextMessageTime get message timestamp stored in context.
// Returns zero time if not set.
func ContextMessageTime(ctx context.Context) time.Time {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.messageTime
	}
	return time.Time{}
}

// ContextSubscriberName returns the subscriber name stored in context.
// Returns empty string if not set.
func ContextSubscriberName(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.subscriberName
	}
	return ""
}

// ContextSubscriberDescription returns the subscriber description stored in context.
// Returns empty string if not set.
func ContextSubscriberDescription(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.subscriberDescription
	}
	return ""
}

// ContextCoalescedCount returns the number of messages that were superseded
// by coalescing before this message was delivered.
// Returns 0 if no coalescing occurred or the subscriber does not use coalescing.
func ContextCoalescedCount(ctx context.Context) int {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.coalescedCount
	}
	return 0
}

// ContextWithMetadata generate a context with event metadata
func ContextWithMetadata(ctx context.Context, m map[string]string) context.Context {
	if m == nil {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		newData := *s
		newData.metadata = m
		return context.WithValue(ctx, eventcontextKey, &newData)
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{metadata: m})
}

// ContextWithEventID generate a context with event id
func ContextWithEventID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		newData := *s
		newData.eventID = id
		return context.WithValue(ctx, eventcontextKey, &newData)
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{eventID: id})
}

// ContextWithLogger generate a context with event logger
func ContextWithLogger(ctx context.Context, l *slog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		newData := *s
		newData.logger = l
		return context.WithValue(ctx, eventcontextKey, &newData)
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{logger: l})
}

// contextInfo groups parameters for contextWithInfo to avoid long parameter lists.
type contextInfo struct {
	id                    string
	name                  string
	source                string
	subID                 string
	metadata              map[string]string
	msgTime               time.Time
	logger                *slog.Logger
	bus                   *Bus
	mode                  DeliveryMode
	subscriberName        string
	subscriberDescription string
	coalescedCount        int
}

func contextWithInfo(ctx context.Context, info contextInfo) context.Context {
	return context.WithValue(ctx, eventcontextKey, &eventContextData{
		eventID:               info.id,
		name:                  info.name,
		subID:                 info.subID,
		source:                info.source,
		metadata:              info.metadata,
		messageTime:           info.msgTime,
		logger:                info.logger,
		bus:                   info.bus,
		deliveryMode:          info.mode,
		subscriberName:        info.subscriberName,
		subscriberDescription: info.subscriberDescription,
		coalescedCount:        info.coalescedCount,
	})
}

// ContextWithRawPayload sets the raw message payload bytes in the event context data.
func ContextWithRawPayload(ctx context.Context, payload []byte) context.Context {
	if len(payload) == 0 {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		newData := *s
		newData.rawPayload = payload
		return context.WithValue(ctx, eventcontextKey, &newData)
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{rawPayload: payload})
}

// ContextRawPayload returns the raw message payload bytes from the context.
// Returns nil if not set.
func ContextRawPayload(ctx context.Context) []byte {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.rawPayload
	}
	return nil
}

// ContextWithEventFromContext copies event context baggage (event ID, name, metadata,
// raw payload, etc.) from one context to another.
func ContextWithEventFromContext(to, from context.Context) context.Context {
	s, ok := from.Value(eventcontextKey).(*eventContextData)
	if ok {
		return context.WithValue(to, eventcontextKey, s)
	}
	return to
}

// NewContext copy context data to a new context
func NewContext(ctx context.Context) context.Context {
	return ContextWithEventFromContext(context.Background(), ctx)
}

// detachedContext returns a context.Background() with OpenTelemetry trace context
// preserved from the original context. Use this for DLQ handlers where the message
// context may be cancelled but trace correlation should be retained.
func detachedContext(ctx context.Context) context.Context {
	bg := context.Background()
	spanCtx := oteltrace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		bg = oteltrace.ContextWithSpanContext(bg, spanCtx)
	}
	return bg
}

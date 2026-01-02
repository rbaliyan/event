package event

import (
	"context"
	"log/slog"
	"time"
)

const (
	eventcontextKey contextKey = iota
)

type eventContextData struct {
	name         string
	source       string
	eventID      string
	subID        string
	metadata     map[string]string
	messageTime  time.Time
	logger       *slog.Logger
	bus          *Bus
	deliveryMode DeliveryMode
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

// ContextMetadata get event metadata stored in context
func ContextMetadata(ctx context.Context) map[string]string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.metadata
	}
	return nil
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

// ContextWithMetadata generate a context with event metadata
func ContextWithMetadata(ctx context.Context, m map[string]string) context.Context {
	if m == nil {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		// Create a new struct to avoid race conditions
		newData := &eventContextData{
			name:         s.name,
			source:       s.source,
			eventID:      s.eventID,
			subID:        s.subID,
			metadata:     m,
			messageTime:  s.messageTime,
			logger:       s.logger,
			bus:          s.bus,
			deliveryMode: s.deliveryMode,
		}
		return context.WithValue(ctx, eventcontextKey, newData)
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
		// Create a new struct to avoid race conditions
		newData := &eventContextData{
			name:         s.name,
			source:       s.source,
			eventID:      id,
			subID:        s.subID,
			metadata:     s.metadata,
			messageTime:  s.messageTime,
			logger:       s.logger,
			bus:          s.bus,
			deliveryMode: s.deliveryMode,
		}
		return context.WithValue(ctx, eventcontextKey, newData)
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
		// Create a new struct to avoid race conditions
		newData := &eventContextData{
			name:         s.name,
			source:       s.source,
			eventID:      s.eventID,
			subID:        s.subID,
			metadata:     s.metadata,
			messageTime:  s.messageTime,
			logger:       l,
			bus:          s.bus,
			deliveryMode: s.deliveryMode,
		}
		return context.WithValue(ctx, eventcontextKey, newData)
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{logger: l})
}

func contextWithInfo(ctx context.Context, id, name, source, subID string, metadata map[string]string, msgTime time.Time, l *slog.Logger, b *Bus, mode DeliveryMode) context.Context {
	return context.WithValue(ctx, eventcontextKey, &eventContextData{
		eventID:      id,
		name:         name,
		subID:        subID,
		source:       source,
		metadata:     metadata,
		messageTime:  msgTime,
		logger:       l,
		bus:          b,
		deliveryMode: mode,
	})
}

// ContextWithEventFromContext copy context baggage
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

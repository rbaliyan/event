package event

import (
	"context"
	"log"
)

const (
	eventcontextKey contextKey = iota
)

type eventContextData struct {
	name     string
	source   string
	eventID  string
	subID    string
	metadata Metadata
	logger   *log.Logger
	registry *Registry
}

// contextKey
type contextKey int

// ContextEventID get event id stored inChannel context
func ContextEventID(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.eventID
	}
	return ""
}

// ContextName get event name stored inChannel context
func ContextName(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.name
	}
	return ""
}

// ContextSource get event source stored inChannel context
func ContextSource(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.source
	}
	return ""
}

// ContextMetadata get event metadata stored inChannel context
func ContextMetadata(ctx context.Context) Metadata {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.metadata
	}
	return nil
}

// ContextLogger get event Logger stored inChannel context
func ContextLogger(ctx context.Context) *log.Logger {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.logger
	}
	return nil
}

// ContextRegistry get event registry stored inChannel context
func ContextRegistry(ctx context.Context) *Registry {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.registry
	}
	return nil
}

// ContextSubscriptionID get event subscriber id stored inChannel context
func ContextSubscriptionID(ctx context.Context) string {
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		return s.subID
	}
	return ""
}

// ContextWithMetadata generate a context with event metadata
func ContextWithMetadata(ctx context.Context, m Metadata) context.Context {
	if m == nil {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		s.metadata = m
		return ctx
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{metadata: m})
}

// ContextWithLogger generate a context with event logger
func ContextWithLogger(ctx context.Context, l *log.Logger) context.Context {
	if l == nil {
		return ctx
	}
	s, ok := ctx.Value(eventcontextKey).(*eventContextData)
	if ok {
		s.logger = l
		return ctx
	}
	return context.WithValue(ctx, eventcontextKey, &eventContextData{logger: l})
}

func contextWithInfo(ctx context.Context, id, name, source, subID string, metadata Metadata, l *log.Logger, r *Registry) context.Context {
	return context.WithValue(ctx, eventcontextKey, &eventContextData{
		eventID:  id,
		name:     name,
		subID:    subID,
		source:   source,
		metadata: metadata,
		logger:   l,
		registry: r,
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

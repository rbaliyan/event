package event

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
)

var (
	eventNamecontextKey      contextKey = "event.name"
	eventIDcontextKey        contextKey = "event.id"
	subscriptionIDcontextKey contextKey = "event.subscription.id"
	sourcecontextKey         contextKey = "event.source"
)

// contextKey
type contextKey string

// EventIDFromContext get event id stored in context
func EventIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(eventIDcontextKey).(string)
	return s
}

// EventNameFromContext get event id stored in context
func EventNameFromContext(ctx context.Context) string {
	s, _ := ctx.Value(eventNamecontextKey).(string)
	return s
}

// SourceFromContext get event id stored in context
func SourceFromContext(ctx context.Context) string {
	s, _ := ctx.Value(sourcecontextKey).(string)
	return s
}

// SenderFromContext get event id stored in context
func SubscriptionIDFromContext(ctx context.Context) string {
	s, _ := ctx.Value(subscriptionIDcontextKey).(string)
	return s
}

// WithEventName generate a context with event id
func WithEventName(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, eventNamecontextKey, id)
}

// WithEventID generate a context with event id
func WithEventID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, eventIDcontextKey, id)
}

// WithSource generate a context with event id
func WithSource(ctx context.Context, s string) context.Context {
	return context.WithValue(ctx, sourcecontextKey, s)
}

// WithSender generate a context with event id
func WithSubscriptionID(ctx context.Context, subID string) context.Context {
	return context.WithValue(ctx, subscriptionIDcontextKey, subID)
}

// ContextWithBaggageFromContext copy context baggage
func ContextWithBaggageFromContext(to, from context.Context) context.Context {
	return baggage.ContextWithBaggage(to, baggage.FromContext(from))
}

// ContextWithEventFromContext copy context baggage
func ContextWithEventFromContext(to, from context.Context) context.Context {
	return WithSubscriptionID(
		WithSource(
			WithEventID(
				WithEventName(to, EventNameFromContext(from)),
				EventIDFromContext(from)),
			SourceFromContext(from)),
		SubscriptionIDFromContext(from))
}

// NewContext copy context data to a new context
func NewContext(ctx context.Context) context.Context {
	return ContextWithEventFromContext(context.Background(), ctx)
}

// AttributesFromBaggage get attribute values from baggage
func AttributesFromBaggage(bag baggage.Baggage) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for _, m := range bag.Members() {
		// Add member properties
		for _, p := range m.Properties() {
			if val, ok := p.Value(); ok {
				attrs = append(attrs, attribute.String(fmt.Sprintf("%s.%s", m.Key(), p.Key()), val))
			}
		}
		// Add key value
		attrs = append(attrs, attribute.String(m.Key(), m.Value()))
	}
	return attrs
}

// AttributesFromContext get attributes stored in context baggage
func AttributesFromContext(ctx context.Context) []attribute.KeyValue {
	return AttributesFromBaggage(baggage.FromContext(ctx))
}

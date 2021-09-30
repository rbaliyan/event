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
	metadatacontextKey       contextKey = "event.metadata"
)

// contextKey
type contextKey string

// ContextEventID get event id stored in context
func ContextEventID(ctx context.Context) string {
	s, _ := ctx.Value(eventIDcontextKey).(string)
	return s
}

// ContextName get event id stored in context
func ContextName(ctx context.Context) string {
	s, _ := ctx.Value(eventNamecontextKey).(string)
	return s
}

// ContextSource get event id stored in context
func ContextSource(ctx context.Context) string {
	s, _ := ctx.Value(sourcecontextKey).(string)
	return s
}

// ContextMetadata get event id stored in context
func ContextMetadata(ctx context.Context) Metadata {
	s, _ := ctx.Value(metadatacontextKey).(Metadata)
	return s
}

// SenderFromContext get event id stored in context
func ContextSubscriptionID(ctx context.Context) string {
	s, _ := ctx.Value(subscriptionIDcontextKey).(string)
	return s
}

// ContextWithName generate a context with event id
func ContextWithName(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, eventNamecontextKey, id)
}

// ContextWithEventID generate a context with event id
func ContextWithEventID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, eventIDcontextKey, id)
}

// ContextWithSource generate a context with event id
func ContextWithSource(ctx context.Context, s string) context.Context {
	return context.WithValue(ctx, sourcecontextKey, s)
}

// ContextWithMetadata generate a context with event id
func ContextWithMetadata(ctx context.Context, m Metadata) context.Context {
	if m == nil {
		return ctx
	}
	return context.WithValue(ctx, metadatacontextKey, m)
}

// ContextWithSubscriptionID generate a context with event id
func ContextWithSubscriptionID(ctx context.Context, subID string) context.Context {
	return context.WithValue(ctx, subscriptionIDcontextKey, subID)
}

// ContextWithBaggageFromContext copy context baggage
func ContextWithBaggageFromContext(to, from context.Context) context.Context {
	// Convert to string
	bag, err := baggage.Parse(baggage.FromContext(from).String())
	if err != nil {
		return to
	}
	return baggage.ContextWithBaggage(to, bag)
}

// ContextWithEventFromContext copy context baggage
func ContextWithEventFromContext(to, from context.Context) context.Context {
	return ContextWithMetadata(
		ContextWithSubscriptionID(
			ContextWithSource(
				ContextWithEventID(
					ContextWithName(to, ContextName(from)),
					ContextEventID(from)),
				ContextSource(from)),
			ContextSubscriptionID(from)),
		ContextMetadata(from))
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

// ContextAttributes get attributes stored in context baggage
func ContextAttributes(ctx context.Context) []attribute.KeyValue {
	return AttributesFromBaggage(baggage.FromContext(ctx))
}

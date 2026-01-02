package event

import (
	"context"
)

// OutboxStore defines the interface for outbox persistence.
// Implementations should store messages atomically within the active database transaction.
//
// When a Bus is configured with an outbox store, calls to Event.Publish() will
// automatically route to the outbox when inside a transaction (detected via context).
//
// Example flow:
//
//	// Normal publish - goes directly to transport
//	orderEvent.Publish(ctx, order)
//
//	// Inside transaction - goes to outbox
//	outbox.Transaction(ctx, mongoClient, func(ctx context.Context) error {
//	    ordersCol.UpdateOne(ctx, filter, update)
//	    return orderEvent.Publish(ctx, order)  // Routed to outbox!
//	})
type OutboxStore interface {
	// Store persists a message in the outbox within the active transaction.
	// The transaction session should be extracted from the context.
	//
	// Parameters:
	//   - ctx: Context containing the transaction session
	//   - eventName: Name of the event for routing
	//   - eventID: Unique identifier for the event
	//   - payload: Encoded event data
	//   - metadata: Optional message metadata
	//
	// Returns error if storage fails (should trigger transaction rollback).
	Store(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error
}

// Transaction context key
type outboxTxKey struct{}

// WithOutboxTx adds a transaction session to the context.
// This signals to the Bus that publishes should go to outbox instead of transport.
//
// The session value is implementation-specific (e.g., mongo.SessionContext for MongoDB).
//
// Example:
//
//	// MongoDB transaction
//	sess.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
//	    ctx := event.WithOutboxTx(sessCtx, sessCtx)
//	    // ... business logic ...
//	    return nil, orderEvent.Publish(ctx, order)  // Goes to outbox
//	})
func WithOutboxTx(ctx context.Context, session any) context.Context {
	return context.WithValue(ctx, outboxTxKey{}, session)
}

// OutboxTx retrieves the transaction session from context.
// Returns nil if no transaction is active.
func OutboxTx(ctx context.Context) any {
	return ctx.Value(outboxTxKey{})
}

// InOutboxTx returns true if the context has an active outbox transaction.
func InOutboxTx(ctx context.Context) bool {
	return ctx.Value(outboxTxKey{}) != nil
}

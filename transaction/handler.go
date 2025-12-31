package transaction

import (
	"context"
	"fmt"

	"github.com/rbaliyan/event/v3/idempotency"
)

// IdempotentHandler wraps a handler with idempotency checking.
//
// This handler ensures that each unique message is processed exactly once,
// even if the same message is delivered multiple times. It uses an idempotency
// store to track which messages have been processed.
//
// The handler is generic over the data type T, allowing it to work with any
// message payload type.
//
// Flow:
//  1. Extract idempotency key from data using keyFunc
//  2. Check if key exists in store (duplicate check)
//  3. If duplicate, return nil (skip processing)
//  4. If new, execute handler
//  5. On success, mark key as processed in store
//
// Note: If the handler succeeds but MarkProcessed fails, the message may
// be processed again on retry. For atomic exactly-once semantics, use
// TransactionalHandler with a TransactionalStore instead.
//
// Example:
//
//	store := idempotency.NewRedisStore(rdb, time.Hour)
//
//	handler := transaction.NewIdempotentHandler(
//	    func(ctx context.Context, order Order) error {
//	        return processOrder(order)
//	    },
//	    store,
//	    func(order Order) string {
//	        return fmt.Sprintf("order:%s", order.ID)
//	    },
//	)
//
//	// Safe to call multiple times with same order
//	err := handler.Handle(ctx, order)
type IdempotentHandler[T any] struct {
	handler func(ctx context.Context, data T) error
	store   idempotency.Store
	keyFunc func(data T) string // Extract idempotency key from data
}

// NewIdempotentHandler creates a new idempotent handler wrapper.
//
// Parameters:
//   - handler: The actual handler function to wrap
//   - store: Idempotency store for tracking processed messages
//   - keyFunc: Function to extract a unique key from the data
//
// The keyFunc should return a string that uniquely identifies the message.
// Common patterns:
//   - Message ID: func(m Message) string { return m.ID }
//   - Composite key: func(o Order) string { return fmt.Sprintf("%s:%s", o.CustomerID, o.ID) }
//   - Hash-based: func(e Event) string { return hash(e.Payload) }
//
// Example:
//
//	handler := transaction.NewIdempotentHandler(
//	    processPayment,
//	    idempotency.NewMemoryStore(time.Hour),
//	    func(p Payment) string { return p.TransactionID },
//	)
func NewIdempotentHandler[T any](
	handler func(ctx context.Context, data T) error,
	store idempotency.Store,
	keyFunc func(data T) string,
) *IdempotentHandler[T] {
	return &IdempotentHandler[T]{
		handler: handler,
		store:   store,
		keyFunc: keyFunc,
	}
}

// Handle processes the data with idempotency checking.
//
// If the message has already been processed (based on the idempotency key),
// this method returns nil without calling the handler.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - data: The data to process
//
// Returns:
//   - nil: Message processed successfully or was a duplicate
//   - error: Handler failed or idempotency check failed
//
// Example:
//
//	for msg := range messages {
//	    if err := handler.Handle(ctx, msg); err != nil {
//	        log.Error("failed to process message", "id", msg.ID, "error", err)
//	        // Handle error (retry, DLQ, etc.)
//	    }
//	}
func (h *IdempotentHandler[T]) Handle(ctx context.Context, data T) error {
	key := h.keyFunc(data)

	// Check if already processed
	isDuplicate, err := h.store.IsDuplicate(ctx, key)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if isDuplicate {
		return nil // Already processed, skip
	}

	// Execute handler
	if err := h.handler(ctx, data); err != nil {
		return err
	}

	// Mark as processed
	return h.store.MarkProcessed(ctx, key)
}

// TransactionalHandler wraps a handler with transaction and idempotency support.
//
// This handler provides true exactly-once processing semantics by:
//  1. Checking idempotency within a database transaction
//  2. Executing the handler within the same transaction
//  3. Marking as processed within the same transaction
//  4. Committing or rolling back atomically
//
// This ensures that the business logic and idempotency tracking are atomic:
// either both succeed or both fail. This is critical for financial
// transactions, inventory updates, and other operations that must not
// be duplicated or lost.
//
// Flow:
//  1. Begin database transaction
//  2. Check idempotency key within transaction (SELECT FOR UPDATE)
//  3. If duplicate, return nil (transaction commits with no changes)
//  4. Execute handler within transaction
//  5. Mark as processed within same transaction (INSERT)
//  6. Commit transaction (or rollback on any error)
//
// Example:
//
//	txManager := transaction.NewSQLManager(db)
//	idempStore := idempotency.NewPostgresStore(db)
//
//	handler := transaction.NewTransactionalHandler(
//	    func(ctx context.Context, tx transaction.Transaction, order Order) error {
//	        sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	        // Update inventory (within transaction)
//	        _, err := sqlTx.ExecContext(ctx,
//	            "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
//	            order.Quantity, order.ProductID)
//	        if err != nil {
//	            return err
//	        }
//
//	        // Insert order record (within transaction)
//	        _, err = sqlTx.ExecContext(ctx,
//	            "INSERT INTO orders (id, product_id, quantity) VALUES ($1, $2, $3)",
//	            order.ID, order.ProductID, order.Quantity)
//	        return err
//	    },
//	    txManager,
//	    idempStore,
//	    func(order Order) string { return fmt.Sprintf("order:%s", order.ID) },
//	)
//
//	// This is truly exactly-once: atomic inventory + order + idempotency
//	err := handler.Handle(ctx, order)
type TransactionalHandler[T any] struct {
	handler   func(ctx context.Context, tx Transaction, data T) error
	txManager Manager
	store     idempotency.TransactionalStore
	keyFunc   func(data T) string
}

// NewTransactionalHandler creates a new transactional handler wrapper.
//
// Parameters:
//   - handler: The handler function receiving the transaction and data
//   - txManager: Transaction manager for creating transactions
//   - store: TransactionalStore that supports within-transaction operations
//   - keyFunc: Function to extract a unique idempotency key from data
//
// The handler receives a Transaction that can be type-asserted to access
// the underlying database transaction (e.g., SQLTransactionProvider for SQL).
//
// Example:
//
//	handler := transaction.NewTransactionalHandler(
//	    func(ctx context.Context, tx transaction.Transaction, payment Payment) error {
//	        sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//	        _, err := sqlTx.ExecContext(ctx, "INSERT INTO payments ...")
//	        return err
//	    },
//	    transaction.NewSQLManager(db),
//	    idempotency.NewPostgresStore(db),
//	    func(p Payment) string { return p.ID },
//	)
func NewTransactionalHandler[T any](
	handler func(ctx context.Context, tx Transaction, data T) error,
	txManager Manager,
	store idempotency.TransactionalStore,
	keyFunc func(data T) string,
) *TransactionalHandler[T] {
	return &TransactionalHandler[T]{
		handler:   handler,
		txManager: txManager,
		store:     store,
		keyFunc:   keyFunc,
	}
}

// Handle processes the data within a transaction with idempotency.
//
// This method provides atomic exactly-once semantics:
//   - Idempotency check, business logic, and marking are all in one transaction
//   - If any step fails, the entire transaction is rolled back
//   - Duplicate messages are detected atomically (no race conditions)
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - data: The data to process
//
// Returns:
//   - nil: Message processed successfully or was a duplicate
//   - error: Handler failed, idempotency check failed, or transaction failed
//
// Example:
//
//	// Process incoming orders - safe against duplicates
//	for order := range orderChannel {
//	    if err := handler.Handle(ctx, order); err != nil {
//	        log.Error("failed to process order",
//	            "order_id", order.ID,
//	            "error", err)
//	        // May need to send to DLQ or retry
//	    }
//	}
func (h *TransactionalHandler[T]) Handle(ctx context.Context, data T) error {
	key := h.keyFunc(data)

	return h.txManager.Execute(ctx, func(tx Transaction) error {
		// Get SQL transaction if available
		sqlTx, ok := tx.(SQLTransactionProvider)
		if !ok {
			return fmt.Errorf("transaction does not support SQL operations")
		}

		// Check if already processed (within transaction)
		isDuplicate, err := h.store.IsDuplicateTx(ctx, sqlTx.Tx(), key)
		if err != nil {
			return fmt.Errorf("idempotency check failed: %w", err)
		}
		if isDuplicate {
			return nil // Already processed, skip
		}

		// Execute handler (within transaction)
		if err := h.handler(ctx, tx, data); err != nil {
			return err
		}

		// Mark as processed (within same transaction)
		return h.store.MarkProcessedTx(ctx, sqlTx.Tx(), key)
	})
}

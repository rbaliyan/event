package transaction_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rbaliyan/event/v3/transaction"
)

// Example demonstrates the transaction package for exactly-once processing.
//
// The transaction package provides database-agnostic transaction management
// with support for SQL databases and MongoDB. It enables atomic operations
// across database updates and message processing.
func Example() {
	// Note: This example shows the conceptual usage pattern.
	// In production, you would use an actual database connection.
	//
	// The key concepts are:
	// 1. Manager - handles transaction lifecycle (Begin, Execute)
	// 2. Transaction - represents an active transaction (Commit, Rollback)
	// 3. Execute - the recommended way to run transactional code
	//
	// Example with a real SQL database:
	//
	//   db, _ := sql.Open("postgres", connString)
	//   manager := transaction.NewSQLManager(db)
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
	//
	//       // All operations are atomic
	//       _, err := sqlTx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, fromID)
	//       if err != nil {
	//           return err // Triggers rollback
	//       }
	//
	//       _, err = sqlTx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, toID)
	//       if err != nil {
	//           return err // Triggers rollback
	//       }
	//
	//       return nil // Triggers commit
	//   })

	fmt.Println("Transaction package provides:")
	fmt.Println("- Automatic commit on success")
	fmt.Println("- Automatic rollback on error")
	fmt.Println("- Automatic rollback on panic")
	fmt.Println("- Database-agnostic interface")

	// Output:
	// Transaction package provides:
	// - Automatic commit on success
	// - Automatic rollback on error
	// - Automatic rollback on panic
	// - Database-agnostic interface
}

// ExampleNewSQLManager demonstrates creating an SQL transaction manager.
//
// SQLManager works with any database that implements database/sql,
// including PostgreSQL, MySQL, SQLite, and SQL Server.
func ExampleNewSQLManager() {
	// Note: This example shows the pattern - actual DB connection not established
	//
	// In production:
	//
	//   db, err := sql.Open("postgres", "postgres://localhost/mydb")
	//   if err != nil {
	//       log.Fatal(err)
	//   }
	//   defer db.Close()
	//
	//   manager := transaction.NewSQLManager(db)
	//
	//   // Now use manager.Execute() for transactional operations
	//   err = manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // Your transactional code here
	//       return nil
	//   })

	fmt.Println("SQLManager supports:")
	fmt.Println("- PostgreSQL")
	fmt.Println("- MySQL")
	fmt.Println("- SQLite")
	fmt.Println("- SQL Server")
	fmt.Println("- Any database/sql compatible driver")

	// Output:
	// SQLManager supports:
	// - PostgreSQL
	// - MySQL
	// - SQLite
	// - SQL Server
	// - Any database/sql compatible driver
}

// ExampleSQLManager_Execute demonstrates the recommended way to run transactions.
//
// Execute handles the complete transaction lifecycle: Begin, Commit/Rollback,
// and panic recovery. This is the preferred method for most use cases.
func ExampleSQLManager_Execute() {
	// Note: This example shows the usage pattern
	//
	// In production with a real database:
	//
	//   manager := transaction.NewSQLManager(db)
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // Get the underlying SQL transaction
	//       sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
	//
	//       // Debit source account
	//       result, err := sqlTx.ExecContext(ctx,
	//           "UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1",
	//           amount, sourceID)
	//       if err != nil {
	//           return err // Rollback
	//       }
	//
	//       // Check if update affected any rows
	//       rows, _ := result.RowsAffected()
	//       if rows == 0 {
	//           return errors.New("insufficient balance") // Rollback
	//       }
	//
	//       // Credit destination account
	//       _, err = sqlTx.ExecContext(ctx,
	//           "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
	//           amount, destID)
	//       if err != nil {
	//           return err // Rollback
	//       }
	//
	//       // Record the transfer
	//       _, err = sqlTx.ExecContext(ctx,
	//           "INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)",
	//           sourceID, destID, amount)
	//       if err != nil {
	//           return err // Rollback
	//       }
	//
	//       return nil // Commit - all operations succeed or none do
	//   })

	fmt.Println("Execute behavior:")
	fmt.Println("- nil return: transaction commits")
	fmt.Println("- error return: transaction rolls back")
	fmt.Println("- panic: transaction rolls back, panic re-raised")

	// Output:
	// Execute behavior:
	// - nil return: transaction commits
	// - error return: transaction rolls back
	// - panic: transaction rolls back, panic re-raised
}

// ExampleSQLManager_Begin demonstrates manual transaction control.
//
// While Execute is preferred, Begin provides fine-grained control when needed.
// Use this when you need to pass the transaction to other functions.
func ExampleSQLManager_Begin() {
	// Note: This example shows the pattern for manual transaction control
	//
	// In production:
	//
	//   manager := transaction.NewSQLManager(db)
	//
	//   tx, err := manager.Begin(ctx)
	//   if err != nil {
	//       return err
	//   }
	//   // Always ensure rollback is called if commit isn't reached
	//   defer tx.Rollback()
	//
	//   sqlTx := tx.(*transaction.SQLTransaction).Tx()
	//
	//   // Do work...
	//   if err := doFirstOperation(ctx, sqlTx); err != nil {
	//       return err // Deferred rollback will execute
	//   }
	//
	//   if err := doSecondOperation(ctx, sqlTx); err != nil {
	//       return err // Deferred rollback will execute
	//   }
	//
	//   // Explicitly commit - this makes the deferred rollback a no-op
	//   return tx.Commit()

	fmt.Println("Manual transaction control:")
	fmt.Println("1. Call Begin() to start transaction")
	fmt.Println("2. Defer Rollback() for safety")
	fmt.Println("3. Do work with the transaction")
	fmt.Println("4. Call Commit() on success")

	// Output:
	// Manual transaction control:
	// 1. Call Begin() to start transaction
	// 2. Defer Rollback() for safety
	// 3. Do work with the transaction
	// 4. Call Commit() on success
}

// ExampleSQLTransactionProvider demonstrates accessing the underlying SQL transaction.
//
// When you need to execute SQL queries within a transaction, use type assertion
// to get the underlying *sql.Tx.
func ExampleSQLTransactionProvider() {
	// The SQLTransactionProvider interface allows access to *sql.Tx
	//
	// In production:
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // Type assertion to get SQL transaction
	//       sqlProvider, ok := tx.(transaction.SQLTransactionProvider)
	//       if !ok {
	//           return errors.New("not an SQL transaction")
	//       }
	//
	//       // Get the underlying *sql.Tx
	//       sqlTx := sqlProvider.Tx()
	//
	//       // Now use standard database/sql methods
	//       row := sqlTx.QueryRowContext(ctx, "SELECT balance FROM accounts WHERE id = $1", accountID)
	//       var balance float64
	//       if err := row.Scan(&balance); err != nil {
	//           return err
	//       }
	//
	//       _, err := sqlTx.ExecContext(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", newBalance, accountID)
	//       return err
	//   })

	fmt.Println("SQLTransactionProvider provides:")
	fmt.Println("- Tx() method returns *sql.Tx")
	fmt.Println("- Full access to ExecContext, QueryContext, etc.")
	fmt.Println("- Works with any database/sql driver")

	// Output:
	// SQLTransactionProvider provides:
	// - Tx() method returns *sql.Tx
	// - Full access to ExecContext, QueryContext, etc.
	// - Works with any database/sql driver
}

// Example_mongoManager demonstrates MongoDB transaction support.
//
// MongoManager provides the same interface for MongoDB transactions,
// enabling atomic operations across multiple collections.
func Example_mongoManager() {
	// Note: This example shows the MongoDB transaction pattern
	//
	// MongoDB transactions require a replica set or sharded cluster.
	//
	// In production:
	//
	//   client, _ := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	//   manager := transaction.NewMongoManager(client)
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // Get MongoDB session context
	//       mongoTx := tx.(transaction.MongoSessionProvider)
	//       sessCtx := mongoTx.SessionContext()
	//
	//       // All operations use the session context
	//       _, err := ordersCol.InsertOne(sessCtx, order)
	//       if err != nil {
	//           return err // Rollback
	//       }
	//
	//       _, err = inventoryCol.UpdateOne(sessCtx,
	//           bson.M{"product_id": order.ProductID},
	//           bson.M{"$inc": bson.M{"quantity": -order.Quantity}})
	//       if err != nil {
	//           return err // Rollback
	//       }
	//
	//       return nil // Commit
	//   })

	fmt.Println("MongoDB transaction requirements:")
	fmt.Println("- MongoDB 4.0+ for replica set transactions")
	fmt.Println("- MongoDB 4.2+ for sharded cluster transactions")
	fmt.Println("- Use SessionContext for all operations")

	// Output:
	// MongoDB transaction requirements:
	// - MongoDB 4.0+ for replica set transactions
	// - MongoDB 4.2+ for sharded cluster transactions
	// - Use SessionContext for all operations
}

// Example_withTransaction demonstrates the standalone MongoDB helper.
//
// WithTransaction is a convenience function for simple MongoDB transactions
// without creating a MongoManager instance.
func Example_withTransaction() {
	// Note: This shows the simpler pattern for one-off transactions
	//
	// In production:
	//
	//   err := transaction.WithTransaction(ctx, client, func(sessCtx mongo.SessionContext) error {
	//       // All operations use sessCtx
	//       _, err := collection.InsertOne(sessCtx, doc)
	//       return err
	//   })
	//
	// This is equivalent to:
	//
	//   manager := transaction.NewMongoManager(client)
	//   err := manager.ExecuteWithSession(ctx, func(sessCtx mongo.SessionContext) error {
	//       _, err := collection.InsertOne(sessCtx, doc)
	//       return err
	//   })

	fmt.Println("WithTransaction is ideal for:")
	fmt.Println("- Simple, one-off transactions")
	fmt.Println("- When you don't need a persistent manager")
	fmt.Println("- Quick prototyping")

	// Output:
	// WithTransaction is ideal for:
	// - Simple, one-off transactions
	// - When you don't need a persistent manager
	// - Quick prototyping
}

// Example_idempotentHandler demonstrates at-least-once processing with idempotency.
//
// IdempotentHandler wraps a handler with duplicate detection, ensuring each
// unique message is processed at most once (after initial processing).
func Example_idempotentHandler() {
	// Note: This shows the idempotent handler pattern
	//
	// IdempotentHandler provides at-least-once delivery with deduplication:
	//
	//   store := idempotency.NewRedisStore(rdb, time.Hour)
	//
	//   handler := transaction.NewIdempotentHandler(
	//       func(ctx context.Context, order Order) error {
	//           // Process the order
	//           return processOrder(order)
	//       },
	//       store,
	//       func(order Order) string {
	//           // Extract unique key for deduplication
	//           return fmt.Sprintf("order:%s", order.ID)
	//       },
	//   )
	//
	//   // Safe to call multiple times - duplicates are skipped
	//   err := handler.Handle(ctx, order)
	//
	// Flow:
	// 1. Extract key from data
	// 2. Check if key exists in store
	// 3. If duplicate, return nil (skip)
	// 4. If new, execute handler
	// 5. On success, mark as processed

	fmt.Println("IdempotentHandler flow:")
	fmt.Println("1. Extract idempotency key from data")
	fmt.Println("2. Check if already processed")
	fmt.Println("3. Skip if duplicate")
	fmt.Println("4. Process if new")
	fmt.Println("5. Mark as processed on success")

	// Output:
	// IdempotentHandler flow:
	// 1. Extract idempotency key from data
	// 2. Check if already processed
	// 3. Skip if duplicate
	// 4. Process if new
	// 5. Mark as processed on success
}

// Example_transactionalHandler demonstrates exactly-once processing.
//
// TransactionalHandler combines transactions with idempotency for true
// exactly-once semantics - the business logic and idempotency tracking
// are atomic.
func Example_transactionalHandler() {
	// Note: This shows the exactly-once processing pattern
	//
	// TransactionalHandler provides exactly-once semantics by wrapping
	// both the handler and idempotency check in a single transaction:
	//
	//   txManager := transaction.NewSQLManager(db)
	//   idempStore := idempotency.NewPostgresStore(db)
	//
	//   handler := transaction.NewTransactionalHandler(
	//       func(ctx context.Context, tx transaction.Transaction, order Order) error {
	//           sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
	//
	//           // Update inventory (within transaction)
	//           _, err := sqlTx.ExecContext(ctx,
	//               "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
	//               order.Quantity, order.ProductID)
	//           if err != nil {
	//               return err
	//           }
	//
	//           // Insert order (within transaction)
	//           _, err = sqlTx.ExecContext(ctx,
	//               "INSERT INTO orders (id, product_id, quantity) VALUES ($1, $2, $3)",
	//               order.ID, order.ProductID, order.Quantity)
	//           return err
	//       },
	//       txManager,
	//       idempStore,
	//       func(order Order) string { return order.ID },
	//   )
	//
	//   // Truly exactly-once: inventory, order, and idempotency are atomic
	//   err := handler.Handle(ctx, order)

	fmt.Println("TransactionalHandler guarantees:")
	fmt.Println("- Idempotency check is within transaction")
	fmt.Println("- Business logic is within transaction")
	fmt.Println("- Mark processed is within transaction")
	fmt.Println("- All succeed or all fail atomically")

	// Output:
	// TransactionalHandler guarantees:
	// - Idempotency check is within transaction
	// - Business logic is within transaction
	// - Mark processed is within transaction
	// - All succeed or all fail atomically
}

// Example_errorHandling demonstrates proper error handling with transactions.
func Example_errorHandling() {
	// Proper error handling pattern:
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // Return errors to trigger rollback
	//       if err := step1(); err != nil {
	//           return fmt.Errorf("step1 failed: %w", err)
	//       }
	//
	//       if err := step2(); err != nil {
	//           return fmt.Errorf("step2 failed: %w", err)
	//       }
	//
	//       return nil
	//   })
	//
	//   // Check for specific error types
	//   if errors.Is(err, transaction.ErrTransactionFailed) {
	//       log.Error("transaction infrastructure failure")
	//   }
	//
	//   // Check for business logic errors
	//   var validationErr *ValidationError
	//   if errors.As(err, &validationErr) {
	//       log.Warn("validation failed", "field", validationErr.Field)
	//   }

	fmt.Println("Error handling best practices:")
	fmt.Println("- Return errors from handler to trigger rollback")
	fmt.Println("- Wrap errors with context using fmt.Errorf")
	fmt.Println("- Use errors.Is/As to check error types")
	fmt.Println("- Check for ErrTransactionFailed for infra issues")

	// Output:
	// Error handling best practices:
	// - Return errors from handler to trigger rollback
	// - Wrap errors with context using fmt.Errorf
	// - Use errors.Is/As to check error types
	// - Check for ErrTransactionFailed for infra issues
}

// Example_panicRecovery demonstrates automatic panic handling.
func Example_panicRecovery() {
	// Execute automatically handles panics:
	//
	//   err := manager.Execute(ctx, func(tx transaction.Transaction) error {
	//       // If this panics...
	//       doSomethingRisky()
	//
	//       return nil
	//   })
	//   // ...the transaction is rolled back and an error wrapping ErrTransactionFailed is returned
	//
	// This ensures data consistency even when unexpected panics occur.
	// The error wraps ErrTransactionFailed and can be checked with errors.Is().

	fmt.Println("Panic recovery behavior:")
	fmt.Println("1. Panic occurs in handler")
	fmt.Println("2. Transaction is rolled back")
	fmt.Println("3. Error wrapping ErrTransactionFailed is returned")
	fmt.Println("4. Caller checks with errors.Is(err, ErrTransactionFailed)")

	// Output:
	// Panic recovery behavior:
	// 1. Panic occurs in handler
	// 2. Transaction is rolled back
	// 3. Error wrapping ErrTransactionFailed is returned
	// 4. Caller checks with errors.Is(err, ErrTransactionFailed)
}

// Example_keyFunctionPatterns demonstrates common patterns for idempotency key extraction.
func Example_keyFunctionPatterns() {
	// Key functions extract unique identifiers from your data types.
	// Good key functions produce unique, deterministic keys.

	type Order struct {
		ID         string
		CustomerID string
	}

	type Payment struct {
		TransactionID string
		OrderID       string
		Amount        float64
	}

	// Pattern 1: Simple ID field
	_ = func(o Order) string {
		return o.ID
	}

	// Pattern 2: Composite key for uniqueness across entities
	_ = func(o Order) string {
		return fmt.Sprintf("order:%s:customer:%s", o.ID, o.CustomerID)
	}

	// Pattern 3: Namespaced keys for multi-tenant systems
	_ = func(p Payment) string {
		return fmt.Sprintf("payment:%s", p.TransactionID)
	}

	// Pattern 4: Event-type specific keys
	_ = func(p Payment) string {
		return fmt.Sprintf("payment-processed:%s:%s", p.OrderID, p.TransactionID)
	}

	fmt.Println("Key function patterns:")
	fmt.Println("- Simple: order.ID")
	fmt.Println("- Composite: order:{id}:customer:{cid}")
	fmt.Println("- Namespaced: payment:{transaction_id}")
	fmt.Println("- Event-specific: payment-processed:{order}:{tx}")

	// Output:
	// Key function patterns:
	// - Simple: order.ID
	// - Composite: order:{id}:customer:{cid}
	// - Namespaced: payment:{transaction_id}
	// - Event-specific: payment-processed:{order}:{tx}
}

// mockDB is a minimal mock for demonstrating SQLManager
type mockDB struct{}

func (m *mockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, fmt.Errorf("mock database")
}

// ExampleErrTransactionFailed demonstrates checking for transaction errors.
func ExampleErrTransactionFailed() {
	// Check if an error is a transaction failure
	//
	//   err := manager.Execute(ctx, fn)
	//   if errors.Is(err, transaction.ErrTransactionFailed) {
	//       // Infrastructure failure - may need retry or alerting
	//       log.Error("transaction could not complete", "error", err)
	//   }

	err := transaction.ErrTransactionFailed
	fmt.Println("Error message:", err.Error())

	// Output:
	// Error message: transaction failed
}

// Package transaction provides transaction management for exactly-once processing.
//
// This package enables atomic operations across database updates and message
// processing, ensuring that either both succeed or both fail. This is critical
// for maintaining data consistency in event-driven architectures.
//
// # Overview
//
// The package provides:
//   - Transaction interface for database-agnostic transaction handling
//   - Manager interface for transaction lifecycle management
//   - SQLManager for SQL database transactions
//   - MongoManager for MongoDB transactions (see mongodb.go)
//   - IdempotentHandler for exactly-once message processing
//   - TransactionalHandler for combining transactions with idempotency
//
// # Basic Usage
//
// Using the SQL transaction manager:
//
//	db, _ := sql.Open("postgres", connString)
//	txManager := transaction.NewSQLManager(db)
//
//	err := txManager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    // Update order status
//	    _, err := sqlTx.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", "paid", orderID)
//	    if err != nil {
//	        return err // Triggers rollback
//	    }
//
//	    // Update inventory
//	    _, err = sqlTx.ExecContext(ctx, "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2", qty, productID)
//	    if err != nil {
//	        return err // Triggers rollback
//	    }
//
//	    return nil // Triggers commit
//	})
//
// # Exactly-Once Processing
//
// Combining transactions with idempotency for exactly-once processing:
//
//	txManager := transaction.NewSQLManager(db)
//	idempStore := idempotency.NewPostgresStore(db)
//
//	handler := transaction.NewTransactionalHandler(
//	    func(ctx context.Context, tx transaction.Transaction, order Order) error {
//	        sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//	        _, err := sqlTx.ExecContext(ctx, "INSERT INTO orders ...", order.ID)
//	        return err
//	    },
//	    txManager,
//	    idempStore,
//	    func(order Order) string { return fmt.Sprintf("order:%s", order.ID) },
//	)
//
//	// This is safe to call multiple times - duplicates are detected
//	err := handler.Handle(ctx, order)
//
// # Best Practices
//
//   - Keep transactions short to reduce lock contention
//   - Use Execute() for automatic commit/rollback handling
//   - Combine with idempotency store for exactly-once processing
//   - Use appropriate isolation levels for your use case
package transaction

import (
	"context"
	"database/sql"
	"errors"
)

// ErrTransactionFailed is returned when a transaction cannot be completed.
//
// This error indicates that the transaction could not be committed or rolled
// back successfully. The caller should check the underlying error for more
// details about what failed.
//
// Example:
//
//	err := txManager.Execute(ctx, fn)
//	if errors.Is(err, transaction.ErrTransactionFailed) {
//	    log.Error("transaction could not complete", "error", err)
//	    // Consider retrying or alerting
//	}
var ErrTransactionFailed = errors.New("transaction failed")

// Transaction represents an active database transaction.
//
// This interface provides database-agnostic transaction operations.
// Implementations may wrap SQL transactions, MongoDB sessions, or other
// database-specific transaction types.
//
// Users typically don't call Commit/Rollback directly - instead use
// Manager.Execute() which handles these automatically.
//
// Example with manual control:
//
//	tx, err := manager.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//
//	if err := doWork(tx); err != nil {
//	    tx.Rollback()
//	    return err
//	}
//
//	return tx.Commit()
type Transaction interface {
	// Commit commits the transaction.
	// After Commit, the transaction is no longer usable.
	Commit() error

	// Rollback aborts the transaction.
	// After Rollback, the transaction is no longer usable.
	// It's safe to call Rollback on an already committed/rolled back transaction.
	Rollback() error
}

// SQLTransactionProvider is implemented by transactions that provide SQL tx access.
//
// Use type assertion to access the underlying *sql.Tx when you need to
// execute SQL queries within the transaction.
//
// Example:
//
//	err := txManager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx, ok := tx.(transaction.SQLTransactionProvider)
//	    if !ok {
//	        return errors.New("not an SQL transaction")
//	    }
//
//	    _, err := sqlTx.Tx().ExecContext(ctx, "INSERT INTO ...", args...)
//	    return err
//	})
type SQLTransactionProvider interface {
	Transaction
	// Tx returns the underlying *sql.Tx for executing SQL queries.
	Tx() *sql.Tx
}

// Manager handles transaction lifecycle.
//
// The Manager interface provides two ways to work with transactions:
//   - Begin/Commit/Rollback: Manual control (use when you need fine-grained control)
//   - Execute: Automatic commit/rollback (recommended for most use cases)
//
// Implementations:
//   - SQLManager: For SQL databases (PostgreSQL, MySQL, SQLite, etc.)
//   - MongoManager: For MongoDB (see mongodb.go)
//
// Example:
//
//	manager := transaction.NewSQLManager(db)
//
//	// Automatic commit/rollback (recommended)
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    // Do work...
//	    return nil // Commits on success
//	})
type Manager interface {
	// Begin starts a new transaction.
	//
	// The returned Transaction must be either committed or rolled back.
	// Prefer using Execute() which handles this automatically.
	Begin(ctx context.Context) (Transaction, error)

	// Execute runs a function within a transaction, automatically handling commit/rollback.
	//
	// If fn returns nil, the transaction is committed.
	// If fn returns an error, the transaction is rolled back.
	// If fn panics, the transaction is rolled back and the panic is re-raised.
	Execute(ctx context.Context, fn func(tx Transaction) error) error
}

// SQLTransaction wraps sql.Tx to implement Transaction.
//
// This type implements both Transaction and SQLTransactionProvider interfaces,
// allowing code to work generically with Transaction while still accessing
// the underlying *sql.Tx when needed.
//
// Example:
//
//	tx, _ := manager.Begin(ctx)
//	sqlTx := tx.(*transaction.SQLTransaction)
//
//	// Use the underlying *sql.Tx
//	_, err := sqlTx.Tx().ExecContext(ctx, "UPDATE ...")
type SQLTransaction struct {
	tx *sql.Tx
}

// Commit commits the SQL transaction.
//
// After calling Commit, the transaction is closed and cannot be used.
// Returns an error if the commit fails.
func (t *SQLTransaction) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the SQL transaction.
//
// After calling Rollback, the transaction is closed and cannot be used.
// It's safe to call Rollback on an already finished transaction.
func (t *SQLTransaction) Rollback() error {
	return t.tx.Rollback()
}

// Tx returns the underlying *sql.Tx for executing SQL queries.
//
// Use this to execute queries within the transaction:
//
//	_, err := tx.Tx().ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", name)
func (t *SQLTransaction) Tx() *sql.Tx {
	return t.tx
}

// SQLManager implements Manager for SQL databases.
//
// SQLManager works with any database that implements the database/sql interface,
// including PostgreSQL, MySQL, SQLite, SQL Server, and others.
//
// Features:
//   - Automatic transaction lifecycle management
//   - Panic recovery with automatic rollback
//   - Context support for cancellation and timeouts
//
// Example:
//
//	db, _ := sql.Open("postgres", "postgres://localhost/mydb")
//	manager := transaction.NewSQLManager(db)
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    _, err := sqlTx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1 WHERE id = $2", amount, fromID)
//	    if err != nil {
//	        return err // Rollback
//	    }
//
//	    _, err = sqlTx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE id = $2", amount, toID)
//	    if err != nil {
//	        return err // Rollback
//	    }
//
//	    return nil // Commit
//	})
type SQLManager struct {
	db *sql.DB
}

// NewSQLManager creates a new SQL transaction manager.
//
// The provided *sql.DB should be already connected and ready to use.
// The manager does not own the connection and will not close it.
//
// Parameters:
//   - db: An open database connection pool
//
// Example:
//
//	db, err := sql.Open("postgres", connString)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	manager := transaction.NewSQLManager(db)
func NewSQLManager(db *sql.DB) *SQLManager {
	return &SQLManager{db: db}
}

// Begin starts a new SQL transaction.
//
// The returned Transaction must be either committed or rolled back.
// For most use cases, prefer Execute() which handles this automatically.
//
// Uses the default transaction options (isolation level, read-only mode).
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//
// Returns:
//   - Transaction: The active transaction
//   - error: If the transaction could not be started
//
// Example:
//
//	tx, err := manager.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback() // Safe to call even if committed
//
//	// Do work...
//
//	return tx.Commit()
func (m *SQLManager) Begin(ctx context.Context) (Transaction, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &SQLTransaction{tx: tx}, nil
}

// Execute runs a function within a transaction.
//
// This is the recommended way to work with transactions. It handles:
//   - Starting the transaction
//   - Committing on success (fn returns nil)
//   - Rolling back on error (fn returns error)
//   - Rolling back on panic (and re-raising the panic)
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - fn: Function to execute within the transaction
//
// Returns the error from fn, or an error if commit fails.
//
// Example:
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    sqlTx := tx.(transaction.SQLTransactionProvider).Tx()
//
//	    // All operations are atomic
//	    if _, err := sqlTx.ExecContext(ctx, "INSERT ..."); err != nil {
//	        return err // Triggers rollback
//	    }
//	    if _, err := sqlTx.ExecContext(ctx, "UPDATE ..."); err != nil {
//	        return err // Triggers rollback
//	    }
//
//	    return nil // Triggers commit
//	})
func (m *SQLManager) Execute(ctx context.Context, fn func(tx Transaction) error) error {
	tx, err := m.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Compile-time checks
var _ Transaction = (*SQLTransaction)(nil)
var _ SQLTransactionProvider = (*SQLTransaction)(nil)
var _ Manager = (*SQLManager)(nil)

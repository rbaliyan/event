package transaction

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MongoTransaction wraps a MongoDB session to implement Transaction.
//
// This type provides MongoDB transaction support with automatic session
// management. It implements both Transaction and MongoSessionProvider
// interfaces, allowing both generic transaction handling and MongoDB-specific
// operations.
//
// MongoDB transactions require a replica set or sharded cluster.
// Standalone MongoDB deployments do not support transactions.
//
// Example:
//
//	manager := transaction.NewMongoManager(client)
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    mongoTx := tx.(transaction.MongoSessionProvider)
//	    ctx := mongoTx.Context()
//
//	    // All operations use the context for transactional consistency
//	    _, err := collection.InsertOne(ctx, doc)
//	    return err
//	})
type MongoTransaction struct {
	session   *mongo.Session
	ctx       context.Context // In v2, session is embedded in context
	closeOnce sync.Once
}

// Commit commits the MongoDB transaction and ends the session.
//
// After calling Commit, the transaction is closed and the session
// should not be used for further operations. The session is automatically
// ended to prevent resource leaks.
//
// Returns an error if the commit fails. MongoDB may automatically
// retry transient errors.
func (t *MongoTransaction) Commit() error {
	err := t.session.CommitTransaction(t.ctx)
	t.endSession()
	return err
}

// Rollback rolls back the MongoDB transaction and ends the session.
//
// After calling Rollback, the transaction is aborted and no changes
// are persisted. The session is automatically ended to prevent resource leaks.
func (t *MongoTransaction) Rollback() error {
	err := t.session.AbortTransaction(t.ctx)
	t.endSession()
	return err
}

// endSession ends the MongoDB session exactly once.
func (t *MongoTransaction) endSession() {
	t.closeOnce.Do(func() {
		t.session.EndSession(t.ctx)
	})
}

// Session returns the MongoDB session.
//
// Use this for advanced session operations like setting transaction options.
// For most use cases, use Context() instead.
func (t *MongoTransaction) Session() *mongo.Session {
	return t.session
}

// Context returns the context with the session embedded.
//
// Use this context for all MongoDB operations within the transaction.
// Operations using this context will be part of the transaction.
//
// Example:
//
//	ctx := mongoTx.Context()
//	_, err := collection.InsertOne(ctx, doc)
//	_, err = collection.UpdateOne(ctx, filter, update)
func (t *MongoTransaction) Context() context.Context {
	return t.ctx
}

// MongoManager implements Manager for MongoDB.
//
// MongoManager provides transaction support for MongoDB replica sets and
// sharded clusters. It handles session creation, transaction lifecycle,
// and automatic retry of transient errors.
//
// Requirements:
//   - MongoDB 4.0+ for replica set transactions
//   - MongoDB 4.2+ for sharded cluster transactions
//   - Replica set or sharded cluster deployment (not standalone)
//
// Features:
//   - Automatic session management
//   - Automatic retry of transient transaction errors
//   - Support for both generic Transaction interface and MongoDB-specific operations
//
// Example:
//
//	client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//	manager := transaction.NewMongoManager(client)
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    mongoTx := tx.(transaction.MongoSessionProvider)
//	    sessCtx := mongoTx.SessionContext()
//
//	    // Transfer money between accounts
//	    _, err := accounts.UpdateOne(sessCtx,
//	        bson.M{"_id": fromID},
//	        bson.M{"$inc": bson.M{"balance": -amount}})
//	    if err != nil {
//	        return err
//	    }
//
//	    _, err = accounts.UpdateOne(sessCtx,
//	        bson.M{"_id": toID},
//	        bson.M{"$inc": bson.M{"balance": amount}})
//	    return err
//	})
type MongoManager struct {
	client *mongo.Client
}

// NewMongoManager creates a new MongoDB transaction manager.
//
// The provided client should be connected to a MongoDB replica set or
// sharded cluster. The manager does not own the client and will not
// close it.
//
// Parameters:
//   - client: A connected MongoDB client
//
// Example:
//
//	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Disconnect(ctx)
//
//	manager := transaction.NewMongoManager(client)
func NewMongoManager(client *mongo.Client) (*MongoManager, error) {
	if client == nil {
		return nil, errors.New("mongodb: client is required")
	}

	return &MongoManager{client: client}, nil
}

// Begin starts a new MongoDB transaction.
//
// This creates a new session and starts a transaction on it. The returned
// Transaction must be committed or rolled back. For most use cases, prefer
// Execute() which handles this automatically.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//
// Returns:
//   - Transaction: The active MongoDB transaction
//   - error: If session creation or transaction start fails
//
// Example:
//
//	tx, err := manager.Begin(ctx)
//	if err != nil {
//	    return err
//	}
//
//	mongoTx := tx.(transaction.MongoSessionProvider)
//	ctx := mongoTx.Context()
//
//	// Do work with ctx...
//
//	if err := tx.Commit(); err != nil {
//	    tx.Rollback()
//	    return err
//	}
func (m *MongoManager) Begin(ctx context.Context) (Transaction, error) {
	session, err := m.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &MongoTransaction{
		session: session,
		ctx:     ctx, // In v2, operations will use session from context
	}, nil
}

// Execute runs a function within a MongoDB transaction.
//
// This is the recommended way to work with MongoDB transactions. It handles:
//   - Session creation and cleanup
//   - Transaction start, commit, and rollback
//   - Automatic retry of transient errors
//
// The function receives a Transaction that can be type-asserted to
// MongoSessionProvider to access the SessionContext.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - fn: Function to execute within the transaction
//
// Returns the error from fn, or an error if transaction handling fails.
//
// Example:
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    mongoTx := tx.(transaction.MongoSessionProvider)
//	    ctx := mongoTx.Context()
//
//	    _, err := orders.InsertOne(ctx, order)
//	    if err != nil {
//	        return err // Triggers rollback
//	    }
//
//	    _, err = inventory.UpdateOne(ctx, filter, update)
//	    return err // Commits on nil, rollbacks on error
//	})
func (m *MongoManager) Execute(ctx context.Context, fn func(tx Transaction) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		tx := &MongoTransaction{
			session: session,
			ctx:     ctx,
		}

		if err := fn(tx); err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

// ExecuteWithContext runs a function within a MongoDB transaction with direct context access.
//
// This is a convenience method when you only need the transaction context and
// don't need the Transaction interface. The context can be used
// directly with MongoDB operations.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - fn: Function receiving the transaction context
//
// Example:
//
//	err := manager.ExecuteWithContext(ctx, func(ctx context.Context) error {
//	    _, err := collection.InsertOne(ctx, doc)
//	    return err
//	})
func (m *MongoManager) ExecuteWithContext(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		if err := fn(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// MongoTxHandler is a function type that handles MongoDB transaction context.
//
// This is the signature for functions passed to WithTransaction.
type MongoTxHandler func(ctx context.Context) error

// WithTransaction executes a function within a MongoDB transaction.
//
// This is a standalone convenience function that handles the complete
// transaction lifecycle without requiring a MongoManager instance.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - client: Connected MongoDB client
//   - fn: Function to execute within the transaction
//
// Example:
//
//	err := transaction.WithTransaction(ctx, client, func(ctx context.Context) error {
//	    _, err := collection.InsertOne(ctx, doc)
//	    return err
//	})
func WithTransaction(ctx context.Context, client *mongo.Client, fn MongoTxHandler) error {
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		if err := fn(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// MongoSessionProvider is implemented by transactions that provide MongoDB session access.
//
// Use type assertion to access MongoDB-specific functionality from a
// generic Transaction.
//
// Example:
//
//	err := manager.Execute(ctx, func(tx transaction.Transaction) error {
//	    mongoTx, ok := tx.(transaction.MongoSessionProvider)
//	    if !ok {
//	        return errors.New("not a MongoDB transaction")
//	    }
//
//	    ctx := mongoTx.Context()
//	    _, err := collection.InsertOne(ctx, doc)
//	    return err
//	})
type MongoSessionProvider interface {
	Transaction

	// Session returns the MongoDB session.
	// Use for advanced session operations.
	Session() *mongo.Session

	// Context returns the transaction context.
	// Use this for all MongoDB operations within the transaction.
	Context() context.Context
}

// Compile-time checks
var _ Manager = (*MongoManager)(nil)
var _ Transaction = (*MongoTransaction)(nil)
var _ MongoSessionProvider = (*MongoTransaction)(nil)

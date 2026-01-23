package transaction

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
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
//	    sessCtx := mongoTx.SessionContext()
//
//	    // All operations use the session context for transactional consistency
//	    _, err := collection.InsertOne(sessCtx, doc)
//	    return err
//	})
type MongoTransaction struct {
	session mongo.Session
	ctx     mongo.SessionContext
}

// Commit commits the MongoDB transaction.
//
// After calling Commit, the transaction is closed and the session
// should not be used for further operations.
//
// Returns an error if the commit fails. MongoDB may automatically
// retry transient errors.
func (t *MongoTransaction) Commit() error {
	return t.session.CommitTransaction(t.ctx)
}

// Rollback rolls back the MongoDB transaction.
//
// After calling Rollback, the transaction is aborted and no changes
// are persisted. The session should not be used for further operations.
func (t *MongoTransaction) Rollback() error {
	return t.session.AbortTransaction(t.ctx)
}

// Session returns the MongoDB session.
//
// Use this for advanced session operations like setting transaction options.
// For most use cases, use SessionContext() instead.
func (t *MongoTransaction) Session() mongo.Session {
	return t.session
}

// SessionContext returns the MongoDB session context.
//
// Use this context for all MongoDB operations within the transaction.
// Operations using this context will be part of the transaction.
//
// Example:
//
//	sessCtx := mongoTx.SessionContext()
//	_, err := collection.InsertOne(sessCtx, doc)
//	_, err = collection.UpdateOne(sessCtx, filter, update)
func (t *MongoTransaction) SessionContext() mongo.SessionContext {
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
func NewMongoManager(client *mongo.Client) *MongoManager {
	return &MongoManager{client: client}
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
//	sessCtx := mongoTx.SessionContext()
//
//	// Do work with sessCtx...
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

	// Create session context
	sessCtx := mongo.NewSessionContext(ctx, session)

	return &MongoTransaction{
		session: session,
		ctx:     sessCtx,
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
//	    sessCtx := mongoTx.SessionContext()
//
//	    _, err := orders.InsertOne(sessCtx, order)
//	    if err != nil {
//	        return err // Triggers rollback
//	    }
//
//	    _, err = inventory.UpdateOne(sessCtx, filter, update)
//	    return err // Commits on nil, rollbacks on error
//	})
func (m *MongoManager) Execute(ctx context.Context, fn func(tx Transaction) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		tx := &MongoTransaction{
			session: session,
			ctx:     sessCtx,
		}

		if err := fn(tx); err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

// ExecuteWithSession runs a function within a MongoDB transaction with direct session access.
//
// This is a convenience method when you only need the SessionContext and
// don't need the Transaction interface. The session context can be used
// directly with MongoDB operations.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - fn: Function receiving the session context
//
// Example:
//
//	err := manager.ExecuteWithSession(ctx, func(sessCtx mongo.SessionContext) error {
//	    _, err := collection.InsertOne(sessCtx, doc)
//	    return err
//	})
func (m *MongoManager) ExecuteWithSession(ctx context.Context, fn func(sessCtx mongo.SessionContext) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		if err := fn(sessCtx); err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// MongoSessionHandler is a function type that handles MongoDB session context.
//
// This is the signature for functions passed to WithTransaction.
type MongoSessionHandler func(sessCtx mongo.SessionContext) error

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
//	err := transaction.WithTransaction(ctx, client, func(sessCtx mongo.SessionContext) error {
//	    _, err := collection.InsertOne(sessCtx, doc)
//	    return err
//	})
func WithTransaction(ctx context.Context, client *mongo.Client, fn MongoSessionHandler) error {
	session, err := client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		if err := fn(sessCtx); err != nil {
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
//	    sessCtx := mongoTx.SessionContext()
//	    _, err := collection.InsertOne(sessCtx, doc)
//	    return err
//	})
type MongoSessionProvider interface {
	Transaction

	// Session returns the MongoDB session.
	// Use for advanced session operations.
	Session() mongo.Session

	// SessionContext returns the MongoDB session context.
	// Use this for all MongoDB operations within the transaction.
	SessionContext() mongo.SessionContext
}

// Compile-time checks
var _ Manager = (*MongoManager)(nil)
var _ Transaction = (*MongoTransaction)(nil)
var _ MongoSessionProvider = (*MongoTransaction)(nil)

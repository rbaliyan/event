// Package mongodb provides a MongoDB change stream transport implementation.
//
// This transport watches MongoDB for changes and delivers them as events.
// Publishing is implicit - writing to MongoDB triggers events automatically.
//
// Watch Levels:
//
// The transport supports three levels of change stream watching:
//
//   - Collection-level: Watch a specific collection
//   - Database-level: Watch all collections in a database
//   - Cluster-level: Watch all databases in a cluster
//
// Features:
//   - Subscribe to changes (insert, update, delete, replace)
//   - Resume tokens for reliable change stream resumption
//   - Optional acknowledgment tracking via MongoDB collection
//   - Automatic reconnection with exponential backoff
//
// Limitations:
//   - Publish() is not supported - changes are triggered by database writes
//   - Only Broadcast delivery mode is supported (WorkerPool mode is ignored)
//   - All subscribers receive all changes
//
// Usage:
//
//	// Watch a specific collection
//	t, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	)
//
//	// Watch all collections in a database
//	t, _ := mongodb.New(db) // No WithCollection = database-level
//
//	// Watch all databases in a cluster
//	t, _ := mongodb.NewClusterWatch(client)
//
//	// Register and subscribe
//	t.RegisterEvent(ctx, "order-changes")
//	sub, _ := t.Subscribe(ctx, "order-changes")
//
//	// Process changes
//	for msg := range sub.Messages() {
//	    var change mongodb.ChangeEvent
//	    bson.Unmarshal(msg.Payload(), &change)
//	    fmt.Printf("Change in %s.%s: %s\n",
//	        change.Database, change.Collection, change.OperationType)
//	    msg.Ack(nil)
//	}
//
// The transport automatically watches and delivers change events.
// "Publishing" happens implicitly when documents are inserted/updated/deleted.
package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/channel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Errors
var (
	ErrClientRequired      = errors.New("mongodb client is required")
	ErrDatabaseRequired    = errors.New("mongodb database is required")
	ErrPublishNotSupported = errors.New("mongodb transport does not support Publish; changes are triggered by database writes")
)

// WatchLevel indicates what level of MongoDB hierarchy to watch.
type WatchLevel int

const (
	// WatchLevelCollection watches a single collection.
	WatchLevelCollection WatchLevel = iota
	// WatchLevelDatabase watches all collections in a database.
	WatchLevelDatabase
	// WatchLevelCluster watches all databases in a cluster.
	WatchLevelCluster
)

// OperationType represents the type of change operation
type OperationType string

const (
	OperationInsert  OperationType = "insert"
	OperationUpdate  OperationType = "update"
	OperationReplace OperationType = "replace"
	OperationDelete  OperationType = "delete"
)

// FullDocumentOption specifies how to return full documents in change events.
//
// For insert and replace operations, the full document is always included.
// This option controls behavior for update and delete operations.
type FullDocumentOption string

const (
	// FullDocumentDefault returns the full document only for insert and replace.
	// Update events only include the changed fields in UpdateDescription.
	// Delete events don't include the document.
	FullDocumentDefault FullDocumentOption = "default"

	// FullDocumentUpdateLookup performs a lookup to return the current document
	// for update events. Note: the document returned is the current state,
	// which may have been modified by subsequent updates.
	// This is the most commonly used option for update events.
	FullDocumentUpdateLookup FullDocumentOption = "updateLookup"

	// FullDocumentWhenAvailable returns the post-image if available.
	// Requires MongoDB 6.0+ with document pre/post images enabled on the collection.
	// Falls back gracefully if not available.
	FullDocumentWhenAvailable FullDocumentOption = "whenAvailable"

	// FullDocumentRequired returns the post-image or fails if not available.
	// Requires MongoDB 6.0+ with document pre/post images enabled on the collection.
	// Use this when you must have the full document for every event.
	FullDocumentRequired FullDocumentOption = "required"
)

// toDriverOption converts our FullDocumentOption to MongoDB driver's option.
func (f FullDocumentOption) toDriverOption() options.FullDocument {
	switch f {
	case FullDocumentUpdateLookup:
		return options.UpdateLookup
	case FullDocumentWhenAvailable:
		return options.WhenAvailable
	case FullDocumentRequired:
		return options.Required
	default:
		return options.Default
	}
}

// ChangeEvent represents a MongoDB change stream event.
// This struct is JSON-serializable for compatibility with the event system's default codec.
type ChangeEvent struct {
	ID            string            `json:"id"`
	OperationType OperationType     `json:"operation_type"`
	Database      string            `json:"database"`
	Collection    string            `json:"collection"`
	DocumentKey   string            `json:"document_key"`             // String representation of _id (hex for ObjectID, string for others)
	FullDocument  json.RawMessage   `json:"full_document,omitempty"`  // Raw JSON of the full document
	UpdateDesc    *UpdateDescription `json:"update_description,omitempty"`
	Timestamp     time.Time         `json:"timestamp"`
	Namespace     string            `json:"namespace"` // "database.collection" format
}

// UpdateDescription contains details about an update operation
type UpdateDescription struct {
	UpdatedFields   map[string]any `json:"updated_fields,omitempty"`
	RemovedFields   []string       `json:"removed_fields,omitempty"`
	TruncatedArrays []string       `json:"truncated_arrays,omitempty"`
}

// ResumeTokenStore persists resume tokens for reliable change stream resumption.
// Implement this interface to store tokens in MongoDB, Redis, or another backend.
type ResumeTokenStore interface {
	// Load retrieves the last resume token for a collection.
	// Returns nil if no token exists.
	Load(ctx context.Context, collection string) (bson.Raw, error)

	// Save persists a resume token for a collection.
	Save(ctx context.Context, collection string, token bson.Raw) error
}

// AckStore tracks which change events have been acknowledged.
// This enables at-least-once delivery by tracking processed events.
type AckStore interface {
	// Store marks an event as pending (not yet acknowledged).
	Store(ctx context.Context, eventID string) error

	// Ack marks an event as acknowledged (successfully processed).
	Ack(ctx context.Context, eventID string) error

	// IsPending checks if an event is still pending acknowledgment.
	IsPending(ctx context.Context, eventID string) (bool, error)
}

// Default collection name for storing resume tokens
const DefaultResumeTokenCollection = "_event_resume_tokens"

// Transport implements transport.Transport using MongoDB change streams.
type Transport struct {
	status           int32
	client           *mongo.Client   // For cluster-level watch
	db               *mongo.Database // For database/collection-level watch
	collectionName   string          // For collection-level watch (empty = database-level)
	watchLevel       WatchLevel
	channelTransport *channel.Transport // Internal channel transport for fan-out
	registeredEvents sync.Map           // map[string]struct{} - tracks registered event names
	logger           *slog.Logger
	onError          func(error)
	bufferSize       int
	resumeTokenStore ResumeTokenStore
	resumeTokenID    string   // Unique identifier for resume tokens (default: hostname)
	ackStore         AckStore
	disableResume    bool // If true, don't persist resume tokens

	// Change stream options
	pipeline     mongo.Pipeline
	fullDocument FullDocumentOption
	batchSize    *int32
	maxAwaitTime *time.Duration

	// Watcher state
	watcherCancel context.CancelFunc
	watcherWg     sync.WaitGroup

	// Payload options
	fullDocumentOnly bool // If true, send only fullDocument as payload instead of ChangeEvent
}

// Option configures the MongoDB transport
type Option func(*Transport)

// WithCollection sets the collection to watch for changes.
func WithCollection(name string) Option {
	return func(t *Transport) {
		t.collectionName = name
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(t *Transport) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// WithErrorHandler sets the error callback.
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithBufferSize sets the default buffer size for subscriptions.
func WithBufferSize(size int) Option {
	return func(t *Transport) {
		if size > 0 {
			t.bufferSize = size
		}
	}
}

// WithResumeTokenStore sets a custom store for persisting resume tokens.
// By default, the transport automatically stores resume tokens in the
// "_event_resume_tokens" collection. Use this option to override the default.
func WithResumeTokenStore(store ResumeTokenStore) Option {
	return func(t *Transport) {
		t.resumeTokenStore = store
	}
}

// WithResumeTokenCollection sets a custom database and collection for resume tokens.
// This is a convenience wrapper around WithResumeTokenStore.
//
// Example:
//
//	// Store resume tokens in a different database
//	transport, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithResumeTokenCollection(client.Database("internal"), "_resume_tokens"),
//	)
func WithResumeTokenCollection(db *mongo.Database, collectionName string) Option {
	return func(t *Transport) {
		t.resumeTokenStore = NewMongoResumeTokenStore(db.Collection(collectionName))
	}
}

// WithoutResume disables resume token persistence.
//
// Without resume tokens:
//   - On restart, the change stream starts from the CURRENT position (latest)
//   - Any changes that occurred while the service was down are MISSED
//
// With resume tokens (default):
//   - On restart, the change stream resumes from where it left off
//   - No changes are missed (as long as they're within MongoDB's oplog window)
//
// Use this only for scenarios where missing changes during restarts is acceptable,
// such as real-time dashboards that don't need historical accuracy.
func WithoutResume() Option {
	return func(t *Transport) {
		t.disableResume = true
	}
}

// WithResumeTokenID sets a unique identifier for resume token storage.
// This allows multiple instances to maintain their own resume positions.
//
// By default, the hostname is used. Each instance will:
//   - Store its own resume token with key: "namespace:id"
//   - Resume from its own last position on restart
//   - Process events independently (may cause duplicates across instances)
//
// Use cases:
//   - Multiple instances processing the same collection independently
//   - Instance-specific recovery after crashes
//   - Development/testing with multiple local instances
//
// For shared resume tokens (all instances resume from the same position),
// set the same ID across all instances or use an empty string.
//
// Example:
//
//	// Each instance uses its own resume token
//	mongodb.New(db, mongodb.WithResumeTokenID(os.Getenv("INSTANCE_ID")))
//
//	// Shared resume token across all instances
//	mongodb.New(db, mongodb.WithResumeTokenID("shared"))
func WithResumeTokenID(id string) Option {
	return func(t *Transport) {
		t.resumeTokenID = id
	}
}

// WithAckStore sets the store for tracking acknowledgments.
// This enables at-least-once delivery semantics.
func WithAckStore(store AckStore) Option {
	return func(t *Transport) {
		t.ackStore = store
	}
}

// WithPipeline sets an aggregation pipeline to filter change events.
//
// Example - only watch insert and update operations:
//
//	pipeline := mongo.Pipeline{
//	    {{Key: "$match", Value: bson.M{
//	        "operationType": bson.M{"$in": []string{"insert", "update"}},
//	    }}},
//	}
//	mongodb.WithPipeline(pipeline)
func WithPipeline(pipeline mongo.Pipeline) Option {
	return func(t *Transport) {
		t.pipeline = pipeline
	}
}

// WithFullDocument configures full document lookup for update events.
//
// Use one of the FullDocument* constants:
//   - FullDocumentDefault: Only include full document for insert/replace
//   - FullDocumentUpdateLookup: Lookup current document for updates (most common)
//   - FullDocumentWhenAvailable: Return post-image if available (MongoDB 6.0+)
//   - FullDocumentRequired: Require post-image or fail (MongoDB 6.0+)
//
// Example:
//
//	mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
func WithFullDocument(option FullDocumentOption) Option {
	return func(t *Transport) {
		t.fullDocument = option
	}
}

// WithBatchSize sets the batch size for change stream operations.
func WithBatchSize(size int32) Option {
	return func(t *Transport) {
		t.batchSize = &size
	}
}

// WithMaxAwaitTime sets the maximum time to wait for new changes.
func WithMaxAwaitTime(d time.Duration) Option {
	return func(t *Transport) {
		t.maxAwaitTime = &d
	}
}

// WithFullDocumentOnly configures the transport to send only the fullDocument
// as the message payload instead of the entire ChangeEvent.
//
// This is useful when you want to subscribe with your document type directly:
//
//	// Instead of subscribing to mongodb.ChangeEvent:
//	event.New[mongodb.ChangeEvent]("order.created")
//
//	// You can subscribe to your document type directly:
//	event.New[Order]("order.created")
//
// IMPORTANT: This requires WithFullDocument(FullDocumentUpdateLookup) or similar
// to ensure fullDocument is populated. Without it:
//   - Insert: fullDocument is always present
//   - Update: fullDocument is empty unless FullDocumentUpdateLookup is set
//   - Delete: fullDocument is always empty (event will be skipped)
//
// Events where fullDocument is empty will be skipped (not delivered to subscribers).
// The operation type, database, and collection are still available in message metadata.
//
// Example:
//
//	transport, _ := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	    mongodb.WithFullDocumentOnly(), // Send just the document
//	)
//
//	// Now subscribe with your type directly
//	orderEvent := event.New[Order]("order.created")
//	orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
//	    fmt.Println("Order:", order.ID)
//	    return nil
//	})
func WithFullDocumentOnly() Option {
	return func(t *Transport) {
		t.fullDocumentOnly = true
	}
}

// New creates a new MongoDB change stream transport.
//
// Watch levels:
//   - With WithCollection("name"): watches a specific collection
//   - Without WithCollection: watches all collections in the database
//
// For cluster-level watching (all databases), use NewClusterWatch instead.
//
// Resume tokens are automatically persisted to enable reliable resumption
// after restarts. Use WithoutResume() to disable this behavior.
//
// Example:
//
//	// Watch a specific collection
//	t, err := mongodb.New(db,
//	    mongodb.WithCollection("orders"),
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
//
//	// Watch all collections in the database
//	t, err := mongodb.New(db)
//
//	// Watch all collections with options
//	t, err := mongodb.New(db,
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
func New(db *mongo.Database, opts ...Option) (*Transport, error) {
	if db == nil {
		return nil, ErrDatabaseRequired
	}

	t := &Transport{
		status:     1,
		db:         db,
		client:     db.Client(),
		logger:     transport.Logger("transport>mongodb"),
		onError:    func(error) {},
		bufferSize: 100,
	}

	for _, opt := range opts {
		opt(t)
	}

	// Create internal channel transport for fan-out
	t.channelTransport = channel.New(
		channel.WithBufferSize(uint(t.bufferSize)),
		channel.WithLogger(t.logger),
	)

	// Determine watch level
	if t.collectionName != "" {
		t.watchLevel = WatchLevelCollection
	} else {
		t.watchLevel = WatchLevelDatabase
	}

	// Default resume token ID to hostname
	if t.resumeTokenID == "" {
		if hostname, err := os.Hostname(); err == nil {
			t.resumeTokenID = hostname
		} else {
			t.resumeTokenID = "default"
		}
	}

	// Auto-create resume token store if not disabled and not provided
	if !t.disableResume && t.resumeTokenStore == nil {
		t.resumeTokenStore = NewMongoResumeTokenStore(db.Collection(DefaultResumeTokenCollection))
		t.logger.Debug("using default resume token collection", "collection", DefaultResumeTokenCollection)
	}

	return t, nil
}

// NewClusterWatch creates a MongoDB transport that watches all databases.
//
// This watches the entire MongoDB cluster/deployment for changes across
// all databases and collections. Use this when you need to monitor
// changes across your entire MongoDB deployment.
//
// Example:
//
//	// Watch all databases in the cluster
//	t, err := mongodb.NewClusterWatch(client)
//
//	// With options
//	t, err := mongodb.NewClusterWatch(client,
//	    mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
//	)
//
//	// Subscribe to changes
//	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
//	    fmt.Printf("Change in %s.%s: %s\n",
//	        change.Database, change.Collection, change.OperationType)
//	    return nil
//	})
func NewClusterWatch(client *mongo.Client, opts ...Option) (*Transport, error) {
	if client == nil {
		return nil, ErrClientRequired
	}

	// Use "admin" database for storing resume tokens
	adminDB := client.Database("admin")

	t := &Transport{
		status:     1,
		client:     client,
		db:         adminDB,
		watchLevel: WatchLevelCluster,
		logger:     transport.Logger("transport>mongodb"),
		onError:    func(error) {},
		bufferSize: 100,
	}

	for _, opt := range opts {
		opt(t)
	}

	// Create internal channel transport for fan-out
	t.channelTransport = channel.New(
		channel.WithBufferSize(uint(t.bufferSize)),
		channel.WithLogger(t.logger),
	)

	// Default resume token ID to hostname
	if t.resumeTokenID == "" {
		if hostname, err := os.Hostname(); err == nil {
			t.resumeTokenID = hostname
		} else {
			t.resumeTokenID = "default"
		}
	}

	// Auto-create resume token store if not disabled and not provided
	if !t.disableResume && t.resumeTokenStore == nil {
		t.resumeTokenStore = NewMongoResumeTokenStore(adminDB.Collection(DefaultResumeTokenCollection))
		t.logger.Debug("using default resume token collection", "database", "admin", "collection", DefaultResumeTokenCollection)
	}

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent creates resources for an event and starts watching.
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Delegate to channel transport
	if err := t.channelTransport.RegisterEvent(ctx, name); err != nil {
		return err
	}

	// Track registered event name
	t.registeredEvents.Store(name, struct{}{})

	// Start the change stream watcher if not already running
	t.startWatcher()

	t.logger.Debug("registered event", "event", name, "collection", t.collectionName)
	return nil
}

// UnregisterEvent cleans up event resources.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Delegate to channel transport
	if err := t.channelTransport.UnregisterEvent(ctx, name); err != nil {
		return err
	}

	// Remove from tracked events
	t.registeredEvents.Delete(name)

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish is not supported - changes are triggered by database writes.
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	return ErrPublishNotSupported
}

// Subscribe creates a subscription to receive change events.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	// Delegate to channel transport - it handles all the fan-out logic
	sub, err := t.channelTransport.Subscribe(ctx, name, opts...)
	if err != nil {
		return nil, err
	}

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID())
	return sub, nil
}

// Close shuts down the transport.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Stop the watcher
	if t.watcherCancel != nil {
		t.watcherCancel()
	}
	t.watcherWg.Wait()

	// Close the channel transport (closes all subscriptions)
	if err := t.channelTransport.Close(ctx); err != nil {
		t.logger.Warn("failed to close channel transport", "error", err)
	}

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check.
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Ping MongoDB
	pingStart := time.Now()
	err := t.db.Client().Ping(ctx, nil)
	pingLatency := time.Since(pingStart)

	if err != nil {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "mongodb ping failed"
		result.Latency = time.Since(start)
		result.Details["ping_error"] = err.Error()
		return result
	}

	// Get stats from channel transport
	channelHealth := t.channelTransport.Health(ctx)

	result.Status = transport.HealthStatusHealthy
	result.Message = "mongodb transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "mongodb-changestream"
	result.Details["watch_level"] = t.watchLevelString()
	if t.db != nil {
		result.Details["database"] = t.db.Name()
	}
	if t.collectionName != "" {
		result.Details["collection"] = t.collectionName
	}
	result.Details["events"] = channelHealth.Details["events"]
	result.Details["subscribers"] = channelHealth.Details["subscribers"]
	result.Details["ping_latency_ms"] = pingLatency.Milliseconds()

	return result
}

// startWatcher starts the change stream watcher goroutine.
func (t *Transport) startWatcher() {
	// Only start once
	if t.watcherCancel != nil {
		return
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	t.watcherCancel = cancel

	t.watcherWg.Add(1)
	go func() {
		defer t.watcherWg.Done()
		t.watchLoop(watchCtx)
	}()
}

// watchLoop continuously watches the change stream with reconnection.
func (t *Transport) watchLoop(ctx context.Context) {
	backoff := base.NewBackoff()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.watchOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			backoffDuration := backoff.Next()
			t.logger.Error("change stream error, reconnecting",
				"error", err, "backoff", backoffDuration)

			// Check if this is a ChangeStreamHistoryLost error
			// This means the resume token points to a position no longer in the oplog
			if isChangeStreamHistoryLost(err) {
				t.logger.Warn("resume token is stale (oplog rolled past), clearing and starting fresh")
				if t.resumeTokenStore != nil {
					resumeKey := t.resumeTokenKey()
					if clearErr := t.resumeTokenStore.Save(ctx, resumeKey, nil); clearErr != nil {
						t.logger.Error("failed to clear stale resume token", "error", clearErr)
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(backoffDuration):
			}
			continue
		}

		// Reset backoff on clean exit (shouldn't happen normally)
		backoff.Reset()
	}
}

// isChangeStreamHistoryLost checks if the error indicates the resume token
// is no longer valid because the oplog has rolled past that position.
func isChangeStreamHistoryLost(err error) bool {
	if err == nil {
		return false
	}
	// Check for MongoDB error code 286 (ChangeStreamHistoryLost)
	// or the error message containing the error name
	errStr := err.Error()
	return strings.Contains(errStr, "ChangeStreamHistoryLost") ||
		strings.Contains(errStr, "resume point may no longer be in the oplog")
}

// watchOnce creates and processes a single change stream.
func (t *Transport) watchOnce(ctx context.Context) error {
	// Build change stream options
	csOpts := options.ChangeStream()

	if t.fullDocument != "" {
		csOpts.SetFullDocument(t.fullDocument.toDriverOption())
	}
	if t.batchSize != nil {
		csOpts.SetBatchSize(*t.batchSize)
	}
	if t.maxAwaitTime != nil {
		csOpts.SetMaxAwaitTime(*t.maxAwaitTime)
	}

	// Resume token key depends on watch level
	resumeKey := t.resumeTokenKey()

	// Try to resume from stored token
	var hasExistingToken bool
	if t.resumeTokenStore != nil {
		token, err := t.resumeTokenStore.Load(ctx, resumeKey)
		if err != nil {
			t.logger.Warn("failed to load resume token", "error", err)
		} else if token != nil {
			csOpts.SetResumeAfter(token)
			hasExistingToken = true
			t.logger.Debug("resuming from stored token", "key", resumeKey)
		}
	}

	// Open change stream based on watch level
	var cs *mongo.ChangeStream
	var err error

	switch t.watchLevel {
	case WatchLevelCollection:
		cs, err = t.db.Collection(t.collectionName).Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened", "level", "collection",
			"database", t.db.Name(), "collection", t.collectionName)
	case WatchLevelDatabase:
		cs, err = t.db.Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened", "level", "database",
			"database", t.db.Name())
	case WatchLevelCluster:
		cs, err = t.client.Watch(ctx, t.pipeline, csOpts)
		t.logger.Info("change stream opened", "level", "cluster")
	}

	if err != nil {
		return err
	}
	defer cs.Close(ctx)

	// If no existing token, save the current position and start from here
	// This prevents processing historical oplog events on first start
	if !hasExistingToken && t.resumeTokenStore != nil {
		// Get a single event to establish our position, or use current token
		// We need to call TryNext to get a resume token without blocking
		if cs.TryNext(ctx) {
			// Got an event - save token and process it
			if err := t.resumeTokenStore.Save(ctx, resumeKey, cs.ResumeToken()); err != nil {
				t.logger.Warn("failed to save initial resume token", "error", err)
			} else {
				t.logger.Info("saved initial resume token, starting from current position", "key", resumeKey)
			}
			// Process this first event
			if err := t.processChange(ctx, cs); err != nil {
				t.logger.Error("failed to process change", "error", err)
			}
		} else if cs.ResumeToken() != nil {
			// No event yet, but we have a resume token from the cursor
			if err := t.resumeTokenStore.Save(ctx, resumeKey, cs.ResumeToken()); err != nil {
				t.logger.Warn("failed to save initial resume token", "error", err)
			} else {
				t.logger.Info("saved initial resume token, starting from current position", "key", resumeKey)
			}
		}
	}

	// Process changes
	for cs.Next(ctx) {
		if err := t.processChange(ctx, cs); err != nil {
			t.logger.Error("failed to process change", "error", err)
			// Continue processing other changes
		}

		// Persist resume token
		if t.resumeTokenStore != nil {
			if err := t.resumeTokenStore.Save(ctx, resumeKey, cs.ResumeToken()); err != nil {
				t.logger.Warn("failed to save resume token", "error", err)
			}
		}
	}

	return cs.Err()
}

// resumeTokenKey returns the key used for storing resume tokens.
// Format: "namespace:id" where namespace is based on watch level
// and id is the resumeTokenID (defaults to hostname).
func (t *Transport) resumeTokenKey() string {
	var namespace string
	switch t.watchLevel {
	case WatchLevelCollection:
		namespace = t.db.Name() + "." + t.collectionName
	case WatchLevelDatabase:
		namespace = t.db.Name() + ".*"
	case WatchLevelCluster:
		namespace = "*.*"
	default:
		namespace = "default"
	}
	return namespace + ":" + t.resumeTokenID
}

// watchLevelString returns a human-readable string for the watch level.
func (t *Transport) watchLevelString() string {
	switch t.watchLevel {
	case WatchLevelCollection:
		return "collection"
	case WatchLevelDatabase:
		return "database"
	case WatchLevelCluster:
		return "cluster"
	default:
		return "unknown"
	}
}

// processChange processes a single change event.
func (t *Transport) processChange(ctx context.Context, cs *mongo.ChangeStream) error {
	// Decode raw change document
	var raw bson.M
	if err := cs.Decode(&raw); err != nil {
		return err
	}

	// Extract change event data
	changeEvent := t.extractChangeEvent(raw)

	// Determine payload and content type based on mode
	var payload []byte
	var err error
	var contentType string

	if t.fullDocumentOnly {
		contentType = "application/bson"
		// Send the raw fullDocument BSON - preserves all MongoDB types
		if fullDoc, ok := raw["fullDocument"]; ok && fullDoc != nil {
			// Marshal the raw fullDocument directly as BSON
			payload, err = bson.Marshal(fullDoc)
			if err != nil {
				return err
			}
		}
		if len(payload) == 0 {
			// For delete events, send just the document ID as a string
			// This allows subscribers using event.Event[string] to receive the ID directly
			if changeEvent.OperationType == OperationDelete {
				// DocumentKey is already extracted as string in changeEvent
				if changeEvent.DocumentKey != "" {
					payload = []byte(changeEvent.DocumentKey)
					// Use plain text content type for string ID
					contentType = "text/plain"
				}
			}
		}
		if len(payload) == 0 {
			// Skip events without fullDocument and without documentKey
			t.logger.Debug("skipping event without fullDocument",
				"operation", changeEvent.OperationType,
				"document_key", changeEvent.DocumentKey)
			return nil
		}
	} else {
		contentType = "application/json"
		// Send the entire ChangeEvent as JSON (for mongodb.ChangeEvent subscribers)
		payload, err = json.Marshal(changeEvent)
		if err != nil {
			return err
		}
	}

	// Create message with ack function
	msg := transport.NewMessageWithAck(
		changeEvent.ID,
		"mongodb://"+changeEvent.Database+"/"+changeEvent.Collection,
		payload,
		map[string]string{
			"Content-Type": contentType,
			"operation":    string(changeEvent.OperationType),
			"database":     changeEvent.Database,
			"collection":   changeEvent.Collection,
			"namespace":    changeEvent.Namespace,
			"document_key": changeEvent.DocumentKey,
		},
		0,
		func(err error) error {
			if err == nil && t.ackStore != nil {
				return t.ackStore.Ack(ctx, changeEvent.ID)
			}
			return nil
		},
	)

	// Store as pending if ack store configured
	if t.ackStore != nil {
		if err := t.ackStore.Store(ctx, changeEvent.ID); err != nil {
			t.logger.Warn("failed to store pending event", "event_id", changeEvent.ID, "error", err)
		}
	}

	// Publish to all registered events via channel transport
	// Each registered event's subscribers receive the change
	t.registeredEvents.Range(func(key, value any) bool {
		eventName := key.(string)
		if err := t.channelTransport.Publish(ctx, eventName, msg); err != nil {
			t.logger.Warn("failed to publish to channel transport", "event", eventName, "error", err)
		}
		return true
	})

	return nil
}

// extractChangeEvent extracts a ChangeEvent from raw BSON.
func (t *Transport) extractChangeEvent(raw bson.M) ChangeEvent {
	event := ChangeEvent{
		Timestamp: time.Now(),
	}

	// Extract namespace (database and collection) from ns field
	// MongoDB change events include: ns: {db: "mydb", coll: "mycoll"}
	if ns, ok := raw["ns"].(bson.M); ok {
		if db, ok := ns["db"].(string); ok {
			event.Database = db
		}
		if coll, ok := ns["coll"].(string); ok {
			event.Collection = coll
		}
	}

	// Fallback to configured values for collection-level watch
	if event.Database == "" && t.db != nil {
		event.Database = t.db.Name()
	}
	if event.Collection == "" && t.collectionName != "" {
		event.Collection = t.collectionName
	}

	// Build namespace string
	event.Namespace = event.Database + "." + event.Collection

	// Extract _id (resume token) as event ID
	if id, ok := raw["_id"].(bson.M); ok {
		if data, ok := id["_data"].(string); ok {
			event.ID = data
		}
	}
	if event.ID == "" {
		event.ID = transport.NewID()
	}

	// Extract operation type
	if opType, ok := raw["operationType"].(string); ok {
		event.OperationType = OperationType(opType)
	}

	// Extract document key - handle any _id type (ObjectID, string, UUID, etc.)
	if docKey, ok := raw["documentKey"].(bson.M); ok {
		event.DocumentKey = formatDocumentKey(docKey["_id"])
	}

	// Extract full document and convert to JSON
	if fullDoc, ok := raw["fullDocument"].(bson.M); ok {
		if jsonData, err := bsonToJSON(fullDoc); err == nil {
			event.FullDocument = jsonData
		}
	} else if fullDoc, ok := raw["fullDocument"].(bson.Raw); ok {
		// Convert bson.Raw to bson.M first, then to JSON
		var doc bson.M
		if err := bson.Unmarshal(fullDoc, &doc); err == nil {
			if jsonData, err := bsonToJSON(doc); err == nil {
				event.FullDocument = jsonData
			}
		}
	}

	// Extract update description
	if updateDesc, ok := raw["updateDescription"].(bson.M); ok {
		event.UpdateDesc = &UpdateDescription{}
		if updated, ok := updateDesc["updatedFields"].(bson.M); ok {
			// Convert bson.M to map[string]any (they're compatible but need explicit conversion)
			event.UpdateDesc.UpdatedFields = bsonMToMap(updated)
		}
		if removed, ok := updateDesc["removedFields"].(bson.A); ok {
			for _, f := range removed {
				if s, ok := f.(string); ok {
					event.UpdateDesc.RemovedFields = append(event.UpdateDesc.RemovedFields, s)
				}
			}
		}
	}

	// Extract timestamp
	if ts, ok := raw["clusterTime"].(primitive.Timestamp); ok {
		event.Timestamp = time.Unix(int64(ts.T), 0)
	}

	return event
}

// formatDocumentKey converts any MongoDB _id type to a string representation.
func formatDocumentKey(id any) string {
	if id == nil {
		return ""
	}
	switch v := id.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case string:
		return v
	case primitive.Binary:
		// UUID or other binary types
		return fmt.Sprintf("%x", v.Data)
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	default:
		// Fallback: try to convert via JSON
		if data, err := json.Marshal(v); err == nil {
			return string(data)
		}
		return fmt.Sprintf("%v", v)
	}
}

// bsonToJSON converts a BSON document to JSON, handling MongoDB-specific types.
func bsonToJSON(doc bson.M) (json.RawMessage, error) {
	// Convert MongoDB types to JSON-friendly types
	converted := convertBSONTypes(doc)
	return json.Marshal(converted)
}

// convertBSONTypes recursively converts BSON-specific types to JSON-friendly types.
// Uses MongoDB Extended JSON format for special types so they can be unmarshaled
// back into their original Go types (e.g., primitive.ObjectID).
func convertBSONTypes(v any) any {
	switch val := v.(type) {
	case bson.M:
		result := make(map[string]any, len(val))
		for k, v := range val {
			result[k] = convertBSONTypes(v)
		}
		return result
	case bson.A:
		result := make([]any, len(val))
		for i, v := range val {
			result[i] = convertBSONTypes(v)
		}
		return result
	case primitive.ObjectID:
		// Use Extended JSON format so primitive.ObjectID can unmarshal it
		return map[string]string{"$oid": val.Hex()}
	case primitive.DateTime:
		// Use ISO string format - compatible with Go's time.Time JSON unmarshal
		return time.Unix(int64(val)/1000, (int64(val)%1000)*1000000).Format(time.RFC3339Nano)
	case primitive.Timestamp:
		// Use ISO string format for timestamps too
		return time.Unix(int64(val.T), 0).Format(time.RFC3339Nano)
	case primitive.Binary:
		return map[string]any{"$binary": map[string]any{"base64": val.Data, "subType": fmt.Sprintf("%02x", val.Subtype)}}
	case primitive.Decimal128:
		return val.String()
	default:
		return val
	}
}

// bsonMToMap converts bson.M to map[string]any with type conversion.
func bsonMToMap(m bson.M) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = convertBSONTypes(v)
	}
	return result
}

// Compile-time checks
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
)

package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// Relay polls the outbox and publishes messages to the transport.
//
// The Relay is the background worker that processes the outbox:
//  1. Polls the store for pending messages
//  2. Publishes each message to the transport
//  3. Marks messages as published (or failed)
//  4. Periodically cleans up old published messages
//
// The Relay should be run as a separate goroutine or process. Multiple
// relay instances can run concurrently - the store uses locking to
// prevent duplicate processing.
//
// Example:
//
//	store := outbox.NewPostgresStore(db)
//	relay := outbox.NewRelay(store, transport).
//	    WithPollDelay(100 * time.Millisecond).
//	    WithBatchSize(100).
//	    WithCleanupAge(7 * 24 * time.Hour)
//
//	// Start relay in background
//	ctx, cancel := context.WithCancel(context.Background())
//	go func() {
//	    if err := relay.Start(ctx); err != nil && err != context.Canceled {
//	        log.Error("relay stopped", "error", err)
//	    }
//	}()
//
//	// Shutdown gracefully
//	cancel()
type Relay struct {
	store      Store
	transport  transport.Transport
	pollDelay  time.Duration
	batchSize  int
	logger     *slog.Logger
	cleanupAge time.Duration // How old published messages should be before deletion
}

// NewRelay creates a new outbox relay.
//
// The relay polls the store for pending messages and publishes them to
// the transport. Default configuration:
//   - Poll delay: 100ms
//   - Batch size: 100 messages
//   - Cleanup age: 24 hours
//
// Parameters:
//   - store: The outbox store to poll for messages
//   - t: The transport to publish messages to
//
// Example:
//
//	relay := outbox.NewRelay(store, transport)
//	go relay.Start(ctx)
func NewRelay(store Store, t transport.Transport) *Relay {
	return &Relay{
		store:      store,
		transport:  t,
		pollDelay:  100 * time.Millisecond,
		batchSize:  100,
		logger:     slog.Default().With("component", "outbox.relay"),
		cleanupAge: 24 * time.Hour,
	}
}

// WithPollDelay sets the polling interval.
//
// Lower values mean lower latency but higher database load.
// Higher values reduce load but increase message delivery latency.
//
// Parameters:
//   - d: The interval between polls (e.g., 100*time.Millisecond)
//
// Returns the relay for method chaining.
//
// Example:
//
//	relay := outbox.NewRelay(store, transport).
//	    WithPollDelay(50 * time.Millisecond) // Low latency
func (r *Relay) WithPollDelay(d time.Duration) *Relay {
	r.pollDelay = d
	return r
}

// WithBatchSize sets the number of messages to process per poll.
//
// Larger batches are more efficient but may cause longer processing times.
// Smaller batches provide more even processing but more database queries.
//
// Parameters:
//   - size: Maximum messages to fetch per poll
//
// Returns the relay for method chaining.
//
// Example:
//
//	relay := outbox.NewRelay(store, transport).
//	    WithBatchSize(50) // Smaller batches for faster individual processing
func (r *Relay) WithBatchSize(size int) *Relay {
	r.batchSize = size
	return r
}

// WithLogger sets a custom logger.
//
// The logger is used for error and debug messages during relay operation.
//
// Parameters:
//   - l: The slog logger to use
//
// Returns the relay for method chaining.
//
// Example:
//
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	relay := outbox.NewRelay(store, transport).
//	    WithLogger(logger.With("service", "outbox"))
func (r *Relay) WithLogger(l *slog.Logger) *Relay {
	r.logger = l
	return r
}

// WithCleanupAge sets how old published messages should be before deletion.
//
// Messages older than this age are deleted during periodic cleanup.
// Longer retention is useful for debugging but uses more storage.
//
// Parameters:
//   - age: How long to keep published messages
//
// Returns the relay for method chaining.
//
// Example:
//
//	relay := outbox.NewRelay(store, transport).
//	    WithCleanupAge(7 * 24 * time.Hour) // Keep for 7 days
func (r *Relay) WithCleanupAge(age time.Duration) *Relay {
	r.cleanupAge = age
	return r
}

// Start begins polling the outbox and publishing messages.
//
// This method blocks until the context is cancelled. It runs two loops:
//  1. Polling loop: Fetches and publishes pending messages
//  2. Cleanup loop: Removes old published messages hourly
//
// Parameters:
//   - ctx: Context for cancellation - cancel to stop the relay
//
// Returns context.Canceled when the context is cancelled.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//
//	go func() {
//	    if err := relay.Start(ctx); err != nil {
//	        if err != context.Canceled {
//	            log.Error("relay error", "error", err)
//	        }
//	    }
//	}()
//
//	// Later, to stop:
//	cancel()
func (r *Relay) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.pollDelay)
	defer ticker.Stop()

	// Also start a cleanup ticker
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.publishPending(ctx)
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		}
	}
}

// publishPending fetches and publishes pending messages.
// This is the main processing loop that runs on each poll tick.
func (r *Relay) publishPending(ctx context.Context) {
	messages, err := r.store.GetPending(ctx, r.batchSize)
	if err != nil {
		r.logger.Error("failed to get pending messages", "error", err)
		return
	}

	for _, msg := range messages {
		if err := r.publishMessage(ctx, msg); err != nil {
			r.logger.Error("failed to publish message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			r.store.MarkFailed(ctx, msg.ID, err)
			continue
		}

		if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
			r.logger.Error("failed to mark message as published",
				"id", msg.ID,
				"error", err)
		}

		r.logger.Debug("published outbox message",
			"id", msg.ID,
			"event", msg.EventName,
			"event_id", msg.EventID)
	}
}

// publishMessage publishes a single message to the transport
func (r *Relay) publishMessage(ctx context.Context, msg *Message) error {
	// msg.Payload is already []byte - pass directly to transport
	transportMsg := message.New(
		msg.EventID,
		"outbox",
		msg.Payload,
		msg.Metadata,
		trace.SpanContext{},
	)

	return r.transport.Publish(ctx, msg.EventName, transportMsg)
}

// cleanup removes old published messages
func (r *Relay) cleanup(ctx context.Context) {
	deleted, err := r.store.Delete(ctx, r.cleanupAge)
	if err != nil {
		r.logger.Error("failed to cleanup old messages", "error", err)
		return
	}

	if deleted > 0 {
		r.logger.Info("cleaned up old outbox messages", "count", deleted)
	}
}

// PublishOnce processes pending messages once (for testing or manual triggering).
//
// Unlike Start(), this method returns immediately after processing one
// batch of messages. Useful for:
//   - Testing: Process messages synchronously in tests
//   - Manual triggers: Process messages on-demand (e.g., via admin API)
//   - Cron jobs: Run as a scheduled task instead of continuous polling
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//
// Example:
//
//	// In tests
//	func TestMessagePublishing(t *testing.T) {
//	    // Setup and store message...
//
//	    // Process synchronously
//	    relay.PublishOnce(ctx)
//
//	    // Verify message was published
//	}
//
//	// As admin endpoint
//	func handleManualPublish(w http.ResponseWriter, r *http.Request) {
//	    if err := relay.PublishOnce(r.Context()); err != nil {
//	        http.Error(w, err.Error(), 500)
//	        return
//	    }
//	    w.Write([]byte("ok"))
//	}
func (r *Relay) PublishOnce(ctx context.Context) error {
	r.publishPending(ctx)
	return nil
}

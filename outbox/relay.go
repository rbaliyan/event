package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
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
//	relay := outbox.NewRelay(store, transport,
//	    outbox.WithPollDelay(100 * time.Millisecond),
//	    outbox.WithBatchSize(100),
//	    outbox.WithCleanupAge(7 * 24 * time.Hour),
//	)
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
	store        Store
	transport    transport.Transport
	pollDelay    time.Duration
	batchSize    int
	logger       *slog.Logger
	cleanupAge   time.Duration // How old published messages should be before deletion
	metrics      *Metrics
	maxRetries   int              // 0 = unlimited retries (default)
	retryBackoff backoff.Strategy // nil = no backoff delay between retries
}

// RelayOption configures a Relay.
type RelayOption func(*relayOptions)

type relayOptions struct {
	pollDelay    time.Duration
	batchSize    int
	logger       *slog.Logger
	cleanupAge   time.Duration
	metrics      *Metrics
	maxRetries   int
	retryBackoff backoff.Strategy
}

// WithPollDelay sets the polling interval.
//
// Lower values mean lower latency but higher database load.
// Higher values reduce load but increase message delivery latency.
func WithPollDelay(d time.Duration) RelayOption {
	return func(o *relayOptions) {
		if d > 0 {
			o.pollDelay = d
		}
	}
}

// WithBatchSize sets the number of messages to process per poll.
func WithBatchSize(size int) RelayOption {
	return func(o *relayOptions) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// WithLogger sets a custom logger for the relay.
func WithLogger(l *slog.Logger) RelayOption {
	return func(o *relayOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithCleanupAge sets how old published messages should be before deletion.
func WithCleanupAge(age time.Duration) RelayOption {
	return func(o *relayOptions) {
		if age > 0 {
			o.cleanupAge = age
		}
	}
}

// WithMetrics enables OpenTelemetry metrics for the relay.
func WithMetrics(m *Metrics) RelayOption {
	return func(o *relayOptions) {
		if m != nil {
			o.metrics = m
		}
	}
}

// WithMaxRetries sets the maximum number of publish attempts before routing to DLQ.
// When a message's RetryCount reaches this limit and a DLQ store is configured,
// the message is moved to the DLQ instead of being retried.
// Set to 0 (default) for unlimited retries.
func WithMaxRetries(n int) RelayOption {
	return func(o *relayOptions) {
		if n > 0 {
			o.maxRetries = n
		}
	}
}

// WithRetryBackoff sets a backoff strategy for failed messages.
// On each failure, the relay skips the message until the backoff delay elapses
// based on the message's retry count.
func WithRetryBackoff(strategy backoff.Strategy) RelayOption {
	return func(o *relayOptions) {
		if strategy != nil {
			o.retryBackoff = strategy
		}
	}
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
//   - opts: Optional configuration options
//
// Example:
//
//	relay := outbox.NewRelay(store, transport,
//	    outbox.WithPollDelay(100 * time.Millisecond),
//	    outbox.WithBatchSize(100),
//	)
//	go relay.Start(ctx)
func NewRelay(store Store, t transport.Transport, opts ...RelayOption) *Relay {
	o := &relayOptions{
		pollDelay:  100 * time.Millisecond,
		batchSize:  100,
		cleanupAge: 24 * time.Hour,
	}
	for _, opt := range opts {
		opt(o)
	}

	logger := o.logger
	if logger == nil {
		logger = slog.Default().With("component", "outbox.relay")
	}

	return &Relay{
		store:        store,
		transport:    t,
		pollDelay:    o.pollDelay,
		batchSize:    o.batchSize,
		logger:       logger,
		cleanupAge:   o.cleanupAge,
		metrics:      o.metrics,
		maxRetries:   o.maxRetries,
		retryBackoff: o.retryBackoff,
	}
}

// log returns the configured logger.
func (r *Relay) log() *slog.Logger {
	return r.logger
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

	var consecutiveFailures int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			failures := r.publishPending(ctx)
			// Adaptive backpressure: back off polling when failures occur
			if r.retryBackoff != nil && failures > 0 {
				consecutiveFailures++
				delay := r.retryBackoff.NextDelay(consecutiveFailures)
				ticker.Reset(delay)
			} else if consecutiveFailures > 0 {
				consecutiveFailures = 0
				ticker.Reset(r.pollDelay)
			}
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		}
	}
}

// shouldSkip returns true if a message has exceeded the max retry limit.
// When max retries is configured, exhausted messages are re-marked as failed
// with a descriptive error to prevent infinite re-fetch loops.
func (r *Relay) shouldSkip(ctx context.Context, msg *Message) bool {
	if r.maxRetries > 0 && msg.RetryCount >= r.maxRetries {
		r.log().Warn("message exceeded max retries",
			"id", msg.ID,
			"event", msg.EventName,
			"retry_count", msg.RetryCount,
			"max_retries", r.maxRetries)
		// Re-mark as failed with descriptive error so GetPending doesn't re-fetch
		// (MarkFailed increments retry_count, pushing it further past the threshold)
		if markErr := r.store.MarkFailed(ctx, msg.ID, fmt.Errorf("exceeded max retries (%d)", r.maxRetries)); markErr != nil {
			r.log().Error("failed to mark exhausted message", "id", msg.ID, "error", markErr)
		}
		return true
	}
	return false
}

// publishPending fetches and publishes pending messages.
// This is the main processing loop that runs on each poll tick.
// Returns the number of messages that failed to publish.
func (r *Relay) publishPending(ctx context.Context) (failures int) {
	// Use ProcessPending for stores that support transactional processing
	// (PostgresStore). This holds row locks for the duration of processing,
	// preventing concurrent relays from picking up the same messages.
	if ps, ok := r.store.(interface {
		ProcessPending(ctx context.Context, limit int, fn func(msg *Message) error) error
	}); ok {
		if err := ps.ProcessPending(ctx, r.batchSize, func(msg *Message) error {
			if r.shouldSkip(ctx, msg) {
				return nil
			}
			if err := r.publishMessage(ctx, msg); err != nil {
				r.log().Error("failed to publish message",
					"id", msg.ID,
					"event", msg.EventName,
					"error", err)
				failures++
				return err
			}
			r.log().Debug("published outbox message",
				"id", msg.ID,
				"event", msg.EventName,
				"event_id", msg.EventID)
			return nil
		}); err != nil {
			r.log().Error("failed to process pending messages", "error", err)
			return failures + 1
		}
		return failures
	}

	// Fallback for stores without ProcessPending
	messages, err := r.store.GetPending(ctx, r.batchSize)
	if err != nil {
		r.log().Error("failed to get pending messages", "error", err)
		return 1
	}

	for _, msg := range messages {
		if r.shouldSkip(ctx, msg) {
			continue
		}

		if err := r.publishMessage(ctx, msg); err != nil {
			r.log().Error("failed to publish message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			if markErr := r.store.MarkFailed(ctx, msg.ID, err); markErr != nil {
				r.log().Error("failed to mark message as failed", "error", markErr)
			}
			failures++
			continue
		}

		if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
			r.log().Error("failed to mark message as published",
				"id", msg.ID,
				"error", err)
		}

		r.log().Debug("published outbox message",
			"id", msg.ID,
			"event", msg.EventName,
			"event_id", msg.EventID)
	}
	return failures
}

// publishMessage publishes a single message to the transport.
// If the message metadata contains W3C trace context headers, a child span is created
// to link the relay publish to the original transaction's trace.
func (r *Relay) publishMessage(ctx context.Context, msg *Message) error {
	start := time.Now()

	// Extract trace context from metadata if present (W3C traceparent/tracestate)
	var spanCtx trace.SpanContext
	if msg.Metadata != nil {
		carrier := propagation.MapCarrier(msg.Metadata)
		extracted := otel.GetTextMapPropagator().Extract(ctx, carrier)
		spanCtx = trace.SpanContextFromContext(extracted)
	}

	// Create a span for the relay publish operation
	if spanCtx.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)
	}
	tracer := otel.Tracer("outbox.relay")
	ctx, span := tracer.Start(ctx, fmt.Sprintf("outbox.publish %s", msg.EventName),
		trace.WithAttributes(
			attribute.String("event.name", msg.EventName),
			attribute.String("event.id", msg.EventID),
			attribute.Int("retry_count", msg.RetryCount),
		),
		trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	// msg.Payload is already []byte - pass directly to transport
	transportMsg := message.New(
		msg.EventID,
		"outbox",
		msg.Payload,
		msg.Metadata,
		message.WithSpanContext(span.SpanContext()),
	)

	err := r.transport.Publish(ctx, msg.EventName, transportMsg)
	duration := time.Since(start)

	if err != nil {
		span.RecordError(err)
		if r.metrics != nil {
			r.metrics.RecordFailed(ctx, msg.EventName)
		}
		return err
	}

	if r.metrics != nil {
		r.metrics.RecordPublished(ctx, msg.EventName, duration)
	}
	return nil
}

// cleanup removes old published messages
func (r *Relay) cleanup(ctx context.Context) {
	deleted, err := r.store.Delete(ctx, r.cleanupAge)
	if err != nil {
		r.log().Error("failed to cleanup old messages", "error", err)
		return
	}

	if deleted > 0 {
		r.log().Info("cleaned up old outbox messages", "count", deleted)
		if r.metrics != nil {
			r.metrics.RecordCleaned(ctx, deleted)
		}
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
	if failures := r.publishPending(ctx); failures > 0 {
		return fmt.Errorf("failed to publish %d message(s)", failures)
	}
	return nil
}

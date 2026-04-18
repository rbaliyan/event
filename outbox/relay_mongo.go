package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// RelayMode defines how the relay watches for new messages.
type RelayMode string

const (
	// RelayModePoll uses polling to check for new messages at regular intervals.
	// Works with all MongoDB deployments (standalone, replica set, sharded).
	RelayModePoll RelayMode = "poll"

	// RelayModeChangeStream uses MongoDB Change Streams for real-time notifications.
	// Requires MongoDB replica set or sharded cluster.
	// More efficient and lower latency than polling.
	RelayModeChangeStream RelayMode = "changestream"
)

// MongoRelay publishes outbox messages to the transport.
// Supports two modes:
//   - Poll mode: Regular polling at configured intervals (works with standalone MongoDB)
//   - ChangeStream mode: Real-time notifications via Change Streams (requires replica set)
type MongoRelay struct {
	store            *MongoStore
	transport        transport.Transport
	mode             RelayMode
	pollDelay        time.Duration
	pollJitter       time.Duration        // Random jitter added to each poll interval to desynchronise pods
	batchSize        int
	logger           *slog.Logger
	cleanupAge       time.Duration
	stuckDuration    time.Duration        // How long before a processing message is considered stuck
	resumeTokenStore ResumeTokenStore     // For change stream resume (optional)
	changeStreamOpts *changeStreamOptions // Internal change stream state
	metrics          *Metrics
}

// changeStreamOptions holds change stream specific options
type changeStreamOptions struct {
	fullDocumentMode string
}

// NewMongoRelay creates a new MongoDB outbox relay.
// Default mode is polling. Use WithMode(RelayModeChangeStream) for real-time notifications.
func NewMongoRelay(store *MongoStore, t transport.Transport) *MongoRelay {
	return &MongoRelay{
		store:         store,
		transport:     t,
		mode:          RelayModePoll,
		pollDelay:     100 * time.Millisecond,
		batchSize:     100,
		cleanupAge:    24 * time.Hour,
		stuckDuration: 5 * time.Minute,
		changeStreamOpts: &changeStreamOptions{
			fullDocumentMode: "updateLookup",
		},
	}
}

// WithMode sets the relay mode (poll or changestream).
//
// Poll mode works with all MongoDB deployments.
// ChangeStream mode requires a replica set or sharded cluster but provides
// lower latency and is more efficient.
//
// Example:
//
//	// Use change streams for real-time notifications
//	relay := outbox.NewMongoRelay(store, transport).
//	    WithMode(outbox.RelayModeChangeStream).
//	    WithResumeTokenStore(tokenStore)
func (r *MongoRelay) WithMode(mode RelayMode) *MongoRelay {
	r.mode = mode
	return r
}

// WithResumeTokenStore sets the store for persisting Change Stream resume tokens.
// Only used in ChangeStream mode. If not set, the relay will start from the
// current time on restart, potentially missing messages inserted during downtime.
func (r *MongoRelay) WithResumeTokenStore(store ResumeTokenStore) *MongoRelay {
	r.resumeTokenStore = store
	return r
}

// WithPollDelay sets the polling interval
func (r *MongoRelay) WithPollDelay(d time.Duration) *MongoRelay {
	r.pollDelay = d
	return r
}

// WithPollJitter sets a maximum random jitter added to each poll interval.
// Each tick sleeps for pollDelay + rand[0, jitter), desynchronising pods that
// share the same nominal poll delay and reducing UpdateMany write contention.
// A jitter of 50% of pollDelay is a reasonable default when running 2+ pods.
func (r *MongoRelay) WithPollJitter(jitter time.Duration) *MongoRelay {
	r.pollJitter = jitter
	return r
}

// WithBatchSize sets the number of messages to process per poll
func (r *MongoRelay) WithBatchSize(size int) *MongoRelay {
	r.batchSize = size
	return r
}

// WithLogger sets a custom logger.
// If not set, slog.Default() is used.
func (r *MongoRelay) WithLogger(l *slog.Logger) *MongoRelay {
	r.logger = l
	return r
}

// log returns the configured logger, falling back to slog.Default().
func (r *MongoRelay) log() *slog.Logger {
	if r.logger != nil {
		return r.logger
	}
	r.logger = slog.Default().With("component", "outbox.mongo_relay")
	return r.logger
}

// WithCleanupAge sets how old published messages should be before deletion
func (r *MongoRelay) WithCleanupAge(age time.Duration) *MongoRelay {
	r.cleanupAge = age
	return r
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
// Messages stuck in processing longer than this duration are moved back to pending.
// This handles relay crashes where messages were claimed but never published.
// Default: 5 minutes
func (r *MongoRelay) WithStuckDuration(d time.Duration) *MongoRelay {
	r.stuckDuration = d
	return r
}

// WithMetrics enables OpenTelemetry metrics for the relay.
func (r *MongoRelay) WithMetrics(m *Metrics) *MongoRelay {
	r.metrics = m
	return r
}

// Start begins watching the outbox and publishing messages.
// This method blocks until the context is cancelled.
//
// In Poll mode: Polls at regular intervals for new messages.
// In ChangeStream mode: Uses MongoDB Change Streams for real-time notifications.
//
// Both modes periodically recover stuck messages and cleanup old published messages.
func (r *MongoRelay) Start(ctx context.Context) error {
	// Recover any stuck messages at startup
	r.recoverStuck(ctx)

	switch r.mode {
	case RelayModeChangeStream:
		return r.startChangeStream(ctx)
	default:
		return r.startPolling(ctx)
	}
}

// startPolling runs the relay in polling mode.
func (r *MongoRelay) startPolling(ctx context.Context) error {
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	r.log().Info("relay started in poll mode", "poll_delay", r.pollDelay, "poll_jitter", r.pollJitter)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		case <-recoveryTicker.C:
			r.recoverStuck(ctx)
		case <-r.nextPollTimer(ctx):
			r.publishPending(ctx)
		}
	}
}

// nextPollTimer returns a channel that fires after pollDelay + rand[0, pollJitter).
func (r *MongoRelay) nextPollTimer(_ context.Context) <-chan time.Time {
	delay := r.pollDelay
	if r.pollJitter > 0 {
		delay += time.Duration(rand.Int64N(int64(r.pollJitter))) // #nosec G404 -- jitter needs no cryptographic security
	}
	return time.After(delay)
}

// startChangeStream runs the relay using MongoDB Change Streams.
func (r *MongoRelay) startChangeStream(ctx context.Context) error {
	// Process any existing pending messages first
	r.processExistingPending(ctx)

	// Start background tasks (cleanup, recovery)
	go r.backgroundTasks(ctx)

	// Create and use the change stream relay
	csRelay := NewChangeStreamRelay(r.store, r.transport).
		WithLogger(r.logger).
		WithCleanupAge(r.cleanupAge).
		WithStuckDuration(r.stuckDuration).
		WithBatchSize(r.batchSize)

	if r.resumeTokenStore != nil {
		csRelay = csRelay.WithResumeTokenStore(r.resumeTokenStore)
	}
	if r.metrics != nil {
		csRelay = csRelay.WithMetrics(r.metrics)
	}

	r.log().Info("relay started in changestream mode")

	return csRelay.Start(ctx)
}

// backgroundTasks runs cleanup and recovery in the background for change stream mode.
func (r *MongoRelay) backgroundTasks(ctx context.Context) {
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		case <-recoveryTicker.C:
			r.recoverStuck(ctx)
		}
	}
}

// processExistingPending processes any messages that were pending before startup.
func (r *MongoRelay) processExistingPending(ctx context.Context) {
	r.log().Info("processing existing pending messages")

	for {
		messages, err := r.store.getPendingMongo(ctx, r.batchSize)
		if err != nil {
			r.log().Error("failed to get pending messages", "error", err)
			return
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			if err := r.publishMessage(ctx, msg); err != nil {
				r.log().Error("failed to publish message",
					"id", msg.ID.Hex(),
					"event", msg.EventName,
					"error", err)
				if markErr := r.store.MarkFailed(ctx, msg.ID, err); markErr != nil {
					r.log().Error("failed to mark message as failed", "error", markErr)
				}
				continue
			}

			if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
				r.log().Error("failed to mark message as published",
					"id", msg.ID.Hex(),
					"error", err)
			}
		}
	}

	r.log().Info("finished processing existing pending messages")
}

// publishPending fetches and publishes pending messages
func (r *MongoRelay) publishPending(ctx context.Context) {
	messages, err := r.store.getPendingMongo(ctx, r.batchSize)
	if err != nil {
		r.log().Error("failed to get pending messages", "error", err)
		return
	}

	for _, msg := range messages {
		if err := r.publishMessage(ctx, msg); err != nil {
			r.log().Error("failed to publish message",
				"id", msg.ID.Hex(),
				"event", msg.EventName,
				"error", err)
			if markErr := r.store.MarkFailed(ctx, msg.ID, err); markErr != nil {
				r.log().Error("failed to mark message as failed", "error", markErr)
			}
			continue
		}

		if err := r.store.MarkPublished(ctx, msg.ID); err != nil {
			r.log().Error("failed to mark message as published",
				"id", msg.ID.Hex(),
				"error", err)
		}

		r.log().Debug("published outbox message",
			"id", msg.ID.Hex(),
			"event", msg.EventName,
			"event_id", msg.EventID)
	}
}

// publishMessage publishes a single message to the transport.
// If the message metadata contains W3C trace context headers, a child span is created
// to link the relay publish to the original transaction's trace.
func (r *MongoRelay) publishMessage(ctx context.Context, msg *mongoMessage) error {
	start := time.Now()

	// Extract trace context from metadata if present (W3C traceparent/tracestate)
	var spanCtx trace.SpanContext
	if msg.Metadata != nil {
		carrier := propagation.MapCarrier(msg.Metadata)
		extracted := otel.GetTextMapPropagator().Extract(ctx, carrier)
		spanCtx = trace.SpanContextFromContext(extracted)
	}

	if spanCtx.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)
	}
	tracer := otel.Tracer("outbox.mongo_relay")
	ctx, span := tracer.Start(ctx, fmt.Sprintf("outbox.publish %s", msg.EventName),
		trace.WithAttributes(
			attribute.String("event.name", msg.EventName),
			attribute.String("event.id", msg.EventID),
		),
		trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

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
func (r *MongoRelay) cleanup(ctx context.Context) {
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

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles relay crashes where messages were claimed but never published.
func (r *MongoRelay) recoverStuck(ctx context.Context) {
	recovered, err := r.store.RecoverStuck(ctx, r.stuckDuration)
	if err != nil {
		r.log().Error("failed to recover stuck messages", "error", err)
		return
	}

	if recovered > 0 {
		r.log().Warn("recovered stuck messages", "count", recovered, "stuck_duration", r.stuckDuration)
	}
}

// PublishOnce processes pending messages once (for testing or manual triggering)
func (r *MongoRelay) PublishOnce(ctx context.Context) error {
	r.publishPending(ctx)
	return nil
}


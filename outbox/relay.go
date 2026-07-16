package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
//  1. Claims a batch of pending messages from the store
//  2. Publishes each message to the transport
//  3. Acks (or fails) each message via the claimed Batch
//  4. Periodically cleans up old published messages
//
// The Relay should be run as a separate goroutine or process. Multiple
// relay instances can run concurrently - the store is responsible for
// exclusive claiming to prevent duplicate processing.
//
// Example:
//
//	store, err := outbox.NewPostgresStore(db)
//	if err != nil { log.Fatal(err) }
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
	store         Store
	readyOnce     sync.Once
	readyErr      error
	transport     transport.Transport
	pollDelay     time.Duration
	batchSize     int
	logger        *slog.Logger
	cleanupAge    time.Duration // How old published messages should be before deletion
	stuckInterval time.Duration
	stuckAge      time.Duration
	metrics       *Metrics
	maxRetries    int              // 0 = unlimited retries (default)
	retryBackoff  backoff.Strategy // nil = no backoff delay between retries
}

// RelayOption configures a Relay.
type RelayOption func(*relayOptions)

type relayOptions struct {
	pollDelay     time.Duration
	batchSize     int
	logger        *slog.Logger
	cleanupAge    time.Duration
	stuckInterval time.Duration
	stuckAge      time.Duration
	metrics       *Metrics
	maxRetries    int
	retryBackoff  backoff.Strategy
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

// WithMaxRetries sets the maximum number of publish attempts before permanently
// marking the message as failed. Once exceeded, the message remains in the outbox
// with StatusFailed and is no longer retried.
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

// WithStuckInterval sets how often a StuckRecoverer store is swept and how old
// a 'processing' message must be to be re-queued. No-op for stores that are not
// StuckRecoverers.
func WithStuckInterval(sweep, age time.Duration) RelayOption {
	return func(o *relayOptions) {
		if sweep > 0 {
			o.stuckInterval = sweep
		}
		if age > 0 {
			o.stuckAge = age
		}
	}
}

// NewRelay creates a new outbox relay.
//
// The relay claims batches of pending messages from the store and publishes
// them to the transport. Default configuration:
//   - Poll delay: 100ms
//   - Batch size: 100 messages
//   - Cleanup age: 24 hours
//   - Stuck sweep interval: 1 minute (stores implementing StuckRecoverer only)
//   - Stuck age: 5 minutes
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
		pollDelay:     100 * time.Millisecond,
		batchSize:     100,
		cleanupAge:    24 * time.Hour,
		stuckInterval: time.Minute,
		stuckAge:      5 * time.Minute,
	}
	for _, opt := range opts {
		opt(o)
	}

	logger := o.logger
	if logger == nil {
		logger = slog.Default().With("component", "outbox.relay")
	}

	return &Relay{
		store:         store,
		transport:     t,
		pollDelay:     o.pollDelay,
		batchSize:     o.batchSize,
		logger:        logger,
		cleanupAge:    o.cleanupAge,
		stuckInterval: o.stuckInterval,
		stuckAge:      o.stuckAge,
		metrics:       o.metrics,
		maxRetries:    o.maxRetries,
		retryBackoff:  o.retryBackoff,
	}
}

// log returns the configured logger.
func (r *Relay) log() *slog.Logger {
	return r.logger
}

// ensureReady runs a Starter store's one-time setup exactly once, whether the
// relay is driven via Start or PublishOnce. Without it, RedisStore.ClaimPending's
// XREADGROUP returns NOGROUP forever.
func (r *Relay) ensureReady(ctx context.Context) error {
	r.readyOnce.Do(func() {
		if s, ok := r.store.(Starter); ok {
			r.readyErr = s.EnsureReady(ctx)
		}
	})
	return r.readyErr
}

// Start begins polling the outbox and publishing messages.
//
// This method blocks until the context is cancelled. It runs:
//  1. A polling loop that claims and publishes pending messages
//  2. An hourly cleanup loop that removes old published messages
//  3. A stuck-message sweep (for stores that implement StuckRecoverer)
//  4. Early-wakeup notifications (for stores that implement Waker)
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
	if err := r.ensureReady(ctx); err != nil {
		return fmt.Errorf("outbox relay: ensure ready: %w", err)
	}

	ticker := time.NewTicker(r.pollDelay)
	defer ticker.Stop()
	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	var notifyC <-chan struct{}
	if w, ok := r.store.(Waker); ok {
		notifyC = w.Notifications() // nil channel blocks forever in select — fine
	}

	// Stuck recovery only for claim-and-release backends.
	var stuckC <-chan time.Time
	if sr, ok := r.store.(StuckRecoverer); ok {
		st := time.NewTicker(r.stuckInterval)
		defer st.Stop()
		stuckC = st.C
		r.recoverStuck(ctx, sr) // sweep once at startup
	}

	var consecutiveFailures int
	publish := func() {
		failures := r.drainOnce(ctx)
		if r.retryBackoff != nil && failures > 0 {
			consecutiveFailures++
			ticker.Reset(r.retryBackoff.NextDelay(consecutiveFailures))
		} else if consecutiveFailures > 0 {
			consecutiveFailures = 0
			ticker.Reset(r.pollDelay)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifyC:
			publish()
		case <-ticker.C:
			publish()
		case <-cleanupTicker.C:
			r.cleanup(ctx)
		case <-stuckC:
			if sr, ok := r.store.(StuckRecoverer); ok {
				r.recoverStuck(ctx, sr)
			}
		}
	}
}

// shouldSkip returns true if a message has exceeded the max retry limit.
func (r *Relay) shouldSkip(msg Message) bool {
	return r.maxRetries > 0 && msg.RetryCount >= r.maxRetries
}

// drainOnce claims one batch, publishes each message, and resolves it via
// Ack/Fail before closing the batch. Returns the number of messages that
// failed to publish or ack, plus 1 if the batch itself failed to claim or
// close.
func (r *Relay) drainOnce(ctx context.Context) (failures int) {
	batch, err := r.store.ClaimPending(ctx, r.batchSize)
	if err != nil {
		r.log().Error("claim failed", "error", err)
		return 1
	}
	defer func() {
		// Close/commit on a context detached from cancellation so a Close that
		// itself does I/O (Postgres commit) is not aborted the instant ctx is
		// cancelled. NOTE: this does not fully guarantee shutdown-safe commit —
		// the Postgres claim tx and its Ack UPDATEs run on the cancellable ctx,
		// so database/sql may still roll them back on cancellation. Semantics
		// remain at-least-once either way. See design spec, Close ordering.
		if cerr := batch.Close(context.WithoutCancel(ctx)); cerr != nil {
			failures++
			r.log().Error("batch close failed", "error", cerr)
		}
	}()

	for _, m := range batch.Messages() {
		if r.shouldSkip(m) {
			r.log().Warn("message exceeded max retries",
				"event_id", m.EventID, "retry_count", m.RetryCount, "max_retries", r.maxRetries)
			if e := batch.Fail(ctx, m, errExhausted); e != nil {
				r.log().Error("mark exhausted failed", "event_id", m.EventID, "error", e)
			}
			continue
		}
		if err := r.publishMessage(ctx, m); err != nil {
			r.log().Error("publish failed", "event_id", m.EventID, "event", m.EventName, "error", err)
			failures++
			if e := batch.Fail(ctx, m, err); e != nil {
				r.log().Error("mark failed", "event_id", m.EventID, "error", e)
			}
			continue
		}
		if err := batch.Ack(ctx, m); err != nil {
			r.log().Error("mark published", "event_id", m.EventID, "error", err)
			failures++
		}
	}
	return failures
}

// publishMessage publishes a single message to the transport.
// If the message metadata contains W3C trace context headers, a child span is created
// to link the relay publish to the original transaction's trace.
func (r *Relay) publishMessage(ctx context.Context, msg Message) error {
	start := time.Now()
	var spanCtx trace.SpanContext
	if msg.Metadata != nil {
		carrier := propagation.MapCarrier(msg.Metadata)
		spanCtx = trace.SpanContextFromContext(otel.GetTextMapPropagator().Extract(ctx, carrier))
	}
	if spanCtx.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)
	}
	ctx, span := otel.Tracer("outbox.relay").Start(ctx,
		fmt.Sprintf("outbox.publish %s", msg.EventName),
		trace.WithAttributes(
			attribute.String("event.name", msg.EventName),
			attribute.String("event.id", msg.EventID),
			attribute.Int("retry_count", msg.RetryCount),
		),
		trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	transportMsg := message.New(msg.EventID, "outbox", msg.Payload, msg.Metadata,
		message.WithSpanContext(span.SpanContext()))

	err := r.transport.Publish(ctx, msg.EventName, transportMsg)
	dur := time.Since(start)
	if err != nil {
		span.RecordError(err)
		if r.metrics != nil {
			r.metrics.RecordFailed(ctx, msg.EventName)
		}
		return err
	}
	if r.metrics != nil {
		r.metrics.RecordPublished(ctx, msg.EventName, dur)
	}
	return nil
}

// cleanup removes old published messages.
func (r *Relay) cleanup(ctx context.Context) {
	deleted, err := r.store.Cleanup(ctx, r.cleanupAge)
	if err != nil {
		r.log().Error("cleanup failed", "error", err)
		return
	}
	if deleted > 0 {
		r.log().Info("cleaned up old outbox messages", "count", deleted)
		if r.metrics != nil {
			r.metrics.RecordCleaned(ctx, deleted)
		}
	}
}

// recoverStuck re-queues messages a crashed relay left claimed.
func (r *Relay) recoverStuck(ctx context.Context, sr StuckRecoverer) {
	n, err := sr.RecoverStuck(ctx, r.stuckAge)
	if err != nil {
		r.log().Error("recover stuck failed", "error", err)
		return
	}
	if n > 0 {
		r.log().Info("recovered stuck outbox messages", "count", n)
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
	if err := r.ensureReady(ctx); err != nil {
		return fmt.Errorf("outbox relay: ensure ready: %w", err)
	}
	if failures := r.drainOnce(ctx); failures > 0 {
		return fmt.Errorf("failed to publish %d message(s)", failures)
	}
	return nil
}

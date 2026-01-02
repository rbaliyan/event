// Package nats provides NATS transport implementations.
//
// This package provides two transport implementations:
//
// # NATS Core (New)
//
// Simple pub/sub with at-most-once delivery. Use for ephemeral events
// where message loss is acceptable. Optional library-level stores can
// be injected for deduplication, DLQ, and poison detection.
//
//	transport := nats.New(conn,
//	    nats.WithIdempotencyStore(store),  // Optional dedup
//	    nats.WithDLQHandler(handler),       // Optional DLQ
//	)
//
// # NATS JetStream (NewJetStream)
//
// Persistent messaging with at-least-once delivery. JetStream provides
// native deduplication, max delivery limits, and message persistence.
// No library stores needed - the broker handles reliability.
//
//	transport := nats.NewJetStream(conn,
//	    nats.WithDeduplication(2 * time.Minute),  // Native dedup
//	    nats.WithMaxDeliver(5),                    // Native poison detection
//	)
//
// Choose NATS Core for simplicity and low latency.
// Choose JetStream for reliability and persistence.
package nats

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// Errors
var (
	ErrConnRequired    = errors.New("nats connection is required")
	ErrJetStreamFailed = errors.New("failed to create jetstream context")
)

// JetStreamTransport implements transport.Transport using NATS JetStream.
//
// JetStream provides at-least-once delivery with message persistence.
// Native features like deduplication and max delivery limits are
// configured via options - no external stores needed.
type JetStreamTransport struct {
	status  int32
	conn    *nats.Conn
	js      jetstream.JetStream
	codec   codec.Codec
	events  sync.Map // map[string]*natsEvent
	logger  *slog.Logger
	onError func(error)

	// Stream configuration
	streamPrefix string
	replicas     int
	maxAge       time.Duration
	sendTimeout  time.Duration // Timeout for sending to subscriber channel (backpressure)

	// Native JetStream features
	dedupEnabled bool          // Enable native deduplication via Nats-Msg-Id
	dedupWindow  time.Duration // Deduplication window duration
	maxDeliver   int           // Max delivery attempts (0 = unlimited)
	ackWait      time.Duration // Time to wait for ack before redelivery
}

// natsEvent tracks event-specific state
type natsEvent struct {
	name   string
	stream jetstream.Stream
	subIdx int64 // For generating unique consumer names in Broadcast mode
}

// jsSubscription implements transport.Subscription for JetStream
type jsSubscription struct {
	id          string
	ch          chan transport.Message
	closedCh    chan struct{}
	closed      int32
	consumer    jetstream.Consumer
	codec       codec.Codec
	cancel      context.CancelFunc
	wg          sync.WaitGroup // Track consumer goroutine for clean shutdown
	sendTimeout time.Duration  // Timeout for sending to channel (backpressure)
}

// Default configuration
var (
	DefaultReplicas = 1
	DefaultMaxAge   = 24 * time.Hour
)

// streamPrefix is the fixed prefix for NATS streams to avoid clashing with user data
const streamPrefix = "evt"

// JSOption configures the JetStream transport
type JSOption func(*JetStreamTransport)

// NewJetStream creates a new NATS JetStream transport.
//
// JetStream provides at-least-once delivery with message persistence.
// Use native feature options to enable deduplication and max delivery
// limits - no external stores needed.
//
// Example:
//
//	transport := nats.NewJetStream(conn,
//	    nats.WithDeduplication(2 * time.Minute),  // Native dedup
//	    nats.WithMaxDeliver(5),                    // Stop after 5 attempts
//	    nats.WithAckWait(30 * time.Second),        // Ack timeout
//	)
//
// For simple pub/sub without persistence, use New() instead.
func NewJetStream(conn *nats.Conn, opts ...JSOption) (*JetStreamTransport, error) {
	if conn == nil {
		return nil, ErrConnRequired
	}

	t := &JetStreamTransport{
		status:       1,
		conn:         conn,
		codec:        codec.Default(),
		streamPrefix: streamPrefix,
		replicas:     DefaultReplicas,
		maxAge:       DefaultMaxAge,
		logger:       transport.Logger("transport>nats-jetstream"),
		onError:      func(error) {},
		dedupWindow:  2 * time.Minute, // JetStream default
		ackWait:      30 * time.Second,
	}

	for _, opt := range opts {
		opt(t)
	}

	// Create JetStream context
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, errors.Join(ErrJetStreamFailed, err)
	}
	t.js = js

	return t, nil
}

func (t *JetStreamTransport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

func (t *JetStreamTransport) streamName(eventName string) string {
	return t.streamPrefix + "_" + eventName
}

// RegisterEvent creates resources for an event
func (t *JetStreamTransport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	streamName := t.streamName(name)

	// Build stream config
	streamConfig := jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{name},
		Replicas: t.replicas,
		MaxAge:   t.maxAge,
	}

	// Enable native deduplication if configured
	if t.dedupEnabled && t.dedupWindow > 0 {
		streamConfig.Duplicates = t.dedupWindow
	}

	// Create or update stream
	stream, err := t.js.CreateOrUpdateStream(ctx, streamConfig)
	if err != nil {
		return err
	}

	ev := &natsEvent{
		name:   name,
		stream: stream,
	}

	if _, loaded := t.events.LoadOrStore(name, ev); loaded {
		return transport.ErrEventAlreadyExists
	}

	t.logger.Debug("registered event", "event", name, "stream", streamName)
	return nil
}

// UnregisterEvent cleans up event resources
func (t *JetStreamTransport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.LoadAndDelete(name); !ok {
		return transport.ErrEventNotRegistered
	}

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish sends a message to an event's subscribers
func (t *JetStreamTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.Load(name); !ok {
		return transport.ErrEventNotRegistered
	}

	// Encode message
	data, err := t.codec.Encode(msg)
	if err != nil {
		return err
	}

	// Build publish options
	var pubOpts []jetstream.PublishOpt

	// Add message ID for native deduplication if enabled
	if t.dedupEnabled {
		pubOpts = append(pubOpts, jetstream.WithMsgID(msg.ID()))
	}

	// Publish to JetStream
	_, err = t.js.Publish(ctx, name, data, pubOpts...)
	if err != nil {
		t.onError(err)
		return err
	}

	t.logger.Debug("published message", "event", name, "msg_id", msg.ID(), "dedup", t.dedupEnabled)
	return nil
}

// Subscribe creates a subscription to receive messages for an event
func (t *JetStreamTransport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	subOpts := transport.ApplySubscribeOptions(opts...)

	val, ok := t.events.Load(name)
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}
	ev := val.(*natsEvent)

	// Map StartFrom to NATS DeliverPolicy
	deliverPolicy := jetstream.DeliverAllPolicy
	var optStartTime *time.Time
	switch subOpts.StartFrom {
	case transport.StartFromLatest:
		deliverPolicy = jetstream.DeliverNewPolicy
	case transport.StartFromTimestamp:
		if !subOpts.StartTime.IsZero() {
			deliverPolicy = jetstream.DeliverByStartTimePolicy
			optStartTime = &subOpts.StartTime
		}
	}

	// Create consumer configuration based on delivery mode
	var consumerConfig jetstream.ConsumerConfig

	if subOpts.DeliveryMode == transport.WorkerPool {
		// WorkerPool: shared durable consumer per event (load balancing)
		consumerConfig = jetstream.ConsumerConfig{
			Durable:       "workers-" + name,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: deliverPolicy,
		}
	} else {
		// Broadcast: ephemeral consumer per subscriber (fan-out)
		consumerConfig = jetstream.ConsumerConfig{
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: deliverPolicy,
		}
	}

	if optStartTime != nil {
		consumerConfig.OptStartTime = optStartTime
	}

	// Apply native feature options
	if t.maxDeliver > 0 {
		consumerConfig.MaxDeliver = t.maxDeliver
	}
	if t.ackWait > 0 {
		consumerConfig.AckWait = t.ackWait
	}

	// Create or get consumer
	consumer, err := ev.stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return nil, err
	}

	subCtx, cancel := context.WithCancel(ctx)

	bufSize := 100
	if subOpts.BufferSize > 0 {
		bufSize = subOpts.BufferSize
	}

	sub := &jsSubscription{
		id:          transport.NewID(),
		ch:          make(chan transport.Message, bufSize),
		closedCh:    make(chan struct{}),
		consumer:    consumer,
		codec:       t.codec,
		cancel:      cancel,
		sendTimeout: t.sendTimeout,
	}

	// Start consuming in background with WaitGroup tracking
	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()
		sub.consumeLoop(subCtx, t.logger)
	}()

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.id, "mode", subOpts.DeliveryMode, "startFrom", subOpts.StartFrom)
	return sub, nil
}

// Close shuts down the transport
func (t *JetStreamTransport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Note: We don't close the connection as it was passed in pre-initialized
	// The caller is responsible for closing it

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the NATS transport
func (t *JetStreamTransport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	// Check if transport is open
	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Check NATS connection status
	connStatus := t.conn.Status()
	if connStatus != nats.CONNECTED {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "nats connection not healthy"
		result.Latency = time.Since(start)
		result.Details["type"] = "nats"
		result.Details["connection_status"] = connStatus.String()
		return result
	}

	// Perform RTT check to verify connection is working
	rttStart := time.Now()
	rtt, err := t.conn.RTT()
	rttDuration := time.Since(rttStart)

	if err != nil {
		result.Status = transport.HealthStatusDegraded
		result.Message = "nats RTT check failed"
		result.Latency = time.Since(start)
		result.Details["type"] = "nats"
		result.Details["connection_status"] = connStatus.String()
		result.Details["rtt_error"] = err.Error()
		return result
	}

	// Count events
	var eventCount int
	t.events.Range(func(key, value any) bool {
		eventCount++
		return true
	})

	result.Status = transport.HealthStatusHealthy
	result.Message = "nats transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "nats"
	result.Details["connection_status"] = connStatus.String()
	result.Details["rtt_ms"] = rtt.Milliseconds()
	result.Details["rtt_check_ms"] = rttDuration.Milliseconds()
	result.Details["events"] = eventCount
	result.Details["server_url"] = t.conn.ConnectedUrl()

	return result
}

// ConsumerLag returns the current consumer lag for all events
func (t *JetStreamTransport) ConsumerLag(ctx context.Context) ([]transport.ConsumerLag, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	var lags []transport.ConsumerLag

	t.events.Range(func(key, value any) bool {
		name := key.(string)
		ev := value.(*natsEvent)

		// Get stream info
		streamInfo, err := ev.stream.Info(ctx)
		if err != nil {
			t.logger.Error("failed to get stream info", "stream", ev.stream.CachedInfo().Config.Name, "error", err)
			return true
		}

		// Get all consumers for this stream
		consumerLister := ev.stream.ListConsumers(ctx)
		for consumerInfo := range consumerLister.Info() {
			lag := transport.ConsumerLag{
				Event:           name,
				ConsumerGroup:   consumerInfo.Name,
				Lag:             int64(consumerInfo.NumPending),
				PendingMessages: int64(consumerInfo.NumAckPending),
			}

			// Calculate oldest pending age from AckFloor
			if consumerInfo.AckFloor.Stream > 0 {
				if streamInfo.State.FirstTime.Before(time.Now()) {
					if streamInfo.State.Msgs > 0 && consumerInfo.NumPending > 0 {
						duration := time.Since(streamInfo.State.FirstTime)
						rate := float64(streamInfo.State.Msgs) / duration.Seconds()
						if rate > 0 {
							lag.OldestPending = time.Duration(float64(consumerInfo.NumPending) / rate * float64(time.Second))
						}
					}
				}
			}

			lags = append(lags, lag)
		}
		if err := consumerLister.Err(); err != nil {
			t.logger.Error("failed to list consumers", "error", err)
		}

		// If no consumers, report total stream messages as lag
		if streamInfo.State.Consumers == 0 {
			lags = append(lags, transport.ConsumerLag{
				Event: name,
				Lag:   int64(streamInfo.State.Msgs),
			})
		}

		return true
	})

	return lags, nil
}

// subscription methods

func (s *jsSubscription) ID() string {
	return s.id
}

func (s *jsSubscription) Messages() <-chan transport.Message {
	return s.ch
}

func (s *jsSubscription) Close(ctx context.Context) error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		close(s.closedCh)
		if s.cancel != nil {
			s.cancel()
		}
		// Wait for consumer goroutine to exit before closing channel
		s.wg.Wait()
		close(s.ch)
	}
	return nil
}

// sendResult indicates the outcome of sending to channel
type sendResult int

const (
	sendOK      sendResult = iota // Message sent successfully
	sendClosed                    // Subscription was closed
	sendTimeout                   // Timeout (message will be nack'd for redelivery)
)

// sendToChannel sends a message to the channel with optional timeout.
func (s *jsSubscription) sendToChannel(msg transport.Message) sendResult {
	if s.sendTimeout > 0 {
		timer := time.NewTimer(s.sendTimeout)
		defer timer.Stop()
		select {
		case <-s.closedCh:
			return sendClosed
		case <-timer.C:
			return sendTimeout
		case s.ch <- msg:
			return sendOK
		}
	}
	select {
	case <-s.closedCh:
		return sendClosed
	case s.ch <- msg:
		return sendOK
	}
}

func (s *jsSubscription) consumeLoop(ctx context.Context, logger *slog.Logger) {
	handler := func(msg jetstream.Msg) {
		select {
		case <-s.closedCh:
			return
		default:
		}

		// Decode message
		decoded, err := s.codec.Decode(msg.Data())
		if err != nil {
			logger.Error("failed to decode message", "error", err)
			msg.Ack() // Ack to avoid redelivery loop
			return
		}

		// Wrap with ack function
		wrappedMsg := transport.NewMessageWithAck(
			decoded.ID(),
			decoded.Source(),
			decoded.Payload(),
			decoded.Metadata(),
			decoded.RetryCount(),
			func(err error) error {
				if err == nil {
					return msg.Ack()
				}
				// Negative ack - will be redelivered
				return msg.Nak()
			},
		)

		// Send message to handler channel
		switch s.sendToChannel(wrappedMsg) {
		case sendClosed:
			return
		case sendTimeout:
			// Timeout: Nack for immediate redelivery - NO message loss
			msg.Nak()
			logger.Warn("message send timeout, nack'd for redelivery")
		case sendOK:
			// Successfully sent to handler
		}
	}

	// Retry loop for Consume() - handles connection errors with backoff
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-s.closedCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Channel to signal consumer errors
		consumerErrCh := make(chan error, 1)

		// Error handler for consumer
		errHandler := jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			logger.Error("consumer error detected", "error", err)
			select {
			case consumerErrCh <- err:
			default:
			}
		})

		cons, err := s.consumer.Consume(handler, errHandler)
		if err != nil {
			jitteredBackoff := transport.Jitter(backoff, 0.3)
			logger.Error("consume error, retrying", "error", err, "backoff", jitteredBackoff)
			select {
			case <-s.closedCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(jitteredBackoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		// Reset backoff on successful connection
		backoff = 100 * time.Millisecond

		// Wait for close signal OR consumer error
		select {
		case <-s.closedCh:
			cons.Stop()
			return
		case <-ctx.Done():
			cons.Stop()
			return
		case err := <-consumerErrCh:
			jitteredBackoff := transport.Jitter(backoff, 0.3)
			logger.Warn("consumer error, reconnecting", "error", err, "backoff", jitteredBackoff)
			cons.Stop()

			select {
			case <-s.closedCh:
				return
			case <-ctx.Done():
				return
			case <-time.After(jitteredBackoff):
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// Compile-time checks
var _ transport.Transport = (*JetStreamTransport)(nil)
var _ transport.HealthChecker = (*JetStreamTransport)(nil)
var _ transport.LagMonitor = (*JetStreamTransport)(nil)
var _ transport.Subscription = (*jsSubscription)(nil)

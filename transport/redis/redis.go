// Package redis provides a Redis Streams-based transport implementation.
//
// This transport uses Redis Streams for at-least-once delivery guarantees.
// Messages are persisted in Redis and redelivered if not acknowledged.
//
// Features:
//   - At-least-once delivery via Redis Streams
//   - Consumer groups for WorkerPool mode (load balancing)
//   - Automatic orphaned message claiming
//   - Stream trimming by count (MAXLEN) or age (MINID)
//   - Health checks and consumer lag monitoring
package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/redis/go-redis/v9"
)

// Client defines the interface for Redis client operations.
// Supports *redis.Client, *redis.ClusterClient, and redis.UniversalClient.
type Client interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd
	XLen(ctx context.Context, stream string) *redis.IntCmd
	XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// ErrClientRequired is returned when no Redis client is provided
var ErrClientRequired = errors.New("redis client is required")

// DefaultBusName is used as default consumer group
const DefaultBusName = "event-bus"

// Transport implements transport.Transport using Redis Streams
type Transport struct {
	status  int32
	client  Client
	groupID string
	codec   codec.Codec
	events  sync.Map // map[string]*redisEvent
	logger  *slog.Logger
	onError func(error)

	// Stream configuration
	streamPrefix  string
	maxLen        int64         // Max stream length (0 = unlimited)
	maxAge        time.Duration // Max message age for MINID trimming (0 = unlimited)
	blockTime     time.Duration
	sendTimeout   time.Duration // Timeout for sending to subscriber channel (backpressure)
	claimInterval time.Duration // Interval for claiming orphaned messages (0 = disabled)
	claimMinIdle  time.Duration // Minimum idle time before claiming a message

}

// redisEvent tracks event-specific state
type redisEvent struct {
	name string
}

// subscription implements transport.Subscription for Redis
type subscription struct {
	*base.Subscription             // Embedded base subscription
	client             Client      // Redis client
	stream             string      // Stream name
	group              string      // Consumer group name
	consumer           string      // Consumer name
	codec              codec.Codec // Message codec
	cancel             context.CancelFunc
	claimInterval      time.Duration // Interval for claiming orphaned messages
	claimMinIdle       time.Duration // Minimum idle time before claiming
	isBroadcast        bool          // If true, consumer group is deleted on close

}

// Default configuration
var (
	DefaultMaxLen    = int64(0) // unlimited
	DefaultBlockTime = 5 * time.Second
)

// streamPrefix is the fixed prefix for Redis streams to avoid clashing with user data
const streamPrefix = "evt"

// New creates a new Redis transport with a pre-initialized client
func New(client Client, opts ...Option) (*Transport, error) {
	if client == nil {
		return nil, ErrClientRequired
	}

	t := &Transport{
		status:       1,
		client:       client,
		groupID:      DefaultBusName,
		codec:        codec.Default(),
		streamPrefix: streamPrefix,
		maxLen:       DefaultMaxLen,
		blockTime:    DefaultBlockTime,
		logger:       transport.Logger("transport>redis"),
		onError:      func(error) {},
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

func (t *Transport) streamName(eventName string) string {
	return t.streamPrefix + ":" + eventName
}

// RegisterEvent creates resources for an event
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	streamName := t.streamName(name)

	// Create consumer group (also creates stream if it doesn't exist)
	err := t.client.XGroupCreateMkStream(ctx, streamName, t.groupID, "$").Err()
	if err != nil && !errors.Is(err, redis.Nil) {
		// Ignore "BUSYGROUP" error (group already exists)
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return err
		}
	}

	ev := &redisEvent{
		name: name,
	}

	if _, loaded := t.events.LoadOrStore(name, ev); loaded {
		return transport.ErrEventAlreadyExists
	}

	t.logger.Debug("registered event", "event", name, "stream", streamName)
	return nil
}

// UnregisterEvent cleans up event resources
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
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
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
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

	streamName := t.streamName(name)

	// XADD to stream
	args := &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"data": data,
		},
	}

	// Apply count-based trimming (MAXLEN)
	if t.maxLen > 0 {
		args.MaxLen = t.maxLen
		args.Approx = true // Use ~ for better performance
	}

	// Apply time-based trimming (MINID)
	if t.maxAge > 0 {
		minTime := time.Now().Add(-t.maxAge).UnixMilli()
		args.MinID = fmt.Sprintf("%d-0", minTime)
		args.Approx = true
	}

	_, err = t.client.XAdd(ctx, args).Result()
	if err != nil {
		t.onError(err)
		return err
	}

	t.logger.Debug("published message", "event", name, "msg_id", msg.ID())
	return nil
}

// Subscribe creates a subscription to receive messages for an event
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	subOpts := transport.ApplySubscribeOptions(opts...)

	if _, ok := t.events.Load(name); !ok {
		return nil, transport.ErrEventNotRegistered
	}

	streamName := t.streamName(name)
	subID := transport.NewID()

	// Determine start position for Redis stream
	// "0" = from beginning, "$" = from latest (new messages only)
	startID := "0"
	if subOpts.StartFrom == transport.StartFromLatest {
		startID = "$"
	} else if subOpts.StartFrom == transport.StartFromTimestamp && !subOpts.StartTime.IsZero() {
		// Redis stream IDs are millisecond timestamps
		startID = fmt.Sprintf("%d-0", subOpts.StartTime.UnixMilli())
	}

	var groupID string
	var needsGroupCreate bool
	if subOpts.DeliveryMode == transport.WorkerPool {
		if subOpts.WorkerGroup != "" {
			// WorkerPool with named group: workers in same group compete
			// Different groups each receive all messages
			groupID = t.groupID + "-" + name + "-" + subOpts.WorkerGroup
			needsGroupCreate = true
		} else {
			// WorkerPool default: all workers share the base group
			groupID = t.groupID
		}
	} else {
		// Broadcast: unique consumer group per subscriber (fan-out)
		groupID = t.groupID + "-" + subID
		needsGroupCreate = true
	}

	// Create consumer group if needed (named worker groups or broadcast)
	if needsGroupCreate {
		err := t.client.XGroupCreateMkStream(ctx, streamName, groupID, startID).Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return nil, err
		}
	}

	subCtx, cancel := context.WithCancel(ctx)

	bufSize := 100
	if subOpts.BufferSize > 0 {
		bufSize = subOpts.BufferSize
	}

	sub := &subscription{
		Subscription:  base.NewSubscription(subID, bufSize, t.sendTimeout),
		client:        t.client,
		stream:        streamName,
		group:         groupID,
		consumer:      subID,
		codec:         t.codec,
		cancel:        cancel,
		claimInterval: t.claimInterval,
		claimMinIdle:  t.claimMinIdle,
		isBroadcast:   subOpts.DeliveryMode == transport.Broadcast,
	}

	// Start consuming in background with WaitGroup tracking
	sub.WaitGroup().Add(1)
	go func() {
		defer sub.WaitGroup().Done()
		sub.consumeLoop(subCtx, t.blockTime, t.logger)
	}()

	// Start orphan message claimer if configured
	if t.claimInterval > 0 {
		sub.WaitGroup().Add(1)
		go func() {
			defer sub.WaitGroup().Done()
			sub.claimOrphanedMessages(subCtx, t.logger)
		}()
	}

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID(), "group", groupID, "mode", subOpts.DeliveryMode, "start", startID)
	return sub, nil
}

// Close shuts down the transport
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	// Note: We don't close the client as it was passed in pre-initialized
	// The caller is responsible for closing it

	t.logger.Debug("transport closed")
	return nil
}

// Health performs a health check on the Redis transport
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
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

	// Ping Redis to check connection
	pingStart := time.Now()
	err := t.client.Ping(ctx).Err()
	pingLatency := time.Since(pingStart)

	if err != nil {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = fmt.Sprintf("redis ping failed: %v", err)
		result.Latency = time.Since(start)
		result.Details["type"] = "redis"
		result.Details["ping_error"] = err.Error()
		return result
	}

	// Count events
	var eventCount int
	t.events.Range(func(key, value any) bool {
		eventCount++
		return true
	})

	result.Status = transport.HealthStatusHealthy
	result.Message = "redis transport is healthy"
	result.Latency = time.Since(start)
	result.Details["type"] = "redis"
	result.Details["ping_latency_ms"] = pingLatency.Milliseconds()
	result.Details["events"] = eventCount
	result.Details["consumer_group"] = t.groupID

	return result
}

// ConsumerLag returns the current consumer lag for all events
func (t *Transport) ConsumerLag(ctx context.Context) ([]transport.ConsumerLag, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	var lags []transport.ConsumerLag

	t.events.Range(func(key, value any) bool {
		name := key.(string)
		streamName := t.streamName(name)

		// Get stream length (total messages)
		streamLen, err := t.client.XLen(ctx, streamName).Result()
		if err != nil {
			t.logger.Error("failed to get stream length", "stream", streamName, "error", err)
			return true
		}

		// Get consumer group info
		groups, err := t.client.XInfoGroups(ctx, streamName).Result()
		if err != nil {
			t.logger.Error("failed to get group info", "stream", streamName, "error", err)
			return true
		}

		for _, group := range groups {
			// Get pending info for more details
			pending, err := t.client.XPending(ctx, streamName, group.Name).Result()
			if err != nil {
				t.logger.Error("failed to get pending info", "stream", streamName, "group", group.Name, "error", err)
				continue
			}

			lag := transport.ConsumerLag{
				Event:           name,
				ConsumerGroup:   group.Name,
				Lag:             group.Lag,
				PendingMessages: pending.Count,
			}

			// Calculate oldest pending message age
			if pending.Lower != "" {
				var timestamp int64
				if _, err := fmt.Sscanf(pending.Lower, "%d-", &timestamp); err == nil {
					lag.OldestPending = time.Since(time.UnixMilli(timestamp))
				}
			}

			lags = append(lags, lag)
		}

		// Also add overall stream info if no groups
		if len(groups) == 0 {
			lags = append(lags, transport.ConsumerLag{
				Event: name,
				Lag:   streamLen,
			})
		}

		return true
	})

	return lags, nil
}

// subscription methods

func (s *subscription) Close(ctx context.Context) error {
	return s.Subscription.Close(func() error {
		if s.cancel != nil {
			s.cancel()
		}
		// For broadcast mode, delete the unique consumer group to prevent resource leak
		if s.isBroadcast && s.client != nil {
			s.client.XGroupDestroy(ctx, s.stream, s.group)
		}
		return nil
	})
}

// sendWithRetry sends a message to the channel with exponential backoff on timeout.
// This is a convenience wrapper around SendWithRetry with Redis-specific logging.
func (s *subscription) sendWithRetry(msg transport.Message, msgID string, logger *slog.Logger) bool {
	return s.SendWithRetry(msg, logger, "msg_id", msgID, "stream", s.stream)
}

// processRedisMessage decodes a Redis stream message and sends it to the subscription channel.
// retryCount overrides the retry count from the decoded message when >= 0.
// Returns true if processing should continue, false if the subscription was closed.
func (s *subscription) processRedisMessage(xmsg redis.XMessage, retryCount int, logger *slog.Logger) bool {
	data, ok := xmsg.Values["data"].(string)
	msgID := xmsg.ID

	if !ok {
		logger.Error("invalid message format", "id", xmsg.ID)
		errorMsg := transport.NewDecodeErrorMessage(
			msgID, nil, transport.ErrDecodeFailure,
			func(err error) error {
				if err == nil {
					return s.client.XAck(context.Background(), s.stream, s.group, msgID).Err()
				}
				return nil
			},
		)
		return s.sendWithRetry(errorMsg, msgID, logger)
	}

	decoded, err := s.codec.Decode([]byte(data))
	if err != nil {
		logger.Error("failed to decode message", "error", err, "id", xmsg.ID)
		errorMsg := transport.NewDecodeErrorMessage(
			msgID, []byte(data), err,
			func(ackErr error) error {
				if ackErr == nil {
					return s.client.XAck(context.Background(), s.stream, s.group, msgID).Err()
				}
				return nil
			},
		)
		return s.sendWithRetry(errorMsg, msgID, logger)
	}

	rc := decoded.RetryCount()
	if retryCount >= 0 {
		rc = retryCount
	}

	wrappedMsg := transport.NewMessageWithAck(
		decoded.ID(),
		decoded.Source(),
		decoded.Payload(),
		decoded.Metadata(),
		rc,
		func(err error) error {
			if err == nil {
				return s.client.XAck(context.Background(), s.stream, s.group, msgID).Err()
			}
			return nil
		},
	)

	return s.sendWithRetry(wrappedMsg, msgID, logger)
}

func (s *subscription) consumeLoop(ctx context.Context, blockTime time.Duration, logger *slog.Logger) {
	// First, process any pending messages (PEL) that weren't acknowledged
	s.processPendingMessages(ctx, logger)

	// Exponential backoff for read errors
	readBackoff := 100 * time.Millisecond
	maxReadBackoff := 30 * time.Second

	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		default:
		}

		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, ">"},
			Count:    10,
			Block:    blockTime,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				readBackoff = 100 * time.Millisecond
				continue
			}
			jitteredBackoff := transport.Jitter(readBackoff, 0.3)
			logger.Error("read error, retrying with backoff", "error", err, "backoff", jitteredBackoff)

			select {
			case <-s.ClosedCh():
				return
			case <-ctx.Done():
				return
			case <-time.After(jitteredBackoff):
			}

			readBackoff *= 2
			if readBackoff > maxReadBackoff {
				readBackoff = maxReadBackoff
			}
			continue
		}

		readBackoff = 100 * time.Millisecond

		for _, stream := range streams {
			for _, xmsg := range stream.Messages {
				select {
				case <-s.ClosedCh():
					return
				default:
				}

				if !s.processRedisMessage(xmsg, -1, logger) {
					return
				}
			}
		}
	}
}

func (s *subscription) processPendingMessages(ctx context.Context, logger *slog.Logger) {
	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		default:
		}

		// First, get delivery counts for pending messages
		pendingInfo, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: s.stream,
			Group:  s.group,
			Start:  "-",
			End:    "+",
			Count:  100,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
			logger.Error("error getting pending info", "error", err)
		}

		// Build a map of message ID to delivery count
		deliveryCounts := make(map[string]int64)
		for _, p := range pendingInfo {
			deliveryCounts[p.ID] = p.RetryCount
		}

		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, "0"},
			Count:    10,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("error reading pending messages", "error", err)
			return
		}

		if len(streams) == 0 {
			return
		}

		hasMessages := false
		for _, stream := range streams {
			if len(stream.Messages) > 0 {
				hasMessages = true
			}
			for _, xmsg := range stream.Messages {
				select {
				case <-s.ClosedCh():
					return
				default:
				}

				retryCount := int(deliveryCounts[xmsg.ID])
				if !s.processRedisMessage(xmsg, retryCount, logger) {
					return
				}
			}
		}

		if !hasMessages {
			return
		}
	}
}

func (s *subscription) claimOrphanedMessages(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(s.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.claimOnce(ctx, logger)
		}
	}
}

func (s *subscription) claimOnce(ctx context.Context, logger *slog.Logger) {
	pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.stream,
		Group:  s.group,
		Start:  "-",
		End:    "+",
		Count:  100,
		Idle:   s.claimMinIdle,
	}).Result()

	if err != nil {
		if !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
			logger.Error("failed to get pending messages for claim", "error", err)
		}
		return
	}

	if len(pending) == 0 {
		return
	}

	// Build a map of message ID to delivery count from pending info
	deliveryCounts := make(map[string]int64)
	var claimIDs []string
	for _, p := range pending {
		if p.Consumer != s.consumer {
			claimIDs = append(claimIDs, p.ID)
			deliveryCounts[p.ID] = p.RetryCount
		}
	}

	if len(claimIDs) == 0 {
		return
	}

	logger.Info("claiming orphaned messages", "count", len(claimIDs), "stream", s.stream)

	messages, err := s.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   s.stream,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  s.claimMinIdle,
		Messages: claimIDs,
	}).Result()

	if err != nil {
		logger.Error("failed to claim messages", "error", err)
		return
	}

	for _, xmsg := range messages {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		default:
		}

		retryCount := int(deliveryCounts[xmsg.ID])
		if !s.processRedisMessage(xmsg, retryCount, logger) {
			return
		}
		logger.Debug("successfully reprocessed claimed message", "msg_id", xmsg.ID)
	}
}

// SupportsRedelivery returns true because Redis Streams natively supports
// re-delivery of unacknowledged messages via consumer group pending entries.
func (t *Transport) SupportsRedelivery() bool { return true }

// Compile-time checks
var _ transport.Transport = (*Transport)(nil)
var _ transport.HealthChecker = (*Transport)(nil)
var _ transport.LagMonitor = (*Transport)(nil)
var _ transport.Redeliverable = (*Transport)(nil)
var _ transport.Subscription = (*subscription)(nil)

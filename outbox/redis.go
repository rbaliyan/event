package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

/*
Redis Schema:

Uses Redis Streams for the outbox:
- Stream: outbox:pending - pending messages
- Stream: outbox:published - published messages (for audit)
- Hash: outbox:failed:{id} - failed message details

Message fields in stream:
- id: message ID
- event_name: event name
- event_id: event UUID
- payload: JSON encoded payload
- metadata: JSON encoded metadata
- created_at: timestamp
- retry_count: number of retries
*/

// RedisMessage represents a message in Redis
type RedisMessage struct {
	StreamID   string            `json:"stream_id,omitempty"`
	ID         string            `json:"id"`
	EventName  string            `json:"event_name"`
	EventID    string            `json:"event_id"`
	Payload    []byte            `json:"payload"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	RetryCount int               `json:"retry_count"`
	LastError  string            `json:"last_error,omitempty"`
}

// ToMessage converts RedisMessage to Message
func (m *RedisMessage) ToMessage() *Message {
	id, _ := strconv.ParseInt(m.ID, 10, 64)
	return &Message{
		ID:         id,
		EventName:  m.EventName,
		EventID:    m.EventID,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		CreatedAt:  m.CreatedAt,
		RetryCount: m.RetryCount,
		LastError:  m.LastError,
		Status:     StatusPending,
	}
}

// RedisStore implements outbox storage using Redis Streams with Consumer Groups.
// Consumer Groups provide exactly-once delivery semantics for HA deployments.
type RedisStore struct {
	client        redis.Cmdable
	pendingKey    string
	publishedKey  string
	failedPrefix  string
	groupName     string // Consumer group name
	consumerName  string // This instance's consumer name
	maxLen        int64  // Max stream length (0 = unlimited)
}

// NewRedisStore creates a new Redis outbox store with consumer group support.
// Each relay instance should have a unique consumerName for proper HA operation.
func NewRedisStore(client redis.Cmdable) *RedisStore {
	return &RedisStore{
		client:       client,
		pendingKey:   "outbox:pending",
		publishedKey: "outbox:published",
		failedPrefix: "outbox:failed:",
		groupName:    "outbox-relay",
		consumerName: uuid.New().String(), // Unique per instance
		maxLen:       0,
	}
}

// WithConsumerName sets a custom consumer name for this relay instance.
// Use a stable name (e.g., hostname or pod name) to properly track pending messages.
func (s *RedisStore) WithConsumerName(name string) *RedisStore {
	s.consumerName = name
	return s
}

// WithGroupName sets a custom consumer group name.
func (s *RedisStore) WithGroupName(name string) *RedisStore {
	s.groupName = name
	return s
}

// WithKeyPrefix sets a custom key prefix
func (s *RedisStore) WithKeyPrefix(prefix string) *RedisStore {
	s.pendingKey = prefix + "pending"
	s.publishedKey = prefix + "published"
	s.failedPrefix = prefix + "failed:"
	return s
}

// WithMaxLen sets the maximum stream length
func (s *RedisStore) WithMaxLen(maxLen int64) *RedisStore {
	s.maxLen = maxLen
	return s
}

// Insert adds a message to the outbox
func (s *RedisStore) Insert(ctx context.Context, msg *RedisMessage) (string, error) {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	metadata, _ := json.Marshal(msg.Metadata)

	args := &redis.XAddArgs{
		Stream: s.pendingKey,
		Values: map[string]interface{}{
			"id":          msg.ID,
			"event_name":  msg.EventName,
			"event_id":    msg.EventID,
			"payload":     msg.Payload,
			"metadata":    metadata,
			"created_at":  msg.CreatedAt.Unix(),
			"retry_count": msg.RetryCount,
		},
	}

	if s.maxLen > 0 {
		args.MaxLen = s.maxLen
		args.Approx = true
	}

	streamID, err := s.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("xadd: %w", err)
	}

	return streamID, nil
}

// EnsureGroup creates the consumer group if it doesn't exist.
// Should be called at relay startup.
func (s *RedisStore) EnsureGroup(ctx context.Context) error {
	// Create group starting from the beginning of the stream
	// MKSTREAM creates the stream if it doesn't exist
	err := s.client.XGroupCreateMkStream(ctx, s.pendingKey, s.groupName, "0").Err()
	if err != nil {
		// Ignore "BUSYGROUP" error - group already exists
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("create group: %w", err)
		}
	}
	return nil
}

// GetPending retrieves pending messages for publishing using consumer groups.
// Uses XREADGROUP for exactly-once delivery in HA deployments.
// Messages are automatically claimed by this consumer and must be acknowledged.
func (s *RedisStore) GetPending(ctx context.Context, count int64) ([]*RedisMessage, error) {
	// First, check for any pending messages this consumer owns but hasn't acked
	// This handles relay restart scenarios
	pendingMsgs, err := s.claimPendingMessages(ctx, count)
	if err != nil {
		return nil, fmt.Errorf("claim pending: %w", err)
	}

	// If we have enough from pending, return them
	if int64(len(pendingMsgs)) >= count {
		return pendingMsgs, nil
	}

	// Read new messages from the stream
	remaining := count - int64(len(pendingMsgs))
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.groupName,
		Consumer: s.consumerName,
		Streams:  []string{s.pendingKey, ">"},
		Count:    remaining,
		Block:    0, // Non-blocking
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return pendingMsgs, nil // No new messages
		}
		return pendingMsgs, fmt.Errorf("xreadgroup: %w", err)
	}

	for _, stream := range streams {
		for _, result := range stream.Messages {
			msg, err := s.parseStreamMessage(result)
			if err != nil {
				continue
			}
			msg.StreamID = result.ID
			pendingMsgs = append(pendingMsgs, msg)
		}
	}

	return pendingMsgs, nil
}

// claimPendingMessages claims messages that are pending for this consumer.
// This handles messages that were read but not acknowledged (e.g., relay crash).
func (s *RedisStore) claimPendingMessages(ctx context.Context, count int64) ([]*RedisMessage, error) {
	// Check pending entries for this consumer
	pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   s.pendingKey,
		Group:    s.groupName,
		Consumer: s.consumerName,
		Start:    "-",
		End:      "+",
		Count:    count,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	if len(pending) == 0 {
		return nil, nil
	}

	// Claim these messages
	var ids []string
	for _, p := range pending {
		ids = append(ids, p.ID)
	}

	claimed, err := s.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   s.pendingKey,
		Group:    s.groupName,
		Consumer: s.consumerName,
		MinIdle:  0,
		Messages: ids,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("xclaim: %w", err)
	}

	var messages []*RedisMessage
	for _, result := range claimed {
		msg, err := s.parseStreamMessage(result)
		if err != nil {
			continue
		}
		msg.StreamID = result.ID
		messages = append(messages, msg)
	}

	return messages, nil
}

// parseStreamMessage parses a Redis stream message
func (s *RedisStore) parseStreamMessage(msg redis.XMessage) (*RedisMessage, error) {
	m := &RedisMessage{
		ID:        msg.Values["id"].(string),
		EventName: msg.Values["event_name"].(string),
		EventID:   msg.Values["event_id"].(string),
	}

	if payload, ok := msg.Values["payload"].(string); ok {
		m.Payload = []byte(payload)
	}

	if metadata, ok := msg.Values["metadata"].(string); ok {
		json.Unmarshal([]byte(metadata), &m.Metadata)
	}

	if createdAt, ok := msg.Values["created_at"].(string); ok {
		ts, _ := strconv.ParseInt(createdAt, 10, 64)
		m.CreatedAt = time.Unix(ts, 0)
	}

	if retryCount, ok := msg.Values["retry_count"].(string); ok {
		m.RetryCount, _ = strconv.Atoi(retryCount)
	}

	return m, nil
}

// MarkPublished acknowledges a message as successfully published.
// Uses XACK to remove from the consumer group's pending list.
func (s *RedisStore) MarkPublished(ctx context.Context, streamID string) error {
	// Acknowledge the message in the consumer group
	if err := s.client.XAck(ctx, s.pendingKey, s.groupName, streamID).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}

	// Optionally delete from stream to save memory (message is already acked)
	s.client.XDel(ctx, s.pendingKey, streamID)

	return nil
}

// MarkFailed records a failed message and acknowledges it
func (s *RedisStore) MarkFailed(ctx context.Context, streamID string, msg *RedisMessage, err error) error {
	key := s.failedPrefix + msg.ID

	fields := map[string]interface{}{
		"stream_id":   streamID,
		"event_name":  msg.EventName,
		"event_id":    msg.EventID,
		"payload":     msg.Payload,
		"error":       err.Error(),
		"retry_count": msg.RetryCount + 1,
		"failed_at":   time.Now().Unix(),
	}

	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	// Acknowledge and delete from stream
	s.client.XAck(ctx, s.pendingKey, s.groupName, streamID)
	s.client.XDel(ctx, s.pendingKey, streamID)

	return nil
}

// RecoverStuck claims messages from other consumers that have been pending too long.
// This handles crashed relay instances. Should be called periodically.
func (s *RedisStore) RecoverStuck(ctx context.Context, stuckDuration time.Duration) (int64, error) {
	// Get all pending messages across all consumers
	pending, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.pendingKey,
		Group:  s.groupName,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("xpending: %w", err)
	}

	var recovered int64
	minIdleMs := stuckDuration.Milliseconds()

	for _, p := range pending {
		// Only claim if idle longer than threshold
		if p.Idle.Milliseconds() < minIdleMs {
			continue
		}

		// Claim the message for this consumer
		_, err := s.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   s.pendingKey,
			Group:    s.groupName,
			Consumer: s.consumerName,
			MinIdle:  stuckDuration,
			Messages: []string{p.ID},
		}).Result()

		if err == nil {
			recovered++
		}
	}

	return recovered, nil
}

// RetryFailed moves failed messages back to pending
func (s *RedisStore) RetryFailed(ctx context.Context, maxRetries int) (int64, error) {
	// Scan for failed messages
	var cursor uint64
	var retried int64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, s.failedPrefix+"*", 100).Result()
		if err != nil {
			return retried, fmt.Errorf("scan: %w", err)
		}

		for _, key := range keys {
			fields, err := s.client.HGetAll(ctx, key).Result()
			if err != nil {
				continue
			}

			retryCount, _ := strconv.Atoi(fields["retry_count"])
			if retryCount >= maxRetries {
				continue
			}

			// Re-add to pending stream
			msg := &RedisMessage{
				ID:         fields["event_id"],
				EventName:  fields["event_name"],
				EventID:    fields["event_id"],
				Payload:    []byte(fields["payload"]),
				RetryCount: retryCount,
			}

			if _, err := s.Insert(ctx, msg); err == nil {
				s.client.Del(ctx, key)
				retried++
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return retried, nil
}

// Delete removes old messages from the published stream
func (s *RedisStore) Delete(ctx context.Context, olderThan time.Duration) (int64, error) {
	// For Redis streams, we use XTRIM with MINID
	// This is approximate cleanup
	cutoff := time.Now().Add(-olderThan)
	cutoffID := fmt.Sprintf("%d-0", cutoff.UnixMilli())

	deleted, err := s.client.XTrimMinID(ctx, s.publishedKey, cutoffID).Result()
	if err != nil {
		return 0, fmt.Errorf("xtrim: %w", err)
	}

	return deleted, nil
}

// Len returns the number of pending messages
func (s *RedisStore) Len(ctx context.Context) (int64, error) {
	return s.client.XLen(ctx, s.pendingKey).Result()
}

// RedisPublisher provides methods for publishing messages through the Redis outbox
type RedisPublisher struct {
	store *RedisStore
	codec codec.Codec
}

// NewRedisPublisher creates a new Redis outbox publisher
func NewRedisPublisher(client redis.Cmdable) *RedisPublisher {
	return &RedisPublisher{
		store: NewRedisStore(client),
		codec: codec.Default(),
	}
}

// WithCodec sets a custom codec
func (p *RedisPublisher) WithCodec(c codec.Codec) *RedisPublisher {
	p.codec = c
	return p
}

// Store returns the underlying store
func (p *RedisPublisher) Store() *RedisStore {
	return p.store
}

// Publish stores a message in the outbox
func (p *RedisPublisher) Publish(ctx context.Context, eventName string, payload any, metadata map[string]string) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	msg := &RedisMessage{
		EventName: eventName,
		EventID:   uuid.New().String(),
		Payload:   encoded,
		Metadata:  metadata,
	}

	_, err = p.store.Insert(ctx, msg)
	return err
}

// RedisRelay polls the Redis outbox and publishes messages to the transport
type RedisRelay struct {
	store         *RedisStore
	transport     transport.Transport
	pollDelay     time.Duration
	batchSize     int64
	logger        *slog.Logger
	stuckDuration time.Duration // How long before claiming messages from other consumers
}

// NewRedisRelay creates a new Redis outbox relay
func NewRedisRelay(store *RedisStore, t transport.Transport) *RedisRelay {
	return &RedisRelay{
		store:         store,
		transport:     t,
		pollDelay:     100 * time.Millisecond,
		batchSize:     100,
		logger:        slog.Default().With("component", "outbox.redis_relay"),
		stuckDuration: 5 * time.Minute, // Claim messages from crashed consumers after 5 min
	}
}

// WithPollDelay sets the polling interval
func (r *RedisRelay) WithPollDelay(d time.Duration) *RedisRelay {
	r.pollDelay = d
	return r
}

// WithBatchSize sets the batch size
func (r *RedisRelay) WithBatchSize(size int64) *RedisRelay {
	r.batchSize = size
	return r
}

// WithLogger sets a custom logger
func (r *RedisRelay) WithLogger(l *slog.Logger) *RedisRelay {
	r.logger = l
	return r
}

// WithStuckDuration sets how long a message can be pending before claiming from other consumers.
// Messages that have been pending longer than this duration in another consumer's queue
// will be claimed by this relay. This handles crashed relay instances.
// Default: 5 minutes
func (r *RedisRelay) WithStuckDuration(d time.Duration) *RedisRelay {
	r.stuckDuration = d
	return r
}

// Start begins polling the outbox.
// Creates the consumer group if it doesn't exist and starts the polling loop.
// Also periodically recovers messages stuck in other consumers (from crashed relays).
func (r *RedisRelay) Start(ctx context.Context) error {
	// Ensure consumer group exists
	if err := r.store.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("ensure group: %w", err)
	}

	r.logger.Info("started redis outbox relay",
		"consumer", r.store.consumerName,
		"group", r.store.groupName)

	ticker := time.NewTicker(r.pollDelay)
	defer ticker.Stop()

	// Recovery ticker for stuck messages (check every minute)
	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	// Recover any stuck messages at startup
	r.recoverStuck(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.publishPending(ctx)
		case <-recoveryTicker.C:
			r.recoverStuck(ctx)
		}
	}
}

// publishPending fetches and publishes pending messages
func (r *RedisRelay) publishPending(ctx context.Context) {
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
			r.store.MarkFailed(ctx, msg.StreamID, msg, err)
			continue
		}

		if err := r.store.MarkPublished(ctx, msg.StreamID); err != nil {
			r.logger.Error("failed to mark published",
				"id", msg.ID,
				"error", err)
		}

		r.logger.Debug("published outbox message",
			"id", msg.ID,
			"event", msg.EventName)
	}
}

// publishMessage publishes a single message
func (r *RedisRelay) publishMessage(ctx context.Context, msg *RedisMessage) error {
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

// recoverStuck claims messages from other consumers that have been pending too long.
// This handles crashed relay instances.
func (r *RedisRelay) recoverStuck(ctx context.Context) {
	recovered, err := r.store.RecoverStuck(ctx, r.stuckDuration)
	if err != nil {
		r.logger.Error("failed to recover stuck messages", "error", err)
		return
	}

	if recovered > 0 {
		r.logger.Warn("recovered stuck messages from other consumers",
			"count", recovered,
			"stuck_duration", r.stuckDuration)
	}
}

// PublishOnce processes pending messages once
func (r *RedisRelay) PublishOnce(ctx context.Context) error {
	r.publishPending(ctx)
	return nil
}

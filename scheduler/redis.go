package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

// RedisScheduler uses Redis sorted sets for scheduling.
//
// RedisScheduler provides distributed scheduled message delivery using Redis.
// Messages are stored in a sorted set with the scheduled time as the score,
// enabling efficient retrieval of due messages.
//
// Redis Data Structure:
//   - Sorted Set: {prefix}messages - score=scheduled_time, member=JSON message
//   - Sorted Set: {prefix}processing - messages being processed (for crash recovery)
//
// The scheduler uses a 2-phase approach for HA safety:
//  1. Atomically move message from "messages" to "processing" set
//  2. Publish to transport
//  3. Remove from "processing" set
//
// If a scheduler crashes after step 1, recoverStuck() will move messages
// back to the main set after stuckDuration.
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	scheduler := scheduler.NewRedisScheduler(rdb, transport,
//	    scheduler.WithPollInterval(100*time.Millisecond),
//	    scheduler.WithBatchSize(100),
//	)
//
//	// Start the scheduler (blocks until stopped)
//	go scheduler.Start(ctx)
//
//	// Schedule messages
//	scheduler.ScheduleAfter(ctx, "orders.reminder", payload, nil, time.Hour)
//
//	// Stop gracefully
//	scheduler.Stop(ctx)
type RedisScheduler struct {
	client        redis.Cmdable
	transport     transport.Transport
	opts          *Options
	logger        *slog.Logger
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	stuckDuration time.Duration // How long before a processing message is considered stuck
}

// NewRedisScheduler creates a new Redis-based scheduler.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - t: Transport for publishing due messages
//   - opts: Optional configuration options
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	scheduler := scheduler.NewRedisScheduler(rdb, transport)
func NewRedisScheduler(client redis.Cmdable, t transport.Transport, opts ...Option) *RedisScheduler {
	o := DefaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &RedisScheduler{
		client:        client,
		transport:     t,
		opts:          o,
		logger:        slog.Default().With("component", "scheduler.redis"),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		stuckDuration: 5 * time.Minute, // Recover stuck messages after 5 min
	}
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
// Messages stuck in processing longer than this duration are moved back to pending.
// This handles scheduler crashes where messages were claimed but never published.
// Default: 5 minutes
func (s *RedisScheduler) WithStuckDuration(d time.Duration) *RedisScheduler {
	s.stuckDuration = d
	return s
}

// WithLogger sets a custom logger for the scheduler.
//
// Returns the scheduler for method chaining.
func (s *RedisScheduler) WithLogger(l *slog.Logger) *RedisScheduler {
	s.logger = l
	return s
}

// Schedule adds a message for future delivery.
//
// The message is stored in a Redis sorted set with ScheduledAt as the score.
// If ID is empty, a UUID is generated. If CreatedAt is zero, it's set to now.
func (s *RedisScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Store in sorted set with scheduled time as score
	err = s.client.ZAdd(ctx, s.key(), redis.Z{
		Score:  float64(msg.ScheduledAt.Unix()),
		Member: data,
	}).Err()

	if err != nil {
		return fmt.Errorf("zadd: %w", err)
	}

	s.logger.Debug("scheduled message",
		"id", msg.ID,
		"event", msg.EventName,
		"scheduled_at", msg.ScheduledAt)

	return nil
}

// ScheduleAt schedules a message for a specific time.
//
// Convenience method that creates a Message and calls Schedule.
// Returns the generated message ID.
func (s *RedisScheduler) ScheduleAt(ctx context.Context, eventName string, payload []byte, metadata map[string]string, at time.Time) (string, error) {
	msg := Message{
		ID:          uuid.New().String(),
		EventName:   eventName,
		Payload:     payload,
		Metadata:    metadata,
		ScheduledAt: at,
		CreatedAt:   time.Now(),
	}

	if err := s.Schedule(ctx, msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

// ScheduleAfter schedules a message after a delay.
//
// Convenience method that calls ScheduleAt with time.Now().Add(delay).
// Returns the generated message ID.
func (s *RedisScheduler) ScheduleAfter(ctx context.Context, eventName string, payload []byte, metadata map[string]string, delay time.Duration) (string, error) {
	return s.ScheduleAt(ctx, eventName, payload, metadata, time.Now().Add(delay))
}

// Cancel cancels a scheduled message before delivery.
//
// Searches the sorted set for the message and removes it.
// Returns error if the message is not found.
func (s *RedisScheduler) Cancel(ctx context.Context, id string) error {
	// Get all scheduled messages and find the one to cancel
	// This is not efficient but Redis ZSET doesn't support direct member lookup by field
	results, err := s.client.ZRange(ctx, s.key(), 0, -1).Result()
	if err != nil {
		return fmt.Errorf("zrange: %w", err)
	}

	for _, result := range results {
		var msg Message
		if err := json.Unmarshal([]byte(result), &msg); err != nil {
			continue
		}

		if msg.ID == id {
			if err := s.client.ZRem(ctx, s.key(), result).Err(); err != nil {
				return fmt.Errorf("zrem: %w", err)
			}

			s.logger.Debug("cancelled scheduled message", "id", id)
			return nil
		}
	}

	return fmt.Errorf("message not found: %s", id)
}

// Get retrieves a scheduled message by ID.
//
// Searches the sorted set for the message.
// Returns error if the message is not found.
func (s *RedisScheduler) Get(ctx context.Context, id string) (*Message, error) {
	results, err := s.client.ZRange(ctx, s.key(), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("zrange: %w", err)
	}

	for _, result := range results {
		var msg Message
		if err := json.Unmarshal([]byte(result), &msg); err != nil {
			continue
		}

		if msg.ID == id {
			return &msg, nil
		}
	}

	return nil, fmt.Errorf("message not found: %s", id)
}

// List returns scheduled messages matching the filter.
//
// Uses ZRANGEBYSCORE to efficiently query by scheduled time.
// Returns empty slice if no matches.
func (s *RedisScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	min := "-inf"
	max := "+inf"

	if !filter.After.IsZero() {
		min = fmt.Sprintf("%d", filter.After.Unix())
	}
	if !filter.Before.IsZero() {
		max = fmt.Sprintf("%d", filter.Before.Unix())
	}

	results, err := s.client.ZRangeByScore(ctx, s.key(), &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("zrangebyscore: %w", err)
	}

	var messages []*Message
	for _, result := range results {
		var msg Message
		if err := json.Unmarshal([]byte(result), &msg); err != nil {
			continue
		}

		if filter.EventName != "" && msg.EventName != filter.EventName {
			continue
		}

		messages = append(messages, &msg)

		if filter.Limit > 0 && len(messages) >= filter.Limit {
			break
		}
	}

	return messages, nil
}

// Start begins the scheduler polling loop.
//
// This method blocks until the context is cancelled or Stop is called.
// It polls Redis at PollInterval for due messages and publishes them.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
//
// Example:
//
//	go scheduler.Start(ctx)
func (s *RedisScheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()

	// Recovery ticker for stuck messages (check every minute)
	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	s.logger.Info("scheduler started",
		"poll_interval", s.opts.PollInterval,
		"batch_size", s.opts.BatchSize)

	// Recover any stuck messages at startup
	s.recoverStuck(ctx)

	for {
		select {
		case <-ctx.Done():
			close(s.stoppedCh)
			return ctx.Err()
		case <-s.stopCh:
			close(s.stoppedCh)
			return nil
		case <-ticker.C:
			s.processDue(ctx)
		case <-recoveryTicker.C:
			s.recoverStuck(ctx)
		}
	}
}

// Stop gracefully stops the scheduler.
//
// Signals the polling loop to stop and waits for it to finish.
// Returns context error if the context expires before stopping.
func (s *RedisScheduler) Stop(ctx context.Context) error {
	close(s.stopCh)

	select {
	case <-s.stoppedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processDue processes messages that are due for delivery.
// Uses a 2-phase approach for crash safety:
// 1. Atomically move message from "messages" to "processing" set
// 2. Publish to transport
// 3. Remove from "processing" set
// If the scheduler crashes after step 1, recoverStuck will move it back.
func (s *RedisScheduler) processDue(ctx context.Context) {
	now := time.Now().Unix()

	for i := 0; i < s.opts.BatchSize; i++ {
		// Atomically move message from pending to processing
		msg, member, err := s.claimDueMessage(ctx, now)
		if err != nil {
			if err == redis.Nil {
				break // No more due messages
			}
			s.logger.Error("failed to claim due message", "error", err)
			break
		}

		// Publish to transport
		if err := s.publishMessage(ctx, msg); err != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			// Move back to pending for retry
			s.client.ZRem(ctx, s.processingKey(), member)
			s.client.ZAdd(ctx, s.key(), redis.Z{
				Score:  float64(msg.ScheduledAt.Unix()),
				Member: member,
			})
			continue
		}

		// Remove from processing set after successful publish
		s.client.ZRem(ctx, s.processingKey(), member)

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
	}
}

// claimDueMessage atomically claims a due message using a Lua script.
// Moves the message from the pending set to the processing set.
// This prevents race conditions in HA deployments where multiple schedulers run.
func (s *RedisScheduler) claimDueMessage(ctx context.Context, now int64) (*Message, string, error) {
	// Lua script to atomically:
	// 1. Get the first item with score <= now from pending
	// 2. Remove it from pending set
	// 3. Add it to processing set with current timestamp as score
	// 4. Return the member
	script := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
		if #items == 0 then
			return nil
		end
		redis.call('ZREM', KEYS[1], items[1])
		redis.call('ZADD', KEYS[2], ARGV[2], items[1])
		return items[1]
	`)

	claimedAt := time.Now().Unix()
	result, err := script.Run(ctx, s.client, []string{s.key(), s.processingKey()}, now, claimedAt).Result()
	if err != nil {
		return nil, "", err
	}

	if result == nil {
		return nil, "", redis.Nil
	}

	member := result.(string)

	var msg Message
	if err := json.Unmarshal([]byte(member), &msg); err != nil {
		// Corrupt message - remove from processing
		s.client.ZRem(ctx, s.processingKey(), member)
		s.logger.Error("failed to unmarshal claimed message", "error", err)
		return nil, "", redis.Nil
	}

	return &msg, member, nil
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles scheduler crashes where messages were claimed but never published.
func (s *RedisScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.stuckDuration).Unix()

	// Get messages that have been in processing too long
	results, err := s.client.ZRangeByScoreWithScores(ctx, s.processingKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", cutoff),
		Count: 100,
	}).Result()

	if err != nil {
		s.logger.Error("failed to get stuck messages", "error", err)
		return
	}

	if len(results) == 0 {
		return
	}

	var recovered int64
	for _, z := range results {
		member := z.Member.(string)

		var msg Message
		if err := json.Unmarshal([]byte(member), &msg); err != nil {
			// Corrupt message - just remove it
			s.client.ZRem(ctx, s.processingKey(), member)
			continue
		}

		// Move back to pending with original scheduled time
		pipe := s.client.Pipeline()
		pipe.ZRem(ctx, s.processingKey(), member)
		pipe.ZAdd(ctx, s.key(), redis.Z{
			Score:  float64(msg.ScheduledAt.Unix()),
			Member: member,
		})
		if _, err := pipe.Exec(ctx); err == nil {
			recovered++
		}
	}

	if recovered > 0 {
		s.logger.Warn("recovered stuck scheduled messages",
			"count", recovered,
			"stuck_duration", s.stuckDuration)
	}
}

// publishMessage publishes a scheduled message to the transport.
//
// Adds scheduler metadata (scheduled_message_id, scheduled_at) to the message.
func (s *RedisScheduler) publishMessage(ctx context.Context, msg *Message) error {
	// Add scheduler metadata
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["scheduled_message_id"] = msg.ID
	metadata["scheduled_at"] = msg.ScheduledAt.Format(time.RFC3339)

	// Create transport message
	transportMsg := message.New(
		msg.ID,
		"scheduler",
		msg.Payload,
		metadata,
		trace.SpanContext{},
	)

	return s.transport.Publish(ctx, msg.EventName, transportMsg)
}

// key returns the Redis key for the scheduled messages sorted set.
func (s *RedisScheduler) key() string {
	return s.opts.KeyPrefix + "messages"
}

// processingKey returns the Redis key for messages currently being processed.
func (s *RedisScheduler) processingKey() string {
	return s.opts.KeyPrefix + "processing"
}

// Compile-time check
var _ Scheduler = (*RedisScheduler)(nil)

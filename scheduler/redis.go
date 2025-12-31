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
//
// The scheduler polls Redis at PollInterval, retrieves messages with
// score <= current_time, publishes them to the transport, and removes them
// from the sorted set.
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
	client    redis.Cmdable
	transport transport.Transport
	opts      *Options
	logger    *slog.Logger
	stopCh    chan struct{}
	stoppedCh chan struct{}
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
		client:    client,
		transport: t,
		opts:      o,
		logger:    slog.Default().With("component", "scheduler.redis"),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
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
//
// Example:
//
//	go scheduler.Start(ctx)
func (s *RedisScheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.PollInterval)
	defer ticker.Stop()

	s.logger.Info("scheduler started",
		"poll_interval", s.opts.PollInterval,
		"batch_size", s.opts.BatchSize)

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
//
// Retrieves messages with score <= now from the sorted set,
// publishes them to the transport, and removes them on success.
func (s *RedisScheduler) processDue(ctx context.Context) {
	now := float64(time.Now().Unix())

	// Get messages due for delivery
	results, err := s.client.ZRangeByScoreWithScores(ctx, s.key(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%f", now),
		Count: int64(s.opts.BatchSize),
	}).Result()

	if err != nil {
		s.logger.Error("failed to get due messages", "error", err)
		return
	}

	for _, z := range results {
		member := z.Member.(string)

		var msg Message
		if err := json.Unmarshal([]byte(member), &msg); err != nil {
			s.logger.Error("failed to unmarshal message", "error", err)
			// Remove corrupt message
			s.client.ZRem(ctx, s.key(), member)
			continue
		}

		// Publish to transport
		if err := s.publishMessage(ctx, &msg); err != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			// Don't remove - will retry on next poll
			continue
		}

		// Remove from scheduled set
		if err := s.client.ZRem(ctx, s.key(), member).Err(); err != nil {
			s.logger.Error("failed to remove delivered message",
				"id", msg.ID,
				"error", err)
		}

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
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

// Compile-time check
var _ Scheduler = (*RedisScheduler)(nil)

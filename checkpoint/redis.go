// Package checkpoint provides checkpoint store implementations for resumable subscriptions.
//
// Checkpoint stores persist subscriber positions, enabling "start from latest,
// resume on reconnect" semantics for event subscriptions.
//
// Available implementations:
//   - RedisStore: Production-ready Redis-backed store
//   - MongoStore: Production-ready MongoDB-backed store
//
// Usage with event subscriptions:
//
//	store := checkpoint.NewRedisStore(redisClient, "myapp:checkpoints")
//	ev.Subscribe(ctx, handler,
//	    event.WithCheckpoint[Order](store, "order-processor"),
//	)
package checkpoint

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// CheckpointInfo contains detailed information about a checkpoint
type CheckpointInfo struct {
	SubscriberID string
	Position     time.Time
	UpdatedAt    time.Time
}

// RedisStore implements CheckpointStore using Redis.
// Checkpoints are stored as Unix timestamps in a Redis hash.
//
// Example:
//
//	store := checkpoint.NewRedisStore(redisClient, "myapp:checkpoints")
//	ev.Subscribe(ctx, handler,
//	    event.WithCheckpoint[Order](store, "order-processor"),
//	)
type RedisStore struct {
	client redis.Cmdable
	key    string
	ttl    time.Duration
}

// RedisOption configures the Redis checkpoint store
type RedisOption func(*RedisStore)

// WithTTL sets a TTL for checkpoint entries.
// After TTL expires, the checkpoint is removed and the subscriber
// will start from latest on next connection (like a first run).
// Default is 0 (no expiration).
func WithTTL(ttl time.Duration) RedisOption {
	return func(s *RedisStore) {
		s.ttl = ttl
	}
}

// NewRedisStore creates a new Redis-backed checkpoint store.
//
// Parameters:
//   - client: Redis client (supports Cmdable interface for universal client compatibility)
//   - key: Redis hash key to store all checkpoints (e.g., "myapp:checkpoints")
//
// Example:
//
//	// With standard client
//	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := checkpoint.NewRedisStore(client, "events:checkpoints")
//
//	// With cluster client
//	cluster := redis.NewClusterClient(&redis.ClusterOptions{...})
//	store := checkpoint.NewRedisStore(cluster, "events:checkpoints")
//
//	// With TTL (checkpoints expire after 7 days)
//	store := checkpoint.NewRedisStore(client, "events:checkpoints",
//	    checkpoint.WithTTL(7*24*time.Hour))
func NewRedisStore(client redis.Cmdable, key string, opts ...RedisOption) *RedisStore {
	s := &RedisStore{
		client: client,
		key:    key,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Save persists the checkpoint position for a subscriber.
func (s *RedisStore) Save(ctx context.Context, subscriberID string, position time.Time) error {
	// Store as Unix nanoseconds for precision
	value := strconv.FormatInt(position.UnixNano(), 10)

	if err := s.client.HSet(ctx, s.key, subscriberID, value).Err(); err != nil {
		return err
	}

	// Refresh TTL if configured
	if s.ttl > 0 {
		s.client.Expire(ctx, s.key, s.ttl)
	}

	return nil
}

// Load retrieves the last saved checkpoint for a subscriber.
// Returns zero time and nil error if no checkpoint exists.
func (s *RedisStore) Load(ctx context.Context, subscriberID string) (time.Time, error) {
	value, err := s.client.HGet(ctx, s.key, subscriberID).Result()
	if err == redis.Nil {
		// No checkpoint exists - this is not an error
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}

	// Parse Unix nanoseconds
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, nanos), nil
}

// Delete removes a checkpoint for a subscriber.
func (s *RedisStore) Delete(ctx context.Context, subscriberID string) error {
	return s.client.HDel(ctx, s.key, subscriberID).Err()
}

// DeleteAll removes all checkpoints (useful for testing or cleanup).
func (s *RedisStore) DeleteAll(ctx context.Context) error {
	return s.client.Del(ctx, s.key).Err()
}

// List returns all subscriber IDs with checkpoints.
func (s *RedisStore) List(ctx context.Context) ([]string, error) {
	return s.client.HKeys(ctx, s.key).Result()
}

// GetAll returns all checkpoints as a map.
func (s *RedisStore) GetAll(ctx context.Context) (map[string]time.Time, error) {
	result, err := s.client.HGetAll(ctx, s.key).Result()
	if err != nil {
		return nil, err
	}

	checkpoints := make(map[string]time.Time, len(result))
	for id, value := range result {
		nanos, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue // Skip invalid entries
		}
		checkpoints[id] = time.Unix(0, nanos)
	}

	return checkpoints, nil
}

// GetCheckpointInfo returns detailed checkpoint information for a subscriber.
// Note: Redis store doesn't track UpdatedAt separately, so it equals Position.
func (s *RedisStore) GetCheckpointInfo(ctx context.Context, subscriberID string) (*CheckpointInfo, error) {
	position, err := s.Load(ctx, subscriberID)
	if err != nil {
		return nil, err
	}
	if position.IsZero() {
		return nil, nil
	}
	return &CheckpointInfo{
		SubscriberID: subscriberID,
		Position:     position,
		UpdatedAt:    position, // Redis doesn't track separately
	}, nil
}

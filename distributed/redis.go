package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisStateValue stores state with timestamps for stale detection.
type redisStateValue struct {
	Status    string    `json:"s"`
	CreatedAt time.Time `json:"c"`
	UpdatedAt time.Time `json:"u"`
}

// RedisStateManager implements StateManager using Redis for distributed deployments.
//
// RedisStateManager uses Redis SETNX (set if not exists) with TTL for atomic,
// race-condition-free message state management. This is the recommended implementation
// for production deployments with multiple application instances.
//
// Design Philosophy:
//
// This implementation uses database atomic operations, not distributed locks:
//   - SETNX provides atomic state transitions (set-if-not-exists)
//   - Redis TTL provides automatic cleanup of expired states
//   - No separate lock acquisition/release - state IS the coordination mechanism
//
// Features:
//   - Atomic state acquisition using SETNX
//   - Automatic expiration using Redis TTL (no cleanup goroutine needed)
//   - Configurable key prefix for multi-tenant or multi-service deployments
//   - Supports Redis single node, Sentinel, Cluster, and UniversalClient
//
// Redis Keys:
//
// States are stored with keys in the format: {prefix}{messageID}
// Default prefix is "state:" so a message "order-123" becomes "state:order-123"
//
// Key values (JSON):
//   - {"s":"processing","c":"...","u":"..."}: Message is being processed
//   - {"s":"completed","c":"...","u":"..."}: Message was successfully processed
//
// Example:
//
//	// Basic setup
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	sm := distributed.NewRedisStateManager(rdb)
//
//	// With custom options
//	sm := distributed.NewRedisStateManager(rdb,
//	    distributed.WithPrefix("myapp:worker:"),
//	    distributed.WithStateTTL(10*time.Minute),
//	    distributed.WithCompletedTTL(48*time.Hour),
//	)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.WorkerPoolMiddleware[Order](sm, 5*time.Minute),
//	    ),
//	)
type RedisStateManager struct {
	client        redis.Cmdable
	prefix        string
	completionTTL time.Duration
}

// NewRedisStateManager creates a new Redis-based state manager.
//
// The state manager uses Redis SETNX for atomic state acquisition, which is both
// efficient and prevents race conditions between workers.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - opts: Optional configuration (prefix, TTLs)
//
// Returns a configured RedisStateManager ready for use.
//
// Example:
//
//	// Simple setup
//	sm := distributed.NewRedisStateManager(redisClient)
//
//	// With Redis Sentinel for HA
//	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
//	    MasterName:    "mymaster",
//	    SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
//	})
//	sm := distributed.NewRedisStateManager(rdb)
//
//	// With Redis Cluster
//	rdb := redis.NewClusterClient(&redis.ClusterOptions{
//	    Addrs: []string{"node1:6379", "node2:6379"},
//	})
//	sm := distributed.NewRedisStateManager(rdb)
func NewRedisStateManager(client redis.Cmdable, opts ...Option) *RedisStateManager {
	o := defaultStateOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &RedisStateManager{
		client:        client,
		prefix:        o.prefix,
		completionTTL: o.completionTTL,
	}
}

// Acquire atomically transitions a message to "processing" state using Redis SETNX.
//
// The transition is atomic: only one worker can successfully acquire each message.
// The TTL ensures that if a worker crashes, the state expires and another
// worker can acquire and process the message.
//
// Redis command: SET {prefix}{messageID} {json_value} NX EX {ttl_seconds}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to acquire
//   - ttl: How long to hold the state
//
// Returns:
//   - (true, nil): Acquisition succeeded, process the message
//   - (false, nil): Already acquired (key exists), skip the message
//   - (false, error): Redis error occurred
func (s *RedisStateManager) Acquire(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	key := s.prefix + messageID
	now := time.Now()

	// Create state value with timestamps for stale detection
	value := redisStateValue{
		Status:    "processing",
		CreatedAt: now,
		UpdatedAt: now,
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("marshal state value: %w", err)
	}

	// Use SET NX (set if not exists) with expiry
	set, err := s.client.SetNX(ctx, key, valueBytes, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}

	if !set {
		// Key already exists - check if it's a completed state or active
		// If completed, we should also skip (prevents reprocessing)
		return false, nil
	}

	return true, nil
}

// MarkProcessed transitions a message to "completed" state.
//
// Updates the state value to "completed" and extends TTL to completionTTL.
// This prevents the message from being reprocessed if delivered again
// within the completion window.
//
// Redis command: SET {prefix}{messageID} {json_value} EX {completionTTL}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message that was successfully processed
//
// Returns nil on success, error if Redis operation fails.
func (s *RedisStateManager) MarkProcessed(ctx context.Context, messageID string) error {
	key := s.prefix + messageID
	now := time.Now()

	// Get existing value to preserve created_at
	existingBytes, err := s.client.Get(ctx, key).Bytes()
	var createdAt time.Time
	if err == nil {
		var existing redisStateValue
		if json.Unmarshal(existingBytes, &existing) == nil {
			createdAt = existing.CreatedAt
		}
	}
	if createdAt.IsZero() {
		createdAt = now
	}

	// Update to completed state with longer TTL
	value := redisStateValue{
		Status:    "completed",
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal state value: %w", err)
	}

	err = s.client.Set(ctx, key, valueBytes, s.completionTTL).Err()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	return nil
}

// Reset removes the message state to allow immediate reacquisition.
//
// Call this when processing fails and you want another worker to retry
// the message immediately instead of waiting for the state TTL to expire.
//
// Redis command: DEL {prefix}{messageID}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to reset
//
// Returns nil on success (including when key doesn't exist), error if Redis fails.
func (s *RedisStateManager) Reset(ctx context.Context, messageID string) error {
	key := s.prefix + messageID

	// Delete the state so another worker can acquire immediately
	err := s.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	return nil
}

// ListStale returns message IDs of states that have been processing
// for longer than staleTimeout.
//
// This uses Redis SCAN to iterate through state keys and checks each one
// for stale processing state. Note: This can be expensive with many states.
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to return (0 = no limit)
//
// Returns list of message IDs that are stale.
func (s *RedisStateManager) ListStale(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	cutoff := time.Now().Add(-staleTimeout)
	var stale []string

	// Use SCAN to iterate through keys with our prefix
	pattern := s.prefix + "*"
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan: %w", err)
		}

		for _, key := range keys {
			// Get value and check if it's a stale processing state
			valueBytes, err := s.client.Get(ctx, key).Bytes()
			if err != nil {
				continue // Key may have expired
			}

			var value redisStateValue
			if err := json.Unmarshal(valueBytes, &value); err != nil {
				// Legacy format (plain string) - skip
				continue
			}

			if value.Status == "processing" && value.UpdatedAt.Before(cutoff) {
				// Extract message ID from key (remove prefix)
				messageID := key[len(s.prefix):]
				stale = append(stale, messageID)

				if limit > 0 && len(stale) >= limit {
					return stale, nil
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return stale, nil
}

// ResetStale resets all stale states, allowing them to be reacquired.
//
// This is a convenience method that combines ListStale and Reset.
// It's useful for batch cleanup of stale states.
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a state can be processing before considered stale
//   - limit: Maximum number of stale states to reset (0 = no limit)
//
// Returns the number of states reset.
func (s *RedisStateManager) ResetStale(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	stale, err := s.ListStale(ctx, staleTimeout, limit)
	if err != nil {
		return 0, err
	}

	if len(stale) == 0 {
		return 0, nil
	}

	// Build keys to delete
	keys := make([]string, len(stale))
	for i, msgID := range stale {
		keys[i] = s.prefix + msgID
	}

	deleted, err := s.client.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis del: %w", err)
	}

	return deleted, nil
}

// Compile-time interface check
var _ StateManager = (*RedisStateManager)(nil)

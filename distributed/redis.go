package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisClaimValue stores claim state with timestamps for orphan detection.
type redisClaimValue struct {
	State     string    `json:"s"`
	CreatedAt time.Time `json:"c"`
	UpdatedAt time.Time `json:"u"`
}

// RedisClaimer implements MessageClaimer using Redis for distributed deployments.
//
// RedisClaimer uses Redis SETNX (set if not exists) with TTL for atomic,
// race-condition-free message claiming. This is the recommended implementation
// for production deployments with multiple application instances.
//
// Features:
//   - Atomic claim acquisition using SETNX
//   - Automatic expiration using Redis TTL (no cleanup goroutine needed)
//   - Configurable key prefix for multi-tenant or multi-service deployments
//   - Supports Redis single node, Sentinel, Cluster, and UniversalClient
//
// Redis Keys:
//
// Claims are stored with keys in the format: {prefix}{messageID}
// Default prefix is "claim:" so a message "order-123" becomes "claim:order-123"
//
// Key values:
//   - "pending": Message is claimed and being processed
//   - "completed": Message was successfully processed
//
// Example:
//
//	// Basic setup
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	claimer := distributed.NewRedisClaimer(rdb)
//
//	// With custom options
//	claimer := distributed.NewRedisClaimer(rdb,
//	    distributed.WithClaimerPrefix("myapp:worker:"),
//	    distributed.WithClaimerTTL(10*time.Minute),
//	    distributed.WithCompletionTTL(48*time.Hour),
//	)
//
//	// Use with middleware
//	event.Subscribe(ctx, handler,
//	    event.WithMiddleware(
//	        distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
//	    ),
//	)
type RedisClaimer struct {
	client        redis.Cmdable
	prefix        string
	completionTTL time.Duration
}

// NewRedisClaimer creates a new Redis-based message claimer.
//
// The claimer uses Redis SETNX for atomic claim acquisition, which is both
// efficient and prevents race conditions between workers.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - opts: Optional configuration (prefix, TTLs)
//
// Returns a configured RedisClaimer ready for use.
//
// Example:
//
//	// Simple setup
//	claimer := distributed.NewRedisClaimer(redisClient)
//
//	// With Redis Sentinel for HA
//	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
//	    MasterName:    "mymaster",
//	    SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
//	})
//	claimer := distributed.NewRedisClaimer(rdb)
//
//	// With Redis Cluster
//	rdb := redis.NewClusterClient(&redis.ClusterOptions{
//	    Addrs: []string{"node1:6379", "node2:6379"},
//	})
//	claimer := distributed.NewRedisClaimer(rdb)
func NewRedisClaimer(client redis.Cmdable, opts ...ClaimerOption) *RedisClaimer {
	o := defaultClaimerOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &RedisClaimer{
		client:        client,
		prefix:        o.prefix,
		completionTTL: o.completionTTL,
	}
}

// TryClaim attempts to claim a message using Redis SETNX.
//
// The claim is atomic: only one worker can successfully claim each message.
// The TTL ensures that if a worker crashes, the claim expires and another
// worker can pick up the message.
//
// Redis command: SET {prefix}{messageID} {json_value} NX EX {ttl_seconds}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to claim
//   - ttl: How long to hold the claim
//
// Returns:
//   - (true, nil): Claim succeeded, process the message
//   - (false, nil): Already claimed (key exists), skip the message
//   - (false, error): Redis error occurred
func (c *RedisClaimer) TryClaim(ctx context.Context, messageID string, ttl time.Duration) (bool, error) {
	key := c.prefix + messageID
	now := time.Now()

	// Create claim value with timestamps for orphan detection
	value := redisClaimValue{
		State:     "pending",
		CreatedAt: now,
		UpdatedAt: now,
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return false, fmt.Errorf("marshal claim value: %w", err)
	}

	// Use SET NX (set if not exists) with expiry
	set, err := c.client.SetNX(ctx, key, valueBytes, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}

	if !set {
		// Key already exists - check if it's a completed claim or active
		// If completed, we should also skip (prevents reprocessing)
		return false, nil
	}

	return true, nil
}

// Complete marks a message as successfully processed.
//
// Updates the claim value to "completed" and extends TTL to completionTTL.
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
func (c *RedisClaimer) Complete(ctx context.Context, messageID string) error {
	key := c.prefix + messageID
	now := time.Now()

	// Get existing value to preserve created_at
	existingBytes, err := c.client.Get(ctx, key).Bytes()
	var createdAt time.Time
	if err == nil {
		var existing redisClaimValue
		if json.Unmarshal(existingBytes, &existing) == nil {
			createdAt = existing.CreatedAt
		}
	}
	if createdAt.IsZero() {
		createdAt = now
	}

	// Update to completed state with longer TTL
	value := redisClaimValue{
		State:     "completed",
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal claim value: %w", err)
	}

	err = c.client.Set(ctx, key, valueBytes, c.completionTTL).Err()
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	return nil
}

// Release removes the claim to allow immediate retry by another worker.
//
// Call this when processing fails and you want another worker to retry
// the message immediately instead of waiting for the claim TTL to expire.
//
// Redis command: DEL {prefix}{messageID}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message to release
//
// Returns nil on success (including when key doesn't exist), error if Redis fails.
func (c *RedisClaimer) Release(ctx context.Context, messageID string) error {
	key := c.prefix + messageID

	// Delete the claim so another worker can claim immediately
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	return nil
}

// ListOrphanedClaims returns message IDs of claims that have been pending
// for longer than staleTimeout.
//
// This uses Redis SCAN to iterate through claim keys and checks each one
// for stale pending state. Note: This can be expensive with many claims.
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a claim can be pending before considered orphaned
//   - limit: Maximum number of orphans to return (0 = no limit)
//
// Returns list of message IDs that are orphaned.
func (c *RedisClaimer) ListOrphanedClaims(ctx context.Context, staleTimeout time.Duration, limit int) ([]string, error) {
	cutoff := time.Now().Add(-staleTimeout)
	var orphans []string

	// Use SCAN to iterate through keys with our prefix
	pattern := c.prefix + "*"
	var cursor uint64

	for {
		keys, nextCursor, err := c.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan: %w", err)
		}

		for _, key := range keys {
			// Get value and check if it's a stale pending claim
			valueBytes, err := c.client.Get(ctx, key).Bytes()
			if err != nil {
				continue // Key may have expired
			}

			var value redisClaimValue
			if err := json.Unmarshal(valueBytes, &value); err != nil {
				// Legacy format (plain "pending"/"completed" string) - skip
				continue
			}

			if value.State == "pending" && value.UpdatedAt.Before(cutoff) {
				// Extract message ID from key (remove prefix)
				messageID := key[len(c.prefix):]
				orphans = append(orphans, messageID)

				if limit > 0 && len(orphans) >= limit {
					return orphans, nil
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return orphans, nil
}

// ReleaseOrphans releases all orphaned claims, allowing them to be reclaimed.
//
// This is a convenience method that combines ListOrphanedClaims and Release.
// It's useful for batch cleanup of stale claims.
//
// Parameters:
//   - ctx: Context for cancellation
//   - staleTimeout: How long a claim can be pending before considered orphaned
//   - limit: Maximum number of orphans to release (0 = no limit)
//
// Returns the number of claims released.
func (c *RedisClaimer) ReleaseOrphans(ctx context.Context, staleTimeout time.Duration, limit int) (int64, error) {
	orphans, err := c.ListOrphanedClaims(ctx, staleTimeout, limit)
	if err != nil {
		return 0, err
	}

	if len(orphans) == 0 {
		return 0, nil
	}

	// Build keys to delete
	keys := make([]string, len(orphans))
	for i, msgID := range orphans {
		keys[i] = c.prefix + msgID
	}

	deleted, err := c.client.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis del: %w", err)
	}

	return deleted, nil
}

// Compile-time interface check
var _ MessageClaimer = (*RedisClaimer)(nil)

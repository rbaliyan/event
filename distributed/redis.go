package distributed

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

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
// Redis command: SET {prefix}{messageID} "pending" NX EX {ttl_seconds}
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

	// Use SET NX (set if not exists) with expiry
	// The value "pending" indicates the message is being processed
	set, err := c.client.SetNX(ctx, key, "pending", ttl).Result()
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
// Redis command: SET {prefix}{messageID} "completed" EX {completionTTL}
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message that was successfully processed
//
// Returns nil on success, error if Redis operation fails.
func (c *RedisClaimer) Complete(ctx context.Context, messageID string) error {
	key := c.prefix + messageID

	// Update to completed state with longer TTL
	// This prevents reprocessing if the same message is delivered again
	err := c.client.Set(ctx, key, "completed", c.completionTTL).Err()
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

// Compile-time interface check
var _ MessageClaimer = (*RedisClaimer)(nil)

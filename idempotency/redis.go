package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements Store using Redis for distributed idempotency.
//
// RedisStore is the recommended store for production deployments with:
//   - Multiple application instances sharing idempotency state
//   - High-availability requirements with Redis Sentinel or Cluster
//   - Need for persistence across application restarts
//
// Features:
//   - Atomic check-and-set using Redis SET NX with expiry
//   - Automatic TTL-based cleanup (no background goroutine needed)
//   - Configurable key prefix for multi-tenant deployments
//   - Thread-safe by design (Redis handles concurrency)
//
// Redis Commands Used:
//   - SET NX with expiry: Atomic duplicate check and mark
//   - SET with expiry: Mark as processed
//   - DEL: Remove entry
//
// Example:
//
//	// Create Redis client
//	rdb := redis.NewClient(&redis.Options{
//	    Addr: "localhost:6379",
//	})
//
//	// Create idempotency store with 24-hour TTL
//	store := idempotency.NewRedisStore(rdb, 24*time.Hour)
//
//	// Optional: customize key prefix for multi-tenant
//	store = store.WithPrefix("myapp:dedup:")
//
//	// Use in message handler
//	func handlePayment(ctx context.Context, payment Payment) error {
//	    key := fmt.Sprintf("payment:%s", payment.ID)
//
//	    isDuplicate, err := store.IsDuplicate(ctx, key)
//	    if err != nil {
//	        return fmt.Errorf("idempotency check: %w", err)
//	    }
//	    if isDuplicate {
//	        log.Info("duplicate payment, skipping", "payment_id", payment.ID)
//	        return nil
//	    }
//
//	    // Process payment...
//	    return processPayment(payment)
//	}
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
	prefix string
}

// NewRedisStore creates a new Redis-based idempotency store.
//
// The store uses Redis SET NX (set if not exists) for atomic duplicate
// detection, which is both efficient and race-condition free.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, and Cluster)
//   - ttl: How long to remember processed message IDs
//
// The default key prefix is "idemp:" which can be customized with WithPrefix().
//
// Example:
//
//	// Basic setup
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := idempotency.NewRedisStore(rdb, time.Hour)
//
//	// With Redis Sentinel for HA
//	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
//	    MasterName:    "mymaster",
//	    SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
//	})
//	store := idempotency.NewRedisStore(rdb, time.Hour)
//
//	// With Redis Cluster
//	rdb := redis.NewClusterClient(&redis.ClusterOptions{
//	    Addrs: []string{"node1:6379", "node2:6379", "node3:6379"},
//	})
//	// Note: ClusterClient requires NewRedisClusterStore (not shown)
func NewRedisStore(client *redis.Client, ttl time.Duration) *RedisStore {
	return &RedisStore{
		client: client,
		ttl:    ttl,
		prefix: "idemp:",
	}
}

// WithPrefix sets a custom prefix for Redis keys.
//
// This is useful for:
//   - Multi-tenant applications (prefix per tenant)
//   - Multiple environments sharing Redis (prefix per environment)
//   - Organizing keys by application or service
//
// The prefix is prepended to all message IDs when creating Redis keys.
// For example, with prefix "orders:" and message ID "123", the Redis
// key will be "orders:123".
//
// Parameters:
//   - prefix: The key prefix (should end with a separator like ":" or "/")
//
// Returns the store for method chaining.
//
// Example:
//
//	// Per-tenant isolation
//	store := idempotency.NewRedisStore(rdb, time.Hour).
//	    WithPrefix(fmt.Sprintf("tenant:%s:dedup:", tenantID))
//
//	// Per-service isolation
//	store := idempotency.NewRedisStore(rdb, time.Hour).
//	    WithPrefix("payment-service:idemp:")
func (s *RedisStore) WithPrefix(prefix string) *RedisStore {
	s.prefix = prefix
	return s
}

// IsDuplicate checks if a message ID has already been processed.
//
// This method performs an atomic check-and-set operation using Redis SET NX
// (set if not exists) with an expiry. This means:
//   - If the key doesn't exist: creates it with TTL and returns false (not duplicate)
//   - If the key exists: returns true (is duplicate)
//
// The atomic nature prevents race conditions where two instances might both
// think they're the first to process a message.
//
// Note: Unlike MemoryStore, this method also marks the message as "in progress"
// when it returns false. This prevents other instances from processing the
// same message concurrently. You should still call MarkProcessed after
// successful processing to refresh the TTL if needed.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to check
//
// Returns:
//   - (true, nil): Message was already processed or is being processed
//   - (false, nil): Message is new, caller should process it
//   - (false, error): Redis operation failed
//
// Example:
//
//	isDuplicate, err := store.IsDuplicate(ctx, "order-123")
//	if err != nil {
//	    // Redis error - decide how to handle (fail open or closed)
//	    return fmt.Errorf("idempotency check failed: %w", err)
//	}
//	if isDuplicate {
//	    log.Debug("skipping duplicate message", "id", "order-123")
//	    return nil
//	}
//	// Process message - we're guaranteed to be the only processor
func (s *RedisStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	key := s.prefix + messageID

	// Use SET NX (set if not exists) with expiry
	// Returns true if key was set (not duplicate), false if already exists (duplicate)
	set, err := s.client.SetNX(ctx, key, "1", s.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}

	return !set, nil // If set succeeded, it's NOT a duplicate
}

// MarkProcessed marks a message ID as processed using the default TTL.
//
// For RedisStore, IsDuplicate already marks the message when it returns false,
// so this method primarily serves to refresh the TTL after successful processing.
// This is useful when processing takes a long time and you want to ensure the
// idempotency key doesn't expire mid-processing.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//
// Returns nil on success, error if the Redis operation fails.
//
// Example:
//
//	// After successful processing, refresh the TTL
//	if err := processOrder(order); err != nil {
//	    return err
//	}
//	return store.MarkProcessed(ctx, fmt.Sprintf("order:%s", order.ID))
func (s *RedisStore) MarkProcessed(ctx context.Context, messageID string) error {
	return s.MarkProcessedWithTTL(ctx, messageID, s.ttl)
}

// MarkProcessedWithTTL marks a message ID as processed with a custom TTL.
//
// Use this when different message types require different retention periods.
// The TTL determines how long the message ID is remembered, preventing
// duplicate processing during that window.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to mark
//   - ttl: Custom time-to-live for this entry
//
// Returns nil on success, error if the Redis operation fails.
//
// Example:
//
//	// High-value payment: remember for 7 days
//	store.MarkProcessedWithTTL(ctx, "payment:xyz", 7*24*time.Hour)
//
//	// Low-value notification: remember for 1 hour
//	store.MarkProcessedWithTTL(ctx, "notif:abc", time.Hour)
func (s *RedisStore) MarkProcessedWithTTL(ctx context.Context, messageID string, ttl time.Duration) error {
	key := s.prefix + messageID
	return s.client.Set(ctx, key, "1", ttl).Err()
}

// Remove removes a message ID from the store.
//
// After removal, the message ID is no longer considered a duplicate and
// can be processed again if redelivered. This is useful for:
//   - Testing: Reset state between test runs
//   - Manual intervention: Allow reprocessing after fixing a bug
//   - Cleanup: Remove entries that shouldn't block reprocessing
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - messageID: The unique message identifier to remove
//
// Returns nil on success (including when the key doesn't exist),
// error if the Redis operation fails.
//
// Example:
//
//	// Allow reprocessing of a specific message after manual fix
//	if err := store.Remove(ctx, "order:failed-123"); err != nil {
//	    log.Error("failed to remove idempotency key", "error", err)
//	}
//
//	// Now the message can be processed again when redelivered
func (s *RedisStore) Remove(ctx context.Context, messageID string) error {
	key := s.prefix + messageID
	return s.client.Del(ctx, key).Err()
}

// Compile-time check
var _ Store = (*RedisStore)(nil)

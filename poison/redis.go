package poison

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is a Redis-based implementation of Store for distributed deployments.
//
// RedisStore enables poison message detection across multiple application instances.
// Failure counts and quarantine status are stored in Redis, ensuring consistent
// behavior regardless of which instance processes a message.
//
// Redis Keys:
//   - {prefix}failures:{messageID} - Failure count (string with TTL)
//   - {prefix}quarantine:{messageID} - Quarantine marker (string with TTL)
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := poison.NewRedisStore(rdb,
//	    poison.WithFailurePrefix("myapp:poison:failures:"),
//	    poison.WithQuarantinePrefix("myapp:poison:quarantine:"),
//	    poison.WithFailureTTL(24 * time.Hour),
//	)
//	detector := poison.NewDetector(store)
type RedisStore struct {
	client           redis.Cmdable
	failurePrefix    string
	quarantinePrefix string
	failureTTL       time.Duration // TTL for failure counts
}

// RedisStoreOption configures the Redis store.
type RedisStoreOption func(*RedisStore)

// WithFailurePrefix sets the prefix for failure count keys.
//
// Use this for multi-tenant deployments or to organize keys by application.
//
// Example:
//
//	store := poison.NewRedisStore(rdb, poison.WithFailurePrefix("myapp:failures:"))
func WithFailurePrefix(prefix string) RedisStoreOption {
	return func(s *RedisStore) {
		s.failurePrefix = prefix
	}
}

// WithQuarantinePrefix sets the prefix for quarantine keys.
//
// Use this for multi-tenant deployments or to organize keys by application.
//
// Example:
//
//	store := poison.NewRedisStore(rdb, poison.WithQuarantinePrefix("myapp:quarantine:"))
func WithQuarantinePrefix(prefix string) RedisStoreOption {
	return func(s *RedisStore) {
		s.quarantinePrefix = prefix
	}
}

// WithFailureTTL sets the TTL for failure count entries.
//
// Failure counts are automatically deleted after this duration. This prevents
// unbounded growth when messages are not processed successfully but also not
// quarantined (e.g., threshold not reached).
//
// Default: 24 hours
//
// Example:
//
//	store := poison.NewRedisStore(rdb, poison.WithFailureTTL(7 * 24 * time.Hour))
func WithFailureTTL(ttl time.Duration) RedisStoreOption {
	return func(s *RedisStore) {
		s.failureTTL = ttl
	}
}

// NewRedisStore creates a new Redis-based poison store.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - opts: Optional configuration options
//
// Default configuration:
//   - Failure prefix: "poison:failures:"
//   - Quarantine prefix: "poison:quarantine:"
//   - Failure TTL: 24 hours
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := poison.NewRedisStore(rdb)
//	detector := poison.NewDetector(store)
func NewRedisStore(client redis.Cmdable, opts ...RedisStoreOption) *RedisStore {
	s := &RedisStore{
		client:           client,
		failurePrefix:    "poison:failures:",
		quarantinePrefix: "poison:quarantine:",
		failureTTL:       24 * time.Hour,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// IncrementFailure atomically increments and returns the failure count.
//
// Uses Redis INCR and sets/refreshes the TTL on the failure key.
func (s *RedisStore) IncrementFailure(ctx context.Context, messageID string) (int, error) {
	key := s.failurePrefix + messageID

	pipe := s.client.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, s.failureTTL)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("redis pipeline: %w", err)
	}

	return int(incr.Val()), nil
}

// GetFailureCount returns the current failure count for a message.
// Returns 0 if the message has no recorded failures.
func (s *RedisStore) GetFailureCount(ctx context.Context, messageID string) (int, error) {
	key := s.failurePrefix + messageID

	val, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redis get: %w", err)
	}

	count, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("parse count: %w", err)
	}

	return count, nil
}

// MarkPoison marks a message as quarantined with the given TTL.
//
// Uses Redis SET with expiration. The quarantine automatically expires
// after TTL without manual cleanup.
func (s *RedisStore) MarkPoison(ctx context.Context, messageID string, ttl time.Duration) error {
	key := s.quarantinePrefix + messageID

	if err := s.client.Set(ctx, key, "1", ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	return nil
}

// IsPoison checks if a message is currently quarantined.
//
// Uses Redis EXISTS to check for the quarantine key.
func (s *RedisStore) IsPoison(ctx context.Context, messageID string) (bool, error) {
	key := s.quarantinePrefix + messageID

	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists: %w", err)
	}

	return exists > 0, nil
}

// ClearPoison removes a message from quarantine immediately.
func (s *RedisStore) ClearPoison(ctx context.Context, messageID string) error {
	key := s.quarantinePrefix + messageID

	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	return nil
}

// ClearFailures resets the failure count for a message.
func (s *RedisStore) ClearFailures(ctx context.Context, messageID string) error {
	key := s.failurePrefix + messageID

	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	return nil
}

// Compile-time check
var _ Store = (*RedisStore)(nil)

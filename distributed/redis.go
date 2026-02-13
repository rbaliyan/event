package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis state status values (matching MongoDB constants for consistency).
const (
	redisStatusProcessing = "processing"
	redisStatusCompleted  = "completed"
)

// redisStateValue stores state with timestamps for stale detection.
type redisStateValue struct {
	Status    string    `json:"s"`
	CreatedAt time.Time `json:"c"`
	UpdatedAt time.Time `json:"u"`
}

// redisPayloadValue stores message payload for recovery re-publishing.
type redisPayloadValue struct {
	Payload   []byte            `json:"p,omitempty"`
	Metadata  map[string]string `json:"m,omitempty"`
	EventName string            `json:"e,omitempty"`
}

// RedisStateManager implements Coordinator and PayloadStore using Redis for distributed deployments.
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
		Status:    redisStatusProcessing,
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

// markProcessedScript atomically reads the existing state (preserving
// created_at), builds a completed state value, and overwrites with new TTL.
// All in a single Lua script — no separate round-trips, no TOCTOU race.
//
// KEYS[1] = state key
// ARGV[1] = completed status string (JSON field value)
// ARGV[2] = updated_at timestamp (RFC3339)
// ARGV[3] = TTL in milliseconds
//
// Returns: 1 if updated, 0 if key no longer exists (no-op)
var markProcessedScript = redis.NewScript(`
local existing = redis.call("GET", KEYS[1])
if not existing then
  return 0
end
local ok, val = pcall(cjson.decode, existing)
local created = ARGV[2]
if ok and val and val.c then
  created = val.c
end
local newVal = cjson.encode({s = ARGV[1], c = created, u = ARGV[2]})
redis.call("SET", KEYS[1], newVal, "PX", ARGV[3])
return 1
`)

// MarkProcessed transitions a message to "completed" state.
//
// Uses a single Lua script to atomically read the existing state (preserving
// created_at), build the new completed value, and overwrite with completion TTL.
// If the key was deleted between Acquire and MarkProcessed (e.g., by a
// concurrent Reset), the update is a no-op — it will not recreate a key
// that was intentionally removed. In this case MarkProcessed returns nil
// (silent no-op) since the message state has already been cleared.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messageID: The message that was successfully processed
//
// Returns nil on success (including no-op when key was deleted).
func (s *RedisStateManager) MarkProcessed(ctx context.Context, messageID string) error {
	key := s.prefix + messageID
	now := time.Now().Format(time.RFC3339Nano)
	ttlMs := s.completionTTL.Milliseconds()

	err := markProcessedScript.Run(ctx, s.client, []string{key}, redisStatusCompleted, now, ttlMs).Err()
	if err != nil {
		return fmt.Errorf("redis mark processed: %w", err)
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

	// Delete the state and payload so another worker can acquire immediately
	err := s.client.Del(ctx, key, s.payloadKey(messageID)).Err()
	if err != nil {
		return fmt.Errorf("redis del: %w", err)
	}

	return nil
}

// scanStaleStates iterates Redis state keys via SCAN and returns stale processing entries.
// Each entry includes the message ID and its parsed state value.
func (s *RedisStateManager) scanStaleStates(ctx context.Context, cutoff time.Time, limit int) ([]staleEntry, error) {
	var entries []staleEntry
	pattern := s.prefix + "*"
	var cursor uint64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan: %w", err)
		}

		for _, key := range keys {
			// Skip payload companion keys
			if strings.HasSuffix(key, ":payload") {
				continue
			}

			valueBytes, err := s.client.Get(ctx, key).Bytes()
			if err != nil {
				continue // Key may have expired
			}

			var value redisStateValue
			if err := json.Unmarshal(valueBytes, &value); err != nil {
				continue
			}

			if value.Status == redisStatusProcessing && value.UpdatedAt.Before(cutoff) {
				messageID := key[len(s.prefix):]
				entries = append(entries, staleEntry{messageID: messageID, value: value})
				if limit > 0 && len(entries) >= limit {
					return entries, nil
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return entries, nil
}

// staleEntry pairs a message ID with its parsed state value.
type staleEntry struct {
	messageID string
	value     redisStateValue
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
	entries, err := s.scanStaleStates(ctx, cutoff, limit)
	if err != nil {
		return nil, err
	}

	stale := make([]string, len(entries))
	for i, e := range entries {
		stale[i] = e.messageID
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

	// Build keys to delete (state + payload companion keys)
	keys := make([]string, 0, len(stale)*2)
	for _, msgID := range stale {
		keys = append(keys, s.prefix+msgID, s.payloadKey(msgID))
	}

	_, err = s.client.Del(ctx, keys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis del: %w", err)
	}

	// Return the number of stale messages reset (not Redis DEL count which
	// includes payload companion keys and would be ~2x the actual count).
	return int64(len(stale)), nil
}

// payloadKey returns the Redis key for storing payload alongside state.
func (s *RedisStateManager) payloadKey(messageID string) string {
	return s.prefix + messageID + ":payload"
}

// storePayloadScript atomically reads the state key TTL and sets the payload
// key with matching expiry. This avoids the TOCTOU race where the state key
// could expire between a separate TTL read and the payload SET.
//
// KEYS[1] = state key
// KEYS[2] = payload key
// ARGV[1] = payload value (JSON)
//
// Returns: 1 if stored, 0 if state key has no TTL (skipped)
var storePayloadScript = redis.NewScript(`
local ttl = redis.call("PTTL", KEYS[1])
if ttl <= 0 then
  return 0
end
redis.call("SET", KEYS[2], ARGV[1], "PX", ttl)
return 1
`)

// StorePayload persists payload in a separate Redis key alongside the state.
// Uses a Lua script to atomically read the state key TTL and set the payload
// with matching expiry, preventing orphaned payload keys.
func (s *RedisStateManager) StorePayload(ctx context.Context, messageID string, data *MessageData) error {
	if data == nil || len(data.Payload) == 0 {
		return nil
	}

	pv := redisPayloadValue{
		Payload:   data.Payload,
		Metadata:  data.Metadata,
		EventName: data.EventName,
	}
	pvBytes, err := json.Marshal(pv)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	stateKey := s.prefix + messageID
	payloadKey := s.payloadKey(messageID)
	err = storePayloadScript.Run(ctx, s.client, []string{stateKey, payloadKey}, pvBytes).Err()
	if err != nil {
		return fmt.Errorf("redis store payload: %w", err)
	}

	return nil
}

// LoadStalePayloads returns stale messages that have stored payload.
//
// Performance note: this scans ALL stale state keys via SCAN, then checks each
// one for a companion payload key. For large key spaces (>10k stale states),
// consider using MongoDB-backed state management or setting a batch limit on
// the RecoveryRunner to cap the number of entries processed per cycle.
func (s *RedisStateManager) LoadStalePayloads(ctx context.Context, staleTimeout time.Duration, limit int) ([]*StaleMessage, error) {
	cutoff := time.Now().Add(-staleTimeout)
	// Scan without limit since we need to filter by payload existence after
	entries, err := s.scanStaleStates(ctx, cutoff, 0)
	if err != nil {
		return nil, err
	}

	var results []*StaleMessage
	for _, e := range entries {
		// Only return entries that actually have stored payload
		payloadBytes, err := s.client.Get(ctx, s.payloadKey(e.messageID)).Bytes()
		if err != nil {
			continue // no payload stored, skip
		}

		var pv redisPayloadValue
		if json.Unmarshal(payloadBytes, &pv) != nil {
			continue
		}

		results = append(results, &StaleMessage{
			MessageID: e.messageID,
			Data:      MessageData(pv),
			CreatedAt: e.value.CreatedAt,
		})
		if limit > 0 && len(results) >= limit {
			return results, nil
		}
	}

	return results, nil
}

// ClearPayload removes stored payload for a message.
func (s *RedisStateManager) ClearPayload(ctx context.Context, messageID string) error {
	return s.client.Del(ctx, s.payloadKey(messageID)).Err()
}

// RedisStateManager does not implement WorkerStore because listing
// worker entries via Redis SCAN is O(N) and impractical at scale.
// Use MongoStateManager or MemoryStateManager for worker observability.

// Compile-time interface checks
var (
	_ Coordinator   = (*RedisStateManager)(nil)
	_ PayloadStore  = (*RedisStateManager)(nil)
	_ StaleResetter = (*RedisStateManager)(nil)
)

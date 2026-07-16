package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

/*
Redis Schema:

Uses a single Redis Stream plus a consumer group as the outbox:
  - Stream: outbox:pending — all outbox entries (XADD-only, immutable)
  - Consumer group: outbox-relay — tracks delivery via the Pending Entries List (PEL)

Stream entries are append-only and immutable, so retry state is NOT stored on the
entry. Instead it is derived from the consumer group's PEL delivery-count: an entry
delivered N times has been retried N-1 times (the first delivery is not a retry).
Delivery flow:
  - Store        -> XADD to the pending stream.
  - ClaimPending -> re-read this consumer's PEL (XPENDING + XCLAIM, crash recovery)
                    then read new entries (XREADGROUP ">").
  - Ack          -> XACK + XDEL (entry fully processed, removed from stream).
  - Fail         -> no XACK; the entry stays in the PEL and is re-delivered with an
                    incremented delivery-count on the next claim.
  - RecoverStuck -> XAUTOCLAIM idle entries from crashed consumers to this one.
*/

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*redisStoreOptions)

type redisStoreOptions struct {
	consumerName string
	groupName    string
	keyPrefix    string
	maxLen       int64
}

// WithConsumerName sets a custom consumer name for this relay instance.
// Use a stable name (e.g., hostname or pod name) to properly track pending messages.
func WithConsumerName(name string) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if name != "" {
			o.consumerName = name
		}
	}
}

// WithGroupName sets a custom consumer group name.
func WithGroupName(name string) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if name != "" {
			o.groupName = name
		}
	}
}

// WithKeyPrefix sets a custom key prefix for all Redis keys.
func WithKeyPrefix(prefix string) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if prefix != "" {
			o.keyPrefix = prefix
		}
	}
}

// WithMaxLen sets the maximum stream length.
func WithMaxLen(maxLen int64) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if maxLen > 0 {
			o.maxLen = maxLen
		}
	}
}

// RedisStore implements Store using Redis Streams with a consumer group.
// Consumer groups provide at-least-once delivery for HA deployments: each relay
// instance is a distinct consumer, and unacked entries stay in that consumer's
// PEL until acked (Ack) or reclaimed by another instance (RecoverStuck).
//
// Note: Redis Streams are append-only and ordered by stream ID. The Priority
// field is not honored — entries are delivered in insertion order. Use the
// PostgreSQL or MongoDB stores when priority-based ordering is required.
type RedisStore struct {
	client       redis.Cmdable
	pendingKey   string
	publishedKey string
	failedPrefix string
	groupName    string // Consumer group name
	consumerName string // This instance's consumer name
	maxLen       int64  // Max stream length (0 = unlimited)
}

// NewRedisStore creates a new Redis outbox store with consumer group support.
// Each relay instance should have a unique consumerName for proper HA operation.
func NewRedisStore(client redis.Cmdable, opts ...RedisStoreOption) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis: client is required")
	}

	o := &redisStoreOptions{
		consumerName: uuid.New().String(),
		groupName:    "outbox-relay",
		keyPrefix:    "outbox:",
	}
	for _, opt := range opts {
		opt(o)
	}

	return &RedisStore{
		client:       client,
		pendingKey:   o.keyPrefix + "pending",
		publishedKey: o.keyPrefix + "published",
		failedPrefix: o.keyPrefix + "failed:",
		groupName:    o.groupName,
		consumerName: o.consumerName,
		maxLen:       o.maxLen,
	}, nil
}

// EnsureGroup creates the consumer group if it doesn't exist.
// Should be called at relay startup (see EnsureReady).
func (s *RedisStore) EnsureGroup(ctx context.Context) error {
	// Create group starting from the beginning of the stream.
	// MKSTREAM creates the stream if it doesn't exist.
	err := s.client.XGroupCreateMkStream(ctx, s.pendingKey, s.groupName, "0").Err()
	if err != nil {
		// Ignore "BUSYGROUP" error — group already exists.
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("create group: %w", err)
		}
	}
	return nil
}

// EnsureReady implements Starter: the generic relay calls this once at startup
// so the consumer group exists before ClaimPending issues XREADGROUP (otherwise
// it returns NOGROUP). Wraps the existing EnsureGroup.
func (s *RedisStore) EnsureReady(ctx context.Context) error { return s.EnsureGroup(ctx) }

// Store appends the event to the pending stream. Best-effort / non-transactional:
// Redis has no cross-DB transaction, so this must not be relied on for atomicity
// with a business database (see design spec, Bus-level integration caveat).
func (s *RedisStore) Store(ctx context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error {
	meta, _ := json.Marshal(metadata)
	args := &redis.XAddArgs{
		Stream: s.pendingKey,
		Values: map[string]any{
			"event_name": eventName, "event_id": eventID,
			"payload": payload, "metadata": meta, "created_at": time.Now().Unix(),
		},
	}
	if s.maxLen > 0 {
		args.MaxLen, args.Approx = s.maxLen, true
	}
	return s.client.XAdd(ctx, args).Err()
}

// ClaimPending re-reads this consumer's PEL (crash recovery) then reads new
// entries. RetryCount is the PEL delivery-count.
func (s *RedisStore) ClaimPending(ctx context.Context, limit int) (Batch, error) {
	count := int64(limit)
	msgs := make([]Message, 0, limit)

	// 1. Own PEL first (delivery-count > 0). XClaim from this consumer.
	pend, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.pendingKey, Group: s.groupName, Consumer: s.consumerName,
		Start: "-", End: "+", Count: count,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("xpending: %w", err)
	}
	deliveries := map[string]int64{}
	if len(pend) > 0 {
		ids := make([]string, len(pend))
		for i, p := range pend {
			ids[i] = p.ID
			deliveries[p.ID] = p.RetryCount // go-redis: delivery count
		}
		claimed, err := s.client.XClaim(ctx, &redis.XClaimArgs{
			Stream: s.pendingKey, Group: s.groupName, Consumer: s.consumerName,
			MinIdle: 0, Messages: ids,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("xclaim own: %w", err)
		}
		for _, x := range claimed {
			msgs = append(msgs, s.toMessage(x, deliveries[x.ID]))
		}
	}

	// 2. New entries.
	if int64(len(msgs)) < count {
		streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: s.groupName, Consumer: s.consumerName,
			Streams: []string{s.pendingKey, ">"}, Count: count - int64(len(msgs)), Block: -1,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("xreadgroup: %w", err)
		}
		for _, st := range streams {
			for _, x := range st.Messages {
				msgs = append(msgs, s.toMessage(x, 0))
			}
		}
	}
	return &redisBatch{store: s, msgs: msgs}, nil
}

// toMessage converts a Redis stream entry to a neutral Message. The stream ID is
// stashed in the unexported token for Ack/Fail resolution. deliveries is the PEL
// delivery-count (0 for a first-time XREADGROUP read).
func (s *RedisStore) toMessage(x redis.XMessage, deliveries int64) Message {
	m := Message{token: x.ID, Status: StatusProcessing}
	if v, ok := x.Values["event_name"].(string); ok {
		m.EventName = v
	}
	if v, ok := x.Values["event_id"].(string); ok {
		m.EventID = v
	}
	if v, ok := x.Values["payload"].(string); ok {
		m.Payload = []byte(v)
	}
	if v, ok := x.Values["metadata"].(string); ok && v != "" {
		_ = json.Unmarshal([]byte(v), &m.Metadata)
	}
	// deliveries is the count of PRIOR failed deliveries: the XPENDING value read
	// before this claim's XClaim re-delivery (0 for a first-time XREADGROUP read).
	// This mirrors Postgres, which increments retry_count on each Fail, so
	// RetryCount == number of prior attempts and shouldSkip/backoff stay in parity.
	if deliveries > 0 {
		m.RetryCount = int(deliveries)
	}
	return m
}

// Cleanup is a no-op for Redis: acked entries are XDEL'd on Ack, so there is
// nothing to sweep. Always returns 0.
func (s *RedisStore) Cleanup(ctx context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

// RecoverStuck reassigns entries idle longer than olderThan from crashed
// consumers to this one, so the next ClaimPending re-reads them from our PEL.
func (s *RedisStore) RecoverStuck(ctx context.Context, olderThan time.Duration) (int64, error) {
	var moved int64
	start := "0-0"
	for {
		msgs, next, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: s.pendingKey, Group: s.groupName, Consumer: s.consumerName,
			MinIdle: olderThan, Start: start, Count: 100,
		}).Result()
		if err != nil {
			return moved, fmt.Errorf("xautoclaim: %w", err)
		}
		moved += int64(len(msgs))
		if next == "0-0" || next == "" {
			break
		}
		start = next
	}
	return moved, nil
}

// redisBatch: Ack=XACK+XDEL (published); Fail=leave in PEL (re-delivered).
type redisBatch struct {
	store *RedisStore
	msgs  []Message
}

func (b *redisBatch) Messages() []Message { return b.msgs }

func (b *redisBatch) Ack(ctx context.Context, msg Message) error {
	id, ok := msg.token.(string)
	if !ok {
		return fmt.Errorf("redis outbox: expected string stream id token, got %T", msg.token)
	}
	if err := b.store.client.XAck(ctx, b.store.pendingKey, b.store.groupName, id).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}
	return b.store.client.XDel(ctx, b.store.pendingKey, id).Err()
}

func (b *redisBatch) Fail(context.Context, Message, error) error {
	// Intentionally no XACK: the entry stays in this consumer's PEL and is
	// re-delivered (delivery-count incremented) on the next claim.
	return nil
}

func (b *redisBatch) Close(context.Context) error { return nil }

// Compile-time checks.
var (
	_ Store          = (*RedisStore)(nil)
	_ StuckRecoverer = (*RedisStore)(nil)
	_ Starter        = (*RedisStore)(nil)
)

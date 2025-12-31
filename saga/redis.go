package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

/*
Redis Schema:

Uses Redis Hashes for saga state:
- Hash: saga:{id} - saga state
- Set: saga:by_name:{name} - saga IDs by name
- Set: saga:by_status:{status} - saga IDs by status
- Sorted Set: saga:by_time - saga IDs sorted by start time
*/

// RedisStore is a Redis-based saga store.
//
// RedisStore provides distributed saga state storage using Redis. It supports:
//   - Hash storage for saga state
//   - Set-based indexes for efficient filtering
//   - Optional TTL for automatic cleanup of completed sagas
//   - Multiple saga instances across application nodes
//
// Redis Schema:
//   - saga:{id} - Hash containing saga state fields
//   - saga:by_name:{name} - Set of saga IDs for a given saga name
//   - saga:by_status:{status} - Set of saga IDs in a given status
//   - saga:by_time - Sorted set of saga IDs by start time
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := saga.NewRedisStore(rdb).
//	    WithKeyPrefix("myapp:saga:").
//	    WithTTL(7 * 24 * time.Hour)
//
//	orderSaga := saga.New("order-creation", steps...).
//	    WithStore(store)
type RedisStore struct {
	client       redis.Cmdable
	prefix       string
	namePrefix   string
	statusPrefix string
	timeKey      string
	ttl          time.Duration // TTL for completed sagas (0 = no expiry)
}

// NewRedisStore creates a new Redis saga store.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//
// Default configuration:
//   - Key prefix: "saga:"
//   - TTL: 0 (no expiry)
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	store := saga.NewRedisStore(rdb)
func NewRedisStore(client redis.Cmdable) *RedisStore {
	return &RedisStore{
		client:       client,
		prefix:       "saga:",
		namePrefix:   "saga:by_name:",
		statusPrefix: "saga:by_status:",
		timeKey:      "saga:by_time",
		ttl:          0,
	}
}

// WithKeyPrefix sets a custom key prefix.
//
// Use this for multi-tenant deployments or to organize keys by application.
//
// Parameters:
//   - prefix: The key prefix (e.g., "myapp:saga:")
//
// Returns the store for method chaining.
func (s *RedisStore) WithKeyPrefix(prefix string) *RedisStore {
	s.prefix = prefix
	s.namePrefix = prefix + "by_name:"
	s.statusPrefix = prefix + "by_status:"
	s.timeKey = prefix + "by_time"
	return s
}

// WithTTL sets the TTL for completed sagas.
//
// When set, completed and compensated sagas are automatically deleted
// after the TTL expires. This prevents unbounded growth of saga data.
//
// Parameters:
//   - ttl: Time-to-live for completed sagas (0 = no expiry)
//
// Returns the store for method chaining.
//
// Example:
//
//	store := saga.NewRedisStore(rdb).
//	    WithTTL(7 * 24 * time.Hour) // Keep for 7 days
func (s *RedisStore) WithTTL(ttl time.Duration) *RedisStore {
	s.ttl = ttl
	return s
}

// Create creates a new saga instance
func (s *RedisStore) Create(ctx context.Context, state *State) error {
	key := s.prefix + state.ID

	// Check if exists
	exists, err := s.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("exists: %w", err)
	}
	if exists > 0 {
		return fmt.Errorf("saga already exists: %s", state.ID)
	}

	// Serialize state
	if err := s.saveState(ctx, key, state); err != nil {
		return err
	}

	// Add to indexes
	s.client.SAdd(ctx, s.namePrefix+state.Name, state.ID)
	s.client.SAdd(ctx, s.statusPrefix+string(state.Status), state.ID)
	s.client.ZAdd(ctx, s.timeKey, redis.Z{
		Score:  float64(state.StartedAt.Unix()),
		Member: state.ID,
	})

	return nil
}

// saveState saves saga state to Redis hash
func (s *RedisStore) saveState(ctx context.Context, key string, state *State) error {
	completedSteps, _ := json.Marshal(state.CompletedSteps)
	data, _ := json.Marshal(state.Data)

	fields := map[string]interface{}{
		"id":              state.ID,
		"name":            state.Name,
		"status":          string(state.Status),
		"current_step":    state.CurrentStep,
		"completed_steps": completedSteps,
		"data":            data,
		"error":           state.Error,
		"started_at":      state.StartedAt.Unix(),
		"last_updated_at": state.LastUpdatedAt.Unix(),
	}

	if state.CompletedAt != nil {
		fields["completed_at"] = state.CompletedAt.Unix()
	}

	if err := s.client.HSet(ctx, key, fields).Err(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	return nil
}

// Get retrieves saga state by ID
func (s *RedisStore) Get(ctx context.Context, id string) (*State, error) {
	key := s.prefix + id

	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("saga not found: %s", id)
	}

	return s.parseState(fields)
}

// parseState converts hash fields to State
func (s *RedisStore) parseState(fields map[string]string) (*State, error) {
	state := &State{
		ID:     fields["id"],
		Name:   fields["name"],
		Status: Status(fields["status"]),
		Error:  fields["error"],
	}

	if cs := fields["current_step"]; cs != "" {
		state.CurrentStep, _ = strconv.Atoi(cs)
	}

	if steps := fields["completed_steps"]; steps != "" {
		json.Unmarshal([]byte(steps), &state.CompletedSteps)
	}

	if data := fields["data"]; data != "" {
		json.Unmarshal([]byte(data), &state.Data)
	}

	if ts := fields["started_at"]; ts != "" {
		unix, _ := strconv.ParseInt(ts, 10, 64)
		state.StartedAt = time.Unix(unix, 0)
	}

	if ts := fields["completed_at"]; ts != "" {
		unix, _ := strconv.ParseInt(ts, 10, 64)
		t := time.Unix(unix, 0)
		state.CompletedAt = &t
	}

	if ts := fields["last_updated_at"]; ts != "" {
		unix, _ := strconv.ParseInt(ts, 10, 64)
		state.LastUpdatedAt = time.Unix(unix, 0)
	}

	return state, nil
}

// Update updates saga state
func (s *RedisStore) Update(ctx context.Context, state *State) error {
	key := s.prefix + state.ID

	// Get old status for index update
	oldStatus, err := s.client.HGet(ctx, key, "status").Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("hget: %w", err)
	}

	// Save new state
	if err := s.saveState(ctx, key, state); err != nil {
		return err
	}

	// Update status index if changed
	if oldStatus != string(state.Status) {
		s.client.SRem(ctx, s.statusPrefix+oldStatus, state.ID)
		s.client.SAdd(ctx, s.statusPrefix+string(state.Status), state.ID)
	}

	// Set TTL for completed sagas
	if s.ttl > 0 && (state.Status == StatusCompleted || state.Status == StatusCompensated) {
		s.client.Expire(ctx, key, s.ttl)
	}

	return nil
}

// List lists sagas matching the filter
func (s *RedisStore) List(ctx context.Context, filter StoreFilter) ([]*State, error) {
	var ids []string

	if filter.Name != "" && len(filter.Status) > 0 {
		// Intersect name and status sets
		nameKey := s.namePrefix + filter.Name
		statusKeys := make([]string, len(filter.Status))
		for i, status := range filter.Status {
			statusKeys[i] = s.statusPrefix + string(status)
		}

		// Get IDs matching name
		nameIDs, _ := s.client.SMembers(ctx, nameKey).Result()

		// Filter by status
		for _, id := range nameIDs {
			for _, statusKey := range statusKeys {
				isMember, _ := s.client.SIsMember(ctx, statusKey, id).Result()
				if isMember {
					ids = append(ids, id)
					break
				}
			}
		}
	} else if filter.Name != "" {
		ids, _ = s.client.SMembers(ctx, s.namePrefix+filter.Name).Result()
	} else if len(filter.Status) > 0 {
		// Union of status sets
		for _, status := range filter.Status {
			statusIDs, _ := s.client.SMembers(ctx, s.statusPrefix+string(status)).Result()
			ids = append(ids, statusIDs...)
		}
	} else {
		// Get all IDs from sorted set
		ids, _ = s.client.ZRevRange(ctx, s.timeKey, 0, -1).Result()
	}

	// Apply limit
	if filter.Limit > 0 && len(ids) > filter.Limit {
		ids = ids[:filter.Limit]
	}

	// Fetch states
	var states []*State
	for _, id := range ids {
		state, err := s.Get(ctx, id)
		if err != nil {
			continue
		}
		states = append(states, state)
	}

	return states, nil
}

// Delete removes a saga by ID
func (s *RedisStore) Delete(ctx context.Context, id string) error {
	state, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	key := s.prefix + id

	// Remove from all indexes
	s.client.Del(ctx, key)
	s.client.SRem(ctx, s.namePrefix+state.Name, id)
	s.client.SRem(ctx, s.statusPrefix+string(state.Status), id)
	s.client.ZRem(ctx, s.timeKey, id)

	return nil
}

// DeleteOlderThan removes sagas older than the specified age
func (s *RedisStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := float64(time.Now().Add(-age).Unix())

	// Get old saga IDs
	ids, err := s.client.ZRangeByScore(ctx, s.timeKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", cutoff),
	}).Result()

	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}

	var deleted int64
	for _, id := range ids {
		if err := s.Delete(ctx, id); err == nil {
			deleted++
		}
	}

	return deleted, nil
}

// GetFailed returns all failed sagas
func (s *RedisStore) GetFailed(ctx context.Context, name string, limit int) ([]*State, error) {
	filter := StoreFilter{
		Status: []Status{StatusFailed},
		Limit:  limit,
	}
	if name != "" {
		filter.Name = name
	}
	return s.List(ctx, filter)
}

// GetPending returns all pending/running sagas
func (s *RedisStore) GetPending(ctx context.Context, limit int) ([]*State, error) {
	return s.List(ctx, StoreFilter{
		Status: []Status{StatusPending, StatusRunning, StatusCompensating},
		Limit:  limit,
	})
}

// Count returns the total number of sagas
func (s *RedisStore) Count(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.timeKey).Result()
}

// CountByStatus returns the count of sagas by status
func (s *RedisStore) CountByStatus(ctx context.Context, status Status) (int64, error) {
	return s.client.SCard(ctx, s.statusPrefix+string(status)).Result()
}

// CountByName returns the count of sagas by name
func (s *RedisStore) CountByName(ctx context.Context, name string) (int64, error) {
	return s.client.SCard(ctx, s.namePrefix+name).Result()
}

// Compile-time check
var _ Store = (*RedisStore)(nil)

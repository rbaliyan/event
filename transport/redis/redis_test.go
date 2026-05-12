package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"github.com/redis/go-redis/v9"
)

// mockRedisClient implements Client for testing
type mockRedisClient struct {
	mu             sync.Mutex
	streams        map[string][]redis.XMessage
	groups         map[string]map[string]string // stream -> group -> lastID
	msgID          int
	closed         bool
	xaddErr        error
	xreadErr       error
	xpendingResult map[string]*redis.XPending // stream:group -> result (nil = use default)
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		streams: make(map[string][]redis.XMessage),
		groups:  make(map[string]map[string]string),
	}
}

func (m *mockRedisClient) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewStringCmd(ctx)
	if m.xaddErr != nil {
		cmd.SetErr(m.xaddErr)
		return cmd
	}

	m.msgID++
	msgID := fmt.Sprintf("%d-0", m.msgID)

	// Convert Values to map[string]any
	values := make(map[string]any)
	if v, ok := a.Values.(map[string]any); ok {
		values = v
	} else if v, ok := a.Values.(map[string]interface{}); ok {
		for k, val := range v {
			values[k] = val
		}
	}

	msg := redis.XMessage{
		ID:     msgID,
		Values: values,
	}
	m.streams[a.Stream] = append(m.streams[a.Stream], msg)
	cmd.SetVal(msgID)
	return cmd
}

func (m *mockRedisClient) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewStatusCmd(ctx)
	if m.groups[stream] == nil {
		m.groups[stream] = make(map[string]string)
	}
	if _, exists := m.groups[stream][group]; exists {
		cmd.SetErr(errors.New("BUSYGROUP Consumer Group name already exists"))
		return cmd
	}
	m.groups[stream][group] = start
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedisClient) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewXStreamSliceCmd(ctx)
	if m.xreadErr != nil {
		cmd.SetErr(m.xreadErr)
		return cmd
	}

	// Return empty if no messages
	stream := a.Streams[0]
	messages := m.streams[stream]
	if len(messages) == 0 {
		cmd.SetErr(redis.Nil)
		return cmd
	}

	// Return all pending messages and clear them
	result := []redis.XStream{
		{
			Stream:   stream,
			Messages: messages,
		},
	}
	m.streams[stream] = nil
	cmd.SetVal(result)
	return cmd
}

func (m *mockRedisClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(ids)))
	return cmd
}

func (m *mockRedisClient) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(ids)))
	return cmd
}

func (m *mockRedisClient) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	cmd := redis.NewXPendingExtCmd(ctx)
	// Return empty pending list for tests
	cmd.SetVal([]redis.XPendingExt{})
	return cmd
}

func (m *mockRedisClient) XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
	cmd := redis.NewXMessageSliceCmd(ctx)
	// Return empty claimed messages for tests
	cmd.SetVal([]redis.XMessage{})
	return cmd
}

func (m *mockRedisClient) XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewIntCmd(ctx)
	if m.groups[stream] != nil {
		delete(m.groups[stream], group)
		cmd.SetVal(1)
	} else {
		cmd.SetVal(0)
	}
	return cmd
}

func (m *mockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("PONG")
	return cmd
}

func (m *mockRedisClient) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd := redis.NewXPendingCmd(ctx)
	key := stream + ":" + group
	if m.xpendingResult != nil {
		if r, ok := m.xpendingResult[key]; ok {
			cmd.SetVal(r)
			return cmd
		}
	}
	cmd.SetVal(&redis.XPending{Count: 0, Lower: "", Higher: ""})
	return cmd
}

func (m *mockRedisClient) XLen(ctx context.Context, stream string) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(m.streams[stream])))
	return cmd
}

func (m *mockRedisClient) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewXInfoGroupsCmd(ctx, stream)
	groups := make([]redis.XInfoGroup, 0)
	if m.groups[stream] != nil {
		for name := range m.groups[stream] {
			pending := int64(0)
			key := stream + ":" + name
			if m.xpendingResult != nil {
				if r, ok := m.xpendingResult[key]; ok {
					pending = r.Count
				}
			}
			groups = append(groups, redis.XInfoGroup{
				Name:      name,
				Consumers: 0,
				Pending:   pending,
				Lag:       0,
			})
		}
	}
	cmd.SetVal(groups)
	return cmd
}

func (m *mockRedisClient) XTrimMinIDApprox(ctx context.Context, key string, minID string, limit int64) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(0)
	return cmd
}

func (m *mockRedisClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// testMessage creates a test message
func testMessage(source, payload string) message.Message {
	return message.New(transport.NewID(), source, []byte(payload), nil)
}

func TestNew(t *testing.T) {
	t.Run("nil client returns error", func(t *testing.T) {
		_, err := New(nil)
		if err != ErrClientRequired {
			t.Errorf("expected ErrClientRequired, got %v", err)
		}
	})

	t.Run("valid client creates transport", func(t *testing.T) {
		client := newMockRedisClient()
		tr, err := New(client)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tr == nil {
			t.Fatal("expected transport, got nil")
		}
		tr.Close(context.Background())
	})
}

func TestTransportOptions(t *testing.T) {
	client := newMockRedisClient()

	tr, err := New(client,
		WithCodec(codec.Default()),
		WithConsumerGroup("custom-group"),
		WithMaxLen(1000),
		WithBlockTime(10*time.Second),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer tr.Close(context.Background())

	if tr.groupID != "custom-group" {
		t.Errorf("expected groupID 'custom-group', got %s", tr.groupID)
	}
	if tr.maxLen != 1000 {
		t.Errorf("expected maxLen 1000, got %d", tr.maxLen)
	}
	if tr.blockTime != 10*time.Second {
		t.Errorf("expected blockTime 10s, got %v", tr.blockTime)
	}
}

func TestTransportRegisterEvent(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()

	t.Run("register event creates stream and group", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify group was created
		if _, ok := client.groups["evt:test-event"]; !ok {
			t.Error("expected stream group to be created")
		}
	})

	t.Run("register same event twice returns error", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != transport.ErrEventAlreadyExists {
			t.Errorf("expected ErrEventAlreadyExists, got %v", err)
		}
	})

	t.Run("register on closed transport returns error", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient())
		tr2.Close(context.Background())
		err := tr2.RegisterEvent(ctx, "new-event")
		if err != transport.ErrTransportClosed {
			t.Errorf("expected ErrTransportClosed, got %v", err)
		}
	})
}

func TestTransportUnregisterEvent(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()

	t.Run("unregister existing event", func(t *testing.T) {
		tr.RegisterEvent(ctx, "to-remove")
		err := tr.UnregisterEvent(ctx, "to-remove")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("unregister non-existent event returns error", func(t *testing.T) {
		err := tr.UnregisterEvent(ctx, "non-existent")
		if err != transport.ErrEventNotRegistered {
			t.Errorf("expected ErrEventNotRegistered, got %v", err)
		}
	})
}

func TestTransportPublish(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()
	tr.RegisterEvent(ctx, "pub-event")

	t.Run("publish to registered event", func(t *testing.T) {
		msg := testMessage("test-source", "test-payload")
		err := tr.Publish(ctx, "pub-event", msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify message was added to stream
		if len(client.streams["evt:pub-event"]) != 1 {
			t.Error("expected message in stream")
		}
	})

	t.Run("publish to unregistered event returns error", func(t *testing.T) {
		msg := testMessage("test-source", "test-payload")
		err := tr.Publish(ctx, "unknown-event", msg)
		if err != transport.ErrEventNotRegistered {
			t.Errorf("expected ErrEventNotRegistered, got %v", err)
		}
	})

	t.Run("publish on closed transport returns error", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient())
		tr2.RegisterEvent(ctx, "event")
		tr2.Close(context.Background())

		msg := testMessage("test-source", "test-payload")
		err := tr2.Publish(ctx, "event", msg)
		if err != transport.ErrTransportClosed {
			t.Errorf("expected ErrTransportClosed, got %v", err)
		}
	})
}

func TestTransportSubscribe(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()
	tr.RegisterEvent(ctx, "sub-event")

	t.Run("subscribe to registered event", func(t *testing.T) {
		sub, err := tr.Subscribe(ctx, "sub-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sub == nil {
			t.Fatal("expected subscription, got nil")
		}
		defer sub.Close(context.Background())

		if sub.ID() == "" {
			t.Error("expected subscription ID")
		}
		if sub.Messages() == nil {
			t.Error("expected messages channel")
		}
	})

	t.Run("subscribe to unregistered event returns error", func(t *testing.T) {
		_, err := tr.Subscribe(ctx, "unknown-event")
		if err != transport.ErrEventNotRegistered {
			t.Errorf("expected ErrEventNotRegistered, got %v", err)
		}
	})

	t.Run("worker pool mode uses shared group", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient(), WithConsumerGroup("workers"))
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "worker-event")
		sub1, _ := tr2.Subscribe(ctx, "worker-event", transport.WithDeliveryMode(transport.WorkerPool))
		sub2, _ := tr2.Subscribe(ctx, "worker-event", transport.WithDeliveryMode(transport.WorkerPool))
		defer sub1.Close(context.Background())
		defer sub2.Close(context.Background())

		// Both should use the same group
		rs1 := sub1.(*subscription)
		rs2 := sub2.(*subscription)
		if rs1.group != rs2.group {
			t.Errorf("expected same group for worker pool, got %s and %s", rs1.group, rs2.group)
		}
	})

	t.Run("broadcast mode uses unique groups", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient())
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "broadcast-event")
		sub1, _ := tr2.Subscribe(ctx, "broadcast-event")
		sub2, _ := tr2.Subscribe(ctx, "broadcast-event")
		defer sub1.Close(context.Background())
		defer sub2.Close(context.Background())

		rs1 := sub1.(*subscription)
		rs2 := sub2.(*subscription)
		if rs1.group == rs2.group {
			t.Error("expected different groups for broadcast mode")
		}
	})

	t.Run("broadcast mode starts at latest to avoid restart replay", func(t *testing.T) {
		// Broadcast subscribers mint a fresh per-Subscribe group, so a default
		// startID of "0" would replay the retained stream on every restart.
		// They must be created with startID="$".
		mock := newMockRedisClient()
		tr2, _ := New(mock)
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "broadcast-startid")
		sub, _ := tr2.Subscribe(ctx, "broadcast-startid")
		defer sub.Close(context.Background())

		rs := sub.(*subscription)
		mock.mu.Lock()
		got := mock.groups[rs.stream][rs.group]
		mock.mu.Unlock()
		if got != "$" {
			t.Errorf("expected broadcast group to be created at $, got %q", got)
		}
	})

	t.Run("worker pool with stable group starts from beginning", func(t *testing.T) {
		// Stable worker groups survive restarts via BUSYGROUP, so it is safe
		// (and useful) to read from the beginning on first creation.
		mock := newMockRedisClient()
		tr2, _ := New(mock)
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "worker-startid")
		sub, _ := tr2.Subscribe(ctx, "worker-startid",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("g1"))
		defer sub.Close(context.Background())

		rs := sub.(*subscription)
		mock.mu.Lock()
		got := mock.groups[rs.stream][rs.group]
		mock.mu.Unlock()
		if got != "0" {
			t.Errorf("expected worker-group to be created at 0, got %q", got)
		}
	})

	t.Run("broadcast honours explicit StartFromLatest and timestamp", func(t *testing.T) {
		mock := newMockRedisClient()
		tr2, _ := New(mock)
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "broadcast-explicit")

		subLatest, _ := tr2.Subscribe(ctx, "broadcast-explicit",
			transport.WithStartFrom(transport.StartFromLatest))
		defer subLatest.Close(context.Background())

		ts := time.Now().Add(-1 * time.Hour)
		subTime, _ := tr2.Subscribe(ctx, "broadcast-explicit",
			transport.WithStartFrom(transport.StartFromTimestamp),
			transport.WithStartTime(ts))
		defer subTime.Close(context.Background())

		rsLatest := subLatest.(*subscription)
		rsTime := subTime.(*subscription)
		mock.mu.Lock()
		gotLatest := mock.groups[rsLatest.stream][rsLatest.group]
		gotTime := mock.groups[rsTime.stream][rsTime.group]
		mock.mu.Unlock()

		if gotLatest != "$" {
			t.Errorf("StartFromLatest: expected $, got %q", gotLatest)
		}
		wantTime := fmt.Sprintf("%d-0", ts.UnixMilli())
		if gotTime != wantTime {
			t.Errorf("StartFromTimestamp: expected %q, got %q", wantTime, gotTime)
		}
	})

	t.Run("worker groups use separate consumer groups", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient(), WithConsumerGroup("test-bus"))
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "worker-group-event")

		// Workers in group-a
		subA1, _ := tr2.Subscribe(ctx, "worker-group-event",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("group-a"))
		subA2, _ := tr2.Subscribe(ctx, "worker-group-event",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("group-a"))

		// Workers in group-b
		subB1, _ := tr2.Subscribe(ctx, "worker-group-event",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("group-b"))
		subB2, _ := tr2.Subscribe(ctx, "worker-group-event",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("group-b"))

		defer subA1.Close(context.Background())
		defer subA2.Close(context.Background())
		defer subB1.Close(context.Background())
		defer subB2.Close(context.Background())

		rsA1 := subA1.(*subscription)
		rsA2 := subA2.(*subscription)
		rsB1 := subB1.(*subscription)
		rsB2 := subB2.(*subscription)

		// Workers in same group should share same consumer group
		if rsA1.group != rsA2.group {
			t.Errorf("expected same group for group-a workers, got %s and %s", rsA1.group, rsA2.group)
		}
		if rsB1.group != rsB2.group {
			t.Errorf("expected same group for group-b workers, got %s and %s", rsB1.group, rsB2.group)
		}

		// Different worker groups should have different consumer groups
		if rsA1.group == rsB1.group {
			t.Errorf("expected different groups for group-a and group-b, both got %s", rsA1.group)
		}

		// Verify group names contain the worker group name
		if !strings.Contains(rsA1.group, "group-a") {
			t.Errorf("expected group name to contain 'group-a', got %s", rsA1.group)
		}
		if !strings.Contains(rsB1.group, "group-b") {
			t.Errorf("expected group name to contain 'group-b', got %s", rsB1.group)
		}
	})

	t.Run("default worker pool vs named worker group use different groups", func(t *testing.T) {
		tr2, _ := New(newMockRedisClient(), WithConsumerGroup("test-bus"))
		defer tr2.Close(context.Background())

		tr2.RegisterEvent(ctx, "mixed-workers")

		// Default worker pool (no group)
		subDefault, _ := tr2.Subscribe(ctx, "mixed-workers",
			transport.WithDeliveryMode(transport.WorkerPool))

		// Named worker group
		subNamed, _ := tr2.Subscribe(ctx, "mixed-workers",
			transport.WithDeliveryMode(transport.WorkerPool),
			transport.WithWorkerGroup("named-group"))

		defer subDefault.Close(context.Background())
		defer subNamed.Close(context.Background())

		rsDefault := subDefault.(*subscription)
		rsNamed := subNamed.(*subscription)

		// They should have different consumer groups
		if rsDefault.group == rsNamed.group {
			t.Errorf("expected different groups for default and named, both got %s", rsDefault.group)
		}
	})
}

func TestTransportStreamName(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	name := tr.streamName("my-event")
	if name != "evt:my-event" {
		t.Errorf("expected 'evt:my-event', got %s", name)
	}
}

func TestSubscriptionClose(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()
	tr.RegisterEvent(ctx, "close-event")

	sub, _ := tr.Subscribe(ctx, "close-event")

	// Close should not error
	err := sub.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}

	// Double close should be safe
	err = sub.Close(context.Background())
	if err != nil {
		t.Errorf("unexpected error on double close: %v", err)
	}
}

func TestTransportErrorHandler(t *testing.T) {
	client := newMockRedisClient()
	client.xaddErr = errors.New("xadd failed")

	var capturedErr error
	tr, _ := New(client,
		WithErrorHandler(func(err error) {
			capturedErr = err
		}),
	)
	defer tr.Close(context.Background())

	ctx := context.Background()
	tr.RegisterEvent(ctx, "error-event")

	msg := testMessage("test", "data")
	tr.Publish(ctx, "error-event", msg)

	if capturedErr == nil {
		t.Error("expected error handler to be called")
	}
}

func TestConsumerLag_OldestPending(t *testing.T) {
	client := newMockRedisClient()
	tr, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	// RegisterEvent creates the stream and default consumer group.
	tr.RegisterEvent(ctx, "lag-event")

	// Inject a known XPending result with a stream entry ID whose millisecond
	// prefix encodes a known past timestamp (2023-11-14T22:13:20Z).
	knownMs := int64(1700000000000)
	streamName := tr.streamName("lag-event")
	groupKey := streamName + ":" + tr.groupID
	client.mu.Lock()
	if client.xpendingResult == nil {
		client.xpendingResult = make(map[string]*redis.XPending)
	}
	client.xpendingResult[groupKey] = &redis.XPending{
		Count: 1,
		Lower: fmt.Sprintf("%d-0", knownMs),
	}
	client.mu.Unlock()

	lags, err := tr.ConsumerLag(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var found *transport.ConsumerLag
	for i := range lags {
		if lags[i].Event == "lag-event" {
			found = &lags[i]
			break
		}
	}
	if found == nil {
		t.Fatal("no lag entry found for lag-event")
	}
	if found.OldestPending == nil {
		t.Fatal("OldestPending is nil, want non-nil for pending > 0")
	}
	// The age should be roughly time.Since(time.UnixMilli(knownMs)) — at least several months.
	if *found.OldestPending < time.Hour {
		t.Errorf("OldestPending %v too small; expected age of known past timestamp", *found.OldestPending)
	}
}

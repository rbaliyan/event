package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/internal/testutil"
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

	// blockReadGroup makes XReadGroup park until either the context is canceled
	// or the consumer group is destroyed. Used to reproduce shutdown races where
	// the consume loop is blocked inside Redis when teardown begins.
	blockReadGroup bool

	xreadgroupActive              int32 // count of in-flight blocking XReadGroup calls (atomic)
	destroyCalledWithActiveReader atomic.Bool
	destroyCh                     map[string]chan struct{} // stream|group -> close on destroy

	// xgroupCreateErr, when non-nil, is returned from XGroupCreateMkStream
	// instead of creating the group. Used to exercise the recreate-fails
	// fallback path.
	xgroupCreateErr error
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		streams:   make(map[string][]redis.XMessage),
		groups:    make(map[string]map[string]string),
		destroyCh: make(map[string]chan struct{}),
	}
}

// destroyKey returns the map key used to track per-group destroy channels.
func destroyKey(stream, group string) string { return stream + "|" + group }

// destroyChannel returns (or creates) the destroy signal channel for (stream, group).
// Caller must hold m.mu.
func (m *mockRedisClient) destroyChannel(stream, group string) chan struct{} {
	k := destroyKey(stream, group)
	ch, ok := m.destroyCh[k]
	if !ok {
		ch = make(chan struct{})
		m.destroyCh[k] = ch
	}
	return ch
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
	if m.xgroupCreateErr != nil {
		cmd.SetErr(m.xgroupCreateErr)
		return cmd
	}
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

	cmd := redis.NewXStreamSliceCmd(ctx)
	if m.xreadErr != nil {
		m.mu.Unlock()
		cmd.SetErr(m.xreadErr)
		return cmd
	}

	stream := a.Streams[0]

	// Real Redis returns NOGROUP immediately (regardless of BLOCK) if the
	// group doesn't exist on the stream. Mimic that so tests can drive
	// recovery paths by destroying groups out-of-band.
	if _, ok := m.groups[stream][a.Group]; !ok {
		m.mu.Unlock()
		cmd.SetErr(errors.New("NOGROUP No such key '" + stream + "' or consumer group '" + a.Group + "' in XREADGROUP with GROUP option"))
		return cmd
	}

	messages := m.streams[stream]

	if len(messages) > 0 {
		// Return all pending messages and clear them.
		result := []redis.XStream{{Stream: stream, Messages: messages}}
		m.streams[stream] = nil
		m.mu.Unlock()
		cmd.SetVal(result)
		return cmd
	}

	// Block only if the caller asked for it AND the mock is configured to
	// simulate parking. Non-blocking reads (Block <= 0) match Redis's
	// non-blocking semantics and return redis.Nil for an empty stream.
	if a.Block <= 0 || !m.blockReadGroup {
		m.mu.Unlock()
		cmd.SetErr(redis.Nil)
		return cmd
	}

	destroyCh := m.destroyChannel(stream, a.Group)
	m.mu.Unlock()

	atomic.AddInt32(&m.xreadgroupActive, 1)
	defer atomic.AddInt32(&m.xreadgroupActive, -1)

	select {
	case <-destroyCh:
		cmd.SetErr(errors.New("NOGROUP No such key '" + stream + "' or consumer group '" + a.Group + "' in XREADGROUP with GROUP option"))
		return cmd
	case <-ctx.Done():
		cmd.SetErr(context.Canceled)
		return cmd
	}
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
	// Record whether the consume loop is still parked inside XReadGroup at the
	// moment destroy lands. This is the invariant the shutdown fix protects.
	if atomic.LoadInt32(&m.xreadgroupActive) > 0 {
		m.destroyCalledWithActiveReader.Store(true)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewIntCmd(ctx)
	if m.groups[stream] != nil {
		delete(m.groups[stream], group)
		cmd.SetVal(1)
	} else {
		cmd.SetVal(0)
	}
	// Wake any blocked XReadGroup with NOGROUP semantics.
	if ch, ok := m.destroyCh[destroyKey(stream, group)]; ok {
		close(ch)
		delete(m.destroyCh, destroyKey(stream, group))
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

	t.Run("register event does not touch redis", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Group/stream creation is deferred to Publish/Subscribe so publish-only
		// or worker-group-only buses don't accrue an unused base consumer group.
		client.mu.Lock()
		_, hasGroups := client.groups["evt:test-event"]
		_, hasStream := client.streams["evt:test-event"]
		client.mu.Unlock()
		if hasGroups {
			t.Error("did not expect RegisterEvent to create a consumer group")
		}
		if hasStream {
			t.Error("did not expect RegisterEvent to create a stream")
		}

		// And the transport should not track the base group until something
		// subscribes via it.
		if _, ok := tr.groups.Load(tr.groupID); ok {
			t.Error("did not expect base groupID to be tracked after RegisterEvent")
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

// TestPublishOnlyDoesNotCreateConsumerGroup verifies that a bus that only
// publishes (never subscribes) does not leave an orphan base consumer group
// behind. The stream itself is created by XADD on first publish; consumer
// groups are created only when Subscribe runs.
func TestPublishOnlyDoesNotCreateConsumerGroup(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client)
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "pub-only"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	if err := tr.Publish(ctx, "pub-only", testMessage("src", "p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	client.mu.Lock()
	hasStream := len(client.streams["evt:pub-only"]) > 0
	groupCount := len(client.groups["evt:pub-only"])
	client.mu.Unlock()

	if !hasStream {
		t.Error("expected stream to be created by Publish")
	}
	if groupCount != 0 {
		t.Errorf("expected no consumer groups on publish-only stream, got %d", groupCount)
	}
}

// TestDefaultWorkerPoolCreatesBaseGroupOnSubscribe verifies the lazy-creation
// invariant for the default worker pool: the base consumer group is created
// by the first Subscribe call, not by RegisterEvent.
func TestDefaultWorkerPoolCreatesBaseGroupOnSubscribe(t *testing.T) {
	client := newMockRedisClient()
	tr, _ := New(client, WithConsumerGroup("bus-a"))
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "wp-evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	client.mu.Lock()
	_, before := client.groups["evt:wp-evt"]
	client.mu.Unlock()
	if before {
		t.Fatal("group present before Subscribe")
	}

	sub, err := tr.Subscribe(ctx, "wp-evt", transport.WithDeliveryMode(transport.WorkerPool))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	client.mu.Lock()
	startID, has := client.groups["evt:wp-evt"]["bus-a"]
	client.mu.Unlock()
	if !has {
		t.Fatal("expected base group to be created by Subscribe")
	}
	if startID != "0" {
		t.Errorf("expected default WorkerPool to create base group at start \"0\", got %q", startID)
	}
	if _, tracked := tr.groups.Load("bus-a"); !tracked {
		t.Error("expected base group to be tracked in transport.groups after Subscribe")
	}
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
	tr.RegisterEvent(ctx, "lag-event")
	// Subscribe creates the default consumer group (group creation is lazy
	// post-fix; RegisterEvent no longer touches Redis).
	sub, err := tr.Subscribe(ctx, "lag-event", transport.WithDeliveryMode(transport.WorkerPool))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

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

// TestBroadcastCloseDoesNotRaceWithConsumeLoop verifies that closing a broadcast
// subscription drains the consume goroutine before destroying its consumer group.
// Otherwise a blocked XREADGROUP would race with XGroupDestroy and log a spurious
// "read error, retrying with backoff" NOGROUP at ERROR level during normal shutdown.
//
// Regression for the shutdown-burst NOGROUP errors seen in event-server with
// per-Subscribe broadcast consumer groups (call-scheduler-<uuid>).
func TestBroadcastCloseDoesNotRaceWithConsumeLoop(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	tr, err := New(client, WithLogger(logger))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "broadcast-event"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, "broadcast-event") // Broadcast is the default
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait until the consume goroutine is parked inside the blocking XREADGROUP.
	testutil.Eventually(t, 2*time.Second,
		func() bool { return atomic.LoadInt32(&client.xreadgroupActive) != 0 },
		"consume goroutine never entered blocking XReadGroup")

	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("sub.Close: %v", err)
	}

	if client.destroyCalledWithActiveReader.Load() {
		t.Error("XGroupDestroy was called while the consume goroutine was still active; teardown ordering bug regressed")
	}

	if strings.Contains(logBuf.String(), "read error, retrying with backoff") {
		t.Errorf("unexpected read-error log during shutdown:\n%s", logBuf.String())
	}
	if strings.Contains(logBuf.String(), "NOGROUP") {
		t.Errorf("unexpected NOGROUP log during shutdown:\n%s", logBuf.String())
	}
}

// safeBuffer is a sync-safe wrapper around bytes.Buffer for capturing slog
// output from goroutines that race with test assertions.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForActiveReader polls until the mock has at least one in-flight blocking
// XReadGroup, or fails the test on timeout.
func waitForActiveReader(t *testing.T, m *mockRedisClient) {
	t.Helper()
	testutil.Eventually(t, 2*time.Second,
		func() bool { return atomic.LoadInt32(&m.xreadgroupActive) != 0 },
		"consume goroutine never entered blocking XReadGroup")
}

// pickBlockedGroup returns the group name that currently has a blocked
// XReadGroup waiting on the given stream. Only the consume goroutine of the
// subscription under test parks in blocking mode, so this targets the right
// group even when the base groupID is also registered on the stream.
func pickBlockedGroup(t *testing.T, m *mockRedisClient, stream string) string {
	t.Helper()
	prefix := stream + "|"
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.destroyCh {
		if strings.HasPrefix(k, prefix) {
			return strings.TrimPrefix(k, prefix)
		}
	}
	t.Fatalf("no blocked reader on stream %q (groups: %v)", stream, m.groups[stream])
	return ""
}

func TestAutoRecreateGroup_BroadcastRecoversFromNoGroup(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	var recreateMode atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		WithAutoRecreateGroup(RecreateBroadcast),
		WithRecreateHandler(func(_, _ string, mode RecreateMode) {
			recreateCount.Add(1)
			recreateMode.Store(int32(mode))
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt") // broadcast (default)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	// Simulate Redis losing the group.
	client.XGroupDestroy(context.Background(), streamName, groupName)

	// Wait for the recreate callback to fire.
	if !testutil.EventuallyOK(2*time.Second, func() bool { return recreateCount.Load() != 0 }) {
		t.Fatalf("recreate handler never fired; logs:\n%s", logBuf.String())
	}

	if got := RecreateMode(recreateMode.Load()); got != RecreateBroadcast {
		t.Errorf("recreate handler mode = %v, want %v", got, RecreateBroadcast)
	}

	// Group should be back.
	client.mu.Lock()
	_, exists := client.groups[streamName][groupName]
	client.mu.Unlock()
	if !exists {
		t.Errorf("group %q not recreated on stream %q", groupName, streamName)
	}

	if strings.Contains(logBuf.String(), "read error, retrying with backoff") {
		t.Errorf("unexpected error log after NOGROUP recovery:\n%s", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "consumer group recreated after NOGROUP") {
		t.Errorf("expected warn log on recreate; got:\n%s", logBuf.String())
	}
}

func TestAutoRecreateGroup_WorkerPoolRecoversFromNoGroup(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	var recreateMode atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		WithAutoRecreateGroup(RecreateWorkerPool),
		WithRecreateHandler(func(_, _ string, mode RecreateMode) {
			recreateCount.Add(1)
			recreateMode.Store(int32(mode))
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("g1"),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	client.XGroupDestroy(context.Background(), streamName, groupName)

	if !testutil.EventuallyOK(2*time.Second, func() bool { return recreateCount.Load() != 0 }) {
		t.Fatalf("recreate handler never fired; logs:\n%s", logBuf.String())
	}

	if got := RecreateMode(recreateMode.Load()); got != RecreateWorkerPool {
		t.Errorf("recreate handler mode = %v, want %v", got, RecreateWorkerPool)
	}

	client.mu.Lock()
	_, exists := client.groups[streamName][groupName]
	client.mu.Unlock()
	if !exists {
		t.Errorf("worker group %q not recreated", groupName)
	}
}

func TestAutoRecreateGroup_DisabledLeavesErrorLog(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		// No WithAutoRecreateGroup — default is zero (no recreation).
		WithRecreateHandler(func(_, _ string, _ RecreateMode) {
			recreateCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	client.XGroupDestroy(context.Background(), streamName, groupName)

	// Wait for the existing error-log + backoff path to log.
	if !testutil.EventuallyOK(2*time.Second, func() bool {
		return strings.Contains(logBuf.String(), "read error, retrying with backoff")
	}) {
		t.Fatalf("expected error log never appeared; logs:\n%s", logBuf.String())
	}

	if recreateCount.Load() != 0 {
		t.Errorf("recreate handler fired %d times with auto-recreate disabled", recreateCount.Load())
	}

	client.mu.Lock()
	_, exists := client.groups[streamName][groupName]
	client.mu.Unlock()
	if exists {
		t.Errorf("group %q was unexpectedly recreated with auto-recreate disabled", groupName)
	}
}

func TestAutoRecreateGroup_WrongModeLeavesErrorLog(t *testing.T) {
	// Worker-pool subscription, but only RecreateBroadcast is enabled. Recovery
	// must NOT fire for the worker subscription.
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		WithAutoRecreateGroup(RecreateBroadcast), // mismatch
		WithRecreateHandler(func(_, _ string, _ RecreateMode) {
			recreateCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("g1"),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	client.XGroupDestroy(context.Background(), streamName, groupName)

	if !testutil.EventuallyOK(2*time.Second, func() bool {
		return strings.Contains(logBuf.String(), "read error, retrying with backoff")
	}) {
		t.Fatalf("expected error log never appeared; logs:\n%s", logBuf.String())
	}

	if recreateCount.Load() != 0 {
		t.Errorf("recreate fired with mismatched mode (got count=%d)", recreateCount.Load())
	}
}

func TestRecreateMode_String(t *testing.T) {
	cases := []struct {
		mode RecreateMode
		want string
	}{
		{0, "none"},
		{RecreateBroadcast, "broadcast"},
		{RecreateWorkerPool, "worker_pool"},
		{RecreateAll, "all"},
		{RecreateMode(0x80), "RecreateMode(0x80)"},
	}
	for _, c := range cases {
		if got := c.mode.String(); got != c.want {
			t.Errorf("RecreateMode(%d).String() = %q, want %q", c.mode, got, c.want)
		}
	}
}

// TestAutoRecreateGroup_RecreateFailsFallsBack verifies that when
// XGroupCreateMkStream returns a non-BUSYGROUP error (e.g., Redis still
// unreachable), the consume loop falls through to the existing error log +
// exponential backoff path and the recreate handler is NOT invoked.
func TestAutoRecreateGroup_RecreateFailsFallsBack(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		WithAutoRecreateGroup(RecreateBroadcast),
		WithRecreateHandler(func(_, _ string, _ RecreateMode) {
			recreateCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	// Arm the mock so the recreate attempt fails with a non-BUSYGROUP error.
	client.mu.Lock()
	client.xgroupCreateErr = errors.New("ERR connection lost")
	client.mu.Unlock()

	client.XGroupDestroy(context.Background(), streamName, groupName)

	// Wait for both the recreate-failure Warn and the fall-through ERROR log.
	if !testutil.EventuallyOK(2*time.Second, func() bool {
		s := logBuf.String()
		return strings.Contains(s, "failed to recreate consumer group after NOGROUP") &&
			strings.Contains(s, "read error, retrying with backoff")
	}) {
		t.Fatalf("expected failure + fall-through logs never both appeared; logs:\n%s", logBuf.String())
	}

	if recreateCount.Load() != 0 {
		t.Errorf("recreate handler fired %d times despite recreate failure", recreateCount.Load())
	}
}

// TestAutoRecreateGroup_BusyGroupTreatedAsSuccess verifies that a concurrent
// sibling who already recreated the group (yielding BUSYGROUP on our retry)
// is treated as success rather than a hard failure.
func TestAutoRecreateGroup_BusyGroupTreatedAsSuccess(t *testing.T) {
	client := newMockRedisClient()
	client.blockReadGroup = true

	logBuf := &safeBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var recreateCount atomic.Int32
	tr, err := New(client,
		WithLogger(logger),
		WithAutoRecreateGroup(RecreateBroadcast),
		WithRecreateHandler(func(_, _ string, _ RecreateMode) {
			recreateCount.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer tr.Close(context.Background())

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "evt")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(context.Background())

	waitForActiveReader(t, client)

	streamName := "evt:evt"
	groupName := pickBlockedGroup(t, client, streamName)

	// Simulate the destroy + sibling-recreate race: the group is gone (so
	// XReadGroup returns NOGROUP), then a concurrent peer recreates it before
	// our XGroupCreateMkStream lands. We model this by having the destroy
	// channel close but the group entry remain present in m.groups, so the
	// mock returns BUSYGROUP from XGroupCreateMkStream.
	client.mu.Lock()
	// Close the destroy channel manually to wake the blocked XReadGroup with
	// NOGROUP semantics, but leave the group registered so XReadGroup will
	// also re-find it on retry (group "still there" from our POV).
	if ch, ok := client.destroyCh[destroyKey(streamName, groupName)]; ok {
		close(ch)
		delete(client.destroyCh, destroyKey(streamName, groupName))
	}
	client.mu.Unlock()

	if !testutil.EventuallyOK(2*time.Second, func() bool { return recreateCount.Load() != 0 }) {
		t.Fatalf("recreate handler never fired (BUSYGROUP not treated as success); logs:\n%s", logBuf.String())
	}

	if strings.Contains(logBuf.String(), "failed to recreate consumer group") {
		t.Errorf("BUSYGROUP was incorrectly treated as failure; logs:\n%s", logBuf.String())
	}
}

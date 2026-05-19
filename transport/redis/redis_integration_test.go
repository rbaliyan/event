//go:build integration

// Real-Redis integration tests for the Redis Streams transport. The unit
// suite (redis_test.go) drives a hand-rolled mockRedisClient that cannot
// faithfully reproduce the distributed-behavior fixes shipped in the recent
// transport/redis PRs:
//
//   - #116 broadcast subscribers must NOT replay the retained stream on restart
//   - #118 broadcast Close must drain the consume goroutine before XGroupDestroy
//   - #119 WithAutoRecreateGroup self-heals after external NOGROUP events
//   - #120 RegisterEvent defers consumer-group creation until Subscribe
//
// Each test asserts the externally-observable invariant against a real broker.
// Run with: just test-integration  (or: REDIS_ADDR=... go test -tags=integration ./transport/redis/...)

package redis

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// newIntegrationTransport returns a Redis transport wired to the real client
// returned by testutil.SetupRedis. The transport's stream prefix is the
// hardcoded "evt:"; isolation is achieved instead by using per-test unique
// event names (eventName) so each test has its own stream + groups.
// Consumer group IDs derive from a per-run base so concurrent runs cannot
// collide on group state.
//
// On cleanup, this helper deletes the test's stream key directly so leaked
// "evt:*" keys do not accumulate on the shared Redis instance across runs.
func newIntegrationTransport(t *testing.T, opts ...Option) (tr *Transport, client *redis.Client, eventName string) {
	t.Helper()
	client, prefix := testutil.SetupRedis(t)

	// Unique per-test event name AND consumer-group base. The stream prefix
	// stays at the transport default ("evt"), so the stream key for this
	// test is "evt:" + eventName — collision-free across parallel runs.
	eventName = "it_" + testutil.UniqueName(t)
	groupID := prefix + "bus"

	allOpts := append([]Option{
		WithConsumerGroup(groupID),
		WithBlockTime(50 * time.Millisecond), // keep XReadGroup polls snappy in tests
	}, opts...)

	tr, err := New(client, allOpts...)
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	t.Cleanup(func() {
		_ = tr.Close(context.Background())
		// Explicitly remove the stream this test created so the shared
		// Redis instance doesn't accumulate "evt:*" keys across runs.
		// SetupRedis's prefix scan won't catch them because the transport
		// uses its own fixed prefix.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Del(ctx, tr.streamName(eventName)).Err()
	})
	return tr, client, eventName
}

func newIntMsg(id string, payload []byte) transport.Message {
	return transport.NewMessage(id, "test", payload, nil, trace.SpanContext{})
}

// drainOne waits for at most timeout for one message on sub.Messages() and
// returns its payload. Returns the empty string and false on timeout.
func drainOne(t *testing.T, sub transport.Subscription, timeout time.Duration) (string, bool) {
	t.Helper()
	select {
	case m := <-sub.Messages():
		if m == nil {
			return "", false
		}
		_ = m.Ack(nil)
		return string(m.Payload()), true
	case <-time.After(timeout):
		return "", false
	}
}

// TestRedisIntegration_LazyGroupCreation pins PR #120: RegisterEvent must NOT
// create the base consumer group on Redis. The stream is created by the
// first XADD on Publish, and the group only materializes when Subscribe
// runs. A publish-only bus therefore leaves zero consumer groups behind,
// fixing the orphan-PEL accumulation that motivated the PR.
func TestRedisIntegration_LazyGroupCreation(t *testing.T) {
	tr, client, eventName := newIntegrationTransport(t)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	streamName := tr.streamName(eventName)

	// 1. After RegisterEvent: no stream, no groups.
	if exists := client.Exists(ctx, streamName).Val(); exists != 0 {
		t.Errorf("RegisterEvent created stream %q on Redis; expected lazy creation only", streamName)
	}

	// 2. After Publish: stream exists, still no groups.
	if err := tr.Publish(ctx, eventName, newIntMsg("m1", []byte("p"))); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if exists := client.Exists(ctx, streamName).Val(); exists != 1 {
		t.Errorf("Publish should have created stream %q (XADD), but it does not exist", streamName)
	}
	if groups := client.XInfoGroups(ctx, streamName); groups.Err() == nil && len(groups.Val()) != 0 {
		t.Errorf("publish-only bus must leave zero consumer groups; got %d", len(groups.Val()))
	}

	// 3. After Subscribe: group materializes.
	sub, err := tr.Subscribe(ctx, eventName, transport.WithDeliveryMode(transport.WorkerPool))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	testutil.Eventually(t, 2*time.Second, func() bool {
		groups := client.XInfoGroups(ctx, streamName)
		return groups.Err() == nil && len(groups.Val()) > 0
	}, "Subscribe should create the consumer group on Redis")
}

// TestRedisIntegration_BroadcastNoReplayOnRestart pins PR #116: a broadcast
// subscriber created AFTER messages have been published must NOT replay the
// retained stream. Broadcast groups have no continuity across restarts;
// startID="$" is the only safe default.
func TestRedisIntegration_BroadcastNoReplayOnRestart(t *testing.T) {
	tr, _, eventName := newIntegrationTransport(t)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Pre-existing retained messages.
	for i := range 3 {
		if err := tr.Publish(ctx, eventName, newIntMsg("old", []byte{byte('a' + i)})); err != nil {
			t.Fatalf("pre-Publish %d: %v", i, err)
		}
	}

	// New broadcast subscriber joins. It must see ZERO of the pre-existing
	// messages and only messages published after subscription.
	sub, err := tr.Subscribe(ctx, eventName) // Broadcast is the default
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	if got, ok := drainOne(t, sub, 300*time.Millisecond); ok {
		t.Fatalf("retained message %q replayed to broadcast subscriber; PR #116 regressed", got)
	}

	// A fresh publish post-subscribe should land.
	if err := tr.Publish(ctx, eventName, newIntMsg("fresh", []byte("new"))); err != nil {
		t.Fatalf("Publish fresh: %v", err)
	}
	got, ok := drainOne(t, sub, 3*time.Second)
	if !ok {
		t.Fatal("fresh message did not arrive on broadcast subscriber")
	}
	if got != "new" {
		t.Errorf("got %q, want %q", got, "new")
	}
}

// TestRedisIntegration_WorkerPoolReplaysRetained pins the complementary contract
// to #116: stable worker-group subscribers DO replay the retained stream on
// first creation, because their offset persists across restarts and replay
// is the documented WorkerPool semantic.
func TestRedisIntegration_WorkerPoolReplaysRetained(t *testing.T) {
	tr, _, eventName := newIntegrationTransport(t)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Pre-existing retained messages.
	for i := range 3 {
		if err := tr.Publish(ctx, eventName, newIntMsg("m", []byte{byte('a' + i)})); err != nil {
			t.Fatalf("pre-Publish %d: %v", i, err)
		}
	}

	sub, err := tr.Subscribe(ctx, eventName,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("stable"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	var received atomic.Int32
	go func() {
		for m := range sub.Messages() {
			_ = m.Ack(nil)
			received.Add(1)
		}
	}()

	testutil.Eventually(t, 3*time.Second, func() bool {
		return received.Load() == 3
	}, "stable worker group should replay all 3 retained messages, got %d", received.Load())
}

// TestRedisIntegration_WorkerGroup_SeparateConsumerGroups pins the worker-group
// fan-out: two named groups must each receive every message (cross-group
// fan-out), and workers within one group must share load (intra-group).
func TestRedisIntegration_WorkerGroup_SeparateConsumerGroups(t *testing.T) {
	tr, client, eventName := newIntegrationTransport(t)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	subA, err := tr.Subscribe(ctx, eventName,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	defer subA.Close(ctx)
	subB, err := tr.Subscribe(ctx, eventName,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	defer subB.Close(ctx)

	// Verify each named group materialized as a distinct Redis consumer
	// group on the same stream.
	streamName := tr.streamName(eventName)
	testutil.Eventually(t, 2*time.Second, func() bool {
		groups, err := client.XInfoGroups(ctx, streamName).Result()
		if err != nil {
			return false
		}
		return len(groups) >= 2
	}, "expected at least 2 consumer groups on stream %q", streamName)

	var a, b atomic.Int64
	w := func(s transport.Subscription, c *atomic.Int64) {
		go func() {
			for m := range s.Messages() {
				_ = m.Ack(nil)
				c.Add(1)
			}
		}()
	}
	w(subA, &a)
	w(subB, &b)

	const total = 5
	for i := range total {
		if err := tr.Publish(ctx, eventName, newIntMsg("id", []byte{byte(i)})); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	testutil.Eventually(t, 5*time.Second, func() bool {
		return a.Load() == total && b.Load() == total
	}, "each named worker group must receive all %d messages independently (got a=%d b=%d)", total, a.Load(), b.Load())
}

// TestRedisIntegration_NOGROUPSelfHealing pins PR #119: when an external actor
// destroys the consumer group out from under a running subscriber and
// WithAutoRecreateGroup is enabled, the consumer must recreate the group at
// its original startID and resume delivery without operator intervention.
func TestRedisIntegration_NOGROUPSelfHealing(t *testing.T) {
	var recreated atomic.Int32
	tr, client, eventName := newIntegrationTransport(t,
		WithAutoRecreateGroup(RecreateBroadcast),
		WithRecreateHandler(func(_, _ string, _ RecreateMode) {
			recreated.Add(1)
		}),
	)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, eventName) // broadcast default
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	streamName := tr.streamName(eventName)

	// Identify the broadcast group that Subscribe created so we can destroy
	// exactly that group. There may be other groups on the stream from
	// concurrent tests on the same Redis instance, but our unique per-run
	// prefix ensures isolation.
	var groupID string
	testutil.Eventually(t, 2*time.Second, func() bool {
		groups, err := client.XInfoGroups(ctx, streamName).Result()
		if err != nil {
			return false
		}
		if len(groups) >= 1 {
			groupID = groups[0].Name
			return true
		}
		return false
	}, "broadcast Subscribe should create a consumer group")

	// Simulate Redis losing the group (DEL, FLUSHDB, failover-to-empty-replica).
	if err := client.XGroupDestroy(ctx, streamName, groupID).Err(); err != nil {
		t.Fatalf("XGroupDestroy: %v", err)
	}

	// The transport should detect NOGROUP on the next XReadGroup poll and
	// recreate the group automatically.
	testutil.Eventually(t, 5*time.Second, func() bool {
		return recreated.Load() >= 1
	}, "WithAutoRecreateGroup callback never fired after external NOGROUP")

	// After recreation, a freshly published message must be delivered —
	// proves the subscriber resumed end-to-end, not just that the group
	// was created.
	if err := tr.Publish(ctx, eventName, newIntMsg("post-heal", []byte("ok"))); err != nil {
		t.Fatalf("Publish post-heal: %v", err)
	}
	got, ok := drainOne(t, sub, 5*time.Second)
	if !ok {
		t.Fatal("subscriber did not resume delivery after NOGROUP self-healing")
	}
	if got != "ok" {
		t.Errorf("post-heal payload: got %q, want %q", got, "ok")
	}
}

// TestRedisIntegration_BroadcastCloseDrains_NoNOGROUPLog pins PR #118: closing a
// broadcast subscription must drain the consume goroutine before destroying
// its consumer group. Otherwise a blocked XReadGroup races with XGroupDestroy
// and surfaces a spurious NOGROUP error during normal shutdown.
//
// The unit test (TestBroadcastCloseDoesNotRaceWithConsumeLoop) uses a mock
// to make the race deterministic; here we just verify that under real Redis,
// Close completes without error and the group is gone afterward.
func TestRedisIntegration_BroadcastCloseDrains_NoLeakedGroup(t *testing.T) {
	tr, client, eventName := newIntegrationTransport(t)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, eventName) // broadcast
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	streamName := tr.streamName(eventName)

	// Wait for the per-Subscribe broadcast group to appear.
	var groupID string
	testutil.Eventually(t, 2*time.Second, func() bool {
		gs, err := client.XInfoGroups(ctx, streamName).Result()
		if err != nil || len(gs) == 0 {
			return false
		}
		groupID = gs[0].Name
		return true
	}, "broadcast group should be created by Subscribe")

	// Close while the consume loop is parked inside a blocking XReadGroup.
	if err := sub.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After Close, the per-Subscribe broadcast group must have been
	// destroyed — that's PR #118's other half (no leaked consumer groups).
	testutil.Eventually(t, 2*time.Second, func() bool {
		gs, err := client.XInfoGroups(ctx, streamName).Result()
		if err != nil {
			// Stream may be gone entirely if no other groups remained.
			return true
		}
		for _, g := range gs {
			if g.Name == groupID {
				return false
			}
		}
		return true
	}, "broadcast group %q should be destroyed on Close", groupID)
}

// TestRedisIntegration_XClaimDeadConsumer verifies that the orphaned-message
// claimer picks up a message left in the pending entries list by a consumer
// that never acknowledged it. This is the production safety net for a
// crashed worker.
func TestRedisIntegration_XClaimDeadConsumer(t *testing.T) {
	tr, _, eventName := newIntegrationTransport(t,
		WithClaimInterval(100*time.Millisecond, 100*time.Millisecond),
	)
	ctx := context.Background()

	if err := tr.RegisterEvent(ctx, eventName); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// First consumer in a stable worker group, registers itself with a
	// fixed consumer ID. We will close it WITHOUT acknowledging the
	// message it consumes, leaving the entry in the PEL.
	deadSub, err := tr.Subscribe(ctx, eventName,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("g"),
		transport.WithConsumerID("dead-consumer"),
	)
	if err != nil {
		t.Fatalf("Subscribe dead: %v", err)
	}

	if err := tr.Publish(ctx, eventName, newIntMsg("orphan", []byte("orphan"))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for dead consumer to receive (but not ack) the message.
	select {
	case m := <-deadSub.Messages():
		if string(m.Payload()) != "orphan" {
			t.Errorf("dead consumer payload: got %q, want %q", m.Payload(), "orphan")
		}
		// NOTE: deliberately NOT ack'ing — leaves the message in PEL.
	case <-time.After(3 * time.Second):
		t.Fatal("dead consumer did not receive the message")
	}

	// Close the dead consumer's subscription without ack. This is what
	// happens after a worker crash.
	if err := deadSub.Close(context.Background()); err != nil {
		t.Fatalf("dead Close: %v", err)
	}

	// New consumer in the SAME worker group. The transport's orphan claimer
	// (ticker at 100ms) should reassign the unacked entry to it.
	liveSub, err := tr.Subscribe(ctx, eventName,
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("g"),
		transport.WithConsumerID("live-consumer"),
	)
	if err != nil {
		t.Fatalf("Subscribe live: %v", err)
	}
	defer liveSub.Close(ctx)

	got, ok := drainOne(t, liveSub, 5*time.Second)
	if !ok {
		t.Fatal("live consumer never received the orphaned message; XCLAIM path regressed")
	}
	if got != "orphan" {
		t.Errorf("live consumer payload: got %q, want %q", got, "orphan")
	}
}

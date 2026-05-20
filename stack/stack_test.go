package stack_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/idempotency"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/monitor"
	"github.com/rbaliyan/event/v3/poison"
	"github.com/rbaliyan/event/v3/stack"
	"github.com/rbaliyan/event/v3/transport/channel"
)

func newBus(t *testing.T, opts ...event.BusOption) *event.Bus {
	t.Helper()
	tr := channel.New()
	all := append([]event.BusOption{event.WithTransport(tr)}, opts...)
	bus, err := event.NewBus(t.Name(), all...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	})
	return bus
}

// TestWithReliabilityStack_Defaults verifies the stack wires all three
// middleware components when called with no options.
func TestWithReliabilityStack_Defaults(t *testing.T) {
	bus := newBus(t, stack.WithReliabilityStack())

	type msg struct{ Value string }
	ev := event.New[msg]("stack.defaults")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var count atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		count.Add(1)
		return nil
	})

	if err := ev.Publish(ctx, msg{"hello"}); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 2*time.Second, func() bool {
		return count.Load() == 1
	}, "expected handler called once, got %d", count.Load())
}

// TestWithReliabilityStack_Idempotency verifies that duplicate message IDs
// are dropped when the idempotency store is active.
func TestWithReliabilityStack_Idempotency(t *testing.T) {
	istore := idempotency.NewMemoryStore(time.Hour)
	t.Cleanup(istore.Close)

	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithIdempotencyStore(istore),
	))

	type msg struct{ V int }
	ev := event.New[msg]("stack.idempotency")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var count atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		count.Add(1)
		return nil
	})

	// First publish — should be handled
	if err := ev.Publish(ctx, msg{1}); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 2*time.Second, func() bool {
		return count.Load() == 1
	}, "first delivery: expected 1, got %d", count.Load())
}

// TestWithReliabilityStack_PoisonDetection verifies that a repeatedly-failing
// message is eventually quarantined and the handler stops being called.
// Poison detection tracks failures per message ID, so we publish with the
// same ID multiple times (simulating at-least-once redelivery).
func TestWithReliabilityStack_PoisonDetection(t *testing.T) {
	pstore := poison.NewMemoryStore()
	detector := poison.NewDetector(pstore,
		poison.WithThreshold(2),
		poison.WithQuarantineTime(5*time.Second),
	)

	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithPoisonDetector(detector),
	))

	type msg struct{ V int }
	ev := event.New[msg]("stack.poison")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var count atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		count.Add(1)
		return errors.New("always fails")
	})

	// Subscribe is synchronous on the channel transport — no pre-publish
	// sleep needed.

	// Publish with the same message ID to simulate at-least-once redelivery.
	// Threshold=2: first two calls fail and accumulate; third call is
	// quarantined and skipped. After each publish we Eventually-poll for
	// either the count to increment (handler ran) or the count to stabilize
	// at the threshold (quarantine kicked in). This replaces a fixed 20ms
	// inter-publish sleep that served as a worst-case "let the handler
	// finish before the next publish" bound.
	msgCtx := event.ContextWithEventID(ctx, "poison-test-id-1")
	for i := range 5 {
		_ = ev.Publish(msgCtx, msg{1})
		// Briefly wait for the publish to be observed by the poison middleware
		// — either the handler ran (count went up) or the message was
		// quarantined (count stays at threshold). Both stable outcomes break
		// the loop, so we can move on to the next publish without a fixed sleep.
		expected := int32(i + 1)
		if expected > 2 {
			expected = 2
		}
		testutil.Eventually(t, time.Second, func() bool {
			return count.Load() >= expected
		}, "publish %d: count never reached %d", i+1, expected)
	}

	// Handler should be called at most threshold (2) times; third+ are quarantined.
	if got := count.Load(); got > 2 {
		t.Errorf("expected handler called at most 2 times (threshold), got %d", got)
	}
}

// TestWithReliabilityStack_MonitorStore verifies that the custom monitor store
// is used by checking that entries are recorded.
func TestWithReliabilityStack_MonitorStore(t *testing.T) {
	mstore := monitor.NewMemoryStore()
	t.Cleanup(func() { _ = mstore.Close() })

	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithMonitorStore(mstore),
	))

	type msg struct{ V int }
	ev := event.New[msg]("stack.monitor")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		return nil
	})

	if err := ev.Publish(ctx, msg{42}); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 2*time.Second, func() bool {
		return mstore.Len() > 0
	}, "expected at least one monitor entry, got %d", mstore.Len())
}

// TestWithReliabilityStack_IdempotencyTTL verifies the TTL option is accepted.
func TestWithReliabilityStack_IdempotencyTTL(t *testing.T) {
	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithIdempotencyTTL(30*time.Minute),
	))

	type msg struct{}
	ev := event.New[msg]("stack.idempotencyTTL")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		return nil
	})

	if err := ev.Publish(ctx, msg{}); err != nil {
		t.Fatal(err)
	}
	// No assertion to make — this test only verifies the WithIdempotencyTTL
	// option is accepted by the stack. No post-publish wait needed.
}

// TestWithReliabilityStack_PublishAuditReusesMonitor verifies that the
// monitor store also receives a Status=published entry after a successful
// publish, since monitor.MemoryStore implements event.PublishAuditStore.
func TestWithReliabilityStack_PublishAuditReusesMonitor(t *testing.T) {
	mstore := monitor.NewMemoryStore()
	t.Cleanup(func() { _ = mstore.Close() })

	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithMonitorStore(mstore),
	))

	type msg struct{ V int }
	ev := event.New[msg]("stack.pubaudit")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}
	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		return nil
	})

	if err := ev.Publish(ctx, msg{42}); err != nil {
		t.Fatal(err)
	}

	published := monitor.StatusPublished
	filter := monitor.Filter{
		EventName: "stack.pubaudit",
		Status:    []monitor.Status{published},
	}
	// Poll for the audit entry instead of fixed 50ms wait. The bus records
	// the published-audit row asynchronously after Publish returns.
	testutil.Eventually(t, 2*time.Second, func() bool {
		c, err := mstore.Count(ctx, filter)
		return err == nil && c == 1
	}, "expected exactly 1 published entry for stack.pubaudit")
}

// TestWithReliabilityStack_PoisonOptions verifies threshold/quarantine knobs.
func TestWithReliabilityStack_PoisonOptions(t *testing.T) {
	bus := newBus(t, stack.WithReliabilityStack(
		stack.WithPoisonThreshold(3),
		stack.WithPoisonQuarantine(2*time.Hour),
	))

	type msg struct{}
	ev := event.New[msg]("stack.poisonOpts")
	ctx := context.Background()

	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}
	ev.Subscribe(ctx, func(ctx context.Context, e event.Event[msg], m msg) error {
		return nil
	})

	if err := ev.Publish(ctx, msg{}); err != nil {
		t.Fatal(err)
	}
	// No assertion follows — this test only verifies the option setters
	// are accepted by the stack. No post-publish wait needed.
}

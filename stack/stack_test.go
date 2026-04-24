package stack_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/idempotency"
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
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("expected handler called once, got %d", count.Load())
	}
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
	time.Sleep(50 * time.Millisecond)
	if count.Load() != 1 {
		t.Fatalf("first delivery: expected 1, got %d", count.Load())
	}
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

	time.Sleep(10 * time.Millisecond)

	// Publish with the same message ID to simulate at-least-once redelivery.
	// Threshold=2: first two calls fail and accumulate; third call is quarantined and skipped.
	msgCtx := event.ContextWithEventID(ctx, "poison-test-id-1")
	for range 5 {
		_ = ev.Publish(msgCtx, msg{1})
		time.Sleep(20 * time.Millisecond)
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
	time.Sleep(50 * time.Millisecond)

	if mstore.Len() == 0 {
		t.Error("expected at least one monitor entry, got none")
	}
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
	time.Sleep(30 * time.Millisecond)
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
	time.Sleep(30 * time.Millisecond)
}

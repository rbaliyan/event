//go:build smoke

// Canonical bus-level smoke test against the channel transport. Establishes
// the patterns subsequent transport smoke tests (Redis, NATS, Kafka) will
// follow: MustNewBus + Register + Subscribe + Publish + Eventually-style
// assertion, no time.Sleep, no global state.
//
// Run with: just test-smoke   (or: go test -tags=smoke -race ./...)

package event_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/transport/channel"
)

func TestSmokeBusChannel_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(channel.New()))
	ev := testutil.MustRegister(t, ctx, bus, event.New[string]("smoke.rt"))

	received := make(chan string, 1)
	if err := ev.Subscribe(ctx, func(_ context.Context, _ event.Event[string], v string) error {
		received <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := ev.Publish(ctx, "hello"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := testutil.WaitFor(t, received, time.Second, "payload should be delivered")
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestSmokeBus_PublishUnboundReturnsSentinel(t *testing.T) {
	t.Parallel()
	// An event constructed but never Register'd should refuse to publish with
	// the documented sentinel. Pins the error-path smoke contract that the
	// reliability stack relies on.
	ev := event.New[int]("smoke.unbound")
	err := ev.Publish(context.Background(), 1)
	if !errors.Is(err, event.ErrEventNotBound) {
		t.Errorf("Publish on unbound event: got %v, want ErrEventNotBound", err)
	}
}

func TestSmokeBus_WorkerPool_FanOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(channel.New()))
	ev := testutil.MustRegister(t, ctx, bus, event.New[int]("smoke.workers"))

	var a, b atomic.Int64
	handler := func(counter *atomic.Int64) event.Handler[int] {
		return func(_ context.Context, _ event.Event[int], _ int) error {
			counter.Add(1)
			return nil
		}
	}
	if err := ev.Subscribe(ctx, handler(&a), event.AsWorker[int](), event.WithWorkerGroup[int]("g")); err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	if err := ev.Subscribe(ctx, handler(&b), event.AsWorker[int](), event.WithWorkerGroup[int]("g")); err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}

	const total = 20
	for i := range total {
		if err := ev.Publish(ctx, i); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	testutil.Eventually(t, 2*time.Second, func() bool {
		return a.Load()+b.Load() == total
	}, "expected a+b=%d, got a=%d b=%d", total, a.Load(), b.Load())

	// Worker semantics: each message must land on exactly one handler. Both
	// counters should be non-zero given the round-robin in the channel
	// transport, but the canonical smoke assertion is on the sum so we don't
	// pin the implementation's distribution policy.
	if a.Load() == 0 || b.Load() == 0 {
		t.Errorf("worker fan-out skewed: a=%d b=%d", a.Load(), b.Load())
	}
}

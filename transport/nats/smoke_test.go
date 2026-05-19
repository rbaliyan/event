//go:build smoke

// Bus-level smoke tests for both NATS Core and JetStream transports.
//
// Run with: just test-smoke   (or: go test -tags=smoke -race ./transport/nats/...)

package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
)

func TestSmokeNATSCoreBus_RoundTrip(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, err := New(conn)
	if err != nil {
		t.Fatalf("nats.New: %v", err)
	}

	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(tr))
	ev := testutil.MustRegister(t, ctx, bus, event.New[string]("smoke_core_rt"))

	received := make(chan string, 1)
	if err := ev.Subscribe(ctx, func(_ context.Context, _ event.Event[string], v string) error {
		received <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// NATS Core sub setup is asynchronous on the wire; wait briefly for the
	// subscription to be registered on the client before publishing.
	// Without this Eventually-style wait, fast publishes can land before the
	// SUB protocol message hits the server.
	testutil.Eventually(t, time.Second, func() bool {
		return conn.NumSubscriptions() >= 1
	}, "subscription should be registered with the NATS client")

	if err := ev.Publish(ctx, "hello-core"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := testutil.WaitFor(t, received, 3*time.Second, "payload should arrive on Core")
	if got != "hello-core" {
		t.Errorf("got %q, want %q", got, "hello-core")
	}
}

func TestSmokeNATSJetStreamBus_RoundTrip(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, err := NewJetStream(conn)
	if err != nil {
		t.Fatalf("nats.NewJetStream: %v", err)
	}

	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(tr))
	ev := testutil.MustRegister(t, ctx, bus, event.New[string]("smoke_js_rt"))

	received := make(chan string, 1)
	if err := ev.Subscribe(ctx, func(_ context.Context, _ event.Event[string], v string) error {
		received <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := ev.Publish(ctx, "hello-js"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got := testutil.WaitFor(t, received, 5*time.Second, "payload should arrive on JetStream")
	if got != "hello-js" {
		t.Errorf("got %q, want %q", got, "hello-js")
	}
}

func TestSmokeNATSJetStreamBus_WorkerPoolFanOut(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)

	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(tr))
	ev := testutil.MustRegister(t, ctx, bus, event.New[int]("smoke_js_workers"))

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

	const total = 10
	for i := range total {
		if err := ev.Publish(ctx, i); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	testutil.Eventually(t, 5*time.Second, func() bool {
		return a.Load()+b.Load() == total
	}, "expected a+b=%d, got a=%d b=%d", total, a.Load(), b.Load())
}

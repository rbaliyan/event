package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// Helper for building a publish-side transport.Message that the Core
// transport's codec can round-trip back into Subscribe channels.
func newCoreMsg(id, source string, payload []byte) transport.Message {
	return transport.NewMessage(id, source, payload, nil, trace.SpanContext{})
}

func TestNew_NilConnReturnsErrConnRequired(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); !errors.Is(err, ErrConnRequired) {
		t.Errorf("New(nil): got %v, want ErrConnRequired", err)
	}
}

func TestNewJetStream_NilConnReturnsErrConnRequired(t *testing.T) {
	t.Parallel()
	if _, err := NewJetStream(nil); !errors.Is(err, ErrConnRequired) {
		t.Errorf("NewJetStream(nil): got %v, want ErrConnRequired", err)
	}
}

func TestCoreTransport_RegisterEvent_DuplicateRejected(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, err := New(conn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt.dup"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := tr.RegisterEvent(ctx, "evt.dup"); !errors.Is(err, transport.ErrEventAlreadyExists) {
		t.Errorf("second register: got %v, want ErrEventAlreadyExists", err)
	}
}

func TestCoreTransport_PublishUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	err := tr.Publish(context.Background(), "evt.unknown", newCoreMsg("id", "src", []byte("p")))
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Publish unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestCoreTransport_SubscribeUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	_, err := tr.Subscribe(context.Background(), "evt.unknown")
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Subscribe unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestCoreTransport_ClosedTransportRejectsOperations(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	if err := tr.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("RegisterEvent on closed: got %v, want ErrTransportClosed", err)
	}
	if err := tr.UnregisterEvent(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("UnregisterEvent on closed: got %v, want ErrTransportClosed", err)
	}
	if err := tr.Publish(ctx, "evt", newCoreMsg("id", "src", []byte("p"))); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Publish on closed: got %v, want ErrTransportClosed", err)
	}
	if _, err := tr.Subscribe(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Subscribe on closed: got %v, want ErrTransportClosed", err)
	}
}

func TestCoreTransport_CloseIdempotent(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	if err := tr.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("second Close should be a no-op; got %v", err)
	}
}

func TestCoreTransport_UnregisterUnknownEventRejected(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	err := tr.UnregisterEvent(context.Background(), "evt.never-registered")
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("UnregisterEvent of unknown: got %v, want ErrEventNotRegistered", err)
	}
}

func TestCoreTransport_PublishSubscribe_BroadcastRoundTrip(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt.bcast"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub1, err := tr.Subscribe(ctx, "evt.bcast")
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}
	defer sub1.Close(ctx)
	sub2, err := tr.Subscribe(ctx, "evt.bcast")
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}
	defer sub2.Close(ctx)

	if err := tr.Publish(ctx, "evt.bcast", newCoreMsg("id-1", "src", []byte("hello"))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Both subscribers receive the same message (broadcast semantics).
	for i, sub := range []transport.Subscription{sub1, sub2} {
		select {
		case m := <-sub.Messages():
			if string(m.Payload()) != "hello" {
				t.Errorf("sub %d: payload %q, want %q", i, m.Payload(), "hello")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("sub %d: timed out waiting for message", i)
		}
	}
}

func TestCoreTransport_WorkerPool_QueueGroupFanOut(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt.wp"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Two workers in the default queue group: each message lands on exactly
	// one of them.
	var a, b atomic.Int64
	w := func(sub transport.Subscription, counter *atomic.Int64) {
		go func() {
			for range sub.Messages() {
				counter.Add(1)
			}
		}()
	}

	sub1, _ := tr.Subscribe(ctx, "evt.wp", transport.WithDeliveryMode(transport.WorkerPool))
	defer sub1.Close(ctx)
	sub2, _ := tr.Subscribe(ctx, "evt.wp", transport.WithDeliveryMode(transport.WorkerPool))
	defer sub2.Close(ctx)
	w(sub1, &a)
	w(sub2, &b)

	const total = 30
	for i := range total {
		if err := tr.Publish(ctx, "evt.wp", newCoreMsg("id", "src", []byte{byte(i)})); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	eventually(t, 2*time.Second, func() bool {
		return a.Load()+b.Load() == total
	}, "workers should split 30 messages but got a+b != 30")

	if a.Load() == 0 || b.Load() == 0 {
		t.Errorf("queue-group fan-out skewed: a=%d b=%d (one worker got everything)", a.Load(), b.Load())
	}
}

func TestCoreTransport_WorkerPool_NamedGroupsBothReceive(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt.named"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Two distinct named worker groups: each group receives every message
	// (cross-group fan-out), workers within a group compete (intra-group
	// load balance).
	var a, b atomic.Int64
	w := func(sub transport.Subscription, counter *atomic.Int64) {
		go func() {
			for range sub.Messages() {
				counter.Add(1)
			}
		}()
	}

	subA, _ := tr.Subscribe(ctx, "evt.named",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))
	defer subA.Close(ctx)
	subB, _ := tr.Subscribe(ctx, "evt.named",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))
	defer subB.Close(ctx)
	w(subA, &a)
	w(subB, &b)

	const total = 10
	for i := range total {
		if err := tr.Publish(ctx, "evt.named", newCoreMsg("id", "src", []byte{byte(i)})); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	eventually(t, 2*time.Second, func() bool {
		return a.Load() == total && b.Load() == total
	}, "each named worker group should receive all 10 messages independently")
}

func TestCoreTransport_Health_HealthyWhenConnected(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	got := tr.Health(context.Background())
	if got.Status != transport.HealthStatusHealthy {
		t.Errorf("Health on connected transport: status=%v, want Healthy (details=%v)", got.Status, got.Details)
	}
	if got.Details["type"] != "nats-core" {
		t.Errorf("Health details.type: got %v, want %q", got.Details["type"], "nats-core")
	}
}

func TestCoreTransport_Health_UnhealthyWhenClosed(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := New(conn)
	_ = tr.Close(context.Background())

	got := tr.Health(context.Background())
	if got.Status != transport.HealthStatusUnhealthy {
		t.Errorf("Health on closed transport: status=%v, want Unhealthy", got.Status)
	}
}

func TestCoreTransport_NameContractStable(t *testing.T) {
	t.Parallel()
	// The transport name is part of the public observability surface
	// (used in metric labels, log attributes). Pin it.
	conn := startTestServer(t)
	tr, _ := New(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })
	if got := tr.Name(); got != "nats" {
		t.Errorf("Name: got %q, want %q", got, "nats")
	}
}

func TestCoreTransport_ErrorHandlerInvokedOnPublishFailure(t *testing.T) {
	t.Parallel()
	// Close the underlying connection to induce a Publish error, then verify
	// the WithCoreErrorHandler callback fires.
	conn := startTestServer(t)
	var captured atomic.Value
	tr, _ := New(conn, WithCoreErrorHandler(func(err error) { captured.Store(err) }))
	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt.err"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	conn.Close()
	t.Cleanup(func() { _ = tr.Close(ctx) })

	err := tr.Publish(ctx, "evt.err", newCoreMsg("id", "src", []byte("p")))
	if err == nil {
		t.Fatal("expected Publish to fail when connection is closed")
	}
	if got, _ := captured.Load().(error); got == nil {
		t.Error("WithCoreErrorHandler callback was not invoked on publish failure")
	}
}

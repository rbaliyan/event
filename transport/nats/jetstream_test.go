package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/rbaliyan/event/v3/transport"
)

func newJSMsg(id, source string, payload []byte) transport.Message {
	return transport.NewMessageWithAck(id, source, payload, nil, 0, nil)
}

func TestJetStream_RegisterEvent_CreatesStreamAndRejectsDuplicate(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, err := NewJetStream(conn)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "js_dup"); err != nil {
		t.Fatalf("first register: %v", err)
	}

	// Stream actually exists on the server.
	js, _ := jetstream.New(conn)
	if _, err := js.Stream(ctx, tr.streamName("js_dup")); err != nil {
		t.Errorf("expected stream %q to exist on server: %v", tr.streamName("js_dup"), err)
	}

	// Second register is rejected with the documented sentinel — but note
	// the stream itself can be re-created idempotently. The sentinel comes
	// from the in-memory event map, not from CreateOrUpdateStream.
	if err := tr.RegisterEvent(ctx, "js_dup"); !errors.Is(err, transport.ErrEventAlreadyExists) {
		t.Errorf("second register: got %v, want ErrEventAlreadyExists", err)
	}
}

func TestJetStream_PublishSubscribe_WorkerPoolRoundTrip(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, err := NewJetStream(conn)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "js_wp"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	sub, err := tr.Subscribe(ctx, "js_wp", transport.WithDeliveryMode(transport.WorkerPool))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	if err := tr.Publish(ctx, "js_wp", newJSMsg("id-1", "src", []byte("ping"))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case m := <-sub.Messages():
		if string(m.Payload()) != "ping" {
			t.Errorf("payload: got %q, want %q", m.Payload(), "ping")
		}
		if err := m.Ack(nil); err != nil {
			t.Errorf("Ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for JetStream message")
	}
}

func TestJetStream_WorkerGroup_NamesYieldDistinctDurables(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "js_named"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	// Two named groups must materialize as two distinct durable consumers on
	// the underlying stream, otherwise cross-group fan-out is impossible.
	subA, err := tr.Subscribe(ctx, "js_named",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	defer subA.Close(ctx)
	subB, err := tr.Subscribe(ctx, "js_named",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	defer subB.Close(ctx)

	js, _ := jetstream.New(conn)
	stream, err := js.Stream(ctx, tr.streamName("js_named"))
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	seen := map[string]bool{}
	lister := stream.ListConsumers(ctx)
	for info := range lister.Info() {
		seen[info.Name] = true
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("ListConsumers: %v", err)
	}

	if !seen["workers-js_named-group-a"] {
		t.Errorf("missing durable consumer for group-a; saw %v", seen)
	}
	if !seen["workers-js_named-group-b"] {
		t.Errorf("missing durable consumer for group-b; saw %v", seen)
	}
}

func TestJetStream_WorkerGroup_NamedGroupsFanOutAcross(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "js_fan"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}

	subA, _ := tr.Subscribe(ctx, "js_fan",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))
	defer subA.Close(ctx)
	subB, _ := tr.Subscribe(ctx, "js_fan",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))
	defer subB.Close(ctx)

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
		if err := tr.Publish(ctx, "js_fan", newJSMsg("id", "src", []byte{byte(i)})); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	eventually(t, 5*time.Second, func() bool {
		return a.Load() == total && b.Load() == total
	}, "each named worker group should receive every message independently")
}

func TestJetStream_NativeDeduplication_DropsDuplicateIDs(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	// 1-minute window is well above any test execution time, so the broker
	// will reject the duplicate.
	tr, _ := NewJetStream(conn, WithDeduplication(time.Minute))
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "js_dedup"); err != nil {
		t.Fatalf("RegisterEvent: %v", err)
	}
	sub, err := tr.Subscribe(ctx, "js_dedup", transport.WithDeliveryMode(transport.WorkerPool))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close(ctx)

	// Publish the same ID twice. With dedup enabled, the second publish
	// should not surface a second message on the subscriber channel.
	for range 2 {
		if err := tr.Publish(ctx, "js_dedup", newJSMsg("same-id", "src", []byte("p"))); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	var received atomic.Int32
	done := make(chan struct{})
	go func() {
		for m := range sub.Messages() {
			_ = m.Ack(nil)
			received.Add(1)
		}
		close(done)
	}()

	// Wait a bounded window — if a duplicate were going to surface it would
	// have done so within a small constant time after the first.
	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if received.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Give a generous extra window for any phantom duplicate to land.
	time.Sleep(250 * time.Millisecond)

	if got := received.Load(); got != 1 {
		t.Errorf("WithDeduplication: received %d messages, want exactly 1 (duplicate should be dropped)", got)
	}
}

func TestJetStream_PublishUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	err := tr.Publish(context.Background(), "js_unknown", newJSMsg("id", "src", []byte("p")))
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Publish unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestJetStream_SubscribeUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	_, err := tr.Subscribe(context.Background(), "js_unknown")
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Subscribe unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestJetStream_ClosedTransportRejectsOperations(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	if err := tr.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "x"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("RegisterEvent: got %v, want ErrTransportClosed", err)
	}
	if err := tr.Publish(ctx, "x", newJSMsg("id", "src", []byte("p"))); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Publish: got %v, want ErrTransportClosed", err)
	}
	if _, err := tr.Subscribe(ctx, "x"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Subscribe: got %v, want ErrTransportClosed", err)
	}
}

func TestJetStream_NameAndRedeliveryContract(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	if got := tr.Name(); got != "nats-jetstream" {
		t.Errorf("Name: got %q, want %q", got, "nats-jetstream")
	}
	if !tr.SupportsRedelivery() {
		t.Error("SupportsRedelivery: got false, want true (JetStream always supports redelivery)")
	}
}

func TestJetStream_Health_HealthyAndUnhealthy(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	got := tr.Health(context.Background())
	if got.Status != transport.HealthStatusHealthy {
		t.Errorf("Health on healthy: got %v, details=%v", got.Status, got.Details)
	}

	_ = tr.Close(context.Background())
	got = tr.Health(context.Background())
	if got.Status != transport.HealthStatusUnhealthy {
		t.Errorf("Health on closed: got %v, want Unhealthy", got.Status)
	}
}

func TestJetStream_StreamNameUsesPrefix(t *testing.T) {
	t.Parallel()
	conn := startTestServer(t)
	tr, _ := NewJetStream(conn)
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	// Pinned form: "evt_" + event name. Consumers of ConsumerLag and
	// operators inspecting NATS via CLI depend on this naming.
	if got := tr.streamName("order_created"); got != "evt_order_created" {
		t.Errorf("streamName: got %q, want %q", got, "evt_order_created")
	}
}

package channel

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

func testMessageWithMeta(id, payload string, metadata map[string]string) transport.Message {
	return message.New(id, "test", []byte(payload), metadata)
}

func TestRouteFilter_BroadcastExactMatch(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	// Subscriber with route filter: only us-east
	filtered, _ := tr.Subscribe(ctx, "orders",
		transport.WithRouteFilters(map[string]string{"X-Route-region": "us-east"}),
	)
	defer filtered.Close(ctx)

	// Subscriber without filter: receives all
	unfiltered, _ := tr.Subscribe(ctx, "orders")
	defer unfiltered.Close(ctx)

	// Publish message with matching routing key
	msg := testMessageWithMeta("1", "order1", map[string]string{"X-Route-region": "us-east"})
	if err := tr.Publish(ctx, "orders", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Both should receive
	select {
	case <-filtered.Messages():
	case <-time.After(time.Second):
		t.Fatal("filtered subscriber should have received matching message")
	}
	select {
	case <-unfiltered.Messages():
	case <-time.After(time.Second):
		t.Fatal("unfiltered subscriber should have received message")
	}

	// Publish message with non-matching routing key
	msg2 := testMessageWithMeta("2", "order2", map[string]string{"X-Route-region": "eu-west"})
	if err := tr.Publish(ctx, "orders", msg2); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Only unfiltered should receive
	select {
	case <-unfiltered.Messages():
	case <-time.After(time.Second):
		t.Fatal("unfiltered subscriber should have received message")
	}
	select {
	case <-filtered.Messages():
		t.Fatal("filtered subscriber should NOT have received non-matching message")
	case <-time.After(100 * time.Millisecond):
		// Expected: filtered subscriber does not receive
	}
}

func TestRouteFilter_MultipleKeys(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	// Subscriber requires both region AND priority
	sub, _ := tr.Subscribe(ctx, "orders",
		transport.WithRouteFilters(map[string]string{
			"X-Route-region":   "us-east",
			"X-Route-priority": "high",
		}),
	)
	defer sub.Close(ctx)

	// Message with only region — should NOT match
	msg1 := testMessageWithMeta("1", "data", map[string]string{"X-Route-region": "us-east"})
	tr.Publish(ctx, "orders", msg1)

	select {
	case <-sub.Messages():
		t.Fatal("should not receive message with only partial routing keys")
	case <-time.After(100 * time.Millisecond):
	}

	// Message with both keys — should match
	msg2 := testMessageWithMeta("2", "data", map[string]string{
		"X-Route-region":   "us-east",
		"X-Route-priority": "high",
	})
	tr.Publish(ctx, "orders", msg2)

	select {
	case <-sub.Messages():
	case <-time.After(time.Second):
		t.Fatal("should have received message matching all routing keys")
	}
}

func TestRouteFilter_NoFilter(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	sub, _ := tr.Subscribe(ctx, "orders")
	defer sub.Close(ctx)

	// Message with routing keys — unfiltered subscriber should still receive
	msg := testMessageWithMeta("1", "data", map[string]string{"X-Route-region": "us-east"})
	tr.Publish(ctx, "orders", msg)

	select {
	case <-sub.Messages():
	case <-time.After(time.Second):
		t.Fatal("unfiltered subscriber should receive all messages")
	}

	// Message without routing keys
	msg2 := testMessageWithMeta("2", "data", nil)
	tr.Publish(ctx, "orders", msg2)

	select {
	case <-sub.Messages():
	case <-time.After(time.Second):
		t.Fatal("unfiltered subscriber should receive messages without routing keys")
	}
}

func TestRouteFilter_WorkerPool(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "tasks")

	// Two workers with same route filter
	w1, _ := tr.Subscribe(ctx, "tasks",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithRouteFilters(map[string]string{"X-Route-region": "us-east"}),
	)
	defer w1.Close(ctx)

	w2, _ := tr.Subscribe(ctx, "tasks",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithRouteFilters(map[string]string{"X-Route-region": "us-east"}),
	)
	defer w2.Close(ctx)

	// Publish matching message — only ONE worker should receive (round-robin)
	msg := testMessageWithMeta("1", "task", map[string]string{"X-Route-region": "us-east"})
	tr.Publish(ctx, "tasks", msg)

	received := 0
	for i := 0; i < 2; i++ {
		select {
		case <-w1.Messages():
			received++
		case <-w2.Messages():
			received++
		case <-time.After(200 * time.Millisecond):
		}
	}
	if received != 1 {
		t.Fatalf("expected exactly 1 worker to receive, got %d", received)
	}

	// Publish non-matching message — neither should receive
	msg2 := testMessageWithMeta("2", "task", map[string]string{"X-Route-region": "eu-west"})
	tr.Publish(ctx, "tasks", msg2)

	select {
	case <-w1.Messages():
		t.Fatal("worker should not receive non-matching message")
	case <-w2.Messages():
		t.Fatal("worker should not receive non-matching message")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRouteFilter_WorkerGroup(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	// Group A: filters for us-east
	ga, _ := tr.Subscribe(ctx, "orders",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"),
		transport.WithRouteFilters(map[string]string{"X-Route-region": "us-east"}),
	)
	defer ga.Close(ctx)

	// Group B: filters for eu-west
	gb, _ := tr.Subscribe(ctx, "orders",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"),
		transport.WithRouteFilters(map[string]string{"X-Route-region": "eu-west"}),
	)
	defer gb.Close(ctx)

	// Publish us-east message — only group A should receive
	msg := testMessageWithMeta("1", "data", map[string]string{"X-Route-region": "us-east"})
	tr.Publish(ctx, "orders", msg)

	select {
	case <-ga.Messages():
	case <-time.After(time.Second):
		t.Fatal("group-a should receive us-east message")
	}
	select {
	case <-gb.Messages():
		t.Fatal("group-b should NOT receive us-east message")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRouteFilter_CustomPredicate(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	// Custom predicate: exclude eu-west
	sub, _ := tr.Subscribe(ctx, "orders",
		transport.WithRouteMatch(func(meta map[string]string) bool {
			return meta["X-Route-region"] != "eu-west"
		}),
	)
	defer sub.Close(ctx)

	// us-east should be received
	msg1 := testMessageWithMeta("1", "data", map[string]string{"X-Route-region": "us-east"})
	tr.Publish(ctx, "orders", msg1)

	select {
	case <-sub.Messages():
	case <-time.After(time.Second):
		t.Fatal("should receive us-east message")
	}

	// eu-west should be filtered
	msg2 := testMessageWithMeta("2", "data", map[string]string{"X-Route-region": "eu-west"})
	tr.Publish(ctx, "orders", msg2)

	select {
	case <-sub.Messages():
		t.Fatal("should NOT receive eu-west message")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRouteFilter_NoRoutingKeys(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "orders")

	// Subscriber with route filter
	filtered, _ := tr.Subscribe(ctx, "orders",
		transport.WithRouteFilters(map[string]string{"X-Route-region": "us-east"}),
	)
	defer filtered.Close(ctx)

	// Subscriber without filter
	unfiltered, _ := tr.Subscribe(ctx, "orders")
	defer unfiltered.Close(ctx)

	// Message without any routing keys — filtered subscriber should NOT receive
	msg := testMessageWithMeta("1", "data", nil)
	tr.Publish(ctx, "orders", msg)

	select {
	case <-unfiltered.Messages():
	case <-time.After(time.Second):
		t.Fatal("unfiltered subscriber should receive message")
	}
	select {
	case <-filtered.Messages():
		t.Fatal("filtered subscriber should NOT receive message without routing keys")
	case <-time.After(100 * time.Millisecond):
	}
}

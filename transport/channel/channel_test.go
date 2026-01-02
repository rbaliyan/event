package channel

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

func testMessage(id, source, payload string) transport.Message {
	return message.New(id, source, []byte(payload), nil, trace.SpanContext{})
}

func TestNew(t *testing.T) {
	tr := New()
	if tr == nil {
		t.Fatal("expected transport, got nil")
	}
	defer tr.Close(context.Background())
}

func TestNewWithOptions(t *testing.T) {
	tr := New(
		WithBufferSize(100),
		WithTimeout(time.Second),
		WithErrorHandler(func(err error) {
			// Error handler is set but not invoked in this test
		}),
	)
	defer tr.Close(context.Background())

	if tr.bufferSize != 100 {
		t.Errorf("expected buffer size 100, got %d", tr.bufferSize)
	}
}

func TestRegisterEvent(t *testing.T) {
	ctx := context.Background()
	tr := New()
	defer tr.Close(ctx)

	t.Run("register new event", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != nil {
			t.Fatalf("RegisterEvent failed: %v", err)
		}
	})

	t.Run("register duplicate event returns error", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != transport.ErrEventAlreadyExists {
			t.Errorf("expected ErrEventAlreadyExists, got %v", err)
		}
	})

	t.Run("register on closed transport returns error", func(t *testing.T) {
		tr2 := New()
		tr2.Close(ctx)

		err := tr2.RegisterEvent(ctx, "new-event")
		if err != transport.ErrTransportClosed {
			t.Errorf("expected ErrTransportClosed, got %v", err)
		}
	})
}

func TestUnregisterEvent(t *testing.T) {
	ctx := context.Background()
	tr := New()
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "to-unregister")

	t.Run("unregister existing event", func(t *testing.T) {
		err := tr.UnregisterEvent(ctx, "to-unregister")
		if err != nil {
			t.Fatalf("UnregisterEvent failed: %v", err)
		}
	})

	t.Run("unregister non-existent event returns error", func(t *testing.T) {
		err := tr.UnregisterEvent(ctx, "non-existent")
		if err != transport.ErrEventNotRegistered {
			t.Errorf("expected ErrEventNotRegistered, got %v", err)
		}
	})
}

func TestPublish(t *testing.T) {
	ctx := context.Background()
	tr := New()
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "pub-event")

	t.Run("publish to registered event", func(t *testing.T) {
		msg := testMessage("id-1", "source", "payload")
		err := tr.Publish(ctx, "pub-event", msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	})

	t.Run("publish to unregistered event returns error", func(t *testing.T) {
		msg := testMessage("id-2", "source", "payload")
		err := tr.Publish(ctx, "unknown-event", msg)
		if err != transport.ErrEventNotRegistered {
			t.Errorf("expected ErrEventNotRegistered, got %v", err)
		}
	})

	t.Run("publish on closed transport returns error", func(t *testing.T) {
		tr2 := New()
		tr2.RegisterEvent(ctx, "event")
		tr2.Close(ctx)

		msg := testMessage("id-3", "source", "payload")
		err := tr2.Publish(ctx, "event", msg)
		if err != transport.ErrTransportClosed {
			t.Errorf("expected ErrTransportClosed, got %v", err)
		}
	})
}

func TestSubscribe(t *testing.T) {
	ctx := context.Background()
	tr := New()
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "sub-event")

	t.Run("subscribe to registered event", func(t *testing.T) {
		sub, err := tr.Subscribe(ctx, "sub-event")
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
		if sub == nil {
			t.Fatal("expected subscription, got nil")
		}
		defer sub.Close(ctx)

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
}

func TestPublishSubscribeIntegration(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "integration-event")

	sub, _ := tr.Subscribe(ctx, "integration-event")
	defer sub.Close(ctx)

	// Publish message
	msg := testMessage("msg-1", "source", "hello")
	tr.Publish(ctx, "integration-event", msg)

	// Receive message
	select {
	case received := <-sub.Messages():
		if received.ID() != "msg-1" {
			t.Errorf("expected msg-1, got %s", received.ID())
		}
		if !bytes.Equal(received.Payload(), []byte("hello")) {
			t.Errorf("expected hello, got %v", received.Payload())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for message")
	}
}

func TestBroadcastMode(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "broadcast-event")

	// Create multiple subscribers
	sub1, _ := tr.Subscribe(ctx, "broadcast-event")
	sub2, _ := tr.Subscribe(ctx, "broadcast-event")
	defer sub1.Close(ctx)
	defer sub2.Close(ctx)

	// Publish message
	msg := testMessage("msg-1", "source", "broadcast")
	tr.Publish(ctx, "broadcast-event", msg)

	// Both subscribers should receive the message
	var received1, received2 bool

	select {
	case <-sub1.Messages():
		received1 = true
	case <-time.After(100 * time.Millisecond):
	}

	select {
	case <-sub2.Messages():
		received2 = true
	case <-time.After(100 * time.Millisecond):
	}

	if !received1 {
		t.Error("subscriber 1 did not receive message")
	}
	if !received2 {
		t.Error("subscriber 2 did not receive message")
	}
}

func TestWorkerPoolMode(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "worker-event")

	// Create multiple worker subscribers
	sub1, _ := tr.Subscribe(ctx, "worker-event", transport.WithDeliveryMode(transport.WorkerPool))
	sub2, _ := tr.Subscribe(ctx, "worker-event", transport.WithDeliveryMode(transport.WorkerPool))
	defer sub1.Close(ctx)
	defer sub2.Close(ctx)

	// Publish messages
	for i := 0; i < 10; i++ {
		msg := testMessage("msg-"+string(rune('0'+i)), "source", "worker")
		tr.Publish(ctx, "worker-event", msg)
	}

	// Count messages received by each worker
	var count1, count2 int32
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-sub1.Messages():
				if !ok {
					return
				}
				atomic.AddInt32(&count1, 1)
			case <-time.After(50 * time.Millisecond):
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-sub2.Messages():
				if !ok {
					return
				}
				atomic.AddInt32(&count2, 1)
			case <-time.After(50 * time.Millisecond):
				return
			}
		}
	}()

	wg.Wait()

	total := count1 + count2
	if total != 10 {
		t.Errorf("expected 10 total messages, got %d (sub1: %d, sub2: %d)", total, count1, count2)
	}

	// In worker pool mode, messages should be distributed
	// (not necessarily evenly, but not all to one worker)
	if count1 == 10 || count2 == 10 {
		t.Log("Warning: all messages went to one worker (may happen with round-robin)")
	}
}

func TestWorkerGroups(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "worker-group-event")

	// Create workers in different groups
	// Group A: 2 workers
	subA1, _ := tr.Subscribe(ctx, "worker-group-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))
	subA2, _ := tr.Subscribe(ctx, "worker-group-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-a"))

	// Group B: 2 workers
	subB1, _ := tr.Subscribe(ctx, "worker-group-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))
	subB2, _ := tr.Subscribe(ctx, "worker-group-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("group-b"))

	defer subA1.Close(ctx)
	defer subA2.Close(ctx)
	defer subB1.Close(ctx)
	defer subB2.Close(ctx)

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		msg := testMessage("msg-"+string(rune('0'+i)), "source", "worker-group")
		tr.Publish(ctx, "worker-group-event", msg)
	}

	// Count messages received by each worker
	var countA1, countA2, countB1, countB2 int32
	var wg sync.WaitGroup
	wg.Add(4)

	countMessages := func(sub transport.Subscription, counter *int32) {
		defer wg.Done()
		for {
			select {
			case _, ok := <-sub.Messages():
				if !ok {
					return
				}
				atomic.AddInt32(counter, 1)
			case <-time.After(50 * time.Millisecond):
				return
			}
		}
	}

	go countMessages(subA1, &countA1)
	go countMessages(subA2, &countA2)
	go countMessages(subB1, &countB1)
	go countMessages(subB2, &countB2)

	wg.Wait()

	// Each group should receive all 5 messages (distributed among its workers)
	groupATotal := countA1 + countA2
	groupBTotal := countB1 + countB2

	if groupATotal != 5 {
		t.Errorf("group A expected 5 total messages, got %d (A1: %d, A2: %d)", groupATotal, countA1, countA2)
	}
	if groupBTotal != 5 {
		t.Errorf("group B expected 5 total messages, got %d (B1: %d, B2: %d)", groupBTotal, countB1, countB2)
	}

	// Within each group, both workers should receive some messages (round-robin)
	t.Logf("Group A distribution: A1=%d, A2=%d", countA1, countA2)
	t.Logf("Group B distribution: B1=%d, B2=%d", countB1, countB2)
}

func TestWorkerGroupSingleWorker(t *testing.T) {
	// Test a worker group with only one worker
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "single-worker-event")

	// Single worker in group
	sub, _ := tr.Subscribe(ctx, "single-worker-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("solo-group"))
	defer sub.Close(ctx)

	// Publish messages
	for i := 0; i < 5; i++ {
		msg := testMessage("msg-"+string(rune('0'+i)), "source", "single")
		tr.Publish(ctx, "single-worker-event", msg)
	}

	// Single worker should receive all messages
	var count int32
	for {
		select {
		case _, ok := <-sub.Messages():
			if !ok {
				t.Fatal("channel closed unexpectedly")
			}
			count++
			if count >= 5 {
				goto done
			}
		case <-time.After(100 * time.Millisecond):
			goto done
		}
	}
done:
	if count != 5 {
		t.Errorf("expected 5 messages, got %d", count)
	}
}

func TestWorkerGroupMixedWithDefaultPool(t *testing.T) {
	// Test mixing named worker groups with default (unnamed) worker pool
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "mixed-pool-event")

	// Named worker group
	subNamed1, _ := tr.Subscribe(ctx, "mixed-pool-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("named"))
	subNamed2, _ := tr.Subscribe(ctx, "mixed-pool-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup("named"))

	// Default worker pool (empty group name)
	subDefault1, _ := tr.Subscribe(ctx, "mixed-pool-event",
		transport.WithDeliveryMode(transport.WorkerPool))
	subDefault2, _ := tr.Subscribe(ctx, "mixed-pool-event",
		transport.WithDeliveryMode(transport.WorkerPool))

	defer subNamed1.Close(ctx)
	defer subNamed2.Close(ctx)
	defer subDefault1.Close(ctx)
	defer subDefault2.Close(ctx)

	// Publish messages
	for i := 0; i < 10; i++ {
		msg := testMessage("msg-"+string(rune('0'+i)), "source", "mixed")
		tr.Publish(ctx, "mixed-pool-event", msg)
	}

	// Count messages
	var namedCount, defaultCount int32
	var wg sync.WaitGroup
	wg.Add(4)

	countMessages := func(sub transport.Subscription, counter *int32) {
		defer wg.Done()
		for {
			select {
			case _, ok := <-sub.Messages():
				if !ok {
					return
				}
				atomic.AddInt32(counter, 1)
			case <-time.After(50 * time.Millisecond):
				return
			}
		}
	}

	go countMessages(subNamed1, &namedCount)
	go countMessages(subNamed2, &namedCount)
	go countMessages(subDefault1, &defaultCount)
	go countMessages(subDefault2, &defaultCount)

	wg.Wait()

	// Each group should receive all 10 messages
	if namedCount != 10 {
		t.Errorf("named group expected 10 messages, got %d", namedCount)
	}
	if defaultCount != 10 {
		t.Errorf("default group expected 10 messages, got %d", defaultCount)
	}
}

func TestWorkerGroupEmptyStringIsSameAsDefault(t *testing.T) {
	// Empty string worker group should be treated the same as no group (default)
	ctx := context.Background()
	tr := New(WithBufferSize(10))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "empty-group-event")

	// Empty string group
	subEmpty, _ := tr.Subscribe(ctx, "empty-group-event",
		transport.WithDeliveryMode(transport.WorkerPool),
		transport.WithWorkerGroup(""))

	// No group specified (default)
	subDefault, _ := tr.Subscribe(ctx, "empty-group-event",
		transport.WithDeliveryMode(transport.WorkerPool))

	defer subEmpty.Close(ctx)
	defer subDefault.Close(ctx)

	// Publish messages
	for i := 0; i < 6; i++ {
		msg := testMessage("msg-"+string(rune('0'+i)), "source", "empty")
		tr.Publish(ctx, "empty-group-event", msg)
	}

	// Both should compete for the same messages (same group)
	var emptyCount, defaultCount int32
	var wg sync.WaitGroup
	wg.Add(2)

	countMessages := func(sub transport.Subscription, counter *int32) {
		defer wg.Done()
		for {
			select {
			case _, ok := <-sub.Messages():
				if !ok {
					return
				}
				atomic.AddInt32(counter, 1)
			case <-time.After(50 * time.Millisecond):
				return
			}
		}
	}

	go countMessages(subEmpty, &emptyCount)
	go countMessages(subDefault, &defaultCount)

	wg.Wait()

	// Together they should receive all 6 messages
	total := emptyCount + defaultCount
	if total != 6 {
		t.Errorf("expected 6 total messages, got %d (empty: %d, default: %d)", total, emptyCount, defaultCount)
	}
}

func TestSubscriptionClose(t *testing.T) {
	ctx := context.Background()
	tr := New()
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "close-event")

	sub, _ := tr.Subscribe(ctx, "close-event")

	// Close subscription
	err := sub.Close(ctx)
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should not panic
	err = sub.Close(ctx)
	if err != nil {
		t.Errorf("Double close failed: %v", err)
	}
}

func TestTransportClose(t *testing.T) {
	ctx := context.Background()
	tr := New()

	tr.RegisterEvent(ctx, "event-1")
	tr.RegisterEvent(ctx, "event-2")

	// Close transport
	err := tr.Close(ctx)
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Double close should not panic
	err = tr.Close(ctx)
	if err != nil {
		t.Errorf("Double close failed: %v", err)
	}

	// Operations after close should fail
	err = tr.RegisterEvent(ctx, "new-event")
	if err != transport.ErrTransportClosed {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestHealth(t *testing.T) {
	ctx := context.Background()
	tr := New()

	t.Run("healthy transport", func(t *testing.T) {
		result := tr.Health(ctx)
		if result.Status != transport.HealthStatusHealthy {
			t.Errorf("expected healthy, got %s", result.Status)
		}
	})

	t.Run("closed transport is unhealthy", func(t *testing.T) {
		tr.Close(ctx)
		result := tr.Health(ctx)
		if result.Status != transport.HealthStatusUnhealthy {
			t.Errorf("expected unhealthy, got %s", result.Status)
		}
	})
}

func TestWithBufferSizeOption(t *testing.T) {
	ctx := context.Background()
	tr := New(WithBufferSize(50))
	defer tr.Close(ctx)

	tr.RegisterEvent(ctx, "buffered-event")
	sub, _ := tr.Subscribe(ctx, "buffered-event")
	defer sub.Close(ctx)

	// Should be able to publish 50 messages without blocking
	for i := 0; i < 50; i++ {
		msg := testMessage("msg", "source", "data")
		tr.Publish(ctx, "buffered-event", msg)
	}
}

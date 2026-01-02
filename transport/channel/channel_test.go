package channel

import (
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
	return message.New(id, source, payload, nil, trace.SpanContext{})
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
		if received.Payload() != "hello" {
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

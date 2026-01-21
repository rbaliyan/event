package noop

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// testMessage creates a test message with sensible defaults
func testMessage(id string, payload []byte) transport.Message {
	return transport.NewMessage(id, "test-source", payload, nil, trace.SpanContext{})
}

func TestNew(t *testing.T) {
	tr := New()
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
	if !tr.isOpen() {
		t.Fatal("expected transport to be open")
	}
}

func TestRegisterEvent(t *testing.T) {
	tr := New()
	ctx := context.Background()

	// Register event
	if err := tr.RegisterEvent(ctx, "test-event"); err != nil {
		t.Fatalf("RegisterEvent failed: %v", err)
	}

	// Duplicate registration should fail
	if err := tr.RegisterEvent(ctx, "test-event"); err != transport.ErrEventAlreadyExists {
		t.Fatalf("expected ErrEventAlreadyExists, got: %v", err)
	}
}

func TestUnregisterEvent(t *testing.T) {
	tr := New()
	ctx := context.Background()

	// Unregister non-existent event
	if err := tr.UnregisterEvent(ctx, "non-existent"); err != transport.ErrEventNotRegistered {
		t.Fatalf("expected ErrEventNotRegistered, got: %v", err)
	}

	// Register and unregister
	tr.RegisterEvent(ctx, "test-event")
	if err := tr.UnregisterEvent(ctx, "test-event"); err != nil {
		t.Fatalf("UnregisterEvent failed: %v", err)
	}

	// Should be able to re-register after unregister
	if err := tr.RegisterEvent(ctx, "test-event"); err != nil {
		t.Fatalf("re-register failed: %v", err)
	}
}

func TestPublish(t *testing.T) {
	tr := New()
	ctx := context.Background()

	// Publish to non-existent event
	msg := testMessage("msg-1", []byte("payload"))
	if err := tr.Publish(ctx, "non-existent", msg); err != transport.ErrEventNotRegistered {
		t.Fatalf("expected ErrEventNotRegistered, got: %v", err)
	}

	// Register and publish
	tr.RegisterEvent(ctx, "test-event")
	if err := tr.Publish(ctx, "test-event", msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Multiple publishes should all succeed
	for i := 0; i < 100; i++ {
		if err := tr.Publish(ctx, "test-event", msg); err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}
}

func TestSubscribe(t *testing.T) {
	tr := New()
	ctx := context.Background()

	// Subscribe to non-existent event
	if _, err := tr.Subscribe(ctx, "non-existent"); err != transport.ErrEventNotRegistered {
		t.Fatalf("expected ErrEventNotRegistered, got: %v", err)
	}

	// Register and subscribe
	tr.RegisterEvent(ctx, "test-event")
	sub, err := tr.Subscribe(ctx, "test-event")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}
	if sub.ID() == "" {
		t.Fatal("expected non-empty subscription ID")
	}

	// Close subscription
	if err := sub.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSubscribeNeverReceivesMessages(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	// Publish some messages
	msg := testMessage("msg-1", []byte("payload"))
	for i := 0; i < 10; i++ {
		tr.Publish(ctx, "test-event", msg)
	}

	// Subscription should not receive any messages
	select {
	case m := <-sub.Messages():
		t.Fatalf("unexpected message received: %v", m)
	case <-time.After(50 * time.Millisecond):
		// Expected - no messages
	}

	sub.Close(ctx)
}

func TestClose(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	// Close transport
	if err := tr.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations should fail after close
	if err := tr.RegisterEvent(ctx, "another-event"); err != transport.ErrTransportClosed {
		t.Fatalf("expected ErrTransportClosed, got: %v", err)
	}

	msg := testMessage("msg-1", []byte("payload"))
	if err := tr.Publish(ctx, "test-event", msg); err != transport.ErrTransportClosed {
		t.Fatalf("expected ErrTransportClosed, got: %v", err)
	}

	if _, err := tr.Subscribe(ctx, "test-event"); err != transport.ErrTransportClosed {
		t.Fatalf("expected ErrTransportClosed, got: %v", err)
	}

	// Subscription should be closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscription channel not closed")
	}

	// Double close should be safe
	if err := tr.Close(ctx); err != nil {
		t.Fatalf("double Close failed: %v", err)
	}
}

func TestHealth(t *testing.T) {
	tr := New()
	ctx := context.Background()

	// Healthy when open
	result := tr.Health(ctx)
	if result.Status != transport.HealthStatusHealthy {
		t.Fatalf("expected healthy status, got: %v", result.Status)
	}
	if result.Details["type"] != "noop" {
		t.Fatalf("expected type=noop, got: %v", result.Details["type"])
	}

	// Register some events
	tr.RegisterEvent(ctx, "event-1")
	tr.RegisterEvent(ctx, "event-2")

	result = tr.Health(ctx)
	if result.Details["events"] != 2 {
		t.Fatalf("expected 2 events, got: %v", result.Details["events"])
	}

	// Unhealthy when closed
	tr.Close(ctx)
	result = tr.Health(ctx)
	if result.Status != transport.HealthStatusUnhealthy {
		t.Fatalf("expected unhealthy status, got: %v", result.Status)
	}
}

func TestUnregisterClosesSubscriptions(t *testing.T) {
	tr := New()
	ctx := context.Background()

	tr.RegisterEvent(ctx, "test-event")
	sub, _ := tr.Subscribe(ctx, "test-event")

	// Unregister should close subscriptions
	tr.UnregisterEvent(ctx, "test-event")

	// Subscription should be closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("subscription channel not closed")
	}
}

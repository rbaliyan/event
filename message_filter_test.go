package event

import (
	"context"
	"testing"

	"github.com/rbaliyan/event/v3/transport/channel"
)

func TestWithMessageFilter(t *testing.T) {
	t.Parallel()
	filter := func(meta map[string]string) bool {
		return meta["collection"] == "orders"
	}
	opts := newEventOptions(WithMessageFilter(filter))
	if opts.messageFilter == nil {
		t.Fatal("expected messageFilter to be set")
	}
	// Verify the filter works correctly
	if !opts.messageFilter(map[string]string{"collection": "orders"}) {
		t.Error("expected filter to return true for matching metadata")
	}
	if opts.messageFilter(map[string]string{"collection": "users"}) {
		t.Error("expected filter to return false for non-matching metadata")
	}
}

func TestWithMessageFilter_Nil(t *testing.T) {
	t.Parallel()
	opts := newEventOptions(WithMessageFilter(nil))
	if opts.messageFilter != nil {
		t.Error("expected nil messageFilter to be stored as nil")
	}
}

func TestMessageFilter_SkipsNonMatching(t *testing.T) {
	t.Parallel()
	bus := mustNewBus(t, "test-filter", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	received := make(chan string, 10)

	e := New[string]("test-filtered",
		WithMessageFilter(func(meta map[string]string) bool {
			return meta["collection"] == "orders"
		}),
	)
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		received <- data
		return nil
	})

	// Publish with matching metadata — should be received
	ctx1 := ContextWithMetadata(context.Background(), map[string]string{"collection": "orders"})
	if err := e.Publish(ctx1, "order-1"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Publish with non-matching metadata — should be filtered out
	ctx2 := ContextWithMetadata(context.Background(), map[string]string{"collection": "users"})
	if err := e.Publish(ctx2, "user-1"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Publish another matching one to confirm ordering
	ctx3 := ContextWithMetadata(context.Background(), map[string]string{"collection": "orders"})
	if err := e.Publish(ctx3, "order-2"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Should receive "order-1"
	data, ok := waitForData(received, waitChTimeoutMS)
	if !ok {
		t.Fatal("expected to receive order-1")
	}
	if data != "order-1" {
		t.Errorf("expected order-1, got %s", data)
	}

	// Should receive "order-2"
	data, ok = waitForData(received, waitChTimeoutMS)
	if !ok {
		t.Fatal("expected to receive order-2")
	}
	if data != "order-2" {
		t.Errorf("expected order-2, got %s", data)
	}

	// Should NOT receive anything else
	_, ok = waitForData(received, waitChTimeoutMS)
	if ok {
		t.Error("expected no more messages, but received one")
	}
}

func TestMessageFilter_NilAcceptsAll(t *testing.T) {
	t.Parallel()
	bus := mustNewBus(t, "test-filter-nil", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	received := make(chan string, 10)

	// No message filter — all messages should pass through
	e := New[string]("test-no-filter")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		received <- data
		return nil
	})

	ctx := ContextWithMetadata(context.Background(), map[string]string{"collection": "anything"})
	if err := e.Publish(ctx, "msg-1"); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	data, ok := waitForData(received, waitChTimeoutMS)
	if !ok {
		t.Fatal("expected to receive msg-1")
	}
	if data != "msg-1" {
		t.Errorf("expected msg-1, got %s", data)
	}
}

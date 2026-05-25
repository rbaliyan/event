package event

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
)

func TestWithDecodeErrorHandler(t *testing.T) {
	t.Parallel()
	handler := func(ctx context.Context, msg message.Message, err error) error {
		return ErrReject
	}
	opts := newEventOptions(WithDecodeErrorHandler(handler))
	if opts.decodeErrorHandler == nil {
		t.Fatal("expected decodeErrorHandler to be set")
	}
}

func TestWithDecodeErrorHandler_Nil(t *testing.T) {
	t.Parallel()
	opts := newEventOptions(WithDecodeErrorHandler(nil))
	if opts.decodeErrorHandler != nil {
		t.Error("expected nil decodeErrorHandler to be stored as nil")
	}
}

func TestDecodeErrorHandler_AckSkipsDLQ(t *testing.T) {
	t.Parallel()
	dlqStore := newTestDLQStore()
	bus := mustNewBus(t, "test-decode-ack", WithTransport(channel.New()), WithDLQ(dlqStore))
	defer bus.Close(context.Background())

	handlerCalled := make(chan struct{}, 1)

	e := New[string]("test-decode-ack",
		WithDecodeErrorHandler(func(ctx context.Context, msg message.Message, err error) error {
			handlerCalled <- struct{}{}
			return nil // Ack — skip DLQ
		}),
	)
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		t.Error("handler should not be called on decode error")
		return nil
	})

	// Send malformed JSON payload directly via bus
	metadata := map[string]string{MetadataContentType: "application/json"}
	if err := bus.Send(context.Background(), "test-decode-ack", "", []byte("{invalid json}"), metadata); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if !wait(handlerCalled, waitChTimeoutMS) {
		t.Fatal("expected decode error handler to be called")
	}

	// DLQ must NOT be called when the decode error handler returns nil.
	// Wait a window long enough to catch a stray async DLQ enqueue.
	select {
	case <-dlqStore.called:
		t.Error("expected DLQ store NOT to be called when decode error handler returns nil")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDecodeErrorHandler_RejectSendsToDLQ(t *testing.T) {
	t.Parallel()
	dlqStore := newTestDLQStore()
	bus := mustNewBus(t, "test-decode-reject", WithTransport(channel.New()), WithDLQ(dlqStore))
	defer bus.Close(context.Background())

	e := New[string]("test-decode-reject",
		WithDecodeErrorHandler(func(ctx context.Context, msg message.Message, err error) error {
			return ErrReject // Send to DLQ
		}),
	)
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		t.Error("handler should not be called on decode error")
		return nil
	})

	metadata := map[string]string{MetadataContentType: "application/json"}
	if err := bus.Send(context.Background(), "test-decode-reject", "", []byte("{invalid json}"), metadata); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if !wait(dlqStore.called, waitChTimeoutMS) {
		t.Fatal("expected DLQ store to be called when decode error handler returns ErrReject")
	}
}

func TestDecodeErrorHandler_DefaultBehaviorWhenNotSet(t *testing.T) {
	t.Parallel()
	dlqStore := newTestDLQStore()
	bus := mustNewBus(t, "test-decode-default", WithTransport(channel.New()), WithDLQ(dlqStore))
	defer bus.Close(context.Background())

	// No WithDecodeErrorHandler — should use default DLQ+ack behavior
	e := New[string]("test-decode-default")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		t.Error("handler should not be called on decode error")
		return nil
	})

	metadata := map[string]string{MetadataContentType: "application/json"}
	if err := bus.Send(context.Background(), "test-decode-default", "", []byte("{invalid json}"), metadata); err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if !wait(dlqStore.called, waitChTimeoutMS) {
		t.Fatal("expected DLQ store to be called with default behavior (no decode error handler set)")
	}
}

package migration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
)

func newMsg(id string) transport.Message {
	return message.New(id, "test", []byte(`{"key":"value"}`), nil)
}

func setup(t *testing.T) (*Transport, *channel.Transport, *channel.Transport) {
	t.Helper()
	old := channel.New(channel.WithBufferSize(100))
	new := channel.New(channel.WithBufferSize(100))
	mt, err := New(old, new)
	if err != nil {
		t.Fatal(err)
	}
	return mt, old, new
}

func TestNew_Validation(t *testing.T) {
	_, err := New(nil, channel.New())
	if !errors.Is(err, ErrOldTransportRequired) {
		t.Fatalf("expected ErrOldTransportRequired, got %v", err)
	}
	_, err = New(channel.New(), nil)
	if !errors.Is(err, ErrNewTransportRequired) {
		t.Fatalf("expected ErrNewTransportRequired, got %v", err)
	}
}

func TestPublish_GoesToNewOnly(t *testing.T) {
	mt, old, new := setup(t)
	ctx := context.Background()
	event := "test.event"

	// Register on both transports directly so we can subscribe separately
	old.RegisterEvent(ctx, event)
	new.RegisterEvent(ctx, event)

	// Subscribe directly on each transport
	oldSub, _ := old.Subscribe(ctx, event)
	newSub, _ := new.Subscribe(ctx, event)

	// Publish via migration transport
	mt.Publish(ctx, event, newMsg("msg-1"))

	// New should receive the message
	select {
	case msg := <-newSub.Messages():
		if msg.ID() != "msg-1" {
			t.Fatalf("expected msg-1, got %s", msg.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message on new transport")
	}

	// Old should NOT receive the message
	select {
	case msg := <-oldSub.Messages():
		t.Fatalf("old transport should not receive messages, got %s", msg.ID())
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	oldSub.Close(ctx)
	newSub.Close(ctx)
	mt.Close(ctx)
}

func TestSubscribe_MergesBothTransports(t *testing.T) {
	mt, old, _ := setup(t)
	ctx := context.Background()
	event := "test.event"

	// Register on old directly (simulating pre-migration state)
	old.RegisterEvent(ctx, event)
	// Register on new via migration transport
	mt.RegisterEvent(ctx, event)

	// Subscribe via migration transport (should get messages from both)
	mergedSub, err := mt.Subscribe(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	// Publish to old transport directly (simulating old producers)
	old.Publish(ctx, event, newMsg("old-msg"))
	// Publish to new transport via migration
	mt.Publish(ctx, event, newMsg("new-msg"))

	received := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case msg := <-mergedSub.Messages():
			received[msg.ID()] = true
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for merged messages")
		}
	}

	if !received["old-msg"] {
		t.Fatal("did not receive message from old transport")
	}
	if !received["new-msg"] {
		t.Fatal("did not receive message from new transport")
	}

	mergedSub.Close(ctx)
	mt.Close(ctx)
}

func TestRegisterEvent_NewOnly(t *testing.T) {
	mt, old, _ := setup(t)
	ctx := context.Background()

	mt.RegisterEvent(ctx, "test.event")

	// Old transport should not have the event — publishing should fail
	err := old.Publish(ctx, "test.event", newMsg("msg-1"))
	if err == nil {
		t.Fatal("expected error publishing to unregistered event on old transport")
	}

	mt.Close(ctx)
}

func TestUnregisterEvent_Both(t *testing.T) {
	mt, old, new := setup(t)
	ctx := context.Background()
	event := "test.event"

	old.RegisterEvent(ctx, event)
	new.RegisterEvent(ctx, event)

	mt.UnregisterEvent(ctx, event)

	// Both should fail to publish
	if err := old.Publish(ctx, event, newMsg("msg")); err == nil {
		t.Fatal("old should be unregistered")
	}
	if err := new.Publish(ctx, event, newMsg("msg")); err == nil {
		t.Fatal("new should be unregistered")
	}

	mt.Close(ctx)
}

func TestClose_ClosesBoth(t *testing.T) {
	mt, old, new := setup(t)
	ctx := context.Background()

	mt.Close(ctx)

	// Both transports should be closed
	if err := old.RegisterEvent(ctx, "test"); err == nil {
		t.Fatal("old transport should be closed")
	}
	if err := new.RegisterEvent(ctx, "test"); err == nil {
		t.Fatal("new transport should be closed")
	}
}

func TestMergedSubscription_Close(t *testing.T) {
	mt, old, _ := setup(t)
	ctx := context.Background()
	event := "test.event"

	old.RegisterEvent(ctx, event)
	mt.RegisterEvent(ctx, event)

	sub, err := mt.Subscribe(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	// Close the merged subscription
	sub.Close(ctx)

	// Channel should be drained and closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel close")
	}

	mt.Close(ctx)
}

func TestOldSubscribeFailure_FallsBackToNew(t *testing.T) {
	old := channel.New(channel.WithBufferSize(100))
	new := channel.New(channel.WithBufferSize(100))
	mt, _ := New(old, new)
	ctx := context.Background()
	event := "test.event"

	// Only register on new, not old — old subscribe will fail
	new.RegisterEvent(ctx, event)

	sub, err := mt.Subscribe(ctx, event)
	if err != nil {
		t.Fatalf("subscribe should succeed with new only, got %v", err)
	}

	// Publish via migration and verify delivery
	mt.Publish(ctx, event, newMsg("msg-1"))

	select {
	case msg := <-sub.Messages():
		if msg.ID() != "msg-1" {
			t.Fatalf("expected msg-1, got %s", msg.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}

	sub.Close(ctx)
	mt.Close(ctx)
}

func TestName(t *testing.T) {
	mt, _, _ := setup(t)
	name := mt.Name()
	if name != "migration(channel->channel)" {
		t.Fatalf("expected migration(channel->channel), got %s", name)
	}
	mt.Close(context.Background())
}

func TestHealth(t *testing.T) {
	mt, _, _ := setup(t)
	ctx := context.Background()

	health := mt.Health(ctx)
	if health.Status != transport.HealthStatusHealthy {
		t.Fatalf("expected healthy, got %s", health.Status)
	}

	mt.Close(ctx)
}

func TestConsumerLag(t *testing.T) {
	mt, _, _ := setup(t)
	ctx := context.Background()

	lags, err := mt.ConsumerLag(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Channel transport doesn't implement LagMonitor, so no lags
	if len(lags) != 0 {
		t.Fatalf("expected 0 lags, got %d", len(lags))
	}

	mt.Close(ctx)
}

func TestCloseThenOperate(t *testing.T) {
	mt, _, _ := setup(t)
	ctx := context.Background()

	mt.Close(ctx)

	if err := mt.RegisterEvent(ctx, "test"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Fatalf("RegisterEvent after close: expected ErrTransportClosed, got %v", err)
	}
	if err := mt.Publish(ctx, "test", newMsg("m")); !errors.Is(err, transport.ErrTransportClosed) {
		t.Fatalf("Publish after close: expected ErrTransportClosed, got %v", err)
	}
	if _, err := mt.Subscribe(ctx, "test"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Fatalf("Subscribe after close: expected ErrTransportClosed, got %v", err)
	}
	if err := mt.UnregisterEvent(ctx, "test"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Fatalf("UnregisterEvent after close: expected ErrTransportClosed, got %v", err)
	}
}

func TestDoubleClose(t *testing.T) {
	mt, _, _ := setup(t)
	ctx := context.Background()
	if err := mt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := mt.Close(ctx); err != nil {
		t.Fatal("second close should return nil", err)
	}
}

func TestWithLogger(t *testing.T) {
	old := channel.New(channel.WithBufferSize(10))
	newT := channel.New(channel.WithBufferSize(10))
	logger := slog.New(slog.NewTextHandler(nil, nil))
	mt, err := New(old, newT, WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	mt.Close(context.Background())
}

func TestWithMergedBufferSize(t *testing.T) {
	old := channel.New(channel.WithBufferSize(10))
	newT := channel.New(channel.WithBufferSize(10))
	mt, err := New(old, newT, WithMergedBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	if mt.mergedBufSize != 128 {
		t.Fatalf("expected mergedBufSize 128, got %d", mt.mergedBufSize)
	}
	mt.Close(context.Background())
}

func TestWithMergedBufferSize_InvalidIgnored(t *testing.T) {
	old := channel.New(channel.WithBufferSize(10))
	newT := channel.New(channel.WithBufferSize(10))
	mt, err := New(old, newT, WithMergedBufferSize(0))
	if err != nil {
		t.Fatal(err)
	}
	if mt.mergedBufSize != defaultMergedBufferSize {
		t.Fatalf("expected default %d, got %d", defaultMergedBufferSize, mt.mergedBufSize)
	}
	mt.Close(context.Background())
}

func TestSupportsRedelivery(t *testing.T) {
	mt, _, _ := setup(t)
	// Channel transport does not implement Redeliverable, so should return false.
	if mt.SupportsRedelivery() {
		t.Fatal("expected false for channel transport")
	}
	mt.Close(context.Background())
}

func TestContextCancellation_StopsSubscription(t *testing.T) {
	mt, old, _ := setup(t)
	event := "test.event"

	ctx := context.Background()
	old.RegisterEvent(ctx, event)
	mt.RegisterEvent(ctx, event)

	subCtx, cancel := context.WithCancel(ctx)
	sub, err := mt.Subscribe(subCtx, event)
	if err != nil {
		t.Fatal(err)
	}

	// Cancel the context — forwarders should stop and channel should close.
	cancel()

	select {
	case _, ok := <-sub.Messages():
		if ok {
			// Could get buffered messages, drain them
			for range sub.Messages() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for channel close after context cancel")
	}

	sub.Close(ctx)
	mt.Close(ctx)
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	mt, old, _ := setup(t)
	ctx := context.Background()
	event := "test.event"

	old.RegisterEvent(ctx, event)
	mt.RegisterEvent(ctx, event)

	sub, err := mt.Subscribe(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mt.Publish(ctx, event, newMsg(fmt.Sprintf("msg-%d", i)))
		}(i)
	}

	var received atomic.Int32
	done := make(chan struct{})
	go func() {
		for range sub.Messages() {
			if received.Add(1) == n {
				close(done)
				return
			}
		}
	}()

	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout: received %d/%d messages", received.Load(), n)
	}

	sub.Close(ctx)
	mt.Close(ctx)
}

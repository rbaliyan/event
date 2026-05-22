package composite

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/persistent"
)

// helper creates a composite transport with memory store and channel signal.
func newTestTransport(t *testing.T, opts ...Option) (*Transport, persistent.Store, transport.Transport) {
	t.Helper()
	store := persistent.NewMemoryStore()
	signal := channel.New(channel.WithBufferSize(100))

	defaults := []Option{
		WithPollInterval(50 * time.Millisecond),
		WithBufferSize(10),
	}
	allOpts := append(defaults, opts...)

	ct, err := New(store, signal, allOpts...)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return ct, store, signal
}

// publishAndWait publishes a message and returns the message ID.
func publishAndWait(t *testing.T, ctx context.Context, ct *Transport, eventName string, id int) string {
	t.Helper()
	msgID := fmt.Sprintf("msg-%d", id)
	msg := transport.NewMessageWithAck(
		msgID, "test", []byte(fmt.Sprintf(`{"id":%d}`, id)),
		map[string]string{"Content-Type": "application/json"},
		0, func(error) error { return nil },
	)

	// Encode through codec so store has proper format
	c := codec.Default()
	data, err := c.Encode(msg)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	_ = data

	if err := ct.Publish(ctx, eventName, msg); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	return msgID
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	signal := channel.New()

	t.Run("nil store", func(t *testing.T) {
		_, err := New(nil, signal)
		if err != ErrStoreRequired {
			t.Fatalf("expected ErrStoreRequired, got %v", err)
		}
	})

	t.Run("nil signal", func(t *testing.T) {
		_, err := New(store, nil)
		if err != ErrSignalRequired {
			t.Fatalf("expected ErrSignalRequired, got %v", err)
		}
	})

	t.Run("valid", func(t *testing.T) {
		ct, err := New(store, signal)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ct == nil {
			t.Fatal("expected non-nil transport")
		}
		ct.Close(context.Background())
	})
}

func TestNew_Options(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	signal := channel.New()

	ct, err := New(store, signal,
		WithPollInterval(10*time.Second),
		WithBufferSize(50),
		WithSignalPrefix("custom:"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ct.Close(context.Background())

	if ct.pollInterval != 10*time.Second {
		t.Errorf("expected pollInterval 10s, got %v", ct.pollInterval)
	}
	if ct.bufferSize != 50 {
		t.Errorf("expected bufferSize 50, got %d", ct.bufferSize)
	}
	if ct.signalPrefix != "custom:" {
		t.Errorf("expected signalPrefix 'custom:', got %q", ct.signalPrefix)
	}
}

func TestRegisterEvent(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()
	defer ct.Close(ctx)

	if err := ct.RegisterEvent(ctx, "orders"); err != nil {
		t.Fatalf("RegisterEvent error: %v", err)
	}

	// Duplicate registration fails
	if err := ct.RegisterEvent(ctx, "orders"); err != transport.ErrEventAlreadyExists {
		t.Fatalf("expected ErrEventAlreadyExists, got %v", err)
	}
}

func TestUnregisterEvent(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	if err := ct.UnregisterEvent(ctx, "orders"); err != nil {
		t.Fatalf("UnregisterEvent error: %v", err)
	}

	// Unregistering again fails
	if err := ct.UnregisterEvent(ctx, "orders"); err != transport.ErrEventNotRegistered {
		t.Fatalf("expected ErrEventNotRegistered, got %v", err)
	}
}

func TestPublish_Success(t *testing.T) {
	t.Parallel()
	ct, store, _ := newTestTransport(t)
	ctx := context.Background()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	msg := transport.NewMessageWithAck(
		"msg-1", "test", []byte(`{"id":1}`),
		map[string]string{"Content-Type": "application/json"},
		0, func(error) error { return nil },
	)

	if err := ct.Publish(ctx, "orders", msg); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	// Verify message is in the store
	stored, err := store.Fetch(ctx, "orders", "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if stored == nil {
		t.Fatal("expected message in store")
	}
}

func TestPublish_DurableFailure(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()
	defer ct.Close(ctx)

	// Publish to unregistered event
	msg := transport.NewMessageWithAck(
		"msg-1", "test", []byte(`{}`),
		nil, 0, func(error) error { return nil },
	)

	err := ct.Publish(ctx, "nonexistent", msg)
	if err != transport.ErrEventNotRegistered {
		t.Fatalf("expected ErrEventNotRegistered, got %v", err)
	}
}

func TestPublish_SignalFailure(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	signal := channel.New()
	ctx := context.Background()

	ct, err := New(store, signal,
		WithPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	// Close signal transport to force signal failure
	signal.Close(ctx)

	msg := transport.NewMessageWithAck(
		"msg-1", "test", []byte(`{"id":1}`),
		map[string]string{"Content-Type": "application/json"},
		0, func(error) error { return nil },
	)

	// Publish should succeed (message is in durable store)
	if err := ct.Publish(ctx, "orders", msg); err != nil {
		t.Fatalf("Publish should succeed even when signal fails, got: %v", err)
	}

	// Verify message is in store
	stored, err := store.Fetch(ctx, "orders", "")
	if err != nil {
		t.Fatalf("Fetch error: %v", err)
	}
	if stored == nil {
		t.Fatal("expected message in store despite signal failure")
	}
}

func TestSubscribe_SignalDriven(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish a message
	publishAndWait(t, ctx, ct, "orders", 1)

	// Should receive quickly via signal (not waiting for poll)
	select {
	case msg := <-sub.Messages():
		if msg.ID() != "msg-1" {
			t.Fatalf("expected msg-1, got %s", msg.ID())
		}
		if err := msg.Ack(nil); err != nil {
			t.Fatalf("Ack error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for signal-driven message")
	}
}

func TestSubscribe_PollFallback(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	signal := channel.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ct, err := New(store, signal,
		WithPollInterval(50*time.Millisecond),
		WithBufferSize(10),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	// Close signal before subscribing to force poll-only mode
	signal.Close(ctx)

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Write directly to store (bypassing Publish to avoid signal error logging)
	c := codec.Default()
	msg := transport.NewMessageWithAck(
		"msg-1", "test", []byte(`{"id":1}`),
		map[string]string{"Content-Type": "application/json"},
		0, func(error) error { return nil },
	)
	data, _ := c.Encode(msg)
	store.Append(ctx, "orders", data)

	// Should receive via poll fallback
	select {
	case received := <-sub.Messages():
		if received.ID() != "msg-1" {
			t.Fatalf("expected msg-1, got %s", received.ID())
		}
		received.Ack(nil)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for poll-delivered message")
	}
}

func TestSubscribe_MultipleMessages(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish multiple messages
	for i := 1; i <= 5; i++ {
		publishAndWait(t, ctx, ct, "orders", i)
	}

	// Should receive all 5 in order
	for i := 1; i <= 5; i++ {
		select {
		case msg := <-sub.Messages():
			expected := fmt.Sprintf("msg-%d", i)
			if msg.ID() != expected {
				t.Fatalf("expected %s, got %s", expected, msg.ID())
			}
			msg.Ack(nil)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}

func TestSubscribe_Checkpoint(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	cpStore := persistent.NewMemoryCheckpointStore()
	signal := channel.New(channel.WithBufferSize(100))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ct, err := New(store, signal,
		WithPollInterval(50*time.Millisecond),
		WithBufferSize(10),
		WithCheckpointStore(cpStore),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	ct.RegisterEvent(ctx, "orders")

	// First subscriber processes 3 messages
	sub1, _ := ct.Subscribe(ctx, "orders")
	subID := sub1.ID()

	for i := 1; i <= 3; i++ {
		publishAndWait(t, ctx, ct, "orders", i)
	}

	for i := 0; i < 3; i++ {
		select {
		case msg := <-sub1.Messages():
			msg.Ack(nil)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}

	// Close first subscriber
	sub1.Close(ctx)

	// Verify checkpoint was saved
	cp, _ := cpStore.Load(ctx, "orders", subID)
	if cp == "" {
		t.Fatal("expected checkpoint to be saved")
	}

	// Publish 2 more messages
	for i := 4; i <= 5; i++ {
		publishAndWait(t, ctx, ct, "orders", i)
	}

	ct.Close(ctx)

	// Create new transport with same stores (simulating restart)
	signal2 := channel.New(channel.WithBufferSize(100))
	ct2, err := New(store, signal2,
		WithPollInterval(50*time.Millisecond),
		WithBufferSize(10),
		WithCheckpointStore(cpStore),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer ct2.Close(ctx)

	ct2.RegisterEvent(ctx, "orders")

	// New subscriber should only get messages 4 and 5
	// We need to set up so it uses the same consumer ID for checkpoint lookup
	// Since consumer IDs are auto-generated, we verify the checkpoint store
	// was written and new messages arrive (from poll since no checkpoint match)
	sub2, _ := ct2.Subscribe(ctx, "orders")
	defer sub2.Close(ctx)

	// Publish one more to trigger signal
	publishAndWait(t, ctx, ct2, "orders", 6)

	// New subscriber starts fresh (new ID, no matching checkpoint) but should
	// get messages from the store. Verify it gets at least some messages.
	var received int
	timeout := time.After(2 * time.Second)
	for received < 1 {
		select {
		case msg := <-sub2.Messages():
			msg.Ack(nil)
			received++
		case <-timeout:
			t.Fatal("timeout: expected at least 1 message from new subscriber")
		}
	}
}

func TestClose_Graceful(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()

	ct.RegisterEvent(ctx, "orders")

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	// Close transport
	if err := ct.Close(ctx); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Subscription should be closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for subscription close")
	}

	// Operations on closed transport should fail
	if err := ct.RegisterEvent(ctx, "new"); err != transport.ErrTransportClosed {
		t.Fatalf("expected ErrTransportClosed, got %v", err)
	}
}

func TestHealth(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()
	defer ct.Close(ctx)

	result := ct.Health(ctx)
	if result.Status != transport.HealthStatusHealthy {
		t.Fatalf("expected healthy, got %s", result.Status)
	}
	if result.Details["type"] != "composite" {
		t.Fatalf("expected type 'composite', got %v", result.Details["type"])
	}
}

func TestHealth_Closed(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx := context.Background()

	ct.Close(ctx)

	result := ct.Health(ctx)
	if result.Status != transport.HealthStatusUnhealthy {
		t.Fatalf("expected unhealthy after close, got %s", result.Status)
	}
}

func TestSubscribe_ConcurrentPublish(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish concurrently
	count := 20
	var published int64
	for i := 0; i < count; i++ {
		go func(id int) {
			msg := transport.NewMessageWithAck(
				fmt.Sprintf("msg-%d", id), "test",
				[]byte(fmt.Sprintf(`{"id":%d}`, id)),
				map[string]string{"Content-Type": "application/json"},
				0, func(error) error { return nil },
			)
			if err := ct.Publish(ctx, "orders", msg); err == nil {
				atomic.AddInt64(&published, 1)
			}
		}(i)
	}

	// Receive all messages
	var received int64
	timeout := time.After(5 * time.Second)
	for received < int64(count) {
		select {
		case msg := <-sub.Messages():
			msg.Ack(nil)
			atomic.AddInt64(&received, 1)
		case <-timeout:
			t.Fatalf("timeout: received %d of %d messages", received, count)
		}
	}

	if received != int64(count) {
		t.Fatalf("expected %d messages, received %d", count, received)
	}
}

func TestSubscribe_NackRedelivery(t *testing.T) {
	t.Parallel()
	ct, _, _ := newTestTransport(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer ct.Close(ctx)

	ct.RegisterEvent(ctx, "orders")

	sub, err := ct.Subscribe(ctx, "orders")
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish a message
	publishAndWait(t, ctx, ct, "orders", 1)

	// Receive and nack (simulate handler failure)
	select {
	case msg := <-sub.Messages():
		if msg.ID() != "msg-1" {
			t.Fatalf("expected msg-1, got %s", msg.ID())
		}
		// Nack: signal processing failure
		msg.Ack(fmt.Errorf("handler failed"))
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first delivery")
	}

	// Message should be redelivered after nack
	select {
	case msg := <-sub.Messages():
		if msg.ID() != "msg-1" {
			t.Fatalf("expected redelivery of msg-1, got %s", msg.ID())
		}
		// Ack this time
		msg.Ack(nil)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for redelivery after nack")
	}
}

func TestSubscribe_ConsumerID(t *testing.T) {
	t.Parallel()
	store := persistent.NewMemoryStore()
	cpStore := persistent.NewMemoryCheckpointStore()
	signal := channel.New(channel.WithBufferSize(100))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ct, err := New(store, signal,
		WithPollInterval(50*time.Millisecond),
		WithBufferSize(10),
		WithCheckpointStore(cpStore),
	)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	ct.RegisterEvent(ctx, "orders")

	// Subscribe with stable consumer ID
	sub1, _ := ct.Subscribe(ctx, "orders",
		transport.WithConsumerID("order-processor-1"),
	)

	// Publish and process 2 messages
	publishAndWait(t, ctx, ct, "orders", 1)
	publishAndWait(t, ctx, ct, "orders", 2)

	for i := 0; i < 2; i++ {
		select {
		case msg := <-sub1.Messages():
			msg.Ack(nil)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i+1)
		}
	}
	sub1.Close(ctx)
	ct.Close(ctx)

	// Publish message 3 before restarting
	signal2 := channel.New(channel.WithBufferSize(100))
	ct2, _ := New(store, signal2,
		WithPollInterval(50*time.Millisecond),
		WithBufferSize(10),
		WithCheckpointStore(cpStore),
	)
	ct2.RegisterEvent(ctx, "orders")
	publishAndWait(t, ctx, ct2, "orders", 3)

	// Subscribe with same stable consumer ID - should resume from checkpoint
	sub2, _ := ct2.Subscribe(ctx, "orders",
		transport.WithConsumerID("order-processor-1"),
	)
	defer sub2.Close(ctx)
	defer ct2.Close(ctx)

	// Should get message 3 (not 1 or 2, since those were checkpointed)
	select {
	case msg := <-sub2.Messages():
		if msg.ID() != "msg-3" {
			t.Fatalf("expected msg-3 after checkpoint resume, got %s", msg.ID())
		}
		msg.Ack(nil)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message after checkpoint resume")
	}
}

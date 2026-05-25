package persistent

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel/trace"
)

// testMessage creates a test message with sensible defaults
func testMessage(payload []byte) transport.Message {
	return transport.NewMessage(
		transport.NewID(),
		"test-source",
		payload,
		nil,
		trace.SpanContext{},
	)
}

// errProcessingFailed is a test error for nack scenarios
var errProcessingFailed = errors.New("processing failed")

func TestNewTransport(t *testing.T) {
	t.Parallel()
	t.Run("requires store", func(t *testing.T) {
		_, err := New(nil)
		if err != ErrStoreRequired {
			t.Errorf("expected ErrStoreRequired, got %v", err)
		}
	})

	t.Run("creates with store", func(t *testing.T) {
		store := NewMemoryStore()
		tr, err := New(store)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tr == nil {
			t.Fatal("expected transport, got nil")
		}
		defer tr.Close(context.Background())
	})
}

func TestRegisterEvent(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store)
	defer tr.Close(context.Background())

	ctx := context.Background()

	t.Run("registers event", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("rejects duplicate", func(t *testing.T) {
		err := tr.RegisterEvent(ctx, "test-event")
		if err != transport.ErrEventAlreadyExists {
			t.Errorf("expected ErrEventAlreadyExists, got %v", err)
		}
	})
}

func TestPublishSubscribe(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store, WithPollInterval(10*time.Millisecond))
	defer tr.Close(context.Background())

	ctx := context.Background()
	eventName := "test-event"

	err := tr.RegisterEvent(ctx, eventName)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	// Subscribe first
	sub, err := tr.Subscribe(ctx, eventName)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish a message
	msg := testMessage([]byte(`{"test": "data"}`))
	err = tr.Publish(ctx, eventName, msg)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Receive the message
	select {
	case received := <-sub.Messages():
		if received.ID() != msg.ID() {
			t.Errorf("expected msg ID %s, got %s", msg.ID(), received.ID())
		}
		// Acknowledge
		if err := received.Ack(nil); err != nil {
			t.Errorf("ack error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestMessageRedelivery(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store, WithPollInterval(10*time.Millisecond))
	defer tr.Close(context.Background())

	ctx := context.Background()
	eventName := "test-event"

	err := tr.RegisterEvent(ctx, eventName)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	sub, err := tr.Subscribe(ctx, eventName)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish a message
	msg := testMessage([]byte(`{"test": "data"}`))
	err = tr.Publish(ctx, eventName, msg)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	// Receive and nack
	select {
	case received := <-sub.Messages():
		if received.RetryCount() != 0 {
			t.Errorf("expected retry count 0, got %d", received.RetryCount())
		}
		// Nack for redelivery
		if err := received.Ack(errProcessingFailed); err != nil {
			t.Errorf("nack error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for first message")
	}

	// Receive redelivered message
	select {
	case received := <-sub.Messages():
		if received.RetryCount() != 1 {
			t.Errorf("expected retry count 1, got %d", received.RetryCount())
		}
		// Acknowledge this time
		if err := received.Ack(nil); err != nil {
			t.Errorf("ack error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for redelivered message")
	}
}

func TestCheckpointPersistence(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	checkpointStore := NewMemoryCheckpointStore()
	tr, _ := New(store,
		WithPollInterval(10*time.Millisecond),
		WithCheckpointStore(checkpointStore),
	)
	defer tr.Close(context.Background())

	ctx := context.Background()
	eventName := "test-event"

	err := tr.RegisterEvent(ctx, eventName)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	// First subscription
	sub1, err := tr.Subscribe(ctx, eventName)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Publish and acknowledge
	msg := testMessage([]byte(`{"test": "data"}`))
	err = tr.Publish(ctx, eventName, msg)
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	select {
	case received := <-sub1.Messages():
		if err := received.Ack(nil); err != nil {
			t.Errorf("ack error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Close first subscription
	sub1.Close(ctx)

	// Verify checkpoint was saved
	checkpoint, err := checkpointStore.Load(ctx, eventName, sub1.ID())
	if err != nil {
		t.Fatalf("load checkpoint error: %v", err)
	}
	if checkpoint == "" {
		t.Error("expected checkpoint to be saved")
	}
}

func TestSequentialProcessing(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store, WithPollInterval(10*time.Millisecond))
	defer tr.Close(context.Background())

	ctx := context.Background()
	eventName := "test-event"

	err := tr.RegisterEvent(ctx, eventName)
	if err != nil {
		t.Fatalf("register error: %v", err)
	}

	sub, err := tr.Subscribe(ctx, eventName)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	defer sub.Close(ctx)

	// Publish multiple messages
	msgCount := 5
	for i := 0; i < msgCount; i++ {
		msg := testMessage([]byte(`{"seq": ` + string(rune('0'+i)) + `}`))
		if err := tr.Publish(ctx, eventName, msg); err != nil {
			t.Fatalf("publish error: %v", err)
		}
	}

	// Verify all messages are received
	var received int32

	for i := 0; i < msgCount; i++ {
		select {
		case msg := <-sub.Messages():
			atomic.AddInt32(&received, 1)
			_ = msg.Ack(nil)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	if atomic.LoadInt32(&received) != int32(msgCount) {
		t.Errorf("expected %d messages, got %d", msgCount, received)
	}
}

func TestHealthCheck(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store)
	defer tr.Close(context.Background())

	ctx := context.Background()

	// Register some events
	tr.RegisterEvent(ctx, "event1")
	tr.RegisterEvent(ctx, "event2")

	health := tr.Health(ctx)
	if health.Status != transport.HealthStatusHealthy {
		t.Errorf("expected healthy status, got %v", health.Status)
	}

	eventCount, ok := health.Details["events"].(int)
	if !ok || eventCount != 2 {
		t.Errorf("expected 2 events, got %v", health.Details["events"])
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	tr, _ := New(store)

	ctx := context.Background()
	eventName := "test-event"

	tr.RegisterEvent(ctx, eventName)

	// Close transport
	err := tr.Close(ctx)
	if err != nil {
		t.Errorf("close error: %v", err)
	}

	// Operations should fail after close
	err = tr.RegisterEvent(ctx, "another-event")
	if err != transport.ErrTransportClosed {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}

	_, err = tr.Subscribe(ctx, eventName)
	if err != transport.ErrTransportClosed {
		t.Errorf("expected ErrTransportClosed, got %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("append and fetch", func(t *testing.T) {
		store := NewMemoryStore()
		eventName := "test-event"
		data := []byte("test data")

		seqID, err := store.Append(ctx, eventName, data)
		if err != nil {
			t.Fatalf("append error: %v", err)
		}
		if seqID == "" {
			t.Error("expected non-empty sequence ID")
		}

		msg, err := store.Fetch(ctx, eventName, "")
		if err != nil {
			t.Fatalf("fetch error: %v", err)
		}
		if msg == nil {
			t.Fatal("expected message, got nil")
		}
		if string(msg.Data) != string(data) {
			t.Errorf("expected data %s, got %s", data, msg.Data)
		}
	})

	t.Run("ack advances checkpoint", func(t *testing.T) {
		store := NewMemoryStore()
		eventName := "test-event"
		data := []byte("test data")

		seqID, _ := store.Append(ctx, eventName, data)

		// Fetch marks message as in-flight
		msg, _ := store.Fetch(ctx, eventName, "")
		if msg == nil {
			t.Fatal("expected message")
		}
		if msg.SequenceID != seqID {
			t.Errorf("expected seqID %s, got %s", seqID, msg.SequenceID)
		}

		// Ack it
		err := store.Ack(ctx, eventName, msg.SequenceID)
		if err != nil {
			t.Fatalf("ack error: %v", err)
		}

		// Fetch should return nil (message acked)
		msg2, _ := store.Fetch(ctx, eventName, "")
		if msg2 != nil {
			t.Error("expected nil after ack")
		}
	})

	t.Run("nack enables redelivery", func(t *testing.T) {
		store := NewMemoryStore()
		eventName := "test-event"
		data := []byte("redelivery test")

		seqID, _ := store.Append(ctx, eventName, data)

		// Fetch marks message as in-flight
		msg, _ := store.Fetch(ctx, eventName, "")
		if msg == nil {
			t.Fatal("expected message")
		}

		// Nack to enable redelivery (also increments retry count)
		err := store.Nack(ctx, eventName, seqID)
		if err != nil {
			t.Fatalf("nack error: %v", err)
		}

		// Fetch should return the message again (no longer in-flight)
		msg2, _ := store.Fetch(ctx, eventName, "")
		if msg2 == nil {
			t.Fatal("expected message after nack")
		}
		if msg2.RetryCount != 1 {
			t.Errorf("expected retry count 1, got %d", msg2.RetryCount)
		}
	})

	t.Run("in-flight prevents duplicate fetch", func(t *testing.T) {
		store := NewMemoryStore()
		eventName := "test-event"

		store.Append(ctx, eventName, []byte("msg1"))
		store.Append(ctx, eventName, []byte("msg2"))

		// First fetch gets msg1 (marks in-flight)
		msg1, _ := store.Fetch(ctx, eventName, "")
		if msg1 == nil {
			t.Fatal("expected first message")
		}

		// Second fetch should get msg2 (msg1 still in-flight)
		msg2, _ := store.Fetch(ctx, eventName, "")
		if msg2 == nil {
			t.Fatal("expected second message")
		}
		if msg2.SequenceID == msg1.SequenceID {
			t.Error("expected different message, got same one")
		}
	})
}

func TestMemoryCheckpointStore(t *testing.T) {
	t.Parallel()
	store := NewMemoryCheckpointStore()
	ctx := context.Background()

	eventName := "test-event"
	consumerID := "consumer-1"
	checkpoint := "seq-123"

	// Save checkpoint
	err := store.Save(ctx, eventName, consumerID, checkpoint)
	if err != nil {
		t.Fatalf("save error: %v", err)
	}

	// Load checkpoint
	loaded, err := store.Load(ctx, eventName, consumerID)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if loaded != checkpoint {
		t.Errorf("expected checkpoint %s, got %s", checkpoint, loaded)
	}

	// Load non-existent returns empty
	loaded2, err := store.Load(ctx, "other-event", "other-consumer")
	if err != nil {
		t.Fatalf("load error: %v", err)
	}
	if loaded2 != "" {
		t.Errorf("expected empty checkpoint, got %s", loaded2)
	}
}

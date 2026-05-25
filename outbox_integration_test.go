package event

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// mockOutboxStore is a mock implementation of OutboxStore for testing
type mockOutboxStore struct {
	mu       sync.Mutex
	messages []mockOutboxMessage
}

type mockOutboxMessage struct {
	eventName string
	eventID   string
	payload   []byte
	metadata  map[string]string
}

func (m *mockOutboxStore) Store(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, mockOutboxMessage{
		eventName: eventName,
		eventID:   eventID,
		payload:   payload,
		metadata:  metadata,
	})
	return nil
}

func (m *mockOutboxStore) Messages() []mockOutboxMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]mockOutboxMessage{}, m.messages...)
}

func TestOutboxIntegration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Create mock outbox store
	outbox := &mockOutboxStore{}

	// Create bus with outbox
	bus := mustNewBus(t, faker.Word(), WithTransport(channel.New()), WithOutbox(outbox))
	defer bus.Close(context.Background())

	// Create and register event
	orderEvent := New[string]("order.created")
	if err := Register(ctx, bus, orderEvent); err != nil {
		t.Fatal(err)
	}

	t.Run("publish without transaction goes to transport", func(t *testing.T) {
		// Track received via subscriber
		received := make(chan string, 1)
		err := orderEvent.Subscribe(ctx, func(ctx context.Context, ev Event[string], data string) error {
			received <- data
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		// Publish without transaction context
		err = orderEvent.Publish(ctx, "order-123")
		if err != nil {
			t.Fatal(err)
		}

		// Should receive via transport
		select {
		case data := <-received:
			if data != "order-123" {
				t.Errorf("expected order-123, got %s", data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("expected to receive message via transport")
		}

		// Outbox should be empty
		if len(outbox.Messages()) != 0 {
			t.Errorf("expected no messages in outbox, got %d", len(outbox.Messages()))
		}
	})

	t.Run("publish inside transaction goes to outbox", func(t *testing.T) {
		// Create transaction context
		txCtx := WithOutboxTx(ctx, "mock-session")

		// Publish inside transaction
		err := orderEvent.Publish(txCtx, "order-456")
		if err != nil {
			t.Fatal(err)
		}

		// Message should be in outbox
		messages := outbox.Messages()
		if len(messages) != 1 {
			t.Fatalf("expected 1 message in outbox, got %d", len(messages))
		}

		msg := messages[0]
		if msg.eventName != "order.created" {
			t.Errorf("expected event name order.created, got %s", msg.eventName)
		}
		if !strings.Contains(string(msg.payload), "order-456") {
			t.Errorf("expected payload to contain order-456, got %s", string(msg.payload))
		}
	})
}

func TestOutboxContextHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("InOutboxTx returns false without transaction", func(t *testing.T) {
		if InOutboxTx(ctx) {
			t.Error("expected InOutboxTx to return false")
		}
	})

	t.Run("InOutboxTx returns true with transaction", func(t *testing.T) {
		txCtx := WithOutboxTx(ctx, "mock-session")
		if !InOutboxTx(txCtx) {
			t.Error("expected InOutboxTx to return true")
		}
	})

	t.Run("OutboxTx returns session", func(t *testing.T) {
		session := "mock-session"
		txCtx := WithOutboxTx(ctx, session)
		got := OutboxTx(txCtx)
		if got != session {
			t.Errorf("expected session %v, got %v", session, got)
		}
	})

	t.Run("OutboxTx returns nil without transaction", func(t *testing.T) {
		got := OutboxTx(ctx)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})
}

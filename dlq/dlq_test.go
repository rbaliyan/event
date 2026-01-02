package dlq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Store and Get", func(t *testing.T) {
		store := NewMemoryStore()

		msg := &Message{
			ID:         "dlq-1",
			EventName:  "order.created",
			OriginalID: "msg-123",
			Payload:    []byte(`{"id":"order-1"}`),
			Metadata:   map[string]string{"key": "value"},
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  time.Now(),
			Source:     "order-service",
		}

		err := store.Store(ctx, msg)
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}

		retrieved, err := store.Get(ctx, "dlq-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.ID != "dlq-1" {
			t.Errorf("expected ID dlq-1, got %s", retrieved.ID)
		}
		if retrieved.EventName != "order.created" {
			t.Errorf("expected event order.created, got %s", retrieved.EventName)
		}
		if retrieved.OriginalID != "msg-123" {
			t.Errorf("expected original ID msg-123, got %s", retrieved.OriginalID)
		}
		if string(retrieved.Payload) != `{"id":"order-1"}` {
			t.Errorf("unexpected payload: %s", retrieved.Payload)
		}
		if retrieved.Metadata["key"] != "value" {
			t.Error("expected metadata key=value")
		}
	})

	t.Run("Get non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()

		_, err := store.Get(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent message")
		}
	})

	t.Run("List with empty filter returns all", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event-1", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event-2", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "event-3", CreatedAt: time.Now()})

		messages, err := store.List(ctx, Filter{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(messages))
		}
	})

	t.Run("List with EventName filter", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "order.created", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "order.updated", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "order.created", CreatedAt: time.Now()})

		messages, err := store.List(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(messages))
		}
	})

	t.Run("List with time filter", func(t *testing.T) {
		store := NewMemoryStore()

		now := time.Now()
		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: now.Add(-2 * time.Hour)})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event", CreatedAt: now.Add(-1 * time.Hour)})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "event", CreatedAt: now})

		messages, err := store.List(ctx, Filter{StartTime: now.Add(-90 * time.Minute)})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(messages))
		}
	})

	t.Run("List with ExcludeRetried filter", func(t *testing.T) {
		store := NewMemoryStore()

		retriedAt := time.Now()
		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event", CreatedAt: time.Now(), RetriedAt: &retriedAt})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "event", CreatedAt: time.Now()})

		messages, err := store.List(ctx, Filter{ExcludeRetried: true})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(messages))
		}
	})

	t.Run("List with Limit and Offset", func(t *testing.T) {
		store := NewMemoryStore()

		for i := 0; i < 10; i++ {
			store.Store(ctx, &Message{ID: "dlq-" + string(rune('0'+i)), EventName: "event", CreatedAt: time.Now()})
		}

		messages, err := store.List(ctx, Filter{Limit: 3, Offset: 0})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(messages))
		}
	})

	t.Run("Count", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "order.created", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "order.updated", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "order.created", CreatedAt: time.Now()})

		count, err := store.Count(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}
	})

	t.Run("MarkRetried", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: time.Now()})

		err := store.MarkRetried(ctx, "dlq-1")
		if err != nil {
			t.Fatalf("MarkRetried failed: %v", err)
		}

		msg, _ := store.Get(ctx, "dlq-1")
		if msg.RetriedAt == nil {
			t.Error("expected RetriedAt to be set")
		}
	})

	t.Run("MarkRetried non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()

		err := store.MarkRetried(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent message")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: time.Now()})

		err := store.Delete(ctx, "dlq-1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = store.Get(ctx, "dlq-1")
		if err == nil {
			t.Error("expected error after delete")
		}
	})

	t.Run("Delete non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()

		err := store.Delete(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent message")
		}
	})

	t.Run("DeleteOlderThan", func(t *testing.T) {
		store := NewMemoryStore()

		now := time.Now()
		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: now.Add(-2 * time.Hour)})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event", CreatedAt: now.Add(-1 * time.Hour)})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "event", CreatedAt: now})

		deleted, err := store.DeleteOlderThan(ctx, 90*time.Minute)
		if err != nil {
			t.Fatalf("DeleteOlderThan failed: %v", err)
		}

		if deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", deleted)
		}

		count, _ := store.Count(ctx, Filter{})
		if count != 2 {
			t.Errorf("expected 2 remaining, got %d", count)
		}
	})

	t.Run("DeleteByFilter", func(t *testing.T) {
		store := NewMemoryStore()

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "order.created", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "order.updated", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "order.created", CreatedAt: time.Now()})

		deleted, err := store.DeleteByFilter(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("DeleteByFilter failed: %v", err)
		}

		if deleted != 2 {
			t.Errorf("expected 2 deleted, got %d", deleted)
		}
	})

	t.Run("Stats", func(t *testing.T) {
		store := NewMemoryStore()

		retriedAt := time.Now()
		store.Store(ctx, &Message{ID: "dlq-1", EventName: "order.created", Error: "connection: timeout", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "order.updated", Error: "validation: failed", CreatedAt: time.Now(), RetriedAt: &retriedAt})
		store.Store(ctx, &Message{ID: "dlq-3", EventName: "order.created", Error: "connection: refused", CreatedAt: time.Now()})

		stats, err := store.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}

		if stats.TotalMessages != 3 {
			t.Errorf("expected 3 total, got %d", stats.TotalMessages)
		}
		if stats.PendingMessages != 2 {
			t.Errorf("expected 2 pending, got %d", stats.PendingMessages)
		}
		if stats.RetriedMessages != 1 {
			t.Errorf("expected 1 retried, got %d", stats.RetriedMessages)
		}
		if stats.MessagesByEvent["order.created"] != 2 {
			t.Errorf("expected 2 order.created, got %d", stats.MessagesByEvent["order.created"])
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		store := NewMemoryStore()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(3)

			go func(id int) {
				defer wg.Done()
				store.Store(ctx, &Message{
					ID:        "dlq-concurrent",
					EventName: "event",
					CreatedAt: time.Now(),
				})
			}(i)

			go func() {
				defer wg.Done()
				store.List(ctx, Filter{})
			}()

			go func() {
				defer wg.Done()
				store.Count(ctx, Filter{})
			}()
		}

		wg.Wait()
	})
}

// mockTransport is a mock transport for testing
type mockTransport struct {
	published []transport.Message
	mu        sync.Mutex
	failOn    string
}

func (m *mockTransport) Publish(ctx context.Context, event string, msg transport.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failOn != "" && event == m.failOn {
		return errors.New("publish failed")
	}

	m.published = append(m.published, msg)
	return nil
}

func (m *mockTransport) Subscribe(ctx context.Context, event string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, nil
}

func (m *mockTransport) RegisterEvent(ctx context.Context, event string) error {
	return nil
}

func (m *mockTransport) UnregisterEvent(ctx context.Context, event string) error {
	return nil
}

func (m *mockTransport) Close(ctx context.Context) error {
	return nil
}

func TestManager(t *testing.T) {
	ctx := context.Background()

	t.Run("Store creates DLQ message", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		err := manager.Store(ctx, "order.created", "msg-123", []byte(`{"id":"order-1"}`),
			map[string]string{"key": "value"}, errors.New("processing failed"), 3, "order-service")

		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}

		count, _ := store.Count(ctx, Filter{})
		if count != 1 {
			t.Errorf("expected 1 message in store, got %d", count)
		}

		messages, _ := store.List(ctx, Filter{})
		if messages[0].EventName != "order.created" {
			t.Errorf("expected event order.created, got %s", messages[0].EventName)
		}
		if messages[0].OriginalID != "msg-123" {
			t.Errorf("expected original ID msg-123, got %s", messages[0].OriginalID)
		}
	})

	t.Run("Get retrieves message", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		manager.Store(ctx, "event", "msg-1", []byte("data"), nil, errors.New("error"), 1, "source")

		messages, _ := store.List(ctx, Filter{})
		msg, err := manager.Get(ctx, messages[0].ID)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if msg.OriginalID != "msg-1" {
			t.Errorf("expected original ID msg-1, got %s", msg.OriginalID)
		}
	})

	t.Run("List returns messages", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		manager.Store(ctx, "event-1", "msg-1", nil, nil, errors.New("error"), 1, "source")
		manager.Store(ctx, "event-2", "msg-2", nil, nil, errors.New("error"), 1, "source")

		messages, err := manager.List(ctx, Filter{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(messages) != 2 {
			t.Errorf("expected 2 messages, got %d", len(messages))
		}
	})

	t.Run("Count returns count", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		manager.Store(ctx, "event", "msg-1", nil, nil, errors.New("error"), 1, "source")
		manager.Store(ctx, "event", "msg-2", nil, nil, errors.New("error"), 1, "source")

		count, err := manager.Count(ctx, Filter{})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}
	})

	t.Run("Replay republishes messages", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		store.Store(ctx, &Message{
			ID:         "dlq-1",
			EventName:  "order.created",
			OriginalID: "msg-123",
			Payload:    []byte(`{"id":"order-1"}`),
			Metadata:   map[string]string{"key": "value"},
			CreatedAt:  time.Now(),
		})

		replayed, err := manager.Replay(ctx, Filter{})
		if err != nil {
			t.Fatalf("Replay failed: %v", err)
		}

		if replayed != 1 {
			t.Errorf("expected 1 replayed, got %d", replayed)
		}

		if len(tr.published) != 1 {
			t.Errorf("expected 1 published message, got %d", len(tr.published))
		}

		// Verify message is marked as retried
		msg, _ := store.Get(ctx, "dlq-1")
		if msg.RetriedAt == nil {
			t.Error("expected RetriedAt to be set")
		}
	})

	t.Run("ReplaySingle replays one message", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		store.Store(ctx, &Message{
			ID:         "dlq-1",
			EventName:  "order.created",
			OriginalID: "msg-123",
			CreatedAt:  time.Now(),
		})

		err := manager.ReplaySingle(ctx, "dlq-1")
		if err != nil {
			t.Fatalf("ReplaySingle failed: %v", err)
		}

		if len(tr.published) != 1 {
			t.Errorf("expected 1 published message, got %d", len(tr.published))
		}
	})

	t.Run("Delete removes message", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: time.Now()})

		err := manager.Delete(ctx, "dlq-1")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		count, _ := store.Count(ctx, Filter{})
		if count != 0 {
			t.Errorf("expected 0 messages, got %d", count)
		}
	})

	t.Run("DeleteByFilter removes matching messages", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "order.created", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "order.updated", CreatedAt: time.Now()})

		deleted, err := manager.DeleteByFilter(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("DeleteByFilter failed: %v", err)
		}

		if deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", deleted)
		}
	})

	t.Run("Cleanup removes old messages", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		now := time.Now()
		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: now.Add(-2 * time.Hour)})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event", CreatedAt: now})

		deleted, err := manager.Cleanup(ctx, 90*time.Minute)
		if err != nil {
			t.Fatalf("Cleanup failed: %v", err)
		}

		if deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", deleted)
		}
	})

	t.Run("Stats returns statistics", func(t *testing.T) {
		store := NewMemoryStore()
		tr := &mockTransport{}
		manager := NewManager(store, tr)

		store.Store(ctx, &Message{ID: "dlq-1", EventName: "event", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-2", EventName: "event", CreatedAt: time.Now()})

		stats, err := manager.Stats(ctx)
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}

		if stats.TotalMessages != 2 {
			t.Errorf("expected 2 total, got %d", stats.TotalMessages)
		}
	})
}

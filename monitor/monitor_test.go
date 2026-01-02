package monitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDeliveryMode(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		if Broadcast.String() != "broadcast" {
			t.Errorf("expected broadcast, got %s", Broadcast.String())
		}
		if WorkerPool.String() != "worker_pool" {
			t.Errorf("expected worker_pool, got %s", WorkerPool.String())
		}
		if DeliveryMode(99).String() != "unknown" {
			t.Errorf("expected unknown, got %s", DeliveryMode(99).String())
		}
	})

	t.Run("ParseDeliveryMode", func(t *testing.T) {
		if ParseDeliveryMode("worker_pool") != WorkerPool {
			t.Error("expected WorkerPool")
		}
		if ParseDeliveryMode("broadcast") != Broadcast {
			t.Error("expected Broadcast")
		}
		if ParseDeliveryMode("unknown") != Broadcast {
			t.Error("expected Broadcast for unknown value")
		}
	})
}

func TestEntry(t *testing.T) {
	t.Run("IsComplete", func(t *testing.T) {
		entry := &Entry{Status: StatusPending}
		if entry.IsComplete() {
			t.Error("pending should not be complete")
		}

		entry.Status = StatusCompleted
		if !entry.IsComplete() {
			t.Error("completed should be complete")
		}

		entry.Status = StatusFailed
		if !entry.IsComplete() {
			t.Error("failed should be complete")
		}

		entry.Status = StatusRetrying
		if entry.IsComplete() {
			t.Error("retrying should not be complete")
		}
	})

	t.Run("HasError", func(t *testing.T) {
		entry := &Entry{}
		if entry.HasError() {
			t.Error("empty error should return false")
		}

		entry.Error = "some error"
		if !entry.HasError() {
			t.Error("non-empty error should return true")
		}
	})
}

func TestFilter(t *testing.T) {
	t.Run("EffectiveLimit default", func(t *testing.T) {
		filter := Filter{}
		if filter.EffectiveLimit() != DefaultLimit {
			t.Errorf("expected %d, got %d", DefaultLimit, filter.EffectiveLimit())
		}
	})

	t.Run("EffectiveLimit respects max", func(t *testing.T) {
		filter := Filter{Limit: MaxLimit + 100}
		if filter.EffectiveLimit() != MaxLimit {
			t.Errorf("expected %d, got %d", MaxLimit, filter.EffectiveLimit())
		}
	})

	t.Run("EffectiveLimit uses custom value", func(t *testing.T) {
		filter := Filter{Limit: 50}
		if filter.EffectiveLimit() != 50 {
			t.Errorf("expected 50, got %d", filter.EffectiveLimit())
		}
	})
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Record and Get", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		entry := &Entry{
			EventID:        "event-1",
			SubscriptionID: "sub-1",
			EventName:      "order.created",
			BusID:          "bus-1",
			DeliveryMode:   Broadcast,
			Metadata:       map[string]string{"key": "value"},
			Status:         StatusPending,
			StartedAt:      time.Now(),
		}

		err := store.Record(ctx, entry)
		if err != nil {
			t.Fatalf("Record failed: %v", err)
		}

		retrieved, err := store.Get(ctx, "event-1", "sub-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if retrieved == nil {
			t.Fatal("expected entry, got nil")
		}

		if retrieved.EventID != "event-1" {
			t.Errorf("expected event-1, got %s", retrieved.EventID)
		}
		if retrieved.Metadata["key"] != "value" {
			t.Error("expected metadata key=value")
		}
	})

	t.Run("Get non-existent returns nil", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		entry, err := store.Get(ctx, "non-existent", "sub")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if entry != nil {
			t.Error("expected nil for non-existent entry")
		}
	})

	t.Run("WorkerPool key is eventID only", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		entry := &Entry{
			EventID:        "event-1",
			SubscriptionID: "sub-1",
			DeliveryMode:   WorkerPool,
			Status:         StatusPending,
			StartedAt:      time.Now(),
		}

		store.Record(ctx, entry)

		// Should be able to get with eventID only
		retrieved, _ := store.Get(ctx, "event-1", "")
		if retrieved == nil {
			t.Fatal("expected entry for WorkerPool")
		}
	})

	t.Run("GetByEventID returns all entries", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "event-1", SubscriptionID: "sub-1", DeliveryMode: Broadcast, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "event-1", SubscriptionID: "sub-2", DeliveryMode: Broadcast, StartedAt: now.Add(time.Second)})
		store.Record(ctx, &Entry{EventID: "event-2", SubscriptionID: "sub-1", DeliveryMode: Broadcast, StartedAt: now})

		entries, err := store.GetByEventID(ctx, "event-1")
		if err != nil {
			t.Fatalf("GetByEventID failed: %v", err)
		}

		if len(entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(entries))
		}
	})

	t.Run("List with empty filter", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "e2", SubscriptionID: "s2", DeliveryMode: Broadcast, StartedAt: now.Add(time.Second)})

		page, err := store.List(ctx, Filter{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(page.Entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(page.Entries))
		}
	})

	t.Run("List with filters", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", EventName: "order.created", DeliveryMode: Broadcast, Status: StatusCompleted, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "e2", SubscriptionID: "s2", EventName: "order.updated", DeliveryMode: Broadcast, Status: StatusFailed, Error: "error", StartedAt: now.Add(time.Second)})
		store.Record(ctx, &Entry{EventID: "e3", SubscriptionID: "s3", EventName: "order.created", DeliveryMode: Broadcast, Status: StatusCompleted, StartedAt: now.Add(2 * time.Second)})

		// Filter by event name
		page, _ := store.List(ctx, Filter{EventName: "order.created"})
		if len(page.Entries) != 2 {
			t.Errorf("expected 2 entries for order.created, got %d", len(page.Entries))
		}

		// Filter by status
		page, _ = store.List(ctx, Filter{Status: []Status{StatusFailed}})
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 failed entry, got %d", len(page.Entries))
		}

		// Filter by has error
		hasError := true
		page, _ = store.List(ctx, Filter{HasError: &hasError})
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 entry with error, got %d", len(page.Entries))
		}
	})

	t.Run("List with pagination", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		for i := 0; i < 5; i++ {
			store.Record(ctx, &Entry{
				EventID:        "e" + string(rune('0'+i)),
				SubscriptionID: "s",
				DeliveryMode:   Broadcast,
				StartedAt:      now.Add(time.Duration(i) * time.Second),
			})
		}

		// First page
		page, _ := store.List(ctx, Filter{Limit: 2})
		if len(page.Entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(page.Entries))
		}
		if !page.HasMore {
			t.Error("expected HasMore=true")
		}
		if page.NextCursor == "" {
			t.Error("expected NextCursor")
		}

		// Second page
		page2, _ := store.List(ctx, Filter{Limit: 2, Cursor: page.NextCursor})
		if len(page2.Entries) != 2 {
			t.Errorf("expected 2 entries, got %d", len(page2.Entries))
		}
	})

	t.Run("Count", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", EventName: "order.created", DeliveryMode: Broadcast, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "e2", SubscriptionID: "s2", EventName: "order.updated", DeliveryMode: Broadcast, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "e3", SubscriptionID: "s3", EventName: "order.created", DeliveryMode: Broadcast, StartedAt: now})

		count, err := store.Count(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2, got %d", count)
		}
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, Status: StatusPending, StartedAt: now})

		err := store.UpdateStatus(ctx, "e1", "s1", StatusCompleted, nil, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("UpdateStatus failed: %v", err)
		}

		entry, _ := store.Get(ctx, "e1", "s1")
		if entry.Status != StatusCompleted {
			t.Errorf("expected completed, got %s", entry.Status)
		}
		if entry.Duration != 100*time.Millisecond {
			t.Errorf("expected 100ms, got %v", entry.Duration)
		}
		if entry.CompletedAt == nil {
			t.Error("expected CompletedAt to be set")
		}
	})

	t.Run("UpdateStatus with error", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, Status: StatusPending, StartedAt: now})

		err := store.UpdateStatus(ctx, "e1", "s1", StatusFailed, errors.New("processing error"), 50*time.Millisecond)
		if err != nil {
			t.Fatalf("UpdateStatus failed: %v", err)
		}

		entry, _ := store.Get(ctx, "e1", "s1")
		if entry.Status != StatusFailed {
			t.Errorf("expected failed, got %s", entry.Status)
		}
		if entry.Error != "processing error" {
			t.Errorf("expected processing error, got %s", entry.Error)
		}
	})

	t.Run("UpdateStatus non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		err := store.UpdateStatus(ctx, "non-existent", "sub", StatusCompleted, nil, 0)
		if err == nil {
			t.Error("expected error for non-existent entry")
		}
	})

	t.Run("DeleteOlderThan", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, StartedAt: now.Add(-2 * time.Hour)})
		store.Record(ctx, &Entry{EventID: "e2", SubscriptionID: "s2", DeliveryMode: Broadcast, StartedAt: now.Add(-1 * time.Hour)})
		store.Record(ctx, &Entry{EventID: "e3", SubscriptionID: "s3", DeliveryMode: Broadcast, StartedAt: now})

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

	t.Run("RecordStart and RecordComplete", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		err := store.RecordStart(ctx, "event-1", "sub-1", "order.created", "bus-1", false, map[string]string{"key": "value"}, "trace-1", "span-1")
		if err != nil {
			t.Fatalf("RecordStart failed: %v", err)
		}

		entry, _ := store.Get(ctx, "event-1", "sub-1")
		if entry.Status != StatusPending {
			t.Errorf("expected pending, got %s", entry.Status)
		}
		if entry.TraceID != "trace-1" {
			t.Errorf("expected trace-1, got %s", entry.TraceID)
		}

		err = store.RecordComplete(ctx, "event-1", "sub-1", string(StatusCompleted), nil, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("RecordComplete failed: %v", err)
		}

		entry, _ = store.Get(ctx, "event-1", "sub-1")
		if entry.Status != StatusCompleted {
			t.Errorf("expected completed, got %s", entry.Status)
		}
	})

	t.Run("Close makes operations fail", func(t *testing.T) {
		store := NewMemoryStore()

		store.Close()

		err := store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, StartedAt: time.Now()})
		if err == nil {
			t.Error("expected error after close")
		}

		_, err = store.Get(ctx, "e1", "s1")
		if err == nil {
			t.Error("expected error after close")
		}
	})

	t.Run("Len returns entry count", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		if store.Len() != 0 {
			t.Errorf("expected 0, got %d", store.Len())
		}

		now := time.Now()
		store.Record(ctx, &Entry{EventID: "e1", SubscriptionID: "s1", DeliveryMode: Broadcast, StartedAt: now})
		store.Record(ctx, &Entry{EventID: "e2", SubscriptionID: "s2", DeliveryMode: Broadcast, StartedAt: now})

		if store.Len() != 2 {
			t.Errorf("expected 2, got %d", store.Len())
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(4)

			go func(id int) {
				defer wg.Done()
				store.Record(ctx, &Entry{
					EventID:        "event-concurrent",
					SubscriptionID: "sub",
					DeliveryMode:   Broadcast,
					StartedAt:      time.Now(),
				})
			}(i)

			go func() {
				defer wg.Done()
				store.Get(ctx, "event-concurrent", "sub")
			}()

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

func TestMakeKey(t *testing.T) {
	t.Run("Broadcast uses composite key", func(t *testing.T) {
		key := makeKey("event-1", "sub-1", Broadcast)
		if key != "event-1:sub-1" {
			t.Errorf("expected event-1:sub-1, got %s", key)
		}
	})

	t.Run("WorkerPool uses eventID only", func(t *testing.T) {
		key := makeKey("event-1", "sub-1", WorkerPool)
		if key != "event-1" {
			t.Errorf("expected event-1, got %s", key)
		}
	})
}

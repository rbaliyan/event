package monitor

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSummary(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("empty store", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		summary, err := store.Summary(ctx, Filter{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if summary.TotalEntries != 0 {
			t.Errorf("expected 0 entries, got %d", summary.TotalEntries)
		}
		if len(summary.ByStatus) != 0 {
			t.Errorf("expected empty ByStatus, got %v", summary.ByStatus)
		}
		if summary.ErrorRate != 0 {
			t.Errorf("expected 0 error rate, got %f", summary.ErrorRate)
		}
	})

	t.Run("aggregates correctly", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		entries := []*Entry{
			{EventID: "1", EventName: "orders.created", BusID: "bus", Status: StatusCompleted, Duration: 100 * time.Millisecond, StartedAt: now.Add(-3 * time.Hour), InstanceID: "pod-1", DeliveryMode: Broadcast},
			{EventID: "2", EventName: "orders.created", BusID: "bus", Status: StatusFailed, Duration: 200 * time.Millisecond, StartedAt: now.Add(-2 * time.Hour), InstanceID: "pod-1", DeliveryMode: Broadcast},
			{EventID: "3", EventName: "orders.updated", BusID: "bus", Status: StatusCompleted, Duration: 50 * time.Millisecond, StartedAt: now.Add(-1 * time.Hour), InstanceID: "pod-2", DeliveryMode: Broadcast},
			{EventID: "4", EventName: "orders.created", BusID: "bus", Status: StatusRetrying, Duration: 300 * time.Millisecond, StartedAt: now, InstanceID: "pod-2", DeliveryMode: Broadcast},
			{EventID: "5", EventName: "orders.created", BusID: "bus", Status: StatusPending, StartedAt: now.Add(time.Second), InstanceID: "pod-1", DeliveryMode: Broadcast},
		}
		for _, e := range entries {
			if err := store.Record(ctx, e); err != nil {
				t.Fatalf("Record: %v", err)
			}
		}

		summary, err := store.Summary(ctx, Filter{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if summary.TotalEntries != 5 {
			t.Errorf("expected 5 entries, got %d", summary.TotalEntries)
		}

		// Status counts
		if summary.ByStatus[StatusCompleted] != 2 {
			t.Errorf("expected 2 completed, got %d", summary.ByStatus[StatusCompleted])
		}
		if summary.ByStatus[StatusFailed] != 1 {
			t.Errorf("expected 1 failed, got %d", summary.ByStatus[StatusFailed])
		}
		if summary.ByStatus[StatusRetrying] != 1 {
			t.Errorf("expected 1 retrying, got %d", summary.ByStatus[StatusRetrying])
		}
		if summary.ByStatus[StatusPending] != 1 {
			t.Errorf("expected 1 pending, got %d", summary.ByStatus[StatusPending])
		}

		// Per-event stats
		ordersCreated := summary.ByEventName["orders.created"]
		if ordersCreated == nil {
			t.Fatal("expected orders.created stats")
		}
		if ordersCreated.Total != 4 {
			t.Errorf("expected 4 orders.created total, got %d", ordersCreated.Total)
		}
		if ordersCreated.Failed != 1 {
			t.Errorf("expected 1 orders.created failed, got %d", ordersCreated.Failed)
		}
		if ordersCreated.ErrorRate != 0.25 {
			t.Errorf("expected 0.25 error rate, got %f", ordersCreated.ErrorRate)
		}

		ordersUpdated := summary.ByEventName["orders.updated"]
		if ordersUpdated == nil {
			t.Fatal("expected orders.updated stats")
		}
		if ordersUpdated.Total != 1 {
			t.Errorf("expected 1 orders.updated total, got %d", ordersUpdated.Total)
		}

		// Instance counts
		if summary.ByInstance["pod-1"] != 3 {
			t.Errorf("expected 3 for pod-1, got %d", summary.ByInstance["pod-1"])
		}
		if summary.ByInstance["pod-2"] != 2 {
			t.Errorf("expected 2 for pod-2, got %d", summary.ByInstance["pod-2"])
		}

		// Error rate
		if summary.ErrorRate != 0.2 {
			t.Errorf("expected 0.2 error rate, got %f", summary.ErrorRate)
		}

		// Time range
		if summary.TimeRange.Oldest == nil || !summary.TimeRange.Oldest.Equal(now.Add(-3*time.Hour)) {
			t.Errorf("unexpected oldest: %v", summary.TimeRange.Oldest)
		}
		if summary.TimeRange.Newest == nil || !summary.TimeRange.Newest.Equal(now.Add(time.Second)) {
			t.Errorf("unexpected newest: %v", summary.TimeRange.Newest)
		}

		// Avg duration in ms (only 4 entries have duration > 0: 100+200+50+300 = 650/4 = 162ms)
		if summary.AvgDurationMs != 162 {
			t.Errorf("expected 162 avg duration ms, got %d", summary.AvgDurationMs)
		}
	})

	t.Run("respects filter", func(t *testing.T) {
		store := NewMemoryStore()
		defer store.Close()

		entries := []*Entry{
			{EventID: "1", EventName: "orders.created", BusID: "bus", Status: StatusCompleted, StartedAt: now, DeliveryMode: Broadcast},
			{EventID: "2", EventName: "orders.updated", BusID: "bus", Status: StatusFailed, StartedAt: now, DeliveryMode: Broadcast},
		}
		for _, e := range entries {
			store.Record(ctx, e)
		}

		summary, err := store.Summary(ctx, Filter{EventName: "orders.created"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if summary.TotalEntries != 1 {
			t.Errorf("expected 1 entry, got %d", summary.TotalEntries)
		}
		if summary.ErrorRate != 0 {
			t.Errorf("expected 0 error rate, got %f", summary.ErrorRate)
		}
	})

	t.Run("closed store returns error", func(t *testing.T) {
		store := NewMemoryStore()
		store.Close()

		_, err := store.Summary(ctx, Filter{})
		if err == nil {
			t.Error("expected error for closed store")
		}
	})

	t.Run("implements SummaryProvider", func(t *testing.T) {
		store := NewMemoryStore()
		var _ SummaryProvider = store
	})
}

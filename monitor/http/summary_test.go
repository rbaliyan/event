package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
)

func TestHandlerSummary(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(0))

	// Seed some entries
	now := time.Now()
	entries := []*monitor.Entry{
		{EventID: "1", EventName: "orders.created", BusID: "bus", Status: monitor.StatusCompleted, Duration: 100 * time.Millisecond, StartedAt: now.Add(-1 * time.Hour), DeliveryMode: monitor.Broadcast, InstanceID: "pod-1"},
		{EventID: "2", EventName: "orders.created", BusID: "bus", Status: monitor.StatusFailed, Duration: 200 * time.Millisecond, StartedAt: now.Add(-30 * time.Minute), DeliveryMode: monitor.Broadcast, InstanceID: "pod-2"},
		{EventID: "3", EventName: "orders.updated", BusID: "bus", Status: monitor.StatusCompleted, Duration: 50 * time.Millisecond, StartedAt: now, DeliveryMode: monitor.Broadcast, InstanceID: "pod-1"},
	}
	for _, e := range entries {
		if err := store.Record(ctx, e); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	t.Run("GET returns summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/summary", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var summary monitor.Summary
		if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if summary.TotalEntries != 3 {
			t.Errorf("expected 3 entries, got %d", summary.TotalEntries)
		}
		if summary.ByStatus[monitor.StatusCompleted] != 2 {
			t.Errorf("expected 2 completed, got %d", summary.ByStatus[monitor.StatusCompleted])
		}
		if summary.ByStatus[monitor.StatusFailed] != 1 {
			t.Errorf("expected 1 failed, got %d", summary.ByStatus[monitor.StatusFailed])
		}
		if len(summary.ByEventName) != 2 {
			t.Errorf("expected 2 event names, got %d", len(summary.ByEventName))
		}
		if summary.ByInstance["pod-1"] != 2 {
			t.Errorf("expected 2 for pod-1, got %d", summary.ByInstance["pod-1"])
		}

		// Verify avg_duration_ms is in milliseconds (not nanoseconds)
		// Entries: 100ms + 200ms + 50ms = 350ms / 3 = 116ms
		var raw map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
			t.Fatalf("unmarshal raw: %v", err)
		}
		avgMs, ok := raw["avg_duration_ms"].(float64)
		if !ok {
			t.Fatal("avg_duration_ms not a number in JSON")
		}
		if avgMs < 1 || avgMs > 1000 {
			t.Errorf("expected avg_duration_ms in reasonable millisecond range, got %f", avgMs)
		}
	})

	t.Run("GET with filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/summary?event_name=orders.created", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var summary monitor.Summary
		if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if summary.TotalEntries != 2 {
			t.Errorf("expected 2 entries, got %d", summary.TotalEntries)
		}
	})

	t.Run("POST not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/monitor/summary", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

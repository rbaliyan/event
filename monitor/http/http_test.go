package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	pb "github.com/rbaliyan/event/v3/monitor/proto"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestHandlerNew(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)
	if h == nil {
		t.Fatal("expected handler, got nil")
	}
}


func TestHandlerList(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	// Add some entries
	now := time.Now()
	for i := 0; i < 5; i++ {
		store.Record(ctx, &monitor.Entry{
			EventID:      "event-" + string(rune('1'+i)),
			EventName:    "orders.created",
			BusID:        "bus-1",
			DeliveryMode: monitor.Broadcast,
			Status:       monitor.StatusCompleted,
			StartedAt:    now.Add(time.Duration(i) * time.Second),
		})
	}

	t.Run("GET /v1/monitor/entries lists all entries", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.ListResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if len(resp.Entries) != 5 {
			t.Errorf("expected 5 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("GET /v1/monitor/entries with query filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?event_name=orders.created", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.ListResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if len(resp.Entries) != 5 {
			t.Errorf("expected 5 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("GET /v1/monitor/entries with limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?limit=3", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.ListResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if len(resp.Entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(resp.Entries))
		}
		if !resp.HasMore {
			t.Error("expected has_more to be true")
		}
	})

	t.Run("POST /v1/monitor/entries returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/monitor/entries", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestHandlerGetEntry(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	// Add an entry
	store.Record(ctx, &monitor.Entry{
		EventID:        "event-1",
		SubscriptionID: "sub-1",
		EventName:      "orders.created",
		BusID:          "bus-1",
		DeliveryMode:   monitor.Broadcast,
		Status:         monitor.StatusCompleted,
		StartedAt:      time.Now(),
	})

	t.Run("GET /v1/monitor/entries/{event_id}/{subscription_id}", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries/event-1/sub-1", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.GetResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Entry == nil {
			t.Fatal("expected entry, got nil")
		}
		if resp.Entry.EventId != "event-1" {
			t.Errorf("expected event-1, got %s", resp.Entry.EventId)
		}
	})

	t.Run("GET /v1/monitor/entries/{event_id}/{subscription_id} not found returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries/non-existent/sub-1", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", w.Code)
		}
	})
}

func TestHandlerGetByEventID(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	// Add entries for same event ID
	now := time.Now()
	for i := 0; i < 3; i++ {
		store.Record(ctx, &monitor.Entry{
			EventID:        "event-1",
			SubscriptionID: "sub-" + string(rune('1'+i)),
			EventName:      "orders.created",
			BusID:          "bus-1",
			DeliveryMode:   monitor.Broadcast,
			Status:         monitor.StatusCompleted,
			StartedAt:      now.Add(time.Duration(i) * time.Millisecond),
		})
	}

	t.Run("GET /v1/monitor/entries/{event_id}", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries/event-1", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.GetByEventIDResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if len(resp.Entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("POST /v1/monitor/entries/{event_id} returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/monitor/entries/event-1", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestHandlerCount(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	// Add entries
	now := time.Now()
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-1",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusCompleted,
		StartedAt:    now,
	})
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-2",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusFailed,
		Error:        "some error",
		StartedAt:    now,
	})

	t.Run("GET /v1/monitor/entries/count", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries/count", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.CountResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Count != 2 {
			t.Errorf("expected 2, got %d", resp.Count)
		}
	})

	t.Run("GET /v1/monitor/entries/count with status filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries/count?status=failed", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.CountResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Count != 1 {
			t.Errorf("expected 1, got %d", resp.Count)
		}
	})

	t.Run("POST /v1/monitor/entries/count returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/monitor/entries/count", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestHandlerDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("DELETE with default 24h", func(t *testing.T) {
		store := monitor.NewMemoryStore()
		defer store.Close()
		h := New(store)

		now := time.Now()
		store.Record(ctx, &monitor.Entry{
			EventID:      "old-event",
			EventName:    "orders.created",
			DeliveryMode: monitor.WorkerPool,
			Status:       monitor.StatusCompleted,
			StartedAt:    now.Add(-25 * time.Hour),
		})
		store.Record(ctx, &monitor.Entry{
			EventID:      "new-event",
			EventName:    "orders.created",
			DeliveryMode: monitor.WorkerPool,
			Status:       monitor.StatusCompleted,
			StartedAt:    now,
		})

		// DELETE without older_than uses default 24h
		req := httptest.NewRequest(http.MethodDelete, "/v1/monitor/entries", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.DeleteOlderThanResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", resp.Deleted)
		}
	})

	t.Run("DELETE newer than 24h without force returns 400", func(t *testing.T) {
		store := monitor.NewMemoryStore()
		defer store.Close()
		h := New(store)

		req := httptest.NewRequest(http.MethodDelete, "/v1/monitor/entries?older_than=1h", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("DELETE newer than 24h with force=true succeeds", func(t *testing.T) {
		store := monitor.NewMemoryStore()
		defer store.Close()
		h := New(store)

		now := time.Now()
		store.Record(ctx, &monitor.Entry{
			EventID:      "old-event",
			EventName:    "orders.created",
			DeliveryMode: monitor.WorkerPool,
			Status:       monitor.StatusCompleted,
			StartedAt:    now.Add(-2 * time.Hour),
		})

		req := httptest.NewRequest(http.MethodDelete, "/v1/monitor/entries?older_than=1h&force=true", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp pb.DeleteOlderThanResponse
		if err := protojson.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", resp.Deleted)
		}
	})

	t.Run("DELETE with invalid duration returns 400", func(t *testing.T) {
		store := monitor.NewMemoryStore()
		defer store.Close()
		h := New(store)

		req := httptest.NewRequest(http.MethodDelete, "/v1/monitor/entries?older_than=invalid", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", w.Code)
		}
	})
}


func TestHandlerQueryParams(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	// Add entries with various attributes
	now := time.Now()
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-1",
		EventName:    "orders.created",
		BusID:        "bus-1",
		DeliveryMode: monitor.Broadcast,
		Status:       monitor.StatusCompleted,
		StartedAt:    now,
		Duration:     100 * time.Millisecond,
	})
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-2",
		EventName:    "orders.created",
		BusID:        "bus-1",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusFailed,
		Error:        "failed",
		StartedAt:    now,
		RetryCount:   3,
	})

	t.Run("filter by status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?status=failed", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp pb.ListResponse
		protojson.Unmarshal(w.Body.Bytes(), &resp)
		if len(resp.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(resp.Entries))
		}
	})

	t.Run("filter by has_error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?has_error=true", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp pb.ListResponse
		protojson.Unmarshal(w.Body.Bytes(), &resp)
		if len(resp.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(resp.Entries))
		}
	})

	t.Run("filter by delivery_mode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?delivery_mode=worker_pool", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}

		var resp pb.ListResponse
		protojson.Unmarshal(w.Body.Bytes(), &resp)
		if len(resp.Entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(resp.Entries))
		}
	})

	t.Run("order descending", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries?order_desc=true", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
	})
}

func TestHandlerContentType(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store)

	req := httptest.NewRequest(http.MethodGet, "/v1/monitor/entries", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected application/json, got %s", contentType)
	}
}

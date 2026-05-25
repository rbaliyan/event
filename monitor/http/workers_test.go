package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/distributed"
	"github.com/rbaliyan/event/v3/monitor"
)

func newWorkerTestHandler(t *testing.T) (*Handler, *distributed.MemoryStateManager) {
	t.Helper()
	store := monitor.NewMemoryStore()
	t.Cleanup(func() { store.Close() })

	sm := distributed.NewMemoryStateManager(distributed.WithCleanup(false, 0))
	t.Cleanup(sm.Close)

	h := New(store, WithWorkerStore(sm), WithSystemRefreshInterval(0))
	return h, sm
}

func TestWorkerEndpointsDisabledWithoutStore(t *testing.T) {
	t.Parallel()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(0)) // No WithWorkerStore

	req := httptest.NewRequest(http.MethodGet, "/v1/workers", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// Without worker store, the route is not registered and the default mux 404 applies
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestWorkerList(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	// Populate some worker entries via Acquire
	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.Acquire(ctx, "msg-3", 5*time.Minute)

	// Mark one as completed
	sm.MarkProcessed(ctx, "msg-2")

	t.Run("GET /v1/workers lists all entries", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workers", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var page distributed.WorkerPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if len(page.Entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(page.Entries))
		}
	})

	t.Run("POST /v1/workers returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/workers", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestWorkerListFilterByStatus(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.MarkProcessed(ctx, "msg-2")

	req := httptest.NewRequest(http.MethodGet, "/v1/workers?status=processing", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var page distributed.WorkerPage
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Errorf("expected 1 processing entry, got %d", len(page.Entries))
	}
	if len(page.Entries) > 0 && page.Entries[0].Status != distributed.WorkerStateProcessing {
		t.Errorf("expected processing status, got %s", page.Entries[0].Status)
	}
}

func TestWorkerListStaleTimeout(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	// Acquire with very short TTL so updatedAt is in the past
	sm.Acquire(ctx, "msg-stale", 5*time.Minute)

	// Sleep briefly to ensure stale detection
	time.Sleep(10 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/v1/workers?stale_timeout=5ms", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var page distributed.WorkerPage
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Errorf("expected 1 stale entry, got %d", len(page.Entries))
	}
}

func TestWorkerGetByID(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-42", 5*time.Minute)

	t.Run("GET /v1/workers/msg-42 returns entry", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workers/msg-42", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var entry distributed.WorkerEntry
		if err := json.Unmarshal(w.Body.Bytes(), &entry); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if entry.MessageID != "msg-42" {
			t.Errorf("expected msg-42, got %s", entry.MessageID)
		}
		if entry.Status != distributed.WorkerStateProcessing {
			t.Errorf("expected processing, got %s", entry.Status)
		}
	})

	t.Run("GET /v1/workers/non-existent returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workers/non-existent", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", w.Code)
		}
	})
}

func TestWorkerCount(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.Acquire(ctx, "msg-3", 5*time.Minute)
	sm.MarkProcessed(ctx, "msg-3")

	t.Run("GET /v1/workers/count returns total", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workers/count", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp map[string]int64
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if resp["count"] != 3 {
			t.Errorf("expected 3, got %d", resp["count"])
		}
	})

	t.Run("GET /v1/workers/count?status=completed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/workers/count?status=completed", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp map[string]int64
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if resp["count"] != 1 {
			t.Errorf("expected 1, got %d", resp["count"])
		}
	})

	t.Run("POST /v1/workers/count returns 405", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/workers/count", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestWorkerPagination(t *testing.T) {
	t.Parallel()
	h, sm := newWorkerTestHandler(t)
	ctx := context.Background()

	// Create entries with slight time gaps for deterministic ordering
	for i := 0; i < 5; i++ {
		sm.Acquire(ctx, "msg-"+string(rune('a'+i)), 5*time.Minute)
		time.Sleep(time.Millisecond) // ensure different updatedAt
	}

	// First page: limit=2
	req := httptest.NewRequest(http.MethodGet, "/v1/workers?limit=2", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var page1 distributed.WorkerPage
	if err := json.Unmarshal(w.Body.Bytes(), &page1); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(page1.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(page1.Entries))
	}
	if !page1.HasMore {
		t.Fatal("expected has_more to be true")
	}
	if page1.NextCursor == "" {
		t.Fatal("expected non-empty next_cursor")
	}

	// Second page using cursor
	req = httptest.NewRequest(http.MethodGet, "/v1/workers?limit=2&cursor="+page1.NextCursor, nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var page2 distributed.WorkerPage
	if err := json.Unmarshal(w.Body.Bytes(), &page2); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(page2.Entries) != 2 {
		t.Fatalf("expected 2 entries on page 2, got %d", len(page2.Entries))
	}
	if !page2.HasMore {
		t.Fatal("expected has_more to be true on page 2")
	}

	// Verify no overlap
	for _, e1 := range page1.Entries {
		for _, e2 := range page2.Entries {
			if e1.MessageID == e2.MessageID {
				t.Errorf("entry %s appears on both pages", e1.MessageID)
			}
		}
	}
}

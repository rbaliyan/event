package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/monitor"
)

// waitForCache polls until the handler's background refresh goroutine has
// populated the systemView cache, then returns. This replaces the previous
// fixed time.Sleep waits that were sized as "100-150ms should be enough"
// against a 50ms refresh interval — worst-case bounds that wasted time on
// the happy path and could still flake on a loaded CI runner.
func waitForCache(t *testing.T, h *Handler) {
	t.Helper()
	testutil.Eventually(t, 2*time.Second, func() bool {
		return h.systemView.Load() != nil
	}, "background refresh did not populate systemView cache")
}

// mockDLQProvider implements DLQProvider for testing.
type mockDLQProvider struct {
	stats  *DLQStats
	health *health.Result
}

func (m *mockDLQProvider) DLQStats(ctx context.Context) (*DLQStats, error) {
	return m.stats, nil
}

func (m *mockDLQProvider) Health(ctx context.Context) *health.Result {
	return m.health
}

func TestHandleSystemView(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	// Seed entries for summary
	now := time.Now()
	store.Record(ctx, &monitor.Entry{
		EventID: "1", EventName: "orders.created", BusID: "bus",
		Status: monitor.StatusCompleted, Duration: 100 * time.Millisecond,
		StartedAt: now, DeliveryMode: monitor.Broadcast,
	})

	t.Run("basic system view without providers", func(t *testing.T) {
		h := New(store, WithSystemRefreshInterval(50*time.Millisecond))
		defer h.Close()
		waitForCache(t, h)
		req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var view SystemView
		if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if view.Health == nil {
			t.Error("expected health to be present")
		}
		// Summary should be populated since MemoryStore implements SummaryProvider
		if view.Summary == nil {
			t.Error("expected summary to be present")
		}
		if view.Summary != nil && view.Summary.TotalEntries != 1 {
			t.Errorf("expected 1 entry in summary, got %d", view.Summary.TotalEntries)
		}
	})

	t.Run("system view with DLQ provider", func(t *testing.T) {
		dlq := &mockDLQProvider{
			stats: &DLQStats{
				TotalMessages:   42,
				PendingMessages: 5,
			},
			health: &health.Result{
				Status:    health.StatusDegraded,
				Message:   "5 messages pending in DLQ",
				CheckedAt: now,
			},
		}

		h := New(store, WithDLQProvider(dlq), WithSystemRefreshInterval(50*time.Millisecond))
		defer h.Close()
		waitForCache(t, h)
		req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var view SystemView
		if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if view.DLQ == nil {
			t.Fatal("expected DLQ stats")
		}
		if view.DLQ.TotalMessages != 42 {
			t.Errorf("expected 42 total messages, got %d", view.DLQ.TotalMessages)
		}
		if view.Health.Status != health.StatusDegraded {
			t.Errorf("expected degraded health, got %s", view.Health.Status)
		}
		if view.Health.Components["dlq"] == nil {
			t.Error("expected dlq component in health")
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		h := New(store, WithSystemRefreshInterval(0))
		req := httptest.NewRequest(http.MethodPost, "/v1/system", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})
}

func TestHandleSystemHealth(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	t.Run("healthy when no providers", func(t *testing.T) {
		h := New(store, WithSystemRefreshInterval(50*time.Millisecond))
		defer h.Close()
		waitForCache(t, h)
		req := httptest.NewRequest(http.MethodGet, "/v1/system/health", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var result health.AggregateResult
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if result.Status != health.StatusHealthy {
			t.Errorf("expected healthy, got %s", result.Status)
		}
	})

	t.Run("unhealthy returns 503", func(t *testing.T) {
		dlq := &mockDLQProvider{
			stats: &DLQStats{},
			health: &health.Result{
				Status:    health.StatusUnhealthy,
				Message:   "store connectivity failed",
				CheckedAt: time.Now(),
			},
		}

		h := New(store, WithDLQProvider(dlq), WithSystemRefreshInterval(50*time.Millisecond))
		defer h.Close()
		waitForCache(t, h)
		req := httptest.NewRequest(http.MethodGet, "/v1/system/health", nil)
		w := httptest.NewRecorder()

		h.ServeHTTP(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 503, got %d", w.Code)
		}

		var result health.AggregateResult
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if result.Status != health.StatusUnhealthy {
			t.Errorf("expected unhealthy, got %s", result.Status)
		}
	})
}

func TestBackgroundRefresh(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	store.Record(ctx, &monitor.Entry{
		EventID: "1", EventName: "orders.created", BusID: "bus",
		Status: monitor.StatusCompleted, Duration: 50 * time.Millisecond,
		StartedAt: time.Now(), DeliveryMode: monitor.Broadcast,
	})

	h := New(store, WithSystemRefreshInterval(50*time.Millisecond))
	defer h.Close()

	// Wait for first refresh
	waitForCache(t, h)

	// Verify cache is populated
	cached := h.systemView.Load()
	if cached == nil {
		t.Fatal("expected cached system view after refresh")
	}
	if cached.CollectedAt.IsZero() {
		t.Error("expected non-zero CollectedAt")
	}
	if cached.Health == nil {
		t.Error("expected health in cached view")
	}

	// Verify API returns cached data instantly
	req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var view SystemView
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if view.CollectedAt.IsZero() {
		t.Error("expected CollectedAt in response")
	}
}

func TestBackgroundRefresh_Health(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(50*time.Millisecond))
	defer h.Close()

	// Wait for first refresh
	waitForCache(t, h)

	req := httptest.NewRequest(http.MethodGet, "/v1/system/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result health.AggregateResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result.Status != health.StatusHealthy {
		t.Errorf("expected healthy, got %s", result.Status)
	}
}

func TestClose(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(50*time.Millisecond))

	// Wait for at least one refresh
	waitForCache(t, h)

	// Close should be idempotent
	h.Close()
	h.Close()

	// After close, cache remains readable but stale
	req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 after close, got %d", w.Code)
	}
}

func TestDisabledRefresh(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	// No goroutine started, cache should be nil
	if h.systemView.Load() != nil {
		t.Error("expected nil cache when refresh disabled")
	}

	// Should return 503 when disabled
	req := httptest.NewRequest(http.MethodGet, "/v1/system", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when refresh disabled, got %d", w.Code)
	}

	// Health endpoint should also return 503
	req = httptest.NewRequest(http.MethodGet, "/v1/system/health", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for health when refresh disabled, got %d", w.Code)
	}
}

func TestCachedTopology(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(50*time.Millisecond))
	defer h.Close()

	// Wait for refresh
	waitForCache(t, h)

	req := httptest.NewRequest(http.MethodGet, "/v1/topology", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

// mockStuckPendingProvider is a test double for StuckPendingStatsProvider.
type mockStuckPendingProvider struct {
	stats *StuckPendingStats
	err   error
}

func (m *mockStuckPendingProvider) StuckPendingStats(_ context.Context) (*StuckPendingStats, error) {
	return m.stats, m.err
}

func TestHandleSystemView_StuckPending(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	oldest := time.Now().Add(-10 * time.Minute)
	provider := &mockStuckPendingProvider{
		stats: &StuckPendingStats{
			Count:     3,
			Threshold: 6 * time.Minute,
			OldestAt:  &oldest,
		},
	}

	h := New(store,
		WithSystemRefreshInterval(50*time.Millisecond),
		WithStuckPendingProvider(provider),
	)
	defer h.Close()
	waitForCache(t, h)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/system", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}

	var view SystemView
	if err := json.NewDecoder(w.Body).Decode(&view); err != nil {
		t.Fatal(err)
	}
	if view.StuckPending == nil {
		t.Fatal("stuck_pending field is nil, want non-nil")
	}
	if view.StuckPending.Count != 3 {
		t.Errorf("count: want 3, got %d", view.StuckPending.Count)
	}
	if view.StuckPending.OldestAt == nil {
		t.Error("oldest_at: want non-nil")
	}
}

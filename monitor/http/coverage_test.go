package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/monitor"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// setupCoverageBus registers a bus with one broadcast and one worker-pool
// subscription, returns the live topology entry for the event, and a cleanup func.
func setupCoverageBus(t *testing.T, busName, evName string) (event.EventInfo, func()) {
	t.Helper()
	tr := channel.New()
	bus, err := event.NewBus(busName, event.WithTransport(tr))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ev := event.New[string](evName)
	if err := event.Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}
	noop := func(_ context.Context, _ event.Event[string], _ string) error { return nil }
	if err := ev.Subscribe(ctx, noop); err != nil {
		t.Fatal(err)
	}
	if err := ev.Subscribe(ctx, noop, event.WithWorkerGroup[string]("testgroup")); err != nil {
		t.Fatal(err)
	}

	// Find the topology entry for this event.
	var evInfo event.EventInfo
	for _, bi := range event.Topology() {
		if bi.Name == busName {
			for _, ei := range bi.Events {
				if ei.Name == evName {
					evInfo = ei
				}
			}
		}
	}

	cleanup := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = bus.Close(closeCtx)
	}
	return evInfo, cleanup
}

func TestHandleCoverage_BroadcastMatch(t *testing.T) {
	t.Parallel()
	busName := "cov-broadcast-" + t.Name()
	evName := "coverage.broadcast"
	ctx := context.Background()

	evInfo, cleanup := setupCoverageBus(t, busName, evName)
	defer cleanup()

	// Find the broadcast subscription ID from topology.
	var broadcastSubID string
	for _, sub := range evInfo.Subscriptions {
		if sub.DeliveryMode == transport.Broadcast {
			broadcastSubID = sub.SubscriptionID
			break
		}
	}
	if broadcastSubID == "" {
		t.Fatal("no broadcast subscription found in topology")
	}

	store := monitor.NewMemoryStore()
	defer store.Close()

	eventID := "evt-broadcast-001"
	_ = store.Record(ctx, &monitor.Entry{
		EventID:        eventID,
		SubscriptionID: broadcastSubID,
		EventName:      evName,
		BusID:          busName,
		DeliveryMode:   monitor.Broadcast,
		Status:         monitor.StatusCompleted,
		StartedAt:      time.Now(),
	})

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/monitor/coverage/"+eventID, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}
	var resp EventCoverageResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.EventID != eventID {
		t.Errorf("event_id: want %s, got %s", eventID, resp.EventID)
	}
	found := false
	for _, c := range resp.Coverage {
		if c.SubscriptionID == broadcastSubID {
			if !c.HasEntry {
				t.Errorf("broadcast sub %s: HasEntry=false", broadcastSubID)
			}
			found = true
		}
	}
	if !found {
		t.Errorf("broadcast sub %s not present in coverage", broadcastSubID)
	}
}

func TestHandleCoverage_WorkerPoolGroupMatch(t *testing.T) {
	t.Parallel()
	busName := "cov-wp-" + t.Name()
	evName := "coverage.workergroup"
	ctx := context.Background()

	_, cleanup := setupCoverageBus(t, busName, evName)
	defer cleanup()

	store := monitor.NewMemoryStore()
	defer store.Close()

	eventID := "evt-wp-001"
	// WorkerPool entries use empty subscription_id; coverage matches by worker_group.
	_ = store.Record(ctx, &monitor.Entry{
		EventID:      eventID,
		EventName:    evName,
		BusID:        busName,
		WorkerGroup:  "testgroup",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusCompleted,
		StartedAt:    time.Now(),
	})

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/monitor/coverage/"+eventID, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}
	var resp EventCoverageResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	for _, c := range resp.Coverage {
		if c.WorkerGroup == "testgroup" {
			if !c.HasEntry {
				t.Errorf("worker_group testgroup: HasEntry=false")
			}
			return
		}
	}
	t.Error("worker_group testgroup not found in coverage response")
}

func TestHandleCoverage_MissingCount(t *testing.T) {
	t.Parallel()
	busName := "cov-missing-" + t.Name()
	evName := "coverage.missing"
	ctx := context.Background()

	_, cleanup := setupCoverageBus(t, busName, evName)
	defer cleanup()

	store := monitor.NewMemoryStore()
	defer store.Close()

	eventID := "evt-missing-001"
	// Seed with a phantom sub ID that exists in the store but not in topology.
	_ = store.Record(ctx, &monitor.Entry{
		EventID:        eventID,
		SubscriptionID: "phantom-sub-id",
		EventName:      evName,
		BusID:          busName,
		DeliveryMode:   monitor.Broadcast,
		Status:         monitor.StatusCompleted,
		StartedAt:      time.Now(),
	})

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/monitor/coverage/"+eventID, nil))

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}
	var resp EventCoverageResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.MissingCount == 0 {
		t.Errorf("want missing_count > 0, got 0 (coverage: %+v)", resp.Coverage)
	}
}

func TestHandleCoverage_EmptyEventID(t *testing.T) {
	t.Parallel()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/monitor/coverage/", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

func TestHandleCoverage_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	store := monitor.NewMemoryStore()
	defer store.Close()

	h := New(store, WithSystemRefreshInterval(0))
	defer h.Close()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/monitor/coverage/evt-001", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

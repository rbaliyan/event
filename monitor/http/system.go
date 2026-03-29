package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/monitor"
)

// SystemView is the unified response for GET /v1/system.
type SystemView struct {
	Topology    []event.BusInfo         `json:"topology"`
	DLQ         *DLQStats               `json:"dlq,omitempty"`
	Scheduler   *SchedulerStats         `json:"scheduler,omitempty"`
	Health      *health.AggregateResult `json:"health"`
	BusHealth   map[string]*event.Status `json:"bus_health,omitempty"`
	ConsumerLag []event.ConsumerLag      `json:"consumer_lag,omitempty"`
	Summary     *monitor.Summary         `json:"summary,omitempty"`
	CollectedAt time.Time                `json:"collected_at"`
}

// runSystemRefresh periodically collects the system view in the background.
func (h *Handler) runSystemRefresh(ctx context.Context, interval time.Duration) {
	defer close(h.done)

	// Collect immediately on start
	h.refreshSystemView(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.refreshSystemView(ctx)
		}
	}
}

// refreshSystemView collects all system data concurrently and stores the result.
func (h *Handler) refreshSystemView(ctx context.Context) {
	// Use a timeout so a slow component doesn't block the refresh cycle
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	topology := event.Topology()
	view := SystemView{
		Topology:    topology,
		CollectedAt: time.Now(),
	}

	components := make(map[string]*health.Result)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// DLQ stats + health
	if h.dlqProvider != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats, err := h.dlqProvider.DLQStats(ctx)
			healthResult := h.dlqProvider.Health(ctx)
			mu.Lock()
			if err == nil {
				view.DLQ = stats
				// Check DLQ alert threshold
				if h.dlqAlertFunc != nil && stats.PendingMessages >= h.dlqAlertThreshold {
					h.dlqAlertFunc(stats)
				}
			} else {
				slog.WarnContext(ctx, "dlq stats query failed", "error", err)
			}
			components["dlq"] = healthResult
			mu.Unlock()
		}()
	}

	// Scheduler stats + health
	if h.schedProvider != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats, err := h.schedProvider.SchedulerStats(ctx)
			healthResult := h.schedProvider.Health(ctx)
			mu.Lock()
			if err == nil {
				view.Scheduler = stats
			} else {
				slog.WarnContext(ctx, "scheduler stats query failed", "error", err)
			}
			components["scheduler"] = healthResult
			mu.Unlock()
		}()
	}

	// Bus health + consumer lag (per-bus concurrent)
	wg.Add(1)
	go func() {
		defer wg.Done()
		busHealth, consumerLag, busComponents := h.collectBusHealthConcurrent(ctx, topology)
		mu.Lock()
		if len(busHealth) > 0 {
			view.BusHealth = busHealth
		}
		if len(consumerLag) > 0 {
			view.ConsumerLag = consumerLag
		}
		for k, v := range busComponents {
			components[k] = v
		}
		mu.Unlock()
	}()

	// Monitor summary
	if sp, ok := h.store.(monitor.SummaryProvider); ok {
		wg.Add(1)
		go func() {
			defer wg.Done()
			filter := monitor.Filter{
				StartTime: time.Now().Add(-24 * time.Hour),
			}
			summary, err := sp.Summary(ctx, filter)
			mu.Lock()
			if err == nil {
				view.Summary = summary
			} else {
				slog.WarnContext(ctx, "summary query failed", "error", err)
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	view.Health = health.Aggregate(components)
	h.systemView.Store(&view)
}

// collectBusHealthConcurrent collects health status and consumer lag from all buses concurrently.
func (h *Handler) collectBusHealthConcurrent(ctx context.Context, topology []event.BusInfo) (map[string]*event.Status, []event.ConsumerLag, map[string]*health.Result) {
	busHealth := make(map[string]*event.Status)
	var consumerLag []event.ConsumerLag
	components := make(map[string]*health.Result)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, info := range topology {
		bus := event.GetBus(info.Name)
		if bus == nil {
			continue
		}
		wg.Add(1)
		go func(name string, bus *event.Bus) {
			defer wg.Done()
			// Per-bus timeout
			busCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			status := bus.Status(busCtx)
			lag, _ := bus.ConsumerLag(busCtx)

			mu.Lock()
			busHealth[name] = status
			components["bus:"+name] = &health.Result{
				Status:    health.Status(status.Code),
				Message:   status.Message,
				Latency:   status.Latency,
				CheckedAt: status.CheckedAt,
			}
			if len(lag) > 0 {
				consumerLag = append(consumerLag, lag...)
			}
			mu.Unlock()
		}(info.Name, bus)
	}

	wg.Wait()
	return busHealth, consumerLag, components
}

// handleSystemView handles GET /v1/system — returns cached system view or computes on demand.
func (h *Handler) handleSystemView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Return cached view if available
	if cached := h.systemView.Load(); cached != nil {
		h.writeJSON(w, cached)
		return
	}

	// Background refresh enabled but first refresh hasn't completed yet
	if h.systemEnabled {
		h.writeError(w, http.StatusServiceUnavailable, "system view is being collected, try again shortly")
		return
	}

	// Background refresh disabled — return error
	h.writeError(w, http.StatusServiceUnavailable, "system view collection is disabled, enable WithSystemRefreshInterval")
}

// handleSystemHealth handles GET /v1/system/health — returns aggregated health status.
// Returns 200 for healthy/degraded, 503 for unhealthy.
func (h *Handler) handleSystemHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Use cached health if available
	if cached := h.systemView.Load(); cached != nil && cached.Health != nil {
		data, err := json.Marshal(cached.Health)
		if err != nil {
			h.writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if cached.Health.Status == health.StatusUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write(data)
		return
	}

	// No cache — return unavailable
	h.writeError(w, http.StatusServiceUnavailable, "health data not available, system view collection may be disabled")
}


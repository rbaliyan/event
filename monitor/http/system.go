package http

import (
	"encoding/json"
	"net/http"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/health"
)

// SystemView is the unified response for GET /v1/system.
type SystemView struct {
	Topology  []event.BusInfo        `json:"topology"`
	DLQ       *DLQStats              `json:"dlq,omitempty"`
	Scheduler *SchedulerStats        `json:"scheduler,omitempty"`
	Health    *health.AggregateResult `json:"health"`
}

// handleSystemView handles GET /v1/system — aggregates topology, DLQ, scheduler, and health.
func (h *Handler) handleSystemView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	view := SystemView{
		Topology: event.Topology(),
	}

	components := make(map[string]*health.Result)

	if h.dlqProvider != nil {
		stats, err := h.dlqProvider.DLQStats(ctx)
		if err == nil {
			view.DLQ = stats
		}
		components["dlq"] = h.dlqProvider.Health(ctx)
	}

	if h.schedProvider != nil {
		stats, err := h.schedProvider.SchedulerStats(ctx)
		if err == nil {
			view.Scheduler = stats
		}
		components["scheduler"] = h.schedProvider.Health(ctx)
	}

	view.Health = health.Aggregate(components)

	h.writeJSON(w, view)
}

// handleSystemHealth handles GET /v1/system/health — returns aggregated health status.
// Returns 200 for healthy/degraded, 503 for unhealthy.
func (h *Handler) handleSystemHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	components := make(map[string]*health.Result)

	if h.dlqProvider != nil {
		components["dlq"] = h.dlqProvider.Health(ctx)
	}
	if h.schedProvider != nil {
		components["scheduler"] = h.schedProvider.Health(ctx)
	}

	result := health.Aggregate(components)

	data, err := json.Marshal(result)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if result.Status == health.StatusUnhealthy {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_, _ = w.Write(data)
}

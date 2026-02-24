package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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
}

// handleSystemView handles GET /v1/system — aggregates topology, DLQ, scheduler, and health.
func (h *Handler) handleSystemView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	topology := event.Topology()
	view := SystemView{
		Topology: topology,
	}

	components := make(map[string]*health.Result)

	if h.dlqProvider != nil {
		stats, err := h.dlqProvider.DLQStats(ctx)
		if err == nil {
			view.DLQ = stats
		} else {
			slog.WarnContext(ctx, "dlq stats query failed", "error", err)
		}
		components["dlq"] = h.dlqProvider.Health(ctx)
	}

	if h.schedProvider != nil {
		stats, err := h.schedProvider.SchedulerStats(ctx)
		if err == nil {
			view.Scheduler = stats
		} else {
			slog.WarnContext(ctx, "scheduler stats query failed", "error", err)
		}
		components["scheduler"] = h.schedProvider.Health(ctx)
	}

	// Collect bus health and consumer lag from registered buses
	busHealth, consumerLag := h.collectBusHealth(ctx, topology, components)
	if len(busHealth) > 0 {
		view.BusHealth = busHealth
	}
	if len(consumerLag) > 0 {
		view.ConsumerLag = consumerLag
	}

	// Include summary if store supports it
	if sp, ok := h.store.(monitor.SummaryProvider); ok {
		filter := monitor.Filter{
			StartTime: time.Now().Add(-24 * time.Hour),
		}
		summary, err := sp.Summary(ctx, filter)
		if err == nil {
			view.Summary = summary
		} else {
			slog.WarnContext(ctx, "summary query failed", "error", err)
		}
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

	// Include bus health in the health check
	h.collectBusHealth(ctx, event.Topology(), components)

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

// collectBusHealth collects health status and consumer lag from all registered buses.
// It populates the components map with per-bus health results for aggregation.
func (h *Handler) collectBusHealth(ctx context.Context, topology []event.BusInfo, components map[string]*health.Result) (map[string]*event.Status, []event.ConsumerLag) {
	busHealth := make(map[string]*event.Status)
	var consumerLag []event.ConsumerLag

	for _, info := range topology {
		bus := event.GetBus(info.Name)
		if bus == nil {
			continue
		}

		status := bus.Status(ctx)
		busHealth[info.Name] = status

		components["bus:"+info.Name] = &health.Result{
			Status:    health.Status(status.Code),
			Message:   status.Message,
			Latency:   status.Latency,
			CheckedAt: status.CheckedAt,
		}

		lag, err := bus.ConsumerLag(ctx)
		if err == nil && len(lag) > 0 {
			consumerLag = append(consumerLag, lag...)
		}
	}

	return busHealth, consumerLag
}

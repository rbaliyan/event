package http

import (
	"net/http"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
)

// handleSummary handles GET /v1/monitor/summary — returns aggregated statistics.
func (h *Handler) handleSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	sp, ok := h.store.(monitor.SummaryProvider)
	if !ok {
		h.writeError(w, http.StatusNotImplemented, "store does not support summary aggregation")
		return
	}

	filter := h.parseFilterFromQuery(r)

	// Default to last 24h if no time range specified
	if filter.StartTime.IsZero() && filter.EndTime.IsZero() {
		filter.StartTime = time.Now().Add(-24 * time.Hour)
	}

	summary, err := sp.Summary(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, summary)
}

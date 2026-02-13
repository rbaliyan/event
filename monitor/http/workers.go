package http

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3/distributed"
)

// handleWorkerList handles GET /v1/workers.
func (h *Handler) handleWorkerList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	filter := parseWorkerFilterFromQuery(r)

	page, err := h.workerStore.ListWorkers(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, page)
}

// handleWorkerCount handles GET /v1/workers/count.
func (h *Handler) handleWorkerCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	filter := parseWorkerFilterFromQuery(r)

	count, err := h.workerStore.CountWorkers(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeJSON(w, map[string]int64{"count": count})
}

// handleWorkerByID handles GET /v1/workers/{message_id} and dispatches /v1/workers/count.
func (h *Handler) handleWorkerByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/workers/")

	// Dispatch /v1/workers/count to the count handler
	if path == "count" {
		h.handleWorkerCount(w, r)
		return
	}

	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if path == "" {
		h.writeError(w, http.StatusBadRequest, "message_id is required")
		return
	}

	entry, err := h.workerStore.GetWorker(r.Context(), path)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entry == nil {
		h.writeError(w, http.StatusNotFound, "worker entry not found")
		return
	}

	h.writeJSON(w, entry)
}

// parseWorkerFilterFromQuery parses a WorkerFilter from URL query parameters.
func parseWorkerFilterFromQuery(r *http.Request) distributed.WorkerFilter {
	q := r.URL.Query()
	filter := distributed.WorkerFilter{}

	if v := q["status"]; len(v) > 0 {
		filter.Status = make([]distributed.WorkerState, len(v))
		for i, s := range v {
			filter.Status[i] = distributed.WorkerState(s)
		}
	}
	if v := q.Get("event_name"); v != "" {
		filter.EventName = v
	}
	if v := q.Get("worker_id"); v != "" {
		filter.WorkerID = v
	}
	if v := q.Get("stale_timeout"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			filter.StaleTimeout = d
		}
	}
	if v := q.Get("created_after"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.CreatedAfter = t
		}
	}
	if v := q.Get("created_before"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.CreatedBefore = t
		}
	}
	if v := q.Get("cursor"); v != "" {
		filter.Cursor = v
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("order_desc"); v != "" {
		filter.OrderDesc = v == "true" || v == "1"
	}

	return filter
}

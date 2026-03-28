package http

import (
	"net/http"
	"strings"

	"github.com/rbaliyan/event/v3"
)

// handleTopology handles GET /v1/topology — returns all bus topologies.
func (h *Handler) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Use cached topology from system view if available
	if cached := h.systemView.Load(); cached != nil {
		h.writeJSON(w, cached.Topology)
		return
	}

	h.writeJSON(w, event.Topology())
}

// handleTopologyWithPath handles GET /v1/topology/{bus_name}.
func (h *Handler) handleTopologyWithPath(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/v1/topology/")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "bus name is required")
		return
	}

	bus := event.GetBus(name)
	if bus == nil {
		h.writeError(w, http.StatusNotFound, "bus not found")
		return
	}

	h.writeJSON(w, bus.Topology())
}

package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/monitor"
	"github.com/rbaliyan/event/v3/transport"
)

// SubscriptionCoverageEntry describes one subscription's delivery status for an event_id.
type SubscriptionCoverageEntry struct {
	SubscriptionID string         `json:"subscription_id"`
	SubscriberName string         `json:"subscriber_name,omitempty"`
	WorkerGroup    string         `json:"worker_group,omitempty"`
	DeliveryMode   string         `json:"delivery_mode"`
	HasEntry       bool           `json:"has_entry"`
	Entry          *monitor.Entry `json:"entry,omitempty"`
}

// EventCoverageResponse is returned by GET /v1/monitor/coverage/{event_id}.
// It cross-references recorded monitor entries with the current in-process
// subscription topology to show which subscriptions processed the event and
// which have no record (either not yet delivered or never published to that group).
//
// Caveat: subscription IDs are regenerated on pod restart. Historical entries
// with old IDs will not match the current topology. TopologyAsOf records when
// the snapshot was taken for context.
type EventCoverageResponse struct {
	EventID      string                      `json:"event_id"`
	TopologyAsOf time.Time                   `json:"topology_as_of"`
	Coverage     []SubscriptionCoverageEntry `json:"coverage"`
	MissingCount int                         `json:"missing_count"`
	PresentCount int                         `json:"present_count"`
}

// handleCoverage handles GET /v1/monitor/coverage/{event_id}.
func (h *Handler) handleCoverage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	eventID := strings.TrimPrefix(r.URL.Path, "/v1/monitor/coverage/")
	if eventID == "" {
		h.writeError(w, http.StatusBadRequest, "event_id is required")
		return
	}

	entries, err := h.store.GetByEventID(r.Context(), eventID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Index recorded entries for O(1) lookup in the topology walk below.
	entryBySubID := make(map[string]*monitor.Entry, len(entries))
	entryByGroup := make(map[string]*monitor.Entry)
	eventNames := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		entryBySubID[e.SubscriptionID] = e
		if e.DeliveryMode == monitor.WorkerPool {
			entryByGroup[e.WorkerGroup] = e
		}
		eventNames[e.EventName] = struct{}{}
	}

	now := time.Now()
	topology := event.Topology()

	// Walk topology to find subscriptions for the event names seen in entries.
	// For WorkerPool mode, the monitor records an empty subscription_id so we
	// match by worker_group across all WorkerPool entries for the same event.
	var coverage []SubscriptionCoverageEntry
	seen := make(map[string]struct{}) // deduplicate by subscription_id

	for _, busInfo := range topology {
		for _, evInfo := range busInfo.Events {
			if _, ok := eventNames[evInfo.Name]; !ok {
				continue
			}
			for _, sub := range evInfo.Subscriptions {
				if _, already := seen[sub.SubscriptionID]; already {
					continue
				}
				seen[sub.SubscriptionID] = struct{}{}

				// monitor.DeliveryMode and transport.DeliveryMode share the same
				// underlying int constants; cast to use monitor's String() method.
				ce := SubscriptionCoverageEntry{
					SubscriptionID: sub.SubscriptionID,
					SubscriberName: sub.SubscriberName,
					WorkerGroup:    sub.WorkerGroup,
					DeliveryMode:   monitor.DeliveryMode(sub.DeliveryMode).String(),
				}

				if e, ok := entryBySubID[sub.SubscriptionID]; ok {
					ce.HasEntry = true
					ce.Entry = e
				} else if sub.DeliveryMode == transport.WorkerPool {
					// WorkerPool entries store empty subscription_id — match by worker_group.
					if e, ok := entryByGroup[sub.WorkerGroup]; ok {
						ce.HasEntry = true
						ce.Entry = e
					}
				}

				coverage = append(coverage, ce)
			}
		}
	}

	missing, present := 0, 0
	for _, c := range coverage {
		if c.HasEntry {
			present++
		} else {
			missing++
		}
	}

	h.writeJSON(w, &EventCoverageResponse{
		EventID:      eventID,
		TopologyAsOf: now,
		Coverage:     coverage,
		MissingCount: missing,
		PresentCount: present,
	})
}

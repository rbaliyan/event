// Package http provides an HTTP handler for the monitor package using protoJSON.
package http

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	pb "github.com/rbaliyan/event/v3/monitor/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Handler implements http.Handler for the monitor service using protoJSON.
type Handler struct {
	store       monitor.Store
	mux         *http.ServeMux
	marshaler   protojson.MarshalOptions
	unmarshaler protojson.UnmarshalOptions
}

// New creates a new HTTP handler for the monitor service.
func New(store monitor.Store) *Handler {
	h := &Handler{
		store: store,
		mux:   http.NewServeMux(),
		marshaler: protojson.MarshalOptions{
			EmitUnpopulated: true,
			UseProtoNames:   true,
		},
		unmarshaler: protojson.UnmarshalOptions{
			DiscardUnknown: true,
		},
	}

	// Register routes following REST conventions
	// GET /v1/monitor/entries - List entries with query params
	// GET /v1/monitor/entries/{event_id} - Get entries by event ID
	// GET /v1/monitor/entries/{event_id}/{subscription_id} - Get specific entry
	// GET /v1/monitor/entries/count - Count entries
	// DELETE /v1/monitor/entries - Delete entries older than specified age
	h.mux.HandleFunc("/v1/monitor/entries", h.handleEntries)
	h.mux.HandleFunc("/v1/monitor/entries/", h.handleEntriesWithPath)
	h.mux.HandleFunc("/v1/monitor/entries/count", h.handleCount)

	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// handleEntries handles GET /v1/entries (list) and DELETE /v1/entries (cleanup)
func (h *Handler) handleEntries(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleList(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleEntriesWithPath handles paths like /v1/entries/{event_id} and /v1/entries/{event_id}/{subscription_id}
func (h *Handler) handleEntriesWithPath(w http.ResponseWriter, r *http.Request) {
	// Handle special paths first
	path := strings.TrimPrefix(r.URL.Path, "/v1/monitor/entries/")

	if path == "count" {
		h.handleCount(w, r)
		return
	}

	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Parse path: {event_id} or {event_id}/{subscription_id}
	parts := strings.SplitN(path, "/", 2)
	eventID := parts[0]

	if eventID == "" {
		h.writeError(w, http.StatusBadRequest, "event_id is required")
		return
	}

	if len(parts) == 2 && parts[1] != "" {
		// GET /v1/entries/{event_id}/{subscription_id}
		h.handleGetEntry(w, r, eventID, parts[1])
	} else {
		// GET /v1/entries/{event_id}
		h.handleGetByEventID(w, r, eventID)
	}
}

// handleList handles GET /v1/entries with query parameters
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	filter := h.parseFilterFromQuery(r)

	page, err := h.store.List(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeResponse(w, pb.PageToListResponse(page))
}

// handleGetEntry handles GET /v1/entries/{event_id}/{subscription_id}
func (h *Handler) handleGetEntry(w http.ResponseWriter, r *http.Request, eventID, subscriptionID string) {
	entry, err := h.store.Get(r.Context(), eventID, subscriptionID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entry == nil {
		h.writeError(w, http.StatusNotFound, "entry not found")
		return
	}

	h.writeResponse(w, &pb.GetResponse{Entry: pb.EntryToProto(entry)})
}

// handleGetByEventID handles GET /v1/entries/{event_id}
func (h *Handler) handleGetByEventID(w http.ResponseWriter, r *http.Request, eventID string) {
	entries, err := h.store.GetByEventID(r.Context(), eventID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeResponse(w, &pb.GetByEventIDResponse{Entries: pb.EntriesToProto(entries)})
}

// handleCount handles GET /v1/entries/count with query parameters
func (h *Handler) handleCount(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	filter := h.parseFilterFromQuery(r)

	count, err := h.store.Count(r.Context(), filter)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeResponse(w, &pb.CountResponse{Count: count})
}

// DefaultDeleteAge is the minimum age for deletion without force flag.
const DefaultDeleteAge = 24 * time.Hour

// handleDelete handles DELETE /v1/monitor/entries?older_than=1h
// By default, only entries older than 24h can be deleted.
// To delete newer entries, use force=true.
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Parse older_than, default to 24h
	age := DefaultDeleteAge
	if olderThanStr := q.Get("older_than"); olderThanStr != "" {
		var err error
		age, err = time.ParseDuration(olderThanStr)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "invalid older_than duration: "+err.Error())
			return
		}
		if age <= 0 {
			h.writeError(w, http.StatusBadRequest, "older_than must be positive")
			return
		}
	}

	// Require force=true to delete entries newer than 24h
	force := q.Get("force") == "true"
	if age < DefaultDeleteAge && !force {
		h.writeError(w, http.StatusBadRequest, "deleting entries newer than 24h requires force=true")
		return
	}

	deleted, err := h.store.DeleteOlderThan(r.Context(), age)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.writeResponse(w, &pb.DeleteOlderThanResponse{Deleted: deleted})
}

// parseFilterFromQuery parses monitor.Filter from URL query parameters
func (h *Handler) parseFilterFromQuery(r *http.Request) monitor.Filter {
	q := r.URL.Query()
	filter := monitor.Filter{}

	if v := q.Get("event_id"); v != "" {
		filter.EventID = v
	}
	if v := q.Get("subscription_id"); v != "" {
		filter.SubscriptionID = v
	}
	if v := q.Get("event_name"); v != "" {
		filter.EventName = v
	}
	if v := q.Get("bus_id"); v != "" {
		filter.BusID = v
	}
	if v := q.Get("delivery_mode"); v != "" {
		dm := monitor.ParseDeliveryMode(v)
		filter.DeliveryMode = &dm
	}
	if v := q["status"]; len(v) > 0 {
		filter.Status = make([]monitor.Status, len(v))
		for i, s := range v {
			filter.Status[i] = monitor.Status(s)
		}
	}
	if v := q.Get("has_error"); v != "" {
		hasErr := v == "true" || v == "1"
		filter.HasError = &hasErr
	}
	if v := q.Get("start_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.StartTime = t
		}
	}
	if v := q.Get("end_time"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.EndTime = t
		}
	}
	if v := q.Get("min_duration"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			filter.MinDuration = d
		}
	}
	if v := q.Get("min_retries"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.MinRetries = n
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

func (h *Handler) readRequest(r *http.Request, msg proto.Message) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	defer r.Body.Close()

	if len(body) == 0 {
		return nil // Empty body is valid (all fields optional)
	}

	return h.unmarshaler.Unmarshal(body, msg)
}

func (h *Handler) writeResponse(w http.ResponseWriter, msg proto.Message) {
	data, err := h.marshaler.Marshal(msg)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (h *Handler) writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(`{"error":"` + message + `"}`))
}

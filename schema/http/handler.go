// Package http provides an HTTP handler for the schema registry.
//
// Mount New(provider) on your HTTP server to expose CRUD operations on
// event schemas. The handler uses plain JSON with no external dependencies.
//
// Routes:
//
//	GET    /v1/schemas            — list all schemas
//	GET    /v1/schemas/{name}     — get a schema by event name
//	PUT    /v1/schemas/{name}     — create or update a schema
//	DELETE /v1/schemas/{name}     — delete a schema
//
// Version management is automatic: the server assigns version=1 on first
// creation and increments by 1 on each update. Callers do not supply a version.
//
// Duration fields (sub_timeout, retry_backoff) are Go time.Duration values
// serialized as nanosecond integers in JSON.
//
// Example:
//
//	provider := schema.NewMemoryProvider()
//	h := schemahttp.New(provider)
//	http.Handle("/", h)
package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3/schema"
)

// Handler is an HTTP handler for schema CRUD operations.
type Handler struct {
	provider schema.SchemaProvider
	mux      *http.ServeMux
}

// New creates a Handler backed by the given provider.
func New(provider schema.SchemaProvider) *Handler {
	h := &Handler{
		provider: provider,
		mux:      http.NewServeMux(),
	}
	h.mux.HandleFunc("/v1/schemas", h.handleSchemas)
	h.mux.HandleFunc("/v1/schemas/", h.handleSchemaByName)
	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// handleSchemas handles GET /v1/schemas (list).
func (h *Handler) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	schemas, err := h.provider.List(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if schemas == nil {
		schemas = []*schema.EventSchema{}
	}
	h.writeJSON(w, map[string]any{"schemas": schemas})
}

// handleSchemaByName handles GET, PUT, DELETE /v1/schemas/{name}.
func (h *Handler) handleSchemaByName(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/schemas/")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "schema name required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, name)
	case http.MethodPut:
		h.handlePut(w, r, name)
	case http.MethodDelete:
		h.handleDelete(w, r, name)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, name string) {
	s, err := h.provider.Get(r.Context(), name)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s == nil {
		h.writeError(w, http.StatusNotFound, "schema not found")
		return
	}
	h.writeJSON(w, s)
}

// schemaInput is the request body for PUT /v1/schemas/{name}.
// Name and Version are excluded — Name comes from the URL path and
// Version is managed server-side (auto-incremented on each update).
type schemaInput struct {
	Description       string            `json:"description,omitempty"`
	SubTimeout        time.Duration     `json:"sub_timeout,omitempty"`
	MaxRetries        int               `json:"max_retries,omitempty"`
	RetryBackoff      time.Duration     `json:"retry_backoff,omitempty"`
	EnableMonitor     bool              `json:"enable_monitor"`
	EnableIdempotency bool              `json:"enable_idempotency"`
	EnablePoison      bool              `json:"enable_poison"`
	Metadata          map[string]string `json:"metadata,omitempty"`
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, name string) {
	var input schemaInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	// Determine next version
	existing, err := h.provider.Get(r.Context(), name)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	version := 1
	if existing != nil {
		version = existing.Version + 1
	}

	s := &schema.EventSchema{
		Name:              name,
		Version:           version,
		Description:       input.Description,
		SubTimeout:        input.SubTimeout,
		MaxRetries:        input.MaxRetries,
		RetryBackoff:      input.RetryBackoff,
		EnableMonitor:     input.EnableMonitor,
		EnableIdempotency: input.EnableIdempotency,
		EnablePoison:      input.EnablePoison,
		Metadata:          input.Metadata,
	}

	if err := h.provider.Set(r.Context(), s); err != nil {
		if errors.Is(err, schema.ErrEmptyName) || errors.Is(err, schema.ErrInvalidVersion) {
			h.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Return the stored schema (with timestamps filled in by provider)
	stored, err := h.provider.Get(r.Context(), name)
	if err != nil || stored == nil {
		// Fall back to what we built
		h.writeJSON(w, s)
		return
	}
	w.WriteHeader(http.StatusOK)
	h.writeJSON(w, stored)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, name string) {
	if err := h.provider.Delete(r.Context(), name); err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data) //nolint:errcheck
}

func (h *Handler) writeError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

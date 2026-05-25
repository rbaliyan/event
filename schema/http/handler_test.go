package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/schema"
	schemahttp "github.com/rbaliyan/event/v3/schema/http"
)

func newHandler(t *testing.T) (*schemahttp.Handler, *schema.MemoryProvider) {
	t.Helper()
	provider := schema.NewMemoryProvider()
	t.Cleanup(func() { _ = provider.Close() })
	return schemahttp.New(provider), provider
}

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.NewDecoder(w.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, w.Body.String())
	}
}

// TestList_Empty verifies an empty provider returns an empty schemas array.
func TestList_Empty(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)
	w := do(t, h, http.MethodGet, "/v1/schemas", nil)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var result struct {
		Schemas []any `json:"schemas"`
	}
	decodeJSON(t, w, &result)
	if len(result.Schemas) != 0 {
		t.Errorf("expected empty schemas, got %d", len(result.Schemas))
	}
}

// TestPutAndGet verifies creating and retrieving a schema.
func TestPutAndGet(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{
		"description":        "Order creation event",
		"sub_timeout":        int64(30 * time.Second),
		"max_retries":        3,
		"enable_monitor":     true,
		"enable_idempotency": false,
		"enable_poison":      false,
	}
	w := do(t, h, http.MethodPut, "/v1/schemas/orders.created", body)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT: want 200, got %d body: %s", w.Code, w.Body)
	}

	var created schema.EventSchema
	decodeJSON(t, w, &created)
	if created.Name != "orders.created" {
		t.Errorf("name: want orders.created, got %s", created.Name)
	}
	if created.Version != 1 {
		t.Errorf("version: want 1, got %d", created.Version)
	}
	if created.MaxRetries != 3 {
		t.Errorf("max_retries: want 3, got %d", created.MaxRetries)
	}
	if !created.EnableMonitor {
		t.Error("expected enable_monitor=true")
	}

	// Now GET it
	w = do(t, h, http.MethodGet, "/v1/schemas/orders.created", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("GET: want 200, got %d", w.Code)
	}
	var got schema.EventSchema
	decodeJSON(t, w, &got)
	if got.Name != "orders.created" || got.Version != 1 {
		t.Errorf("GET: unexpected schema %+v", got)
	}
}

// TestPut_AutoIncrementVersion verifies version is auto-incremented on update.
func TestPut_AutoIncrementVersion(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{"description": "v1", "enable_monitor": false, "enable_idempotency": false, "enable_poison": false}
	do(t, h, http.MethodPut, "/v1/schemas/test.event", body)

	body["description"] = "v2"
	w := do(t, h, http.MethodPut, "/v1/schemas/test.event", body)
	if w.Code != http.StatusOK {
		t.Fatalf("second PUT: want 200, got %d", w.Code)
	}

	var s schema.EventSchema
	decodeJSON(t, w, &s)
	if s.Version != 2 {
		t.Errorf("version: want 2, got %d", s.Version)
	}
}

// TestList_WithSchemas verifies the list endpoint returns all schemas.
func TestList_WithSchemas(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{"enable_monitor": false, "enable_idempotency": false, "enable_poison": false}
	do(t, h, http.MethodPut, "/v1/schemas/event.a", body)
	do(t, h, http.MethodPut, "/v1/schemas/event.b", body)

	w := do(t, h, http.MethodGet, "/v1/schemas", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("LIST: want 200, got %d", w.Code)
	}

	var result struct {
		Schemas []schema.EventSchema `json:"schemas"`
	}
	decodeJSON(t, w, &result)
	if len(result.Schemas) != 2 {
		t.Errorf("want 2 schemas, got %d", len(result.Schemas))
	}
}

// TestDelete verifies deletion removes a schema.
func TestDelete(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{"enable_monitor": false, "enable_idempotency": false, "enable_poison": false}
	do(t, h, http.MethodPut, "/v1/schemas/deleteme", body)

	w := do(t, h, http.MethodDelete, "/v1/schemas/deleteme", nil)
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE: want 204, got %d", w.Code)
	}

	// Now GET should 404
	w = do(t, h, http.MethodGet, "/v1/schemas/deleteme", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE: want 404, got %d", w.Code)
	}
}

// TestGet_NotFound verifies 404 for missing schemas.
func TestGet_NotFound(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)
	w := do(t, h, http.MethodGet, "/v1/schemas/nonexistent", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

// TestPut_InvalidJSON verifies 400 for malformed bodies.
func TestPut_InvalidJSON(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)
	req := httptest.NewRequest(http.MethodPut, "/v1/schemas/test", bytes.NewBufferString("not json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

// TestMethodNotAllowed verifies 405 for wrong HTTP methods.
func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)
	w := do(t, h, http.MethodPost, "/v1/schemas", nil)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

// TestPut_MissingName verifies 400 for /v1/schemas/ with no name.
func TestPut_MissingName(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)
	w := do(t, h, http.MethodPut, "/v1/schemas/", map[string]any{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", w.Code)
	}
}

// TestPut_MetadataRoundtrip verifies metadata is preserved.
func TestPut_MetadataRoundtrip(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{
		"enable_monitor":     false,
		"enable_idempotency": false,
		"enable_poison":      false,
		"metadata":           map[string]string{"owner": "payments-team", "sla": "99.9"},
	}
	do(t, h, http.MethodPut, "/v1/schemas/payments.charged", body)

	w := do(t, h, http.MethodGet, "/v1/schemas/payments.charged", nil)
	var s schema.EventSchema
	decodeJSON(t, w, &s)

	if s.Metadata["owner"] != "payments-team" {
		t.Errorf("metadata owner: want payments-team, got %s", s.Metadata["owner"])
	}
}

// TestPut_ServerFieldsIgnored verifies that version supplied in the PUT body is ignored.
func TestPut_ServerFieldsIgnored(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{
		"version":            999,
		"enable_monitor":     false,
		"enable_idempotency": false,
		"enable_poison":      false,
	}
	w := do(t, h, http.MethodPut, "/v1/schemas/server.fields", body)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT: want 200, got %d body: %s", w.Code, w.Body)
	}

	var s schema.EventSchema
	decodeJSON(t, w, &s)
	if s.Version != 1 {
		t.Errorf("version: want 1 (server-assigned), got %d", s.Version)
	}
}

// TestPut_ConcurrentVersionIncrement verifies that concurrent PUTs serialize version increments.
func TestPut_ConcurrentVersionIncrement(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	body := map[string]any{"enable_monitor": false, "enable_idempotency": false, "enable_poison": false}

	// Seed version 1.
	w := do(t, h, http.MethodPut, "/v1/schemas/concurrent.event", body)
	if w.Code != http.StatusOK {
		t.Fatalf("seed PUT: want 200, got %d", w.Code)
	}

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			do(t, h, http.MethodPut, "/v1/schemas/concurrent.event", body)
		}()
	}
	wg.Wait()

	// After 1 seed + 10 concurrent PUTs, version must be exactly 11.
	w = do(t, h, http.MethodGet, "/v1/schemas/concurrent.event", nil)
	var s schema.EventSchema
	decodeJSON(t, w, &s)
	if s.Version != workers+1 {
		t.Errorf("version: want %d, got %d (concurrent increment lost)", workers+1, s.Version)
	}
}

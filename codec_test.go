package event

import (
	"encoding/json"
	"testing"

	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

func TestJSONCodecEncode(t *testing.T) {
	// Use the default codec
	c := codec.Default()

	msg := message.New("test-id", "test-source", "hello world", map[string]string{"key": "value"}, trace.SpanContext{})

	data, err := c.Encode(msg)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Verify JSON structure
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if decoded["id"] != "test-id" {
		t.Errorf("expected id 'test-id', got %v", decoded["id"])
	}

	if decoded["source"] != "test-source" {
		t.Errorf("expected source 'test-source', got %v", decoded["source"])
	}

	metadata, ok := decoded["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("expected metadata to be a map")
	}
	if metadata["key"] != "value" {
		t.Errorf("expected metadata key 'value', got %v", metadata["key"])
	}
}

func TestJSONCodecDecode(t *testing.T) {
	c := codec.Default()

	jsonData := `{"id":"msg-123","source":"src-456","payload":"test data","metadata":{"foo":"bar"}}`

	msg, err := c.Decode([]byte(jsonData))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if msg.ID() != "msg-123" {
		t.Errorf("expected id 'msg-123', got %s", msg.ID())
	}

	if msg.Source() != "src-456" {
		t.Errorf("expected source 'src-456', got %s", msg.Source())
	}

	if msg.Metadata() == nil {
		t.Fatal("expected metadata")
	}

	// Metadata returns map[string]string
	if msg.Metadata()["foo"] != "bar" {
		t.Errorf("expected metadata foo='bar', got %s", msg.Metadata()["foo"])
	}
}

func TestJSONCodecRoundTrip(t *testing.T) {
	c := codec.Default()

	type TestPayload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	original := message.New(
		"round-trip-id",
		"round-trip-source",
		TestPayload{Name: "test", Count: 42},
		map[string]string{"env": "test", "version": "1.0"},
		trace.SpanContext{},
	)

	// Encode
	data, err := c.Encode(original)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode
	decoded, err := c.Decode(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify
	if decoded.ID() != original.ID() {
		t.Errorf("ID mismatch: got %s, want %s", decoded.ID(), original.ID())
	}

	if decoded.Source() != original.Source() {
		t.Errorf("Source mismatch: got %s, want %s", decoded.Source(), original.Source())
	}

	if decoded.Metadata()["env"] != "test" {
		t.Errorf("Metadata env mismatch: got %s, want 'test'", decoded.Metadata()["env"])
	}

	if decoded.Metadata()["version"] != "1.0" {
		t.Errorf("Metadata version mismatch: got %s, want '1.0'", decoded.Metadata()["version"])
	}

	// Payload is json.RawMessage after decode, need to unmarshal
	var payload TestPayload
	rawPayload, ok := decoded.Payload().(json.RawMessage)
	if !ok {
		t.Fatalf("expected payload to be json.RawMessage, got %T", decoded.Payload())
	}
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if payload.Name != "test" || payload.Count != 42 {
		t.Errorf("Payload mismatch: got %+v", payload)
	}
}

func TestJSONCodecDecodeInvalidJSON(t *testing.T) {
	c := codec.Default()

	_, err := c.Decode([]byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestJSONCodecEncodeNilMetadata(t *testing.T) {
	c := codec.Default()

	msg := message.New("test-id", "test-source", "data", nil, trace.SpanContext{})

	data, err := c.Encode(msg)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Should still produce valid JSON
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Metadata should be omitted or null
	if metadata, ok := decoded["metadata"]; ok && metadata != nil {
		t.Errorf("expected nil metadata, got %v", metadata)
	}
}

func TestJSONCodecDecodeNoMetadata(t *testing.T) {
	c := codec.Default()

	jsonData := `{"id":"msg-123","source":"src-456","payload":"test"}`

	msg, err := c.Decode([]byte(jsonData))
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Should handle missing metadata gracefully
	if msg.Metadata() != nil && len(msg.Metadata()) > 0 {
		t.Errorf("expected nil or empty metadata, got %v", msg.Metadata())
	}
}

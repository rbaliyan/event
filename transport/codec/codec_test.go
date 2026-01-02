package codec

import (
	"encoding/json"
	"testing"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

func TestJSONCodec(t *testing.T) {
	codec := JSON{}

	t.Run("Name and ContentType", func(t *testing.T) {
		if codec.Name() != "json" {
			t.Errorf("expected json, got %s", codec.Name())
		}
		if codec.ContentType() != "application/json" {
			t.Errorf("expected application/json, got %s", codec.ContentType())
		}
	})

	t.Run("Encode and Decode simple payload", func(t *testing.T) {
		msg := message.New("id-1", "source-1", "hello", nil, trace.SpanContext{})

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.ID() != "id-1" {
			t.Errorf("expected id-1, got %s", decoded.ID())
		}
		if decoded.Source() != "source-1" {
			t.Errorf("expected source-1, got %s", decoded.Source())
		}

		// Payload is json.RawMessage, unmarshal to verify
		var payload string
		if err := json.Unmarshal(decoded.Payload().(json.RawMessage), &payload); err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		if payload != "hello" {
			t.Errorf("expected hello, got %s", payload)
		}
	})

	t.Run("Encode and Decode with metadata", func(t *testing.T) {
		metadata := map[string]string{"key": "value", "env": "test"}
		msg := message.New("id-2", "source-2", "data", metadata, trace.SpanContext{})

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.Metadata()["key"] != "value" {
			t.Error("expected metadata key=value")
		}
		if decoded.Metadata()["env"] != "test" {
			t.Error("expected metadata env=test")
		}
	})

	t.Run("Encode and Decode with retry count", func(t *testing.T) {
		msg := message.NewWithRetry("id-3", "source-3", "data", nil, trace.SpanContext{}, 5)

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.RetryCount() != 5 {
			t.Errorf("expected retry count 5, got %d", decoded.RetryCount())
		}
	})

	t.Run("Encode and Decode struct payload", func(t *testing.T) {
		type Order struct {
			ID     string  `json:"id"`
			Amount float64 `json:"amount"`
		}

		order := Order{ID: "ORD-123", Amount: 99.99}
		msg := message.New("id-4", "source-4", order, nil, trace.SpanContext{})

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		var decodedOrder Order
		if err := json.Unmarshal(decoded.Payload().(json.RawMessage), &decodedOrder); err != nil {
			t.Fatalf("failed to unmarshal order: %v", err)
		}

		if decodedOrder.ID != "ORD-123" {
			t.Errorf("expected ORD-123, got %s", decodedOrder.ID)
		}
		if decodedOrder.Amount != 99.99 {
			t.Errorf("expected 99.99, got %f", decodedOrder.Amount)
		}
	})

	t.Run("Decode invalid JSON returns error", func(t *testing.T) {
		_, err := codec.Decode([]byte("invalid json"))
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("Encode with nil metadata", func(t *testing.T) {
		msg := message.New("id-5", "source-5", "data", nil, trace.SpanContext{})

		data, err := codec.Encode(msg)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode(data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.Metadata() != nil {
			t.Error("expected nil metadata")
		}
	})
}

func TestDefaultCodec(t *testing.T) {
	codec := Default()

	if codec.Name() != "json" {
		t.Errorf("expected default codec to be json, got %s", codec.Name())
	}
}

func TestCodecErrors(t *testing.T) {
	if ErrEncodeFailure.Error() != "failed to encode message" {
		t.Error("unexpected error message for ErrEncodeFailure")
	}
	if ErrDecodeFailure.Error() != "failed to decode message" {
		t.Error("unexpected error message for ErrDecodeFailure")
	}
}

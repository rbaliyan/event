package codec

import (
	"encoding/json"
	"errors"
	"maps"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// JSON implements Codec using JSON serialization.
// This is the default codec, providing human-readable output.
//
// Payload is stored as pre-encoded bytes (base64 in JSON wire format).
type JSON struct{}

// jsonMessage is the JSON wire format
type jsonMessage struct {
	ID         string            `json:"id"`
	Source     string            `json:"source"`
	Payload    []byte            `json:"payload"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	RetryCount int               `json:"retry_count,omitempty"`
}

// Encode serializes a message to JSON bytes
func (c JSON) Encode(msg Message) ([]byte, error) {
	jm := jsonMessage{
		ID:         msg.ID(),
		Source:     msg.Source(),
		Payload:    msg.Payload(),
		RetryCount: msg.RetryCount(),
	}

	if msg.Metadata() != nil {
		jm.Metadata = make(map[string]string)
		maps.Copy(jm.Metadata, msg.Metadata())
	}

	data, err := json.Marshal(jm)
	if err != nil {
		return nil, errors.Join(ErrEncodeFailure, err)
	}

	return data, nil
}

// Decode deserializes JSON bytes to a message
func (c JSON) Decode(data []byte) (Message, error) {
	var jm jsonMessage
	if err := json.Unmarshal(data, &jm); err != nil {
		return nil, errors.Join(ErrDecodeFailure, err)
	}

	var metadata map[string]string
	if jm.Metadata != nil {
		metadata = make(map[string]string)
		maps.Copy(metadata, jm.Metadata)
	}

	return message.NewWithRetry(
		jm.ID,
		jm.Source,
		jm.Payload,
		metadata,
		trace.SpanContext{},
		jm.RetryCount,
	), nil
}

// ContentType returns the MIME type for JSON
func (c JSON) ContentType() string {
	return "application/json"
}

// Name returns the codec identifier
func (c JSON) Name() string {
	return "json"
}

// Compile-time check
var _ Codec = JSON{}

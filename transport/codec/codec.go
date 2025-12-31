// Package codec provides message serialization/deserialization implementations
// for event transport systems.
//
// Supported formats:
//   - JSON (default, human-readable)
//   - MessagePack (binary, compact)
//   - Protocol Buffers (binary, schema-based)
package codec

import (
	"errors"

	"github.com/rbaliyan/event/v3/transport/message"
)

// Codec errors
var (
	ErrEncodeFailure = errors.New("failed to encode message")
	ErrDecodeFailure = errors.New("failed to decode message")
)

// Message is the message interface used by codecs
type Message = message.Message

// Codec handles message serialization/deserialization for external transports.
// Implementations must be safe for concurrent use.
type Codec interface {
	// Encode serializes a message to bytes.
	// Returns ErrEncodeFailure if serialization fails.
	Encode(msg Message) ([]byte, error)

	// Decode deserializes bytes to a message.
	// Returns ErrDecodeFailure if deserialization fails.
	// The returned Message contains raw payload data that may need
	// further deserialization by the handler.
	Decode(data []byte) (Message, error)

	// ContentType returns the MIME type for this codec (e.g., "application/json").
	// Useful for HTTP transports and content negotiation.
	ContentType() string

	// Name returns a short identifier for this codec (e.g., "json", "msgpack", "proto").
	Name() string
}

// Default returns the default codec (JSON)
func Default() Codec {
	return JSON{}
}

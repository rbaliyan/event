// Package payload provides event payload serialization/deserialization.
//
// The payload package handles encoding and decoding of event data at the
// application level, separate from transport-level message serialization.
//
// Usage:
//
//	// Use JSON codec (default)
//	event := New[Order]("orders")
//
//	// Use protobuf codec
//	event := New[*pb.Order]("orders", WithPayloadCodec(payload.Proto{}))
//
//	// Use msgpack codec
//	event := New[Order]("orders", WithPayloadCodec(payload.MsgPack{}))
package payload

// Codec encodes/decodes event payload data.
// Implementations must be safe for concurrent use.
type Codec interface {
	// Encode serializes the payload to bytes.
	Encode(v any) ([]byte, error)

	// Decode deserializes bytes to the target type.
	// The target must be a pointer.
	Decode(data []byte, v any) error

	// ContentType returns the MIME type (e.g., "application/json").
	ContentType() string
}

// Default returns the default codec (JSON).
func Default() Codec {
	return JSON{}
}

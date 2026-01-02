package payload

import (
	"errors"

	"google.golang.org/protobuf/proto"
)

// Proto implements Codec using Protocol Buffers serialization.
// For best performance and type safety, use proto.Message types for payloads.
//
// Usage:
//
//	event := New[*pb.Order]("orders", WithPayloadCodec(payload.Proto{}))
type Proto struct{}

// Encode serializes the payload to Protocol Buffer bytes.
// The payload must implement proto.Message.
func (Proto) Encode(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("payload must implement proto.Message")
	}
	return proto.Marshal(msg)
}

// Decode deserializes Protocol Buffer bytes to the target type.
// The target must be a pointer to a proto.Message.
func (Proto) Decode(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return errors.New("target must implement proto.Message")
	}
	return proto.Unmarshal(data, msg)
}

// ContentType returns the MIME type for Protocol Buffers.
func (Proto) ContentType() string {
	return "application/protobuf"
}

// Compile-time check.
var _ Codec = Proto{}

func init() {
	Register(Proto{})
}

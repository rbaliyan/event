package codec

import (
	"errors"
	"maps"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Proto implements Codec using Protocol Buffers serialization.
// Protocol Buffers provide efficient binary encoding with strong typing.
//
// Payload is stored as pre-encoded bytes.
type Proto struct{}

// Encode serializes a message to Protocol Buffer bytes
func (c Proto) Encode(msg Message) ([]byte, error) {
	pm := &message.ProtoMessage{
		Id:         msg.ID(),
		Source:     msg.Source(),
		Payload:    msg.Payload(),
		RetryCount: int32(msg.RetryCount()),
	}

	// Handle metadata
	if msg.Metadata() != nil {
		pm.Metadata = make(map[string]string)
		maps.Copy(pm.Metadata, msg.Metadata())
	}

	data, err := proto.Marshal(pm)
	if err != nil {
		return nil, errors.Join(ErrEncodeFailure, err)
	}

	return data, nil
}

// Decode deserializes Protocol Buffer bytes to a message
func (c Proto) Decode(data []byte) (Message, error) {
	var pm message.ProtoMessage
	if err := proto.Unmarshal(data, &pm); err != nil {
		return nil, errors.Join(ErrDecodeFailure, err)
	}

	var metadata map[string]string
	if pm.Metadata != nil {
		metadata = make(map[string]string)
		maps.Copy(metadata, pm.Metadata)
	}

	return message.NewWithRetry(
		pm.Id,
		pm.Source,
		pm.Payload,
		metadata,
		trace.SpanContext{},
		int(pm.RetryCount),
	), nil
}

// ContentType returns the MIME type for Protocol Buffers
func (c Proto) ContentType() string {
	return "application/x-protobuf"
}

// Name returns the codec identifier
func (c Proto) Name() string {
	return "proto"
}

// Compile-time check
var _ Codec = Proto{}

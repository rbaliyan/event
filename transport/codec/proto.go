package codec

import (
	"errors"
	"maps"

	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Proto implements Codec using Protocol Buffers serialization.
// Protocol Buffers provide efficient binary encoding with strong typing.
//
// Payload handling:
//   - If payload implements proto.Message, it's wrapped in Any
//   - Otherwise, it's converted to structpb.Value (supports JSON-like values)
//   - Decode returns the raw bytes for handler to unmarshal
//
// For best performance, use proto.Message types for payloads.
type Proto struct{}

// Encode serializes a message to Protocol Buffer bytes
func (c Proto) Encode(msg Message) ([]byte, error) {
	pm := &message.ProtoMessage{
		Id:         msg.ID(),
		Source:     msg.Source(),
		RetryCount: int32(msg.RetryCount()),
	}

	// Handle metadata
	if msg.Metadata() != nil {
		pm.Metadata = make(map[string]string)
		maps.Copy(pm.Metadata, msg.Metadata())
	}

	// Handle payload
	if msg.Payload() != nil {
		switch p := msg.Payload().(type) {
		case proto.Message:
			// Wrap proto.Message in Any
			anyPayload, err := anypb.New(p)
			if err != nil {
				return nil, errors.Join(ErrEncodeFailure, err)
			}
			pm.Payload = &message.ProtoMessage_AnyPayload{AnyPayload: anyPayload}
		default:
			// Convert to structpb.Value for generic types
			structVal, err := structpb.NewValue(p)
			if err != nil {
				return nil, errors.Join(ErrEncodeFailure, err)
			}
			pm.Payload = &message.ProtoMessage_StructPayload{StructPayload: structVal}
		}
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

	// Extract payload based on type
	var payload any
	switch p := pm.Payload.(type) {
	case *message.ProtoMessage_AnyPayload:
		// Return the Any message - handler can unmarshal to specific type
		payload = p.AnyPayload
	case *message.ProtoMessage_StructPayload:
		// Convert structpb.Value back to Go interface{}
		payload = p.StructPayload.AsInterface()
	}

	return message.NewWithRetry(
		pm.Id,
		pm.Source,
		payload,
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

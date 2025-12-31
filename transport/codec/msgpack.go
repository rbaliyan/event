package codec

import (
	"errors"
	"maps"

	"github.com/rbaliyan/event/v3/transport/message"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/trace"
)

// MsgPack implements Codec using MessagePack serialization.
// MessagePack is a binary format that's more compact than JSON
// while maintaining schema-less flexibility.
//
// Benefits:
//   - Smaller message size than JSON
//   - Faster encoding/decoding
//   - Supports binary data natively
//
// Payload handling:
//   - Encode: marshals payload to MessagePack
//   - Decode: payload is msgpack.RawMessage, unmarshaled by handler
type MsgPack struct{}

// msgpackMessage is the MessagePack wire format
type msgpackMessage struct {
	ID         string             `msgpack:"id"`
	Source     string             `msgpack:"source"`
	Payload    msgpack.RawMessage `msgpack:"payload"`
	Metadata   map[string]string  `msgpack:"metadata,omitempty"`
	RetryCount int                `msgpack:"retry_count,omitempty"`
}

// Encode serializes a message to MessagePack bytes
func (c MsgPack) Encode(msg Message) ([]byte, error) {
	payload, err := msgpack.Marshal(msg.Payload())
	if err != nil {
		return nil, errors.Join(ErrEncodeFailure, err)
	}

	mm := msgpackMessage{
		ID:         msg.ID(),
		Source:     msg.Source(),
		Payload:    payload,
		RetryCount: msg.RetryCount(),
	}

	if msg.Metadata() != nil {
		mm.Metadata = make(map[string]string)
		maps.Copy(mm.Metadata, msg.Metadata())
	}

	data, err := msgpack.Marshal(mm)
	if err != nil {
		return nil, errors.Join(ErrEncodeFailure, err)
	}

	return data, nil
}

// Decode deserializes MessagePack bytes to a message
func (c MsgPack) Decode(data []byte) (Message, error) {
	var mm msgpackMessage
	if err := msgpack.Unmarshal(data, &mm); err != nil {
		return nil, errors.Join(ErrDecodeFailure, err)
	}

	var metadata map[string]string
	if mm.Metadata != nil {
		metadata = make(map[string]string)
		maps.Copy(metadata, mm.Metadata)
	}

	// Payload remains as msgpack.RawMessage, will be decoded by handler
	return message.NewWithRetry(
		mm.ID,
		mm.Source,
		mm.Payload,
		metadata,
		trace.SpanContext{},
		mm.RetryCount,
	), nil
}

// ContentType returns the MIME type for MessagePack
func (c MsgPack) ContentType() string {
	return "application/msgpack"
}

// Name returns the codec identifier
func (c MsgPack) Name() string {
	return "msgpack"
}

// Compile-time check
var _ Codec = MsgPack{}

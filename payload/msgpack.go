package payload

import "github.com/vmihailenco/msgpack/v5"

// MsgPack implements Codec using MessagePack serialization.
// MessagePack is a binary format that's more compact than JSON
// while maintaining schema-less flexibility.
//
// Usage:
//
//	event := New[Order]("orders", WithPayloadCodec(payload.MsgPack{}))
type MsgPack struct{}

// Encode serializes the payload to MessagePack bytes.
func (MsgPack) Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

// Decode deserializes MessagePack bytes to the target type.
func (MsgPack) Decode(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

// ContentType returns the MIME type for MessagePack.
func (MsgPack) ContentType() string {
	return "application/msgpack"
}

// Compile-time check.
var _ Codec = MsgPack{}

func init() {
	Register(MsgPack{})
}

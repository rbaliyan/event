package payload

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	// Register BSON codec in the global registry
	Register(BSON{})
}

// BSON codec uses MongoDB's BSON format for serialization.
// This is ideal for MongoDB change stream events where the payload
// is already BSON and contains MongoDB-specific types like ObjectID.
//
// Usage:
//
//	event := New[Model]("orders", WithPayloadCodec(payload.BSON{}))
type BSON struct{}

// Encode serializes the payload to BSON bytes.
func (BSON) Encode(v any) ([]byte, error) {
	return bson.Marshal(v)
}

// Decode deserializes BSON bytes to the target type.
func (BSON) Decode(data []byte, v any) error {
	return bson.Unmarshal(data, v)
}

// ContentType returns the MIME type for BSON.
func (BSON) ContentType() string {
	return "application/bson"
}

// Compile-time check
var _ Codec = BSON{}

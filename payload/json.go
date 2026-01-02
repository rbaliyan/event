package payload

import "encoding/json"

// JSON implements Codec using JSON serialization.
// This is the default codec.
type JSON struct{}

// Encode serializes the payload to JSON bytes.
func (JSON) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode deserializes JSON bytes to the target type.
func (JSON) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// ContentType returns the MIME type for JSON.
func (JSON) ContentType() string {
	return "application/json"
}

// Compile-time check.
var _ Codec = JSON{}

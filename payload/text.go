package payload

import (
	"fmt"
)

func init() {
	// Register text codec in the global registry
	Register(Text{})
}

// Text codec handles plain text strings.
// This is useful for simple string payloads like document IDs.
//
// Usage:
//
//	event := New[string]("doc.deleted", WithPayloadCodec(payload.Text{}))
type Text struct{}

// Encode serializes a string to bytes.
func (Text) Encode(v any) ([]byte, error) {
	switch s := v.(type) {
	case string:
		return []byte(s), nil
	case *string:
		if s == nil {
			return nil, nil
		}
		return []byte(*s), nil
	case []byte:
		return s, nil
	default:
		return nil, fmt.Errorf("text codec: expected string, got %T", v)
	}
}

// Decode deserializes bytes to a string.
func (Text) Decode(data []byte, v any) error {
	switch s := v.(type) {
	case *string:
		*s = string(data)
		return nil
	default:
		return fmt.Errorf("text codec: expected *string, got %T", v)
	}
}

// ContentType returns the MIME type for plain text.
func (Text) ContentType() string {
	return "text/plain"
}

// Compile-time check
var _ Codec = Text{}

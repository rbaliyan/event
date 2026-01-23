package store

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// Cursor represents a pagination position.
// It encodes both a timestamp and an ID for stable pagination
// even when items have the same timestamp.
type Cursor struct {
	// Timestamp is the creation/sort time of the last item.
	Timestamp time.Time `json:"t,omitempty"`

	// ID is the unique identifier of the last item.
	// Used to break ties when multiple items have the same timestamp.
	ID string `json:"id,omitempty"`
}

// EncodeCursor encodes a cursor to a URL-safe string.
// Returns empty string if cursor is zero value.
func EncodeCursor(c Cursor) string {
	if c.Timestamp.IsZero() && c.ID == "" {
		return ""
	}
	data, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(data)
}

// DecodeCursor decodes a cursor string back to a Cursor.
// Returns zero Cursor if string is empty or invalid.
func DecodeCursor(s string) Cursor {
	if s == "" {
		return Cursor{}
	}
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}
	}
	var c Cursor
	if err := json.Unmarshal(data, &c); err != nil {
		return Cursor{}
	}
	return c
}

// NewCursor creates a cursor from a timestamp and ID.
func NewCursor(ts time.Time, id string) Cursor {
	return Cursor{Timestamp: ts, ID: id}
}

// String returns the encoded cursor string.
func (c Cursor) String() string {
	return EncodeCursor(c)
}

// IsZero returns true if the cursor is unset.
func (c Cursor) IsZero() bool {
	return c.Timestamp.IsZero() && c.ID == ""
}

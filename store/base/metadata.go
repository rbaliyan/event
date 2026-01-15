package base

import (
	"encoding/json"
	"fmt"
)

// MarshalMetadata converts a metadata map to JSON bytes.
// Returns nil for nil or empty maps without error.
func MarshalMetadata(metadata map[string]string) ([]byte, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	return data, nil
}

// UnmarshalMetadata converts JSON bytes to a metadata map.
// Returns nil for nil or empty bytes without error.
func UnmarshalMetadata(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var metadata map[string]string
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}
	return metadata, nil
}

// MarshalJSON converts any value to JSON bytes with error wrapping.
func MarshalJSON(v any, fieldName string) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", fieldName, err)
	}
	return data, nil
}

// UnmarshalJSON converts JSON bytes to a value with error wrapping.
func UnmarshalJSON(data []byte, v any, fieldName string) error {
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshal %s: %w", fieldName, err)
	}
	return nil
}

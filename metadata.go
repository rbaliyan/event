package event

import (
	"fmt"
	"strings"
)

// Metadata metadata
type Metadata map[string]string

func NewMetadata() Metadata {
	return Metadata{}
}

// Get returns the metadata value for the provided key.
func (m Metadata) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}
	return ""
}

// Set sets the metadata key to provided value.
func (m Metadata) Set(key, value string) Metadata {
	m[key] = value
	return m
}

// Convert metadata to string
func (m Metadata) String() string {
	if m == nil {
		return ""
	}
	vals := make([]string, 0, len(m))
	for key, val := range m {
		vals = append(vals, fmt.Sprintf("%s=%s", key, val))
	}
	return fmt.Sprintf("Metadata{%s}", strings.Join(vals, ", "))
}

// Copy metadata
func (m Metadata) Copy() Metadata {
	if m == nil {
		return nil
	}
	m1 := Metadata{}
	for key, val := range m {
		m1[key] = val
	}
	return m1
}

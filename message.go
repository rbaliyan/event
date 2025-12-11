package event

import "context"

// Message transport message
type Message interface {
	// ID message ID
	ID() string
	// Source message source
	Source() string
	// Metadata message metadata
	Metadata() Metadata
	// Payload data after unmarshall
	Payload() any
	// Context create context with data, includes tracing information
	Context() context.Context
}

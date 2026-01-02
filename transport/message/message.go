// Package message provides the core Message type used throughout the event system.
//
// This package is imported by both codec and transport packages to avoid circular
// dependencies while providing a unified message type.
package message

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Message is an event message that travels through the transport
type Message interface {
	// ID returns the unique message identifier
	ID() string
	// Source returns the source that published this message
	Source() string
	// Payload returns the message payload as bytes
	Payload() []byte
	// Metadata returns optional key-value metadata
	Metadata() map[string]string
	// Timestamp returns when the message was created/published
	Timestamp() time.Time
	// RetryCount returns the number of times this message has been retried
	RetryCount() int
	// Context returns a context with trace information (if available)
	Context() context.Context
	// Ack acknowledges the message. Pass nil for success, or an error to trigger redelivery.
	Ack(error) error
}

// message is the default Message implementation
type message struct {
	id         string
	source     string
	payload    []byte
	metadata   map[string]string
	timestamp  time.Time
	span       trace.SpanContext
	retryCount int
	ackFn      func(error) error
}

func (m *message) ID() string                  { return m.id }
func (m *message) Source() string              { return m.source }
func (m *message) Payload() []byte             { return m.payload }
func (m *message) Metadata() map[string]string { return m.metadata }
func (m *message) Timestamp() time.Time        { return m.timestamp }
func (m *message) RetryCount() int             { return m.retryCount }
func (m *message) Context() context.Context {
	return trace.ContextWithRemoteSpanContext(context.Background(), m.span)
}
func (m *message) Ack(err error) error {
	if m.ackFn != nil {
		return m.ackFn(err)
	}
	return nil
}

// New creates a new message with current timestamp
func New(id, source string, payload []byte, metadata map[string]string, spanCtx trace.SpanContext) Message {
	return &message{
		id:        id,
		source:    source,
		payload:   payload,
		metadata:  metadata,
		timestamp: time.Now(),
		span:      spanCtx,
	}
}

// NewWithRetry creates a new message with retry count
func NewWithRetry(id, source string, payload []byte, metadata map[string]string, spanCtx trace.SpanContext, retryCount int) Message {
	return &message{
		id:         id,
		source:     source,
		payload:    payload,
		metadata:   metadata,
		timestamp:  time.Now(),
		span:       spanCtx,
		retryCount: retryCount,
	}
}

// NewWithTimestamp creates a new message with a specific timestamp.
// Use this when reconstructing messages from storage where the original timestamp is known.
func NewWithTimestamp(id, source string, payload []byte, metadata map[string]string, spanCtx trace.SpanContext, timestamp time.Time) Message {
	return &message{
		id:        id,
		source:    source,
		payload:   payload,
		metadata:  metadata,
		timestamp: timestamp,
		span:      spanCtx,
	}
}

// NewWithAck creates a new message with retry count and ack function.
// This is used by transports that need custom acknowledgment behavior.
func NewWithAck(id, source string, payload []byte, metadata map[string]string, retryCount int, ackFn func(error) error) Message {
	return &message{
		id:         id,
		source:     source,
		payload:    payload,
		metadata:   metadata,
		timestamp:  time.Now(),
		retryCount: retryCount,
		ackFn:      ackFn,
	}
}

// NewFull creates a new message with all fields specified.
// This is the most flexible constructor for advanced use cases.
func NewFull(id, source string, payload []byte, metadata map[string]string, timestamp time.Time, retryCount int, ackFn func(error) error) Message {
	return &message{
		id:         id,
		source:     source,
		payload:    payload,
		metadata:   metadata,
		timestamp:  timestamp,
		retryCount: retryCount,
		ackFn:      ackFn,
	}
}

// Compile-time interface check
var _ Message = (*message)(nil)

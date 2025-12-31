// Package message provides the core Message type used throughout the event system.
//
// This package is imported by both codec and transport packages to avoid circular
// dependencies while providing a unified message type.
package message

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// Message is an event message that travels through the transport
type Message interface {
	// ID returns the unique message identifier
	ID() string
	// Source returns the source that published this message
	Source() string
	// Payload returns the message payload
	Payload() any
	// Metadata returns optional key-value metadata
	Metadata() map[string]string
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
	payload    any
	metadata   map[string]string
	span       trace.SpanContext
	retryCount int
	ackFn      func(error) error
}

func (m *message) ID() string              { return m.id }
func (m *message) Source() string          { return m.source }
func (m *message) Payload() any            { return m.payload }
func (m *message) Metadata() map[string]string { return m.metadata }
func (m *message) RetryCount() int         { return m.retryCount }
func (m *message) Context() context.Context {
	return trace.ContextWithRemoteSpanContext(context.Background(), m.span)
}
func (m *message) Ack(err error) error {
	if m.ackFn != nil {
		return m.ackFn(err)
	}
	return nil
}

// New creates a new message
func New(id, source string, payload any, metadata map[string]string, spanCtx trace.SpanContext) Message {
	return &message{
		id:       id,
		source:   source,
		payload:  payload,
		metadata: metadata,
		span:     spanCtx,
	}
}

// NewWithRetry creates a new message with retry count
func NewWithRetry(id, source string, payload any, metadata map[string]string, spanCtx trace.SpanContext, retryCount int) Message {
	return &message{
		id:         id,
		source:     source,
		payload:    payload,
		metadata:   metadata,
		span:       spanCtx,
		retryCount: retryCount,
	}
}

// NewWithAck creates a new message with retry count and ack function.
// This is used by transports that need custom acknowledgment behavior.
func NewWithAck(id, source string, payload any, metadata map[string]string, retryCount int, ackFn func(error) error) Message {
	return &message{
		id:         id,
		source:     source,
		payload:    payload,
		metadata:   metadata,
		retryCount: retryCount,
		ackFn:      ackFn,
	}
}

// Compile-time interface check
var _ Message = (*message)(nil)

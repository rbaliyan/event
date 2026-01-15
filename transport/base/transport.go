package base

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// Transport provides common transport functionality that can be embedded
// in transport-specific implementations.
//
// Example usage:
//
//	type RedisTransport struct {
//	    base.Transport
//	    client Client
//	    // ... redis-specific fields
//	}
type Transport struct {
	status  int32
	logger  *slog.Logger
	onError func(error)
	codec   codec.Codec
	events  sync.Map // map[string]any - transport-specific event data
}

// NewTransport creates a new base transport with the given logger name.
func NewTransport(loggerName string) *Transport {
	return &Transport{
		status:  1, // Open
		logger:  transport.Logger(loggerName),
		onError: func(error) {},
		codec:   codec.Default(),
	}
}

// IsOpen returns true if the transport is open.
func (t *Transport) IsOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// MarkClosed atomically marks the transport as closed.
// Returns true if the transport was open and is now closed.
// Returns false if already closed.
func (t *Transport) MarkClosed() bool {
	return atomic.CompareAndSwapInt32(&t.status, 1, 0)
}

// EnsureOpen returns ErrTransportClosed if the transport is closed.
func (t *Transport) EnsureOpen() error {
	if !t.IsOpen() {
		return transport.ErrTransportClosed
	}
	return nil
}

// Logger returns the transport's logger.
func (t *Transport) Logger() *slog.Logger {
	return t.logger
}

// SetLogger sets the transport's logger.
func (t *Transport) SetLogger(logger *slog.Logger) {
	t.logger = logger
}

// OnError returns the error callback function.
func (t *Transport) OnError() func(error) {
	return t.onError
}

// SetOnError sets the error callback function.
func (t *Transport) SetOnError(fn func(error)) {
	if fn != nil {
		t.onError = fn
	}
}

// ReportError calls the error callback with the given error.
func (t *Transport) ReportError(err error) {
	if err != nil && t.onError != nil {
		t.onError(err)
	}
}

// Codec returns the transport's codec.
func (t *Transport) Codec() codec.Codec {
	return t.codec
}

// SetCodec sets the transport's codec.
func (t *Transport) SetCodec(c codec.Codec) {
	if c != nil {
		t.codec = c
	}
}

// Events returns the events sync.Map for direct access.
func (t *Transport) Events() *sync.Map {
	return &t.events
}

// LoadEvent loads an event by name. Returns nil if not found.
func (t *Transport) LoadEvent(name string) (any, bool) {
	return t.events.Load(name)
}

// StoreEvent stores an event. Returns true if this is a new event.
func (t *Transport) StoreEvent(name string, event any) bool {
	_, loaded := t.events.LoadOrStore(name, event)
	return !loaded
}

// DeleteEvent deletes an event by name. Returns the deleted event or nil.
func (t *Transport) DeleteEvent(name string) (any, bool) {
	return t.events.LoadAndDelete(name)
}

// CountEvents returns the number of registered events.
func (t *Transport) CountEvents() int {
	count := 0
	t.events.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// RangeEvents iterates over all events.
func (t *Transport) RangeEvents(fn func(name string, event any) bool) {
	t.events.Range(func(key, value any) bool {
		return fn(key.(string), value)
	})
}

package event

import (
	"log/slog"
	"time"
)

// Default transport configuration values
var (
	// DefaultTransportBufferSize is the buffer size when async is enabled
	DefaultTransportBufferSize uint = 100

	// DefaultTransportWorkerPool is the default worker pool size for async mode
	DefaultTransportWorkerPool int64 = 100
)

// transportOptions holds configuration for transport (unexported)
type transportOptions struct {
	bufferSize     uint
	async          bool
	workerPoolSize int64
	timeout        time.Duration
	onError        func(error)
	logger         *slog.Logger
}

// TransportOption option function for transport configuration
type TransportOption func(*transportOptions)

// WithTransportBufferSize sets the buffer size for transport channels
func WithTransportBufferSize(size uint) TransportOption {
	return func(o *transportOptions) {
		o.bufferSize = size
	}
}

// WithTransportAsync enables/disables async handler execution
// When enabled, handlers run in separate goroutines and buffer size
// defaults to DefaultTransportBufferSize if not explicitly set
// When disabled (default), handlers run synchronously with blocking channels
func WithTransportAsync(enabled bool) TransportOption {
	return func(o *transportOptions) {
		o.async = enabled
	}
}

// WithTransportWorkerPoolSize sets the maximum number of concurrent handlers
// Only applies when async is enabled
// Set to 0 to disable worker pool (unlimited concurrency)
func WithTransportWorkerPoolSize(size int64) TransportOption {
	return func(o *transportOptions) {
		o.workerPoolSize = size
	}
}

// WithTransportTimeout sets the timeout for sending to each subscriber
// Set to 0 for no timeout (block indefinitely)
func WithTransportTimeout(d time.Duration) TransportOption {
	return func(o *transportOptions) {
		o.timeout = d
	}
}

// WithTransportErrorHandler sets the error handler callback
// Called when transport encounters errors (e.g., send timeout)
func WithTransportErrorHandler(fn func(error)) TransportOption {
	return func(o *transportOptions) {
		if fn != nil {
			o.onError = fn
		}
	}
}

// WithTransportLogger sets the logger for transport
func WithTransportLogger(l *slog.Logger) TransportOption {
	return func(o *transportOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// newTransportOptions creates options with defaults and applies provided options
func newTransportOptions(opts ...TransportOption) *transportOptions {
	o := &transportOptions{
		bufferSize:     0, // blocking by default
		async:          false,
		workerPoolSize: DefaultTransportWorkerPool,
		timeout:        0,
		onError:        func(error) {}, // no-op default
		logger:         Logger("transport>"),
	}

	// Apply all options
	for _, opt := range opts {
		opt(o)
	}

	// Finalize dependent fields: if async enabled and buffer not set, use default
	if o.async && o.bufferSize == 0 {
		o.bufferSize = DefaultTransportBufferSize
	}

	return o
}

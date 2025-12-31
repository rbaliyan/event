package channel

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// Default configuration values
var (
	// DefaultBufferSize is the buffer size when async is enabled
	DefaultBufferSize uint = 100

	// DefaultWorkerPool is the default worker pool size for async mode
	DefaultWorkerPool int64 = 100
)

// options holds configuration for transport (unexported)
type options struct {
	bufferSize     uint
	async          bool
	workerPoolSize int64
	timeout        time.Duration
	onError        func(error)
	logger         *slog.Logger
}

// Option configures the channel transport
type Option func(*options)

// WithBufferSize sets the buffer size for transport channels
func WithBufferSize(size uint) Option {
	return func(o *options) {
		o.bufferSize = size
	}
}

// WithAsync enables/disables async handler execution.
// When enabled, handlers run in separate goroutines and buffer size
// defaults to DefaultBufferSize if not explicitly set.
// When disabled (default), handlers run synchronously with blocking channels.
func WithAsync(enabled bool) Option {
	return func(o *options) {
		o.async = enabled
	}
}

// WithWorkerPoolSize sets the maximum number of concurrent handlers.
// Only applies when async is enabled.
// Set to 0 to disable worker pool (unlimited concurrency).
func WithWorkerPoolSize(size int64) Option {
	return func(o *options) {
		o.workerPoolSize = size
	}
}

// WithTimeout sets the timeout for sending to each subscriber.
// Set to 0 for no timeout (block indefinitely).
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithErrorHandler sets the error handler callback.
// Called when transport encounters errors (e.g., send timeout).
func WithErrorHandler(fn func(error)) Option {
	return func(o *options) {
		if fn != nil {
			o.onError = fn
		}
	}
}

// WithLogger sets the logger for transport
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// newOptions creates options with defaults and applies provided options
func newOptions(opts ...Option) *options {
	o := &options{
		bufferSize:     0, // blocking by default
		async:          false,
		workerPoolSize: DefaultWorkerPool,
		timeout:        0,
		onError:        func(error) {}, // no-op default
		logger:         transport.Logger("transport>channel"),
	}

	// Apply all options
	for _, opt := range opts {
		opt(o)
	}

	// Finalize dependent fields: if async enabled and buffer not set, use default
	if o.async && o.bufferSize == 0 {
		o.bufferSize = DefaultBufferSize
	}

	return o
}

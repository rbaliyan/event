package event

import (
	"log/slog"
	"time"
)

// Default event configuration values
var (
	// DefaultSubscriberTimeout default subscriber timeout (0 = no timeout)
	DefaultSubscriberTimeout time.Duration = 0
)

// eventOptions holds configuration for events (unexported)
type eventOptions struct {
	registry          *Registry
	recoveryEnabled   bool
	tracingEnabled    bool
	metricsEnabled    bool
	metrics           Metrics
	subTimeout        time.Duration
	channelBufferSize uint
	onError           func(BaseEvent, error)
	logger            *slog.Logger
	transport         Transport
}

// newEventOptions creates options with defaults and applies provided options
func newEventOptions(opts ...Option) *eventOptions {
	o := &eventOptions{
		registry:          defaultRegistry,
		tracingEnabled:    true,
		recoveryEnabled:   true,
		metricsEnabled:    true,
		onError:           func(BaseEvent, error) {}, // no-op default
		logger:            Logger("event>"),
		channelBufferSize: 0, // blocking by default (sync)
		subTimeout:        DefaultSubscriberTimeout,
	}

	// Apply all options
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Option event options
type Option func(*eventOptions)

// WithSubscriberTimeout set subscriber timeout for event handlers
// if set to 0, timeout will be disabled and handlers will
// run indefinitely.
func WithSubscriberTimeout(v time.Duration) Option {
	return func(o *eventOptions) {
		o.subTimeout = v
	}
}

// WithTracing enable/disable tracing for event
func WithTracing(v bool) Option {
	return func(o *eventOptions) {
		o.tracingEnabled = v
	}
}

// WithRecovery enable/disable recovery for event
// recovery should always be enabled, can be disabled for testing.
func WithRecovery(v bool) Option {
	return func(o *eventOptions) {
		o.recoveryEnabled = v
	}
}

// WithMetrics enable/disable prometheus metrics for event
func WithMetrics(v bool, metrics Metrics) Option {
	return func(o *eventOptions) {
		o.metricsEnabled = v
		o.metrics = metrics
	}
}

// WithErrorHandler set error handler for panic recovery
func WithErrorHandler(v func(BaseEvent, error)) Option {
	return func(o *eventOptions) {
		if v != nil {
			o.onError = v
		}
	}
}

// WithLogger set logger for event
func WithLogger(l *slog.Logger) Option {
	return func(o *eventOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithTransport set transport for event
// Use this to provide a pre-configured transport with custom options
func WithTransport(t Transport) Option {
	return func(o *eventOptions) {
		if t != nil {
			o.transport = t
		}
	}
}

// WithChannelBufferSize set channel buffer size
// Only used when creating default transport
func WithChannelBufferSize(s uint) Option {
	return func(o *eventOptions) {
		o.channelBufferSize = s
	}
}

// WithRegistry set registry for event
func WithRegistry(r *Registry) Option {
	return func(o *eventOptions) {
		if r != nil {
			o.registry = r
		}
	}
}

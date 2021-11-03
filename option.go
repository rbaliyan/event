package event

import (
	"log"
)

var (
	// DefaultPublishTimeout default publish timeout in milliseconds
	DefaultPublishTimeout uint = 1000

	// DefaultAsyncTimeout default async timeout in milliseconds
	DefaultAsyncTimeout uint = 5000

	// DefaultSubscriberTimeout default  subscriber timeout in milliseconds
	DefaultSubscriberTimeout uint = 3000

	// DefaultChannelBufferSize default channel buffer size
	DefaultChannelBufferSize uint = 100

	// DefaultWorkerPoolSize max parallel active handlers
	DefaultWorkerPoolSize uint = 100
)

// eventConfig event configuration
type eventConfig struct {
	registry          *Registry
	asyncEnabled      bool
	recoveryEnabled   bool
	tracingEnabled    bool
	metricsEnabled    bool
	metrics           Metrics
	timeout           uint
	asyncTimeout           uint
	subTimeout uint
	channelBufferSize uint
	workerPoolSize 	  uint
	onError           func(Event, error)
	logger            *log.Logger
	transport Transport
}

// defaultErrorHandler default error handler
func defaultErrorHandler(Event, error) {}

// newEventOptions get new event options
func newEventOptions() *eventConfig {
	return &eventConfig{
		registry:          defaultRegistry,
		asyncEnabled:      true,
		tracingEnabled:    true,
		recoveryEnabled:   true,
		metricsEnabled:    true,
		onError:           defaultErrorHandler,
		logger:            Logger("Event>"),
		channelBufferSize: DefaultChannelBufferSize,
		timeout:           DefaultPublishTimeout,
		asyncTimeout: DefaultAsyncTimeout,
		subTimeout: DefaultSubscriberTimeout,
		workerPoolSize:    DefaultWorkerPoolSize,
	}
}

// Option event options
type Option func(*eventConfig)

// WithPublishTimeout set timeout for event publishing.
// if set to 0, timeout will be disabled and publisher will
// wait indefinitely.
func WithPublishTimeout(v uint) Option {
	return func(e *eventConfig) {
		e.timeout = v
	}
}

// WithAsyncTimeout set async timeout for event
// if set to 0, timeout will be disabled and handlers will
// wait indefinitely.
func WithAsyncTimeout(v uint) Option {
	return func(e *eventConfig) {
		e.asyncTimeout = v
	}
}

// WithSubscriberTimeout set subscriber timeout for event
// if set to 0, timeout will be disabled and handlers will
// wait indefinitely.
func WithSubscriberTimeout(v uint) Option {
	return func(e *eventConfig) {
		e.subTimeout = v
	}
}

// WithTracing enable/disable tracing for event
func WithTracing(v bool) Option {
	return func(e *eventConfig) {
		e.tracingEnabled = v
	}
}

// WithRecovery enable/disable recovery for event
// recovery should always be enabled, can be disabled for testing.
func WithRecovery(v bool) Option {
	return func(e *eventConfig) {
		e.recoveryEnabled = v
	}
}

// WithAsync enable/disable async handlers for event.
// if async handlers are disabled, event handlers are run in
// one single go routine and eventConfig.timeout is applied
// on publishing time. So if all handlers takes more than
// eventConfig.timeout milliseconds it will start dropping events.
func WithAsync(v bool) Option {
	return func(e *eventConfig) {
		e.asyncEnabled = v
	}
}

// WithMetrics  enable/disable  prometheus  metrics for event
func WithMetrics(v bool, metrics Metrics) Option {
	return func(e *eventConfig) {
		e.metricsEnabled = v
		e.metrics = metrics
	}
}

// WithErrorHandler set error handler for event
func WithErrorHandler(v func(Event, error)) Option {
	return func(e *eventConfig) {
		if v != nil {
			e.onError = v
		}
	}
}

// WithLogger set logger for event
func WithLogger(l *log.Logger) Option {
	return func(e *eventConfig) {
		if l != nil {
			e.logger = l
		}
	}
}

// WithTransport set transport for event
func WithTransport(l Transport) Option {
	return func(e *eventConfig) {
		if l != nil {
			e.transport = l
		}
	}
}

// WithChannelBufferSize set channel buffer size
func WithChannelBufferSize(s uint) Option {
	return func(e *eventConfig) {
		e.channelBufferSize = s
	}
}

// WithWorkerPoolSize set worker pool size.
// This value decides subscribers can execute in parallel.
func WithWorkerPoolSize(s uint) Option {
	return func(e *eventConfig) {
		e.workerPoolSize = s
	}
}

// WithRegistry set registry for event
func WithRegistry(r *Registry) Option {
	return func(e *eventConfig) {
		if r != nil {
			e.registry = r
		}
	}
}

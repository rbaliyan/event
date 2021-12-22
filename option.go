package event

import (
	"log"
	"time"
)

var (
	// DefaultPublishTimeout default publish pubTimeout in milliseconds
	DefaultPublishTimeout uint = 0

	// DefaultPoolTimeout default async pubTimeout in milliseconds
	DefaultPoolTimeout uint = 5000

	// DefaultSubscriberTimeout default  subscriber pubTimeout in milliseconds
	DefaultSubscriberTimeout uint = 0

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
	pubTimeout        time.Duration
	poolTimeout       time.Duration
	subTimeout        time.Duration
	channelBufferSize uint
	workerPoolSize    uint
	onError           func(Event, error)
	logger            *log.Logger
	transport         Transport
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
		logger:            Logger("event>"),
		channelBufferSize: DefaultChannelBufferSize,
		workerPoolSize:    DefaultWorkerPoolSize,
		pubTimeout:        time.Duration(DefaultPublishTimeout) * time.Millisecond,
		poolTimeout:       time.Duration(DefaultPoolTimeout) * time.Millisecond,
		subTimeout:        time.Duration(DefaultSubscriberTimeout) * time.Millisecond,
	}
}

// Option event options
type Option func(*eventConfig)

// WithPublishTimeout set pubTimeout for event publishing.
// if set to 0, pubTimeout will be disabled and publisher will
// wait indefinitely.
func WithPublishTimeout(v time.Duration) Option {
	return func(e *eventConfig) {
		e.pubTimeout = v
	}
}

// WithPoolTimeout set async pubTimeout for event
// if set to 0, pubTimeout will be disabled and handlers will
// wait indefinitely.
func WithPoolTimeout(v time.Duration) Option {
	return func(e *eventConfig) {
		e.poolTimeout = v
	}
}

// WithSubscriberTimeout set subscriber pubTimeout for event
// if set to 0, pubTimeout will be disabled and handlers will
// wait indefinitely.
func WithSubscriberTimeout(v time.Duration) Option {
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
// if async handlers are disabled, event handlers are run inChannel
// one single go routine and eventConfig.pubTimeout is applied
// on publishing time. So if all handlers takes more than
// eventConfig.pubTimeout milliseconds it will start dropping events.
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
// This value decides subscribers can execute inChannel parallel.
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

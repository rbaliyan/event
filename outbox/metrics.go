package outbox

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics provides OpenTelemetry metrics for outbox operations.
//
// Metrics tracks:
//   - outbox_messages_stored_total: Counter of messages stored in outbox
//   - outbox_messages_published_total: Counter of successfully published messages
//   - outbox_messages_failed_total: Counter of failed publish attempts
//   - outbox_messages_cleaned_total: Counter of messages cleaned up
//   - outbox_messages_pending: Gauge of current pending messages
//   - outbox_publish_duration_seconds: Histogram of publish duration
//
// Example:
//
//	metrics, err := outbox.NewMetrics()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	relay := outbox.NewRelay(store, transport).
//	    WithMetrics(metrics)
type Metrics struct {
	storedCounter     metric.Int64Counter
	publishedCounter  metric.Int64Counter
	failedCounter     metric.Int64Counter
	cleanedCounter    metric.Int64Counter
	pendingGauge      metric.Int64ObservableGauge
	publishDuration   metric.Float64Histogram

	pendingCallback func() int64
	pendingReg      metric.Registration
	mu              sync.Mutex
}

// MetricsOption is a functional option for configuring Metrics.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// WithMeterProvider sets a custom OpenTelemetry meter provider.
//
// If not set, the global meter provider is used.
func WithMeterProvider(mp metric.MeterProvider) MetricsOption {
	return func(o *metricsOptions) {
		o.meterProvider = mp
	}
}

// WithMetricsNamespace sets a prefix for all metric names.
//
// Example: WithMetricsNamespace("myapp") results in metrics like
// "myapp_outbox_messages_published_total"
func WithMetricsNamespace(ns string) MetricsOption {
	return func(o *metricsOptions) {
		o.namespace = ns
	}
}

// NewMetrics creates a new Metrics instance.
//
// The metrics are registered with the global OpenTelemetry meter provider
// by default. Use WithMeterProvider to use a custom provider.
//
// Example:
//
//	metrics, err := outbox.NewMetrics()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer metrics.Close()
func NewMetrics(opts ...MetricsOption) (*Metrics, error) {
	o := &metricsOptions{
		meterProvider: otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(o)
	}

	prefix := ""
	if o.namespace != "" {
		prefix = o.namespace + "_"
	}

	meter := o.meterProvider.Meter("github.com/rbaliyan/event/v3/outbox")

	m := &Metrics{}
	var err error

	m.storedCounter, err = meter.Int64Counter(
		prefix+"outbox_messages_stored_total",
		metric.WithDescription("Total number of messages stored in the outbox"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.publishedCounter, err = meter.Int64Counter(
		prefix+"outbox_messages_published_total",
		metric.WithDescription("Total number of messages successfully published from the outbox"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.failedCounter, err = meter.Int64Counter(
		prefix+"outbox_messages_failed_total",
		metric.WithDescription("Total number of messages that failed to publish"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.cleanedCounter, err = meter.Int64Counter(
		prefix+"outbox_messages_cleaned_total",
		metric.WithDescription("Total number of old messages cleaned up"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.publishDuration, err = meter.Float64Histogram(
		prefix+"outbox_publish_duration_seconds",
		metric.WithDescription("Time spent publishing a message from the outbox"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
	)
	if err != nil {
		return nil, err
	}

	// Register observable gauge for pending messages
	m.pendingGauge, err = meter.Int64ObservableGauge(
		prefix+"outbox_messages_pending",
		metric.WithDescription("Current number of pending messages in the outbox"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	// Register the callback
	m.pendingReg, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			m.mu.Lock()
			cb := m.pendingCallback
			m.mu.Unlock()
			if cb != nil {
				observer.ObserveInt64(m.pendingGauge, cb())
			}
			return nil
		},
		m.pendingGauge,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetPendingCallback sets the function to call when reading the pending gauge.
//
// The callback should return the current count of pending messages.
// This is typically connected to the store's count method.
//
// Example:
//
//	metrics.SetPendingCallback(func() int64 {
//	    count, _ := store.CountPending(ctx)
//	    return count
//	})
func (m *Metrics) SetPendingCallback(fn func() int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingCallback = fn
}

// RecordStored records that a message was stored in the outbox.
func (m *Metrics) RecordStored(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	m.storedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_name", eventName),
	))
}

// RecordPublished records that a message was successfully published.
func (m *Metrics) RecordPublished(ctx context.Context, eventName string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("event_name", eventName))
	m.publishedCounter.Add(ctx, 1, attrs)
	m.publishDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordFailed records that a message failed to publish.
func (m *Metrics) RecordFailed(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	m.failedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_name", eventName),
	))
}

// RecordCleaned records that messages were cleaned up.
func (m *Metrics) RecordCleaned(ctx context.Context, count int64) {
	if m == nil {
		return
	}
	m.cleanedCounter.Add(ctx, count)
}

// Close unregisters the metric callbacks.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	if m.pendingReg != nil {
		return m.pendingReg.Unregister()
	}
	return nil
}

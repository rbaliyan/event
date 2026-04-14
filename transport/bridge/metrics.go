package bridge

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const bridgeMeterName = "github.com/rbaliyan/event/v3/transport/bridge"

// Metrics provides OpenTelemetry instruments for the bridge pipeline.
//
// All methods are nil-safe — calling any method on a nil *Metrics is a
// no-op. Use [NewMetrics] to create an instance with the global meter
// provider, or pass [WithMeterProvider] for a custom provider (tests).
//
// Available instruments:
//
//   - bridge_messages_received_total:  counter of messages entering the pipeline
//   - bridge_messages_forwarded_total: counter of messages successfully forwarded to the sink
//   - bridge_messages_failed_total:    counter of pipeline errors (sink unavailable, middleware error)
//   - bridge_messages_skipped_total:   counter of messages intentionally dropped (dedup, filter)
//   - bridge_forward_duration_seconds: histogram of end-to-end pipeline latency
type Metrics struct {
	meter metric.Meter

	receivedTotal   metric.Int64Counter
	forwardedTotal  metric.Int64Counter
	failedTotal     metric.Int64Counter
	skippedTotal    metric.Int64Counter
	forwardDuration metric.Float64Histogram
}

// MetricsOption configures [NewMetrics].
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// WithMeterProvider sets a custom OTel meter provider. By default, the
// global provider is used.
func WithMeterProvider(provider metric.MeterProvider) MetricsOption {
	return func(o *metricsOptions) {
		if provider != nil {
			o.meterProvider = provider
		}
	}
}

// WithMetricsNamespace prefixes all instrument names. Useful when
// multiple bridge instances coexist and their metrics must be
// distinguished.
//
//	m, _ := bridge.NewMetrics(bridge.WithMetricsNamespace("orders"))
//	// → orders_bridge_messages_forwarded_total
func WithMetricsNamespace(namespace string) MetricsOption {
	return func(o *metricsOptions) {
		if namespace != "" {
			o.namespace = namespace + "_"
		}
	}
}

// NewMetrics creates a Metrics instance and registers all instruments
// on the configured meter provider.
func NewMetrics(opts ...MetricsOption) (*Metrics, error) {
	o := &metricsOptions{
		meterProvider: otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(o)
	}

	meter := o.meterProvider.Meter(bridgeMeterName)
	p := o.namespace

	m := &Metrics{meter: meter}
	var err error

	m.receivedTotal, err = meter.Int64Counter(
		p+"bridge_messages_received_total",
		metric.WithDescription("Total messages entering the bridge pipeline"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.forwardedTotal, err = meter.Int64Counter(
		p+"bridge_messages_forwarded_total",
		metric.WithDescription("Messages successfully forwarded to the sink"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.failedTotal, err = meter.Int64Counter(
		p+"bridge_messages_failed_total",
		metric.WithDescription("Messages that failed to forward (sink error, middleware error)"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.skippedTotal, err = meter.Int64Counter(
		p+"bridge_messages_skipped_total",
		metric.WithDescription("Messages intentionally dropped (dedup, filter)"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.forwardDuration, err = meter.Float64Histogram(
		p+"bridge_forward_duration_seconds",
		metric.WithDescription("End-to-end bridge pipeline latency"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.0005, 0.001, 0.005, 0.01, 0.025, 0.05,
			0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// RecordSkip increments the skipped counter. Call this from
// [Dedup]'s OnSkip callback or [Filter]'s drop path to count
// messages intentionally not forwarded to the sink.
//
//	bridge.Dedup(coord, keyFn, ttl,
//	    bridge.WithDedupOnSkip(func(event string, msg transport.Message) {
//	        m.RecordSkip(context.Background(), event)
//	    }),
//	)
func (m *Metrics) RecordSkip(ctx context.Context, event string) {
	if m == nil {
		return
	}
	m.skippedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", event),
	))
}

// MetricsMiddleware returns a [Middleware] that records pipeline
// throughput and latency using the provided [Metrics]. If m is nil
// the middleware is a no-op passthrough.
//
// The middleware increments received_total on every call, then
// delegates to next. On success it increments forwarded_total; on
// error, failed_total. Duration covers the full downstream call
// (including any inner middleware).
//
// Place this FIRST in the middleware chain to measure the complete
// pipeline, or LAST to measure only the sink publish:
//
//	bridge.WithMiddleware(
//	    bridge.MetricsMiddleware(m),   // outermost — sees everything
//	    bridge.Dedup(coord, keyFn, ttl),
//	    bridge.DLQ(dlqSink, "failed"),
//	)
func MetricsMiddleware(m *Metrics) Middleware {
	return func(next Handler) Handler {
		if m == nil {
			return next
		}
		return func(ctx context.Context, event string, msg transport.Message) error {
			attrs := metric.WithAttributes(attribute.String("event", event))

			m.receivedTotal.Add(ctx, 1, attrs)

			start := time.Now()
			err := next(ctx, event, msg)
			duration := time.Since(start).Seconds()

			m.forwardDuration.Record(ctx, duration, attrs)

			if err != nil {
				m.failedTotal.Add(ctx, 1, attrs)
			} else {
				m.forwardedTotal.Add(ctx, 1, attrs)
			}
			return err
		}
	}
}

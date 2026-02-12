package distributed

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecoveryMetrics provides OpenTelemetry metrics for recovery operations.
//
// Metrics tracked:
//   - recovery_events_recovered_total: Counter of events recovered (reset or re-published)
//   - recovery_events_republished_total: Counter of events re-published with payload
//   - recovery_events_reset_total: Counter of events reset without payload (legacy)
//   - recovery_errors_total: Counter of recovery errors
//   - recovery_events_skipped_total: Counter of events skipped (bus not found, etc.)
//   - recovery_pass_duration_seconds: Histogram of recovery pass duration
//
// Example:
//
//	metrics, err := distributed.NewRecoveryMetrics()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	runner := distributed.NewRecoveryRunner(sm,
//	    distributed.WithRecoveryMetrics(metrics),
//	)
type RecoveryMetrics struct {
	recoveredCounter   metric.Int64Counter
	republishedCounter metric.Int64Counter
	resetCounter       metric.Int64Counter
	errorsCounter      metric.Int64Counter
	skippedCounter     metric.Int64Counter
	passDuration       metric.Float64Histogram
}

// RecoveryMetricsOption configures RecoveryMetrics.
type RecoveryMetricsOption func(*recoveryMetricsOptions)

type recoveryMetricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// WithRecoveryMeterProvider sets a custom OpenTelemetry meter provider.
func WithRecoveryMeterProvider(mp metric.MeterProvider) RecoveryMetricsOption {
	return func(o *recoveryMetricsOptions) {
		o.meterProvider = mp
	}
}

// WithRecoveryMetricsNamespace sets a prefix for all metric names.
func WithRecoveryMetricsNamespace(ns string) RecoveryMetricsOption {
	return func(o *recoveryMetricsOptions) {
		o.namespace = ns
	}
}

// NewRecoveryMetrics creates recovery metrics registered with the OTel meter provider.
func NewRecoveryMetrics(opts ...RecoveryMetricsOption) (*RecoveryMetrics, error) {
	o := &recoveryMetricsOptions{
		meterProvider: otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(o)
	}

	prefix := ""
	if o.namespace != "" {
		prefix = o.namespace + "_"
	}

	meter := o.meterProvider.Meter("github.com/rbaliyan/event/v3/distributed")

	m := &RecoveryMetrics{}
	var err error

	m.recoveredCounter, err = meter.Int64Counter(
		prefix+"recovery_events_recovered_total",
		metric.WithDescription("Total number of events recovered (reset or re-published)"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	m.republishedCounter, err = meter.Int64Counter(
		prefix+"recovery_events_republished_total",
		metric.WithDescription("Total number of stale events re-published with stored payload"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	m.resetCounter, err = meter.Int64Counter(
		prefix+"recovery_events_reset_total",
		metric.WithDescription("Total number of stale events reset without payload (legacy entries)"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	m.errorsCounter, err = meter.Int64Counter(
		prefix+"recovery_errors_total",
		metric.WithDescription("Total number of recovery errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	m.skippedCounter, err = meter.Int64Counter(
		prefix+"recovery_events_skipped_total",
		metric.WithDescription("Total number of stale events skipped during recovery (bus not found, re-publish failed)"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	m.passDuration, err = meter.Float64Histogram(
		prefix+"recovery_pass_duration_seconds",
		metric.WithDescription("Duration of a single recovery pass"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *RecoveryMetrics) recordRecovered(ctx context.Context) {
	if m == nil {
		return
	}
	m.recoveredCounter.Add(ctx, 1)
}

func (m *RecoveryMetrics) recordRecoveredN(ctx context.Context, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.recoveredCounter.Add(ctx, n)
}

func (m *RecoveryMetrics) recordRepublished(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	m.republishedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_name", eventName),
	))
}

func (m *RecoveryMetrics) recordReset(ctx context.Context) {
	if m == nil {
		return
	}
	m.resetCounter.Add(ctx, 1)
}

func (m *RecoveryMetrics) recordResetN(ctx context.Context, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.resetCounter.Add(ctx, n)
}

func (m *RecoveryMetrics) recordError(ctx context.Context, operation string) {
	if m == nil {
		return
	}
	m.errorsCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
	))
}

func (m *RecoveryMetrics) recordSkipped(ctx context.Context, reason string, eventName string) {
	if m == nil {
		return
	}
	m.skippedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("reason", reason),
		attribute.String("event_name", eventName),
	))
}

func (m *RecoveryMetrics) recordPassDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.passDuration.Record(ctx, d.Seconds())
}

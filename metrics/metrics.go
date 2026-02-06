// Package metrics provides common types and interfaces for metrics recording
// across the event ecosystem.
//
// This package defines patterns and interfaces for consistent metrics recording
// across event-scheduler, event-dlq, and event-extras. It uses OpenTelemetry
// as the underlying metrics system.
//
// Usage:
//
//	import "github.com/rbaliyan/event/v3/metrics"
//
//	// Create recorder with consistent naming
//	meter := otel.Meter("myapp")
//	counters, _ := NewCounters(meter, "scheduled_messages",
//	    WithCounters("total", "delivered", "failed"))
//
//	// Use common attribute keys
//	counters.Inc(ctx, "total", metrics.AttrEventName("orders.created"))
package metrics

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Common attribute keys used across the ecosystem.
// Using consistent keys enables better correlation in observability tools.
const (
	// AttrKeyEventName is the event name being processed.
	AttrKeyEventName = "event.name"

	// AttrKeyStatus is the outcome status (success, error, etc.).
	AttrKeyStatus = "status"

	// AttrKeyError is the error type or category.
	AttrKeyError = "error"

	// AttrKeySagaName is the saga name.
	AttrKeySagaName = "saga.name"

	// AttrKeyStepName is the saga step name.
	AttrKeyStepName = "saga.step"

	// AttrKeySource is the source service or component.
	AttrKeySource = "source"
)

// AttrEventName creates an attribute for the event name.
func AttrEventName(name string) attribute.KeyValue {
	return attribute.String(AttrKeyEventName, name)
}

// AttrStatus creates an attribute for the status.
func AttrStatus(status string) attribute.KeyValue {
	return attribute.String(AttrKeyStatus, status)
}

// AttrError creates an attribute for the error type.
func AttrError(errType string) attribute.KeyValue {
	return attribute.String(AttrKeyError, errType)
}

// AttrSagaName creates an attribute for the saga name.
func AttrSagaName(name string) attribute.KeyValue {
	return attribute.String(AttrKeySagaName, name)
}

// AttrStepName creates an attribute for the step name.
func AttrStepName(name string) attribute.KeyValue {
	return attribute.String(AttrKeyStepName, name)
}

// AttrSource creates an attribute for the source.
func AttrSource(source string) attribute.KeyValue {
	return attribute.String(AttrKeySource, source)
}

// Recorder is a minimal interface for metrics recording.
// Components can implement this interface to allow mocking in tests.
type Recorder interface {
	// RecordSuccess records a successful operation.
	RecordSuccess(ctx context.Context, operation string, attrs ...attribute.KeyValue)

	// RecordError records a failed operation.
	RecordError(ctx context.Context, operation string, err error, attrs ...attribute.KeyValue)
}

// NopRecorder is a no-op implementation of Recorder for testing.
type NopRecorder struct{}

func (NopRecorder) RecordSuccess(ctx context.Context, operation string, attrs ...attribute.KeyValue) {
}
func (NopRecorder) RecordError(ctx context.Context, operation string, err error, attrs ...attribute.KeyValue) {
}

var _ Recorder = NopRecorder{}

// CounterSet manages a group of related counters with a common prefix.
type CounterSet struct {
	meter    metric.Meter
	prefix   string
	counters map[string]metric.Int64Counter
}

// CounterSetOption configures a CounterSet.
type CounterSetOption func(*counterSetOptions)

type counterSetOptions struct {
	names []string
}

// WithCounters specifies which counters to create.
func WithCounters(names ...string) CounterSetOption {
	return func(o *counterSetOptions) {
		o.names = names
	}
}

// NewCounterSet creates a new set of related counters.
//
// Example:
//
//	counters, _ := NewCounterSet(meter, "scheduled_messages",
//	    WithCounters("total", "delivered", "failed"))
//	counters.Add(ctx, "total", 1)
//	counters.Add(ctx, "delivered", 1)
func NewCounterSet(meter metric.Meter, prefix string, opts ...CounterSetOption) (*CounterSet, error) {
	cfg := &counterSetOptions{}
	for _, opt := range opts {
		opt(cfg)
	}

	cs := &CounterSet{
		meter:    meter,
		prefix:   prefix,
		counters: make(map[string]metric.Int64Counter, len(cfg.names)),
	}

	for _, name := range cfg.names {
		fullName := prefix + "_" + name
		counter, err := meter.Int64Counter(fullName)
		if err != nil {
			return nil, err
		}
		cs.counters[name] = counter
	}

	return cs, nil
}

// Add increments a counter by the given value.
func (cs *CounterSet) Add(ctx context.Context, name string, value int64, attrs ...attribute.KeyValue) {
	if counter, ok := cs.counters[name]; ok {
		counter.Add(ctx, value, metric.WithAttributes(attrs...))
	}
}

// Inc increments a counter by 1.
func (cs *CounterSet) Inc(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	cs.Add(ctx, name, 1, attrs...)
}

// HistogramSet manages a group of related histograms with a common prefix.
type HistogramSet struct {
	meter      metric.Meter
	prefix     string
	histograms map[string]metric.Float64Histogram
}

// HistogramSetOption configures a HistogramSet.
type HistogramSetOption func(*histogramSetOptions)

type histogramSetOptions struct {
	names []string
}

// WithHistograms specifies which histograms to create.
func WithHistograms(names ...string) HistogramSetOption {
	return func(o *histogramSetOptions) {
		o.names = names
	}
}

// NewHistogramSet creates a new set of related histograms.
//
// Example:
//
//	histograms, _ := NewHistogramSet(meter, "scheduler",
//	    WithHistograms("delivery_delay_seconds", "processing_duration_seconds"))
//	histograms.Record(ctx, "delivery_delay_seconds", 0.5)
func NewHistogramSet(meter metric.Meter, prefix string, opts ...HistogramSetOption) (*HistogramSet, error) {
	cfg := &histogramSetOptions{}
	for _, opt := range opts {
		opt(cfg)
	}

	hs := &HistogramSet{
		meter:      meter,
		prefix:     prefix,
		histograms: make(map[string]metric.Float64Histogram, len(cfg.names)),
	}

	for _, name := range cfg.names {
		fullName := prefix + "_" + name
		histogram, err := meter.Float64Histogram(fullName)
		if err != nil {
			return nil, err
		}
		hs.histograms[name] = histogram
	}

	return hs, nil
}

// Record records a value in a histogram.
func (hs *HistogramSet) Record(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) {
	if histogram, ok := hs.histograms[name]; ok {
		histogram.Record(ctx, value, metric.WithAttributes(attrs...))
	}
}

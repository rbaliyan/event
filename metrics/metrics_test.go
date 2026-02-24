package metrics

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestAttrFunctions(t *testing.T) {
	tests := []struct {
		fn    func(string) attribute.KeyValue
		key   string
		value string
	}{
		{AttrEventName, AttrKeyEventName, "orders.created"},
		{AttrStatus, AttrKeyStatus, "success"},
		{AttrError, AttrKeyError, "timeout"},
		{AttrSagaName, AttrKeySagaName, "order-saga"},
		{AttrStepName, AttrKeyStepName, "validate"},
		{AttrSource, AttrKeySource, "order-service"},
	}

	for _, tt := range tests {
		kv := tt.fn(tt.value)
		if string(kv.Key) != tt.key {
			t.Errorf("expected key %q, got %q", tt.key, kv.Key)
		}
		if kv.Value.AsString() != tt.value {
			t.Errorf("expected value %q, got %q", tt.value, kv.Value.AsString())
		}
	}
}

func TestNopRecorder(t *testing.T) {
	var r Recorder = NopRecorder{}
	ctx := context.Background()

	// Should not panic
	r.RecordSuccess(ctx, "test", AttrEventName("foo"))
	r.RecordError(ctx, "test", errors.New("err"), AttrEventName("foo"))
}

func TestCounterSet(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	cs, err := NewCounterSet(meter, "test_prefix",
		WithCounters("total", "success", "failed"))
	if err != nil {
		t.Fatalf("NewCounterSet: %v", err)
	}

	ctx := context.Background()
	cs.Inc(ctx, "total")
	cs.Inc(ctx, "success")
	cs.Add(ctx, "failed", 3)

	// Non-existent counter should be silently ignored
	cs.Inc(ctx, "nonexistent")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	counters := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					counters[m.Name] = dp.Value
				}
			}
		}
	}

	if counters["test_prefix_total"] != 1 {
		t.Errorf("total: got %d, want 1", counters["test_prefix_total"])
	}
	if counters["test_prefix_success"] != 1 {
		t.Errorf("success: got %d, want 1", counters["test_prefix_success"])
	}
	if counters["test_prefix_failed"] != 3 {
		t.Errorf("failed: got %d, want 3", counters["test_prefix_failed"])
	}
}

func TestHistogramSet(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	hs, err := NewHistogramSet(meter, "test_prefix",
		WithHistograms("duration", "delay"))
	if err != nil {
		t.Fatalf("NewHistogramSet: %v", err)
	}

	ctx := context.Background()
	hs.Record(ctx, "duration", 1.5)
	hs.Record(ctx, "delay", 0.25)

	// Non-existent histogram should be silently ignored
	hs.Record(ctx, "nonexistent", 1.0)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	histograms := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
				for _, dp := range hist.DataPoints {
					histograms[m.Name] = dp.Sum
				}
			}
		}
	}

	if histograms["test_prefix_duration"] != 1.5 {
		t.Errorf("duration: got %f, want 1.5", histograms["test_prefix_duration"])
	}
	if histograms["test_prefix_delay"] != 0.25 {
		t.Errorf("delay: got %f, want 0.25", histograms["test_prefix_delay"])
	}
}

func TestCounterSet_Empty(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	cs, err := NewCounterSet(meter, "empty")
	if err != nil {
		t.Fatalf("NewCounterSet: %v", err)
	}

	// Should not panic on empty set
	cs.Inc(context.Background(), "anything")
}

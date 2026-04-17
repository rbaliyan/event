package bridge_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/bridge"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// testBridgeMetrics creates a Metrics backed by a ManualReader.
func testBridgeMetrics(t *testing.T) (*bridge.Metrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := bridge.NewMetrics(bridge.WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	return m, reader
}

func collectBridgeMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return rm
}

func findBridgeMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func sumBridgeCounter(m *metricdata.Metrics) int64 {
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func histBridgeCount(m *metricdata.Metrics) uint64 {
	if m == nil {
		return 0
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		return 0
	}
	var total uint64
	for _, dp := range h.DataPoints {
		total += dp.Count
	}
	return total
}

// ---------- MetricsMiddleware ----------

func TestMetricsMiddleware_ForwardedCounter(t *testing.T) {
	m, reader := testBridgeMetrics(t)
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.MetricsMiddleware(m)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("m1"))
	src.inject("x", newMsg("m2"))

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 2 }, time.Second)

	rm := collectBridgeMetrics(t, reader)

	received := findBridgeMetric(rm, "bridge_messages_received_total")
	if got := sumBridgeCounter(received); got != 2 {
		t.Errorf("received_total = %d, want 2", got)
	}

	forwarded := findBridgeMetric(rm, "bridge_messages_forwarded_total")
	if got := sumBridgeCounter(forwarded); got != 2 {
		t.Errorf("forwarded_total = %d, want 2", got)
	}

	failed := findBridgeMetric(rm, "bridge_messages_failed_total")
	if got := sumBridgeCounter(failed); got != 0 {
		t.Errorf("failed_total = %d, want 0", got)
	}
}

func TestMetricsMiddleware_FailedCounter(t *testing.T) {
	m, reader := testBridgeMetrics(t)
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	sink.setPublishErr(errors.New("boom"))

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.MetricsMiddleware(m)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	var acked sync.WaitGroup
	acked.Add(1)
	msg := newMsg("f1")
	msg.ackFn = func(error) error { acked.Done(); return nil }
	src.inject("x", msg)
	acked.Wait()

	rm := collectBridgeMetrics(t, reader)

	received := findBridgeMetric(rm, "bridge_messages_received_total")
	if got := sumBridgeCounter(received); got != 1 {
		t.Errorf("received_total = %d, want 1", got)
	}

	forwarded := findBridgeMetric(rm, "bridge_messages_forwarded_total")
	if got := sumBridgeCounter(forwarded); got != 0 {
		t.Errorf("forwarded_total = %d, want 0", got)
	}

	failed := findBridgeMetric(rm, "bridge_messages_failed_total")
	if got := sumBridgeCounter(failed); got != 1 {
		t.Errorf("failed_total = %d, want 1", got)
	}
}

func TestMetricsMiddleware_Duration(t *testing.T) {
	m, reader := testBridgeMetrics(t)
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	// Add artificial latency via a middleware so the histogram records > 0.
	slow := func(next bridge.Handler) bridge.Handler {
		return func(ctx context.Context, ev string, msg transport.Message) error {
			time.Sleep(10 * time.Millisecond)
			return next(ctx, ev, msg)
		}
	}

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.MetricsMiddleware(m), slow),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("d1"))
	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)

	rm := collectBridgeMetrics(t, reader)

	dur := findBridgeMetric(rm, "bridge_forward_duration_seconds")
	if dur == nil {
		t.Fatal("forward_duration metric not found")
	}
	if got := histBridgeCount(dur); got != 1 {
		t.Errorf("duration count = %d, want 1", got)
	}
	h := dur.Data.(metricdata.Histogram[float64])
	if h.DataPoints[0].Sum < 0.01 {
		t.Errorf("duration sum = %f, want >= 0.01", h.DataPoints[0].Sum)
	}
}

func TestMetricsMiddleware_NilMetrics(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	// Should not panic with nil metrics.
	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.MetricsMiddleware(nil)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("m1"))
	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)
}

// ---------- RecordSkip ----------

func TestRecordSkip(t *testing.T) {
	m, reader := testBridgeMetrics(t)
	ctx := context.Background()

	m.RecordSkip(ctx, "orders", "insert")
	m.RecordSkip(ctx, "orders", "update")
	m.RecordSkip(ctx, "payments", "insert")

	rm := collectBridgeMetrics(t, reader)

	skipped := findBridgeMetric(rm, "bridge_messages_skipped_total")
	if got := sumBridgeCounter(skipped); got != 3 {
		t.Errorf("skipped_total = %d, want 3", got)
	}
}

func TestRecordSkip_NilMetrics(t *testing.T) {
	var m *bridge.Metrics
	// Must not panic.
	m.RecordSkip(context.Background(), "x", "insert")
}

// ---------- Dedup + Metrics composition ----------

func TestDedupWithMetricsSkip(t *testing.T) {
	m, reader := testBridgeMetrics(t)
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	coord := bridge.NewMemoryCoordinator()
	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(
			bridge.MetricsMiddleware(m),
			bridge.Dedup(coord, bridge.DefaultDedupKey, time.Minute,
				bridge.WithDedupOnSkip(func(event string, msg transport.Message) {
					m.RecordSkip(ctx, event, msg.Metadata()["operation"])
				}),
			),
		),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	// First is forwarded; second and third are deduped.
	src.inject("x", newMsg("dup"))
	src.inject("x", newMsg("dup"))
	src.inject("x", newMsg("dup"))

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)
	// Also wait for the skips to be recorded.
	waitFor(t, func() bool {
		var rm metricdata.ResourceMetrics
		reader.Collect(ctx, &rm)
		sk := findBridgeMetric(rm, "bridge_messages_skipped_total")
		return sumBridgeCounter(sk) == 2
	}, time.Second)

	rm := collectBridgeMetrics(t, reader)

	forwarded := findBridgeMetric(rm, "bridge_messages_forwarded_total")
	// Dedup drops 2 messages by returning nil → outer metrics sees
	// them as "success". Only 1 actually reached the sink.
	// The forwarded_total therefore reflects the metrics middleware's
	// view: 3 received, 3 "succeeded" (nil return).  The skipped_total
	// counter, fed by RecordSkip, gives the true picture.
	if got := sumBridgeCounter(forwarded); got != 3 {
		t.Errorf("forwarded_total = %d, want 3 (metrics sees nil return from dedup)", got)
	}

	skipped := findBridgeMetric(rm, "bridge_messages_skipped_total")
	if got := sumBridgeCounter(skipped); got != 2 {
		t.Errorf("skipped_total = %d, want 2", got)
	}
}

// ---------- Namespace ----------

func TestMetrics_WithNamespace(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	m, err := bridge.NewMetrics(bridge.WithMeterProvider(provider), bridge.WithMetricsNamespace("orders"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.RecordSkip(context.Background(), "x", "")

	rm := collectBridgeMetrics(t, reader)

	skipped := findBridgeMetric(rm, "orders_bridge_messages_skipped_total")
	if skipped == nil {
		t.Fatal("namespaced skipped metric not found")
	}
	if got := sumBridgeCounter(skipped); got != 1 {
		t.Errorf("skipped = %d, want 1", got)
	}

	nonPrefixed := findBridgeMetric(rm, "bridge_messages_skipped_total")
	if nonPrefixed != nil {
		t.Error("non-prefixed metric should not exist")
	}
}

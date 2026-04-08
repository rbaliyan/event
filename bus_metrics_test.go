package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// setupMetricsProvider installs a test meter provider and returns the reader
// and a cleanup function that restores the previous global provider.
func setupMetricsProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

// collectMetrics reads all metrics from the reader and returns maps keyed by metric name.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) (
	counters map[string]int64,
	histogramCounts map[string]uint64,
	int64Gauges map[string]int64,
	float64Gauges map[string]float64,
) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	counters = make(map[string]int64)
	histogramCounts = make(map[string]uint64)
	int64Gauges = make(map[string]int64)
	float64Gauges = make(map[string]float64)

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					counters[m.Name] += dp.Value
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					histogramCounts[m.Name] += dp.Count
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					int64Gauges[m.Name] += dp.Value
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					float64Gauges[m.Name] += dp.Value
				}
			}
		}
	}
	return
}

func TestHandlerDurationAndErrorMetrics(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus := mustNewBus(t, randomString(8), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ev := New[string]("handler-metrics-test")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatal(err)
	}

	// Subscribe a handler that alternates success/failure.
	done := make(chan struct{}, 2)
	var callCount int
	ev.Subscribe(context.Background(), func(ctx context.Context, e Event[string], data string) error {
		defer func() { done <- struct{}{} }()
		callCount++
		if data == "fail" {
			return errors.New("handler error")
		}
		return nil
	})

	// Publish success and failure.
	ev.Publish(context.Background(), "ok")
	if !wait(done, 500) {
		t.Fatal("timeout waiting for success handler")
	}
	ev.Publish(context.Background(), "fail")
	if !wait(done, 500) {
		t.Fatal("timeout waiting for failure handler")
	}

	// Allow metrics propagation.
	time.Sleep(10 * time.Millisecond)

	counters, histogramCounts, _, _ := collectMetrics(t, reader)

	// handler_duration should have been recorded for both calls.
	if histogramCounts["event.handler_duration_seconds"] != 2 {
		t.Errorf("handler_duration_seconds count: got %d, want 2", histogramCounts["event.handler_duration_seconds"])
	}

	// handler_errors should have been recorded for the failure.
	if counters["event.handler_errors_total"] != 1 {
		t.Errorf("handler_errors_total: got %d, want 1", counters["event.handler_errors_total"])
	}
}

func TestPublishAndSubscribeCounterMetrics(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus := mustNewBus(t, randomString(8), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ev := New[string]("counter-metrics-test")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{}, 1)
	ev.Subscribe(context.Background(), func(ctx context.Context, e Event[string], data string) error {
		done <- struct{}{}
		return nil
	})

	ev.Publish(context.Background(), "hello")
	if !wait(done, 500) {
		t.Fatal("timeout")
	}

	time.Sleep(10 * time.Millisecond)

	counters, histogramCounts, _, _ := collectMetrics(t, reader)

	if counters["event.published"] < 1 {
		t.Errorf("event.published: got %d, want >= 1", counters["event.published"])
	}
	if counters["event.subscribed"] < 1 {
		t.Errorf("event.subscribed: got %d, want >= 1", counters["event.subscribed"])
	}
	if histogramCounts["event.publish_duration_seconds"] < 1 {
		t.Errorf("event.publish_duration_seconds: got %d, want >= 1", histogramCounts["event.publish_duration_seconds"])
	}
}

// mockLagTransport wraps a channel transport and implements LagMonitor.
type mockLagTransport struct {
	transport.Transport
	lags []transport.ConsumerLag
}

func (m *mockLagTransport) ConsumerLag(_ context.Context) ([]transport.ConsumerLag, error) {
	return m.lags, nil
}

var _ transport.LagMonitor = (*mockLagTransport)(nil)

func TestConsumerLagGauges(t *testing.T) {
	reader := setupMetricsProvider(t)

	lagTransport := &mockLagTransport{
		Transport: channel.New(),
		lags: []transport.ConsumerLag{
			{
				Event:           "orders.created",
				ConsumerGroup:   "processor",
				Lag:             42,
				PendingMessages: 5,
				OldestPending:   10 * time.Second,
			},
		},
	}

	bus := mustNewBus(t, randomString(8), WithTransport(lagTransport))
	defer bus.Close(context.Background())

	_, _, int64Gauges, float64Gauges := collectMetrics(t, reader)

	if int64Gauges["event.consumer_lag"] != 42 {
		t.Errorf("event.consumer_lag: got %d, want 42", int64Gauges["event.consumer_lag"])
	}
	if int64Gauges["event.pending_messages"] != 5 {
		t.Errorf("event.pending_messages: got %d, want 5", int64Gauges["event.pending_messages"])
	}
	if float64Gauges["event.oldest_pending_seconds"] != 10.0 {
		t.Errorf("event.oldest_pending_seconds: got %f, want 10.0", float64Gauges["event.oldest_pending_seconds"])
	}
}

func TestConsumerLagGaugesNotRegisteredWithoutLagMonitor(t *testing.T) {
	reader := setupMetricsProvider(t)

	// Channel transport does not implement LagMonitor.
	bus := mustNewBus(t, randomString(8), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	_, _, int64Gauges, float64Gauges := collectMetrics(t, reader)

	// No lag gauges should be present.
	if _, ok := int64Gauges["event.consumer_lag"]; ok {
		t.Error("event.consumer_lag should not be registered without LagMonitor")
	}
	if _, ok := float64Gauges["event.oldest_pending_seconds"]; ok {
		t.Error("event.oldest_pending_seconds should not be registered without LagMonitor")
	}
}

func TestMetricsDisabled(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus := mustNewBus(t, randomString(8),
		WithTransport(channel.New()),
		WithMetrics(false),
	)
	defer bus.Close(context.Background())

	ev := New[string]("no-metrics-test")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{}, 1)
	ev.Subscribe(context.Background(), func(ctx context.Context, e Event[string], data string) error {
		done <- struct{}{}
		return nil
	})

	ev.Publish(context.Background(), "x")
	if !wait(done, 500) {
		t.Fatal("timeout")
	}

	time.Sleep(10 * time.Millisecond)

	counters, histogramCounts, _, _ := collectMetrics(t, reader)

	if counters["event.published"] != 0 {
		t.Errorf("event.published should be 0 when metrics disabled, got %d", counters["event.published"])
	}
	if histogramCounts["event.handler_duration_seconds"] != 0 {
		t.Errorf("handler_duration_seconds should be 0 when metrics disabled, got %d", histogramCounts["event.handler_duration_seconds"])
	}
}

func TestLagCallbackUnregisteredOnClose(t *testing.T) {
	reader := setupMetricsProvider(t)

	lagTransport := &mockLagTransport{
		Transport: channel.New(),
		lags: []transport.ConsumerLag{
			{Event: "test", Lag: 100},
		},
	}

	bus := mustNewBus(t, randomString(8), WithTransport(lagTransport))

	// Verify lag is reported before close.
	_, _, int64Gauges, _ := collectMetrics(t, reader)
	if int64Gauges["event.consumer_lag"] != 100 {
		t.Fatalf("expected lag 100 before close, got %d", int64Gauges["event.consumer_lag"])
	}

	bus.Close(context.Background())

	// After close, the callback should be unregistered.
	// Update the transport lags — they should NOT appear.
	lagTransport.lags = []transport.ConsumerLag{
		{Event: "test", Lag: 999},
	}

	_, _, int64Gauges, _ = collectMetrics(t, reader)
	if int64Gauges["event.consumer_lag"] == 999 {
		t.Error("lag callback still active after Close()")
	}
}

func TestMultiBusMetricsDistinguishedByBusLabel(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus1 := mustNewBus(t, "bus-alpha", WithTransport(channel.New()))
	defer bus1.Close(context.Background())
	bus2 := mustNewBus(t, "bus-beta", WithTransport(channel.New()))
	defer bus2.Close(context.Background())

	ev1 := New[string]("shared-event")
	ev2 := New[string]("shared-event")
	Register(context.Background(), bus1, ev1)
	Register(context.Background(), bus2, ev2)

	// Collect and verify we get two distinct registered_events data points.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	busValues := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "event.registered_events" {
				continue
			}
			if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok {
				for _, dp := range gauge.DataPoints {
					for _, attr := range dp.Attributes.ToSlice() {
						if string(attr.Key) == "bus" {
							busValues[attr.Value.AsString()] = dp.Value
						}
					}
				}
			}
		}
	}

	if len(busValues) != 2 {
		t.Fatalf("expected 2 bus labels, got %d: %v", len(busValues), busValues)
	}
	if busValues["bus-alpha"] != 1 {
		t.Errorf("bus-alpha registered_events: got %d, want 1", busValues["bus-alpha"])
	}
	if busValues["bus-beta"] != 1 {
		t.Errorf("bus-beta registered_events: got %d, want 1", busValues["bus-beta"])
	}
}

func TestRegisteredEventsAndActiveSubscribersGauges(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus := mustNewBus(t, randomString(8), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Before registering any events.
	_, _, int64Gauges, _ := collectMetrics(t, reader)
	if int64Gauges["event.registered_events"] != 0 {
		t.Errorf("registered_events before registration: got %d, want 0", int64Gauges["event.registered_events"])
	}

	// Register two events.
	ev1 := New[string]("gauge-event-1")
	ev2 := New[string]("gauge-event-2")
	if err := Register(context.Background(), bus, ev1); err != nil {
		t.Fatal(err)
	}
	if err := Register(context.Background(), bus, ev2); err != nil {
		t.Fatal(err)
	}

	_, _, int64Gauges, _ = collectMetrics(t, reader)
	if int64Gauges["event.registered_events"] != 2 {
		t.Errorf("registered_events: got %d, want 2", int64Gauges["event.registered_events"])
	}

	// Add subscribers to ev1.
	ctx := context.Background()
	noop := func(_ context.Context, _ Event[string], _ string) error { return nil }
	ev1.Subscribe(ctx, noop)
	ev1.Subscribe(ctx, noop)
	ev2.Subscribe(ctx, noop)

	// Allow goroutines to start.
	time.Sleep(10 * time.Millisecond)

	_, _, int64Gauges, _ = collectMetrics(t, reader)

	// active_subscribers is summed across all events in collectMetrics,
	// so total should be 3 (2 on ev1 + 1 on ev2).
	if int64Gauges["event.active_subscribers"] != 3 {
		t.Errorf("active_subscribers total: got %d, want 3", int64Gauges["event.active_subscribers"])
	}
}

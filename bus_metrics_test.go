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
	"go.opentelemetry.io/otel/trace"
)

// resetGauges resets gauge registration state so tests can use a fresh meter provider.
func resetGauges() {
	gaugesMu.Lock()
	defer gaugesMu.Unlock()
	gaugesInit = false
}

// setupMetricsProvider installs a test meter provider and returns the reader
// and a cleanup function that restores the previous global provider.
func setupMetricsProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	resetGauges() // Allow gauge re-registration with the new provider
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		resetGauges()
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

// eventuallyMetrics polls collectMetrics until the predicate returns true or
// the deadline fires. Replaces the time.Sleep + collect + assert pattern:
// OTel metric recording is asynchronous, so the handler's recordHandlerDuration
// call may not have updated the reader's view by the time the test reads it.
// Polling exits the instant the metric arrives, not after a fixed wait.
//
// The predicate receives the four metric maps the test would otherwise read
// inline; tests express their actual contract directly inside the predicate.
func eventuallyMetrics(
	t *testing.T,
	reader *sdkmetric.ManualReader,
	timeout time.Duration,
	predicate func(counters map[string]int64, histograms map[string]uint64, int64Gauges map[string]int64, float64Gauges map[string]float64) bool,
	msg string,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		counters, histograms, int64Gauges, float64Gauges := collectMetrics(t, reader)
		if predicate(counters, histograms, int64Gauges, float64Gauges) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("eventuallyMetrics: %s (after %s)", msg, timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
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

	// Poll until both metrics arrive — OTel recording is async after the
	// handler returns, but at most ~tens of ms in practice.
	eventuallyMetrics(t, reader, 2*time.Second,
		func(counters map[string]int64, histograms map[string]uint64, _ map[string]int64, _ map[string]float64) bool {
			return histograms["event.handler_duration_seconds"] == 2 &&
				counters["event.handler_errors_total"] == 1
		},
		"expected 2 handler_duration_seconds samples and 1 handler_errors_total",
	)
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

	// Three metrics need to settle: published counter, subscribed counter,
	// publish_duration histogram. They're recorded in different places in
	// the bus pipeline so they may arrive at the reader at slightly
	// different times — the AND predicate ensures all three are present
	// before the assertions run.
	eventuallyMetrics(t, reader, 2*time.Second,
		func(counters map[string]int64, histograms map[string]uint64, _ map[string]int64, _ map[string]float64) bool {
			return counters["event.published"] >= 1 &&
				counters["event.subscribed"] >= 1 &&
				histograms["event.publish_duration_seconds"] >= 1
		},
		"expected event.published, event.subscribed counters and publish_duration_seconds histogram",
	)
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
				OldestPending:   durationPtr(10 * time.Second),
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

	// With metrics disabled the bus skips ALL metric emission — there is
	// nothing to "propagate", so we read the metrics state immediately.
	// The previous 10ms sleep was a defensive wait; now redundant because
	// the contract is "metrics are never emitted when disabled", not
	// "metrics are emitted but eventually we observe zero".
	counters, histogramCounts, _, _ := collectMetrics(t, reader)

	if counters["event.published"] != 0 {
		t.Errorf("event.published should be 0 when metrics disabled, got %d", counters["event.published"])
	}
	if histogramCounts["event.handler_duration_seconds"] != 0 {
		t.Errorf("handler_duration_seconds should be 0 when metrics disabled, got %d", histogramCounts["event.handler_duration_seconds"])
	}
}

func TestLagNotReportedAfterClose(t *testing.T) {
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

	// After close, bus is removed from registry so the callback skips it.
	lagTransport.lags = []transport.ConsumerLag{
		{Event: "test", Lag: 999},
	}

	_, _, int64Gauges, _ = collectMetrics(t, reader)
	if int64Gauges["event.consumer_lag"] == 999 {
		t.Error("lag still reported after Close()")
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

	// Poll for the active_subscribers gauge to reach 3 — Subscribe registers
	// the gauge contribution asynchronously after spawning the subscriber
	// goroutine, so a single immediate read may catch it mid-update.
	eventuallyMetrics(t, reader, 2*time.Second,
		func(_ map[string]int64, _ map[string]uint64, int64Gauges map[string]int64, _ map[string]float64) bool {
			return int64Gauges["event.active_subscribers"] == 3
		},
		"expected active_subscribers total to reach 3 (2 on ev1 + 1 on ev2)",
	)
}

// TestFilterDropCounter verifies that event_messages_filter_dropped_total is
// incremented when WithMessageFilter rejects a message before handler dispatch.
// This is the key observable for cross-stream delivery bugs: if an insert
// lands in an update-only consumer group, the counter records
// {event="...", operation="insert"} rather than the expected operation type.
func TestFilterDropCounter(t *testing.T) {
	reader := setupMetricsProvider(t)

	bus := mustNewBus(t, randomString(8), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Event accepts only messages with operation="update".
	ev := New[string]("filter-drop-test",
		WithMessageFilter(func(meta map[string]string) bool {
			return meta["operation"] == "update"
		}),
	)
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatal(err)
	}

	received := make(chan string, 5)
	if err := ev.Subscribe(context.Background(), func(_ context.Context, _ Event[string], data string) error {
		received <- data
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Publish one matching message and two that will be filtered.
	// We inject directly via the bus transport so we can set the operation
	// metadata that WithMessageFilter inspects.
	pub := func(op, val string) {
		t.Helper()
		msg := transport.NewMessage(transport.NewID(), "test", []byte(`"`+val+`"`),
			map[string]string{"operation": op}, trace.SpanContext{})
		if err := bus.Transport().Publish(ctx, "filter-drop-test", msg); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	pub("insert", "ins1")  // filtered
	pub("update", "upd1")  // passes
	pub("delete", "del1")  // filtered

	// Wait for the passing message to arrive.
	select {
	case got := <-received:
		if got != "upd1" {
			t.Errorf("received %q, want upd1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Poll until both filter drops have been recorded — the filter
	// middleware increments the counter asynchronously after the message
	// is rejected, separate from the receive-side ack path.
	eventuallyMetrics(t, reader, 2*time.Second,
		func(counters map[string]int64, _ map[string]uint64, _ map[string]int64, _ map[string]float64) bool {
			return counters["event_messages_filter_dropped_total"] == 2
		},
		"expected event_messages_filter_dropped_total == 2 (insert + delete were filtered)",
	)
}

func durationPtr(d time.Duration) *time.Duration { return &d }

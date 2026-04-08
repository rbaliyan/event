package event

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/rbaliyan/event/v3/transport"
)

const meterName = "github.com/rbaliyan/event/v3"

var (
	gaugesMu   sync.Mutex
	gaugesInit bool
)

// sharedMeter returns the single OTel meter used by all buses.
// Using one meter avoids duplicate instrument registrations that cause
// Prometheus exporter conflicts when multiple buses exist.
func sharedMeter() metric.Meter {
	return otel.Meter(meterName)
}

// resetGauges allows tests to reset the gauge registration state
// so that a new meter provider can be used.
func resetGauges() {
	gaugesMu.Lock()
	defer gaugesMu.Unlock()
	gaugesInit = false
}

// initGauges registers package-level observable gauges once.
// A single callback iterates all buses in the global registry,
// emitting per-bus data points distinguished by a "bus" attribute.
func initGauges(meter metric.Meter) {
	gaugesMu.Lock()
	defer gaugesMu.Unlock()
	if gaugesInit {
		return
	}
	gaugesInit = true

	{
		// Event/subscriber gauges
		registeredGauge, _ := meter.Int64ObservableGauge("event.registered_events",
			metric.WithDescription("Number of events currently registered on this bus"),
			metric.WithUnit("{event}"))
		subscribersGauge, _ := meter.Int64ObservableGauge("event.active_subscribers",
			metric.WithDescription("Number of active subscribers per event"),
			metric.WithUnit("{subscriber}"))

		// Consumer lag gauges
		lagGauge, _ := meter.Int64ObservableGauge("event.consumer_lag",
			metric.WithDescription("Number of unprocessed messages per event and consumer group"),
			metric.WithUnit("{message}"))
		pendingGauge, _ := meter.Int64ObservableGauge("event.pending_messages",
			metric.WithDescription("Messages delivered but not yet acknowledged per event and consumer group"),
			metric.WithUnit("{message}"))
		oldestPendingGauge, _ := meter.Float64ObservableGauge("event.oldest_pending_seconds",
			metric.WithDescription("Age of the oldest unacknowledged message per event and consumer group"),
			metric.WithUnit("s"))

		meter.RegisterCallback(
			func(ctx context.Context, observer metric.Observer) error {
				busRegistry.Range(func(key, value any) bool {
					bus := value.(*Bus)
					if !bus.metricsEnabled {
						return true
					}
					busAttr := bus.busAttr

					// Event and subscriber counts
					bus.eventMutex.RLock()
					observer.ObserveInt64(registeredGauge, int64(len(bus.events)),
						metric.WithAttributes(busAttr))
					for _, ev := range bus.events {
						if topo, ok := ev.(eventTopology); ok {
							observer.ObserveInt64(subscribersGauge, topo.subscriberCount(),
								metric.WithAttributes(busAttr, attribute.String("event", topo.eventName())))
						}
					}
					bus.eventMutex.RUnlock()

					// Consumer lag (if transport supports it)
					if lm, ok := bus.transport.(transport.LagMonitor); ok {
						lags, err := lm.ConsumerLag(ctx)
						if err == nil {
							for _, lag := range lags {
								attrs := metric.WithAttributes(
									busAttr,
									attribute.String("event", lag.Event),
									attribute.String("consumer_group", lag.ConsumerGroup),
								)
								observer.ObserveInt64(lagGauge, lag.Lag, attrs)
								observer.ObserveInt64(pendingGauge, lag.PendingMessages, attrs)
								observer.ObserveFloat64(oldestPendingGauge, lag.OldestPending.Seconds(), attrs)
							}
						}
					}

					return true
				})
				return nil
			},
			registeredGauge, subscribersGauge, lagGauge, pendingGauge, oldestPendingGauge,
		)
	}
}

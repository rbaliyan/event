package event

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

var _ Metrics = &metrics{}

type dummyMetrics struct{}

type metrics struct {
	registered prometheus.Counter
	published  prometheus.Counter
	publishing prometheus.Gauge
	subscribed prometheus.Counter
	processed  prometheus.Counter
	processing prometheus.Gauge
}

// NewMetric create new metrics
func NewMetric(namespace, name string) Metrics {
	if namespace == "" {
		namespace = "event"
	}
	return &metrics{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: name,
			Name:      "published_total",
			Help:      "Total messages published",
		}),
		publishing: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: name,
			Name:      "publishing_count",
			Help:      "Counte of messages being processed",
		}),
		subscribed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: name,
			Name:      "subscribed_total",
			Help:      "Total subscribers added",
		}),
		processed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: name,
			Name:      "processed_total",
			Help:      "Total messages processed",
		}),
		processing: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: name,
			Name:      "processing_count",
			Help:      "Count of messages being processed",
		}),
	}
}

// Register register metrics
func (m *metrics) Register(r prometheus.Registerer) error {
	var mErr error
	if r == nil {
		r = prometheus.DefaultRegisterer
	}
	if err := r.Register(m.published); err != nil {
		mErr = multierr.Append(mErr, err)
	}
	if err := r.Register(m.subscribed); err != nil {
		mErr = multierr.Append(mErr, err)
	}
	if err := r.Register(m.publishing); err != nil {
		mErr = multierr.Append(mErr, err)
	}
	if err := r.Register(m.processing); err != nil {
		mErr = multierr.Append(mErr, err)
	}
	if err := r.Register(m.processed); err != nil {
		mErr = multierr.Append(mErr, err)
	}
	return mErr
}

// Processing event processing started
func (m *metrics) Publishing() {
	m.publishing.Inc()
}

// Published event published
func (m *metrics) Published() {
	m.publishing.Dec()
	m.published.Inc()
}

// Processing event processing started
func (m *metrics) Processing() {
	m.processing.Inc()
}

// Done event processing done
func (m *metrics) Processed() {
	m.publishing.Dec()
	m.processed.Inc()
}

// Subscribed a subscriber was added inChannel the event
func (m *metrics) Subscribed() {
	m.subscribed.Inc()
}

func (dummyMetrics) Register(r prometheus.Registerer) error { return nil }

// Processing event processing started
func (dummyMetrics) Publishing() {}

// Published event published
func (dummyMetrics) Published() {}

// Processing event processing started
func (dummyMetrics) Processing() {}

// Done event processing done
func (dummyMetrics) Processed() {}

// Subscribed a subscriber was added inChannel the event
func (dummyMetrics) Subscribed() {}

package bridge

import (
	"log/slog"

	"github.com/rbaliyan/event/v3/transport"
)

// Option configures a bridge [Transport].
type Option func(*Transport)

// WithMiddleware installs pipeline middleware that runs on every source
// message before the terminal sink publish. Middleware compose in
// declaration order: the first middleware is the outermost wrapper.
//
// Multiple calls to WithMiddleware append to the existing chain,
// preserving order. Pass middleware in the order you want them to run:
//
//	bridge.WithMiddleware(
//	    bridge.Observe(metricsHooks), // outermost: sees every message
//	    bridge.Dedup(coord, key, ttl), // drop duplicates early
//	    bridge.Filter(keep),           // drop uninteresting events
//	    bridge.DLQ(dlqSink, "failed"), // innermost: catch sink errors
//	)
//
// The bridge installs no middleware by default — a bridge with no
// middleware is a pure source→sink forwarder.
func WithMiddleware(mws ...Middleware) Option {
	return func(t *Transport) {
		t.middleware = append(t.middleware, mws...)
	}
}

// WithLogger sets the logger used for internal diagnostics.
func WithLogger(l *slog.Logger) Option {
	return func(t *Transport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithErrorHandler installs a callback invoked for asynchronous errors
// from the pump (pipeline errors not caught by middleware, ack errors).
// Intended for metrics and alerting. The callback MUST NOT block.
//
// For per-message observability, prefer [Observe] middleware — it runs
// inside the pipeline and has access to the event name and message.
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithPumpBuffer sets the buffer size for each pump's source
// subscription. Must be >= 1. Defaults to 256. Increase when source
// message bursts outpace the middleware pipeline.
func WithPumpBuffer(n int) Option {
	return func(t *Transport) {
		if n >= 1 {
			t.pumpBuffer = n
		}
	}
}

// WithPumpSubscribeOptions appends options passed to the source
// transport's Subscribe call made by each pump. Useful for sources
// that support consumer IDs, routing filters, or custom start
// positions. Applied after the bridge's own default options so they
// can override defaults.
func WithPumpSubscribeOptions(opts ...transport.SubscribeOption) Option {
	return func(t *Transport) {
		t.pumpSubscribeOpts = append(t.pumpSubscribeOpts, opts...)
	}
}

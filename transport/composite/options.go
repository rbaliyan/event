package composite

import (
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/persistent"
)

// Option configures the composite transport.
type Option func(*Transport)

// WithCheckpointStore sets the checkpoint store for resume after restart.
// Without a checkpoint store, subscribers start from the beginning on restart.
func WithCheckpointStore(store persistent.CheckpointStore) Option {
	return func(t *Transport) {
		t.checkpointStore = store
	}
}

// WithCodec sets the codec for message serialization.
// Default: JSON codec.
func WithCodec(c codec.Codec) Option {
	return func(t *Transport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(t *Transport) {
		if logger != nil {
			t.logger = logger
		}
	}
}

// WithSignalPrefix sets the prefix for signal event names.
// This prevents collision between durable event names and signal events.
// Default: "_sig:".
func WithSignalPrefix(prefix string) Option {
	return func(t *Transport) {
		if prefix != "" {
			t.signalPrefix = prefix
		}
	}
}

// WithPollInterval sets the fallback poll interval.
// When signal transport is unavailable or misses a notification,
// this interval determines how often to poll the durable store.
// Default: 5 seconds.
func WithPollInterval(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.pollInterval = d
		}
	}
}

// WithBufferSize sets the subscription message channel buffer size.
// Default: 1 (sequential processing).
func WithBufferSize(size int) Option {
	return func(t *Transport) {
		if size > 0 {
			t.bufferSize = size
		}
	}
}

// WithErrorHandler sets the error callback for non-fatal errors.
// This is called for signal transport failures that don't affect durability.
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

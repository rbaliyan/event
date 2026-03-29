package migration

import "log/slog"

// Option configures the migration transport.
type Option func(*options)

type options struct {
	logger          *slog.Logger
	mergedBufSize   int
}

func defaultOptions() *options {
	return &options{
		logger:        nil, // resolved in New
		mergedBufSize: defaultMergedBufferSize,
	}
}

// WithLogger sets a custom logger.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithMergedBufferSize sets the buffer size for the merged subscription channel
// that fans-in messages from both transports. Defaults to 64.
func WithMergedBufferSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.mergedBufSize = size
		}
	}
}

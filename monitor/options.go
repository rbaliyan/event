package monitor

import (
	"time"
)

// storeOptions holds configuration for monitor stores.
type storeOptions struct {
	tableName       string
	cleanupInterval time.Duration
	asyncWrites     bool
	asyncBufferSize int
	batchSize       int
	flushInterval   time.Duration
	samplingRate    float64
}

// defaultStoreOptions returns the default store options.
func defaultStoreOptions() *storeOptions {
	return &storeOptions{
		tableName:       "monitor_entries",
		cleanupInterval: time.Minute,
		asyncWrites:     false,
		asyncBufferSize: 0,
		batchSize:       0,
		flushInterval:   0,
		samplingRate:    1.0, // 100% - record all events
	}
}

// StoreOption configures a Store.
type StoreOption func(*storeOptions)

// WithTableName sets the table name for database stores.
//
// Default is "monitor_entries". Use this for multi-tenant deployments
// or when you need multiple monitor stores.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithTableName("orders_monitor"),
//	)
func WithTableName(name string) StoreOption {
	return func(o *storeOptions) {
		if name != "" {
			o.tableName = name
		}
	}
}

// WithCleanupInterval sets how often expired entries are removed.
//
// Default is 1 minute. Set to 0 to disable automatic cleanup.
// When disabled, you should call DeleteOlderThan manually.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithCleanupInterval(5 * time.Minute),
//	)
func WithCleanupInterval(interval time.Duration) StoreOption {
	return func(o *storeOptions) {
		o.cleanupInterval = interval
	}
}

// WithAsyncWrites enables asynchronous writes with the specified buffer size.
//
// When enabled, Record and UpdateStatus calls return immediately and writes
// are performed in a background goroutine. This significantly reduces latency
// but may lose monitor data if the process crashes before the buffer is flushed.
//
// Use this for high-throughput scenarios where some data loss is acceptable.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithAsyncWrites(1000),
//	)
func WithAsyncWrites(bufferSize int) StoreOption {
	return func(o *storeOptions) {
		o.asyncWrites = true
		if bufferSize > 0 {
			o.asyncBufferSize = bufferSize
		} else {
			o.asyncBufferSize = 1000
		}
	}
}

// WithBatchSize enables batch writes with the specified batch size.
//
// When enabled, writes are accumulated and written in batches for efficiency.
// Use together with WithFlushInterval to control how often batches are flushed.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithBatchSize(100),
//	    monitor.WithFlushInterval(time.Second),
//	)
func WithBatchSize(size int) StoreOption {
	return func(o *storeOptions) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// WithFlushInterval sets the maximum time between batch flushes.
//
// Use together with WithBatchSize. Even if the batch is not full,
// it will be flushed after this interval.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithBatchSize(100),
//	    monitor.WithFlushInterval(time.Second),
//	)
func WithFlushInterval(interval time.Duration) StoreOption {
	return func(o *storeOptions) {
		if interval > 0 {
			o.flushInterval = interval
		}
	}
}

// WithSampling enables probabilistic sampling of monitor entries.
//
// Rate must be between 0.0 and 1.0:
//   - 1.0: Record all events (default)
//   - 0.1: Record 10% of events
//   - 0.01: Record 1% of events
//
// Use this for extremely high-volume scenarios where recording every
// event is not feasible. Note that sampling may miss important events.
//
// Example:
//
//	store := monitor.NewPostgresStore(db,
//	    monitor.WithSampling(0.1), // Record 10% of events
//	)
func WithSampling(rate float64) StoreOption {
	return func(o *storeOptions) {
		if rate >= 0 && rate <= 1.0 {
			o.samplingRate = rate
		}
	}
}

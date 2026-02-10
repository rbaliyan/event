// Package batch provides batch processing capabilities for high-throughput event handling.
//
// Batch processing improves throughput by:
//   - Reducing per-message overhead
//   - Enabling bulk database operations
//   - Amortizing network latency across multiple messages
//
// # Overview
//
// The package provides two main components:
//   - Processor: Automatic batch processing with timeout-based flushing
//   - Collector: Manual batch collection for custom processing logic
//
// # When to Use Batching
//
// Use batching when:
//   - Processing high volumes of messages
//   - Database operations support bulk inserts/updates
//   - Network calls can be combined (e.g., batch API calls)
//   - Per-message overhead is significant
//
// Avoid batching when:
//   - Low latency is critical for each message
//   - Messages must be processed individually
//   - Batch failures should not affect other messages
//
// # Basic Usage
//
// Use Processor for automatic batch handling:
//
//	processor := batch.NewProcessor[Order](
//	    batch.WithBatchSize(100),
//	    batch.WithTimeout(time.Second),
//	)
//
//	err := processor.Process(ctx, subscription.Messages(), func(ctx context.Context, orders []Order) error {
//	    return db.BulkInsert(ctx, orders)
//	})
//
// # Manual Collection
//
// Use Collector for more control over batch processing:
//
//	collector := batch.NewCollector[Order](100, time.Second)
//
//	for msg := range messages {
//	    order := msg.Payload().(Order)
//	    if collector.Add(msg, order) {
//	        batch, msgs := collector.Flush()
//	        processBatch(batch, msgs)
//	    }
//	}
//
// # Error Handling
//
// When a batch fails, all messages in the batch are Nack'd. Configure
// error handling with WithOnError:
//
//	processor := batch.NewProcessor[Order](
//	    batch.WithOnError(func(batch []any, err error) {
//	        log.Error("batch failed", "size", len(batch), "error", err)
//	    }),
//	)
package batch

import (
	"context"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/payload"
	"github.com/rbaliyan/event/v3/transport"
)

// MetadataContentType is the metadata key for payload encoding.
// Defined canonically in payload.MetadataContentType.
const MetadataContentType = payload.MetadataContentType

// Handler processes a batch of messages.
//
// The handler receives all messages in the batch and should process them
// together (e.g., bulk insert). Return nil on success, or an error to
// Nack all messages in the batch.
//
// Example:
//
//	func processBatch(ctx context.Context, orders []Order) error {
//	    return db.BulkInsert(ctx, orders)
//	}
type Handler[T any] func(ctx context.Context, batch []T) error

// options configures batch processing behavior.
type options struct {
	batchSize  int
	timeout    time.Duration
	maxRetries int
	onError    func(batch []any, err error)
}

// defaultOptions returns the default batch options.
func defaultOptions() *options {
	return &options{
		batchSize:  100,
		timeout:    time.Second,
		maxRetries: 3,
		onError:    func(batch []any, err error) {},
	}
}

// Option configures batch processing behavior.
type Option func(*options)

// WithBatchSize sets the maximum number of messages per batch.
//
// When this limit is reached, the batch is flushed immediately regardless
// of the timeout. Values <= 0 are ignored.
//
// Example:
//
//	// Process up to 500 messages at a time
//	processor := batch.NewProcessor[Order](batch.WithBatchSize(500))
func WithBatchSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// WithTimeout sets the maximum time to wait before flushing a partial batch.
//
// Even if the batch hasn't reached BatchSize, it will be flushed after this
// duration to prevent messages from waiting too long. Values <= 0 are ignored.
//
// Example:
//
//	// Flush after 5 seconds even if batch isn't full
//	processor := batch.NewProcessor[Order](batch.WithTimeout(5 * time.Second))
func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		if timeout > 0 {
			o.timeout = timeout
		}
	}
}

// WithMaxRetries sets the maximum number of retry attempts for failed batches.
//
// If a batch fails, it will be retried up to this many times before being
// Nack'd. Set to 0 to disable retries. Values < 0 are ignored.
//
// Example:
//
//	// Retry failed batches up to 5 times
//	processor := batch.NewProcessor[Order](batch.WithMaxRetries(5))
func WithMaxRetries(retries int) Option {
	return func(o *options) {
		if retries >= 0 {
			o.maxRetries = retries
		}
	}
}

// WithOnError sets the error handler for failed batches.
//
// The handler is called after all retries are exhausted. Use it for logging,
// metrics, or custom error handling. The batch parameter contains the failed
// messages as []any.
//
// Example:
//
//	processor := batch.NewProcessor[Order](
//	    batch.WithOnError(func(batch []any, err error) {
//	        log.Error("batch processing failed",
//	            "size", len(batch),
//	            "error", err)
//	        metrics.FailedBatches.Inc()
//	    }),
//	)
func WithOnError(fn func(batch []any, err error)) Option {
	return func(o *options) {
		if fn != nil {
			o.onError = fn
		}
	}
}

// Processor handles automatic batch processing of messages.
//
// Processor accumulates messages until either:
//   - The batch reaches BatchSize
//   - The Timeout expires
//
// At which point it flushes the batch to the handler. Messages are automatically
// Ack'd or Nack'd based on the handler's return value.
//
// Example:
//
//	processor := batch.NewProcessor[Order](
//	    batch.WithBatchSize(100),
//	    batch.WithTimeout(time.Second),
//	)
//
//	go processor.Process(ctx, subscription.Messages(), func(ctx context.Context, orders []Order) error {
//	    return db.BulkInsert(ctx, orders)
//	})
type Processor[T any] struct {
	opts *options
}

// NewProcessor creates a new batch processor with the given options.
//
// Example:
//
//	processor := batch.NewProcessor[Order](
//	    batch.WithBatchSize(100),
//	    batch.WithTimeout(time.Second),
//	)
func NewProcessor[T any](opts ...Option) *Processor[T] {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return &Processor[T]{opts: o}
}

// Process reads messages from the channel and processes them in batches.
//
// This method blocks until the context is cancelled or the channel is closed.
// It continuously reads messages, accumulates them into batches, and calls
// the handler when the batch is ready.
//
// Batches are flushed when:
//   - The batch reaches BatchSize
//   - The Timeout expires (partial batch)
//   - The context is cancelled (final flush)
//   - The channel is closed (final flush)
//
// All messages in a batch are Ack'd or Nack'd together based on the handler's
// return value.
//
// Parameters:
//   - ctx: Context for cancellation
//   - messages: Channel of transport.Message to read from
//   - handler: Function to process each batch
//
// Returns context.Err() if cancelled, nil if channel closed normally.
//
// Example:
//
//	err := processor.Process(ctx, sub.Messages(), func(ctx context.Context, orders []Order) error {
//	    return db.BulkInsert(ctx, orders)
//	})
//	if err != nil && err != context.Canceled {
//	    log.Error("batch processing failed", "error", err)
//	}
func (p *Processor[T]) Process(
	ctx context.Context,
	messages <-chan transport.Message,
	handler Handler[T],
) error {
	batch := make([]transport.Message, 0, p.opts.batchSize)
	data := make([]T, 0, p.opts.batchSize)
	timer := time.NewTimer(p.opts.timeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		var err error
		for attempt := 0; attempt <= p.opts.maxRetries; attempt++ {
			if err = handler(ctx, data); err == nil {
				break
			}
			// Stop retrying if context is cancelled
			if ctx.Err() != nil {
				break
			}
		}

		if err != nil {
			// All retries exhausted, report via OnError
			anyBatch := make([]any, len(data))
			for i, d := range data {
				anyBatch[i] = d
			}
			p.opts.onError(anyBatch, err)
		}

		// Ack/Nack all messages in batch
		for _, msg := range batch {
			_ = msg.Ack(err)
		}

		// Reset batch
		batch = batch[:0]
		data = data[:0]
		timer.Reset(p.opts.timeout)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()

		case <-timer.C:
			flush()
			timer.Reset(p.opts.timeout)

		case msg, ok := <-messages:
			if !ok {
				flush()
				return nil
			}

			// Decode payload from bytes
			contentType := msg.Metadata()[MetadataContentType]
			if contentType == "" {
				contentType = "application/json"
			}
			codec, codecOk := payload.Get(contentType)
			if !codecOk {
				_ = msg.Ack(nil) // Skip unknown content type
				continue
			}
			var typed T
			if err := codec.Decode(msg.Payload(), &typed); err != nil {
				_ = msg.Ack(err) // Nack with decode error
				p.opts.onError([]any{msg}, err)
				continue
			}
			batch = append(batch, msg)
			data = append(data, typed)

			if len(batch) >= p.opts.batchSize {
				flush()
			}
		}
	}
}

// Collector provides manual batch collection for custom processing logic.
//
// Unlike Processor, Collector gives you full control over when batches are
// flushed. Use it when you need custom batching logic, such as:
//   - Flushing based on external triggers
//   - Combining messages from multiple sources
//   - Implementing custom timeout behavior
//
// Collector is safe for concurrent use.
//
// Example:
//
//	collector := batch.NewCollector[Order](100, time.Second)
//	ticker := time.NewTicker(time.Second)
//
//	for {
//	    select {
//	    case msg := <-messages:
//	        order := msg.Payload().(Order)
//	        if collector.Add(msg, order) {
//	            batch, msgs := collector.Flush()
//	            processBatch(batch, msgs)
//	        }
//	    case <-ticker.C:
//	        if collector.Size() > 0 {
//	            batch, msgs := collector.Flush()
//	            processBatch(batch, msgs)
//	        }
//	    }
//	}
type Collector[T any] struct {
	mu       sync.Mutex
	batch    []T
	messages []transport.Message
	size     int
	timeout  time.Duration
}

// NewCollector creates a new batch collector.
//
// Parameters:
//   - size: Maximum batch size (used for capacity hints)
//   - timeout: Hint for timeout-based flushing (not enforced by Collector)
//
// Example:
//
//	collector := batch.NewCollector[Order](100, time.Second)
func NewCollector[T any](size int, timeout time.Duration) *Collector[T] {
	return &Collector[T]{
		batch:    make([]T, 0, size),
		messages: make([]transport.Message, 0, size),
		size:     size,
		timeout:  timeout,
	}
}

// Add adds a message and its data to the batch.
//
// Returns true if the batch is full and should be flushed. The caller is
// responsible for calling Flush() when Add returns true.
//
// Example:
//
//	if collector.Add(msg, order) {
//	    batch, msgs := collector.Flush()
//	    processBatch(batch, msgs)
//	}
func (c *Collector[T]) Add(msg transport.Message, data T) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batch = append(c.batch, data)
	c.messages = append(c.messages, msg)

	return len(c.batch) >= c.size
}

// Flush returns the current batch and clears it.
//
// Returns the accumulated data and their corresponding transport messages.
// After Flush, the collector is empty and ready for new messages.
//
// The caller should Ack or Nack the returned messages based on processing
// success.
//
// Example:
//
//	batch, msgs := collector.Flush()
//	err := processBatch(batch)
//	for _, msg := range msgs {
//	    msg.Ack(err)
//	}
func (c *Collector[T]) Flush() ([]T, []transport.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	batch := c.batch
	messages := c.messages

	c.batch = make([]T, 0, c.size)
	c.messages = make([]transport.Message, 0, c.size)

	return batch, messages
}

// Size returns the current number of messages in the batch.
//
// Use this to check if the batch has any pending messages before
// timeout-based flushing.
//
// Example:
//
//	if collector.Size() > 0 {
//	    batch, msgs := collector.Flush()
//	    processBatch(batch, msgs)
//	}
func (c *Collector[T]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batch)
}

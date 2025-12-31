package partition

// PublishOptions contains options for partitioned publishing.
//
// Use PublishOptions to specify routing and metadata for partitioned messages.
// The PartitionKey is the most important field - it determines which partition
// receives the message and ensures ordering for messages with the same key.
//
// Example:
//
//	opts := partition.NewPublishOptions("user-123").
//	    WithHeader("correlation-id", correlationID).
//	    WithPriority(10)
//
//	publisher.Publish(ctx, event, opts)
type PublishOptions struct {
	// PartitionKey determines which partition the message is routed to.
	// Messages with the same key are guaranteed to be delivered in order.
	// Choose keys that represent the entity being processed (e.g., user ID,
	// order ID, account number).
	PartitionKey string

	// Headers are additional key-value pairs attached to the message.
	// Use for correlation IDs, tracing context, or custom metadata.
	Headers map[string]string

	// Priority sets the message priority (higher = more important).
	// The interpretation depends on the transport implementation.
	Priority int
}

// NewPublishOptions creates new publish options with the given partition key.
//
// The partition key determines which partition receives the message. All messages
// with the same key are guaranteed to be delivered to the same partition in order.
//
// Parameters:
//   - partitionKey: The key used for partition routing (e.g., "user-123", "order-456")
//
// Example:
//
//	// Route all events for user-123 to the same partition
//	opts := partition.NewPublishOptions("user-123")
func NewPublishOptions(partitionKey string) *PublishOptions {
	return &PublishOptions{
		PartitionKey: partitionKey,
		Headers:      make(map[string]string),
	}
}

// WithHeader adds a header to the options.
//
// Headers are key-value pairs attached to the message for metadata.
// Common uses include correlation IDs, tracing context, and custom metadata.
//
// Returns the options for method chaining.
//
// Example:
//
//	opts := partition.NewPublishOptions("user-123").
//	    WithHeader("correlation-id", corrID).
//	    WithHeader("source", "order-service")
func (o *PublishOptions) WithHeader(key, value string) *PublishOptions {
	if o.Headers == nil {
		o.Headers = make(map[string]string)
	}
	o.Headers[key] = value
	return o
}

// WithPriority sets the message priority.
//
// Higher priority values indicate more important messages. The exact behavior
// depends on the transport implementation (e.g., priority queues).
//
// Returns the options for method chaining.
//
// Example:
//
//	// High-priority payment event
//	opts := partition.NewPublishOptions("payment-789").
//	    WithPriority(100)
func (o *PublishOptions) WithPriority(priority int) *PublishOptions {
	o.Priority = priority
	return o
}

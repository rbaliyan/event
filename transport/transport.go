// Package transport provides shared types and interfaces for event transport implementations.
//
// Transport implementations (channel, redis, nats, kafka) should import this package
// rather than the parent event package to avoid import cycles.
package transport

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// Transport errors
var (
	ErrTransportClosed    = errors.New("transport closed")
	ErrEventNotRegistered = errors.New("event not registered")
	ErrEventAlreadyExists = errors.New("event already registered")
	ErrNoSubscribers      = errors.New("no subscribers")
	ErrSubscriptionClosed = errors.New("subscription closed")
	ErrPublishTimeout     = errors.New("publish timeout")
	ErrDecodeFailure      = errors.New("message decode failed")
)

// DecodeError represents a message that failed to decode.
// This is sent through the message channel so the event layer can route it to DLQ.
type DecodeError struct {
	RawData []byte // The raw message data that failed to decode
	Err     error  // The decode error
	MsgID   string // Transport-specific message ID (e.g., Redis stream ID)
}

func (e *DecodeError) Error() string {
	return "decode error: " + e.Err.Error()
}

func (e *DecodeError) Unwrap() error {
	return e.Err
}

// MetadataDecodeError is the metadata key indicating a decode error occurred.
// When this key is present in metadata, the payload contains raw bytes that failed to decode.
const MetadataDecodeError = "X-Decode-Error"

// IsDecodeError checks if message metadata indicates a decode error.
// Returns the error message if present.
func IsDecodeError(metadata map[string]string) (string, bool) {
	if metadata == nil {
		return "", false
	}
	errMsg, ok := metadata[MetadataDecodeError]
	return errMsg, ok
}

// NewDecodeErrorMessage creates a message that indicates a decode error.
// The raw bytes are passed as payload, and the error is stored in metadata.
func NewDecodeErrorMessage(msgID string, rawData []byte, err error, ackFn func(error) error) Message {
	metadata := map[string]string{
		MetadataDecodeError: err.Error(),
	}
	return NewMessageWithAck(msgID, "", rawData, metadata, 0, ackFn)
}

// HealthStatus represents the health state of a component
type HealthStatus string

const (
	// HealthStatusHealthy indicates the component is functioning normally
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusDegraded indicates the component is functioning but with issues
	HealthStatusDegraded HealthStatus = "degraded"
	// HealthStatusUnhealthy indicates the component is not functioning
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// HealthCheckResult contains detailed health information
type HealthCheckResult struct {
	Status     HealthStatus                  `json:"status"`
	Message    string                        `json:"message,omitempty"`
	Latency    time.Duration                 `json:"latency,omitempty"`
	Details    map[string]any                `json:"details,omitempty"`
	Components map[string]*HealthCheckResult `json:"components,omitempty"`
	CheckedAt  time.Time                     `json:"checked_at"`
}

// IsHealthy returns true if the status is healthy
func (h *HealthCheckResult) IsHealthy() bool {
	return h.Status == HealthStatusHealthy
}

// HealthChecker is an optional interface that transports can implement
// to provide health check capabilities for monitoring and readiness probes.
type HealthChecker interface {
	// Health performs a health check and returns the result.
	// The context can be used to set a timeout for the health check.
	Health(ctx context.Context) *HealthCheckResult
}

// ConsumerLag contains information about consumer lag for an event
type ConsumerLag struct {
	Event           string        `json:"event"`
	ConsumerGroup   string        `json:"consumer_group,omitempty"`
	Lag             int64         `json:"lag"`              // Number of unprocessed messages
	OldestPending   time.Duration `json:"oldest_pending"`   // Age of oldest unacknowledged message
	PendingMessages int64         `json:"pending_messages"` // Messages delivered but not yet acked
}

// LagMonitor is an optional interface that transports can implement
// to report consumer lag metrics for monitoring and alerting.
type LagMonitor interface {
	// ConsumerLag returns the current consumer lag for all events.
	// This is useful for monitoring dashboards and alerting systems.
	ConsumerLag(ctx context.Context) ([]ConsumerLag, error)
}

// DeliveryMode determines how messages are distributed to subscribers
type DeliveryMode int

const (
	// Broadcast delivers message to ALL subscribers (pub/sub fan-out)
	Broadcast DeliveryMode = iota
	// WorkerPool delivers message to ONE subscriber (load balancing across workers)
	WorkerPool
)

// StartPosition determines where to start reading messages from
type StartPosition int

const (
	// StartFromBeginning processes all available historical messages.
	// Use this for event sourcing or when you need complete history.
	StartFromBeginning StartPosition = iota

	// StartFromLatest only receives messages published after subscription.
	// Use this for real-time dashboards or notifications that don't need history.
	StartFromLatest

	// StartFromTimestamp starts reading from a specific point in time.
	// Use with SubscribeOptions.StartTime to specify the timestamp.
	StartFromTimestamp
)

// SubscribeOptions configures subscription behavior
type SubscribeOptions struct {
	// DeliveryMode determines how messages are distributed.
	// Default: Broadcast (all subscribers receive every message)
	DeliveryMode DeliveryMode

	// WorkerGroup specifies a named group for WorkerPool mode.
	// Workers with the same group name compete for messages (load balancing).
	// Different groups each receive all messages (like broadcast between groups).
	// Only used when DeliveryMode is WorkerPool.
	// Empty string means all WorkerPool subscribers share the default group.
	WorkerGroup string

	// StartFrom determines where to start reading messages.
	// Default: StartFromBeginning (process all historical messages)
	StartFrom StartPosition

	// StartTime is used with StartFromTimestamp to specify the start point.
	// Messages before this time will be skipped.
	StartTime time.Time

	// MaxAge filters out messages older than this duration.
	// Messages with a timestamp older than (now - MaxAge) are skipped.
	// Zero means no age filtering.
	// This is useful for scenarios where stale messages are not relevant.
	MaxAge time.Duration

	// LatestOnly enables "sampling" mode where only the most recent message
	// is delivered. If multiple messages arrive while processing, only the
	// latest is kept and intermediate messages are dropped.
	// Useful for real-time state updates where only current value matters.
	LatestOnly bool

	// BufferSize overrides the default message channel buffer size.
	// Zero uses the transport's default buffer size.
	BufferSize int
}

// SubscribeOption is a functional option for configuring subscriptions
type SubscribeOption func(*SubscribeOptions)

// WithStartFrom sets where to start reading messages.
//
// Example:
//
//	// Only receive new messages
//	sub, err := transport.Subscribe(ctx, "events", Broadcast,
//	    transport.WithStartFrom(transport.StartFromLatest))
func WithStartFrom(pos StartPosition) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.StartFrom = pos
	}
}

// WithStartTime sets the start timestamp for StartFromTimestamp mode.
//
// Example:
//
//	// Resume from last checkpoint
//	sub, err := transport.Subscribe(ctx, "events", Broadcast,
//	    transport.WithStartFrom(transport.StartFromTimestamp),
//	    transport.WithStartTime(lastProcessedTime))
func WithStartTime(t time.Time) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.StartFrom = StartFromTimestamp
		o.StartTime = t
	}
}

// WithMaxAge filters out messages older than the specified duration.
// Messages older than (now - maxAge) are silently skipped.
//
// Example:
//
//	// Only process messages from the last 5 minutes
//	sub, err := transport.Subscribe(ctx, "events", Broadcast,
//	    transport.WithMaxAge(5*time.Minute))
func WithMaxAge(maxAge time.Duration) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.MaxAge = maxAge
	}
}

// WithLatestOnly enables sampling mode where only the most recent message
// is kept. Intermediate messages are dropped if they arrive faster than
// they can be processed.
//
// Example:
//
//	// Real-time price updates - only care about current price
//	sub, err := transport.Subscribe(ctx, "prices", Broadcast,
//	    transport.WithLatestOnly())
func WithLatestOnly() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.LatestOnly = true
	}
}

// WithBufferSize sets the message channel buffer size.
//
// Example:
//
//	sub, err := transport.Subscribe(ctx, "events",
//	    transport.WithBufferSize(1000))
func WithBufferSize(size int) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.BufferSize = size
	}
}

// WithDeliveryMode sets the message delivery mode.
//
// Modes:
//   - Broadcast (default): all subscribers receive every message
//   - WorkerPool: each message is delivered to only ONE subscriber (load balancing)
//
// Example:
//
//	// Load balance across multiple workers
//	sub, err := transport.Subscribe(ctx, "tasks",
//	    transport.WithDeliveryMode(transport.WorkerPool))
func WithDeliveryMode(mode DeliveryMode) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.DeliveryMode = mode
	}
}

// WithWorkerGroup sets the worker group name for WorkerPool mode.
// Workers with the same group name compete for messages (load balancing).
// Different groups each receive all messages (like broadcast between groups).
//
// This enables patterns like:
//   - Multiple processing pipelines on the same event
//   - Separate scaling for different workloads
//
// Example:
//
//	// Order processors compete within their group
//	sub1, err := transport.Subscribe(ctx, "orders",
//	    transport.WithDeliveryMode(transport.WorkerPool),
//	    transport.WithWorkerGroup("order-processors"))
//
//	// Inventory updaters compete within their group (separate from order processors)
//	sub2, err := transport.Subscribe(ctx, "orders",
//	    transport.WithDeliveryMode(transport.WorkerPool),
//	    transport.WithWorkerGroup("inventory-updaters"))
func WithWorkerGroup(group string) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.WorkerGroup = group
	}
}

// DefaultSubscribeOptions returns the default subscription options
func DefaultSubscribeOptions() *SubscribeOptions {
	return &SubscribeOptions{
		DeliveryMode: Broadcast,
		StartFrom:    StartFromBeginning,
	}
}

// ApplyOptions applies functional options to SubscribeOptions
func ApplySubscribeOptions(opts ...SubscribeOption) *SubscribeOptions {
	o := DefaultSubscribeOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Transport manages message delivery for events
type Transport interface {
	// RegisterEvent creates resources for an event (topic, stream, etc.)
	// Must be called before Publish or Subscribe
	RegisterEvent(ctx context.Context, name string) error

	// UnregisterEvent cleans up event resources and closes all subscriptions
	UnregisterEvent(ctx context.Context, name string) error

	// Publish sends a message to an event's subscribers
	// Returns ErrEventNotRegistered if event not registered
	// Returns nil if no subscribers (message is dropped)
	Publish(ctx context.Context, name string, msg Message) error

	// Subscribe creates a subscription to receive messages for an event.
	// Default is Broadcast mode (all subscribers receive every message).
	//
	// Options:
	//   - WithDeliveryMode: set delivery mode (Broadcast or WorkerPool)
	//   - WithStartFrom: where to start reading (beginning, latest, timestamp)
	//   - WithMaxAge: filter out messages older than a duration
	//   - WithLatestOnly: only deliver most recent message (sampling mode)
	//
	// Example:
	//
	//   // Default: broadcast to all subscribers
	//   sub, err := transport.Subscribe(ctx, "events")
	//
	//   // Worker pool with latest messages only
	//   sub, err := transport.Subscribe(ctx, "tasks",
	//       transport.WithDeliveryMode(transport.WorkerPool),
	//       transport.WithStartFrom(transport.StartFromLatest))
	//
	// Returns ErrEventNotRegistered if event not registered
	Subscribe(ctx context.Context, name string, opts ...SubscribeOption) (Subscription, error)

	// Close shuts down the transport and all events
	Close(ctx context.Context) error
}

// Subscription represents a subscriber's connection to an event
type Subscription interface {
	// ID returns the unique subscription identifier
	ID() string

	// Messages returns the channel to receive messages
	Messages() <-chan Message

	// Close unsubscribes and closes the message channel
	Close(ctx context.Context) error
}

// Message is the message interface from the message package
type Message = message.Message

// Codec is the codec interface from the codec package
type Codec = codec.Codec

// DefaultCodec returns the default codec used by transports (JSON)
func DefaultCodec() Codec {
	return codec.Default()
}

// NewMessage creates a new message
func NewMessage(id, source string, payload []byte, metadata map[string]string, spanCtx trace.SpanContext) Message {
	return message.New(id, source, payload, metadata, spanCtx)
}

// NewMessageWithRetry creates a new message with retry count
func NewMessageWithRetry(id, source string, payload []byte, metadata map[string]string, spanCtx trace.SpanContext, retryCount int) Message {
	return message.NewWithRetry(id, source, payload, metadata, spanCtx, retryCount)
}

// NewMessageWithAck creates a new message with retry count and ack function.
// This is used by transports that need custom acknowledgment behavior.
func NewMessageWithAck(id, source string, payload []byte, metadata map[string]string, retryCount int, ackFn func(error) error) Message {
	return message.NewWithAck(id, source, payload, metadata, retryCount, ackFn)
}

// ID generation
var counter uint64

// NewID generates a new unique ID
func NewID() string {
	u, err := uuid.NewRandom()
	if err == nil {
		return u.String()
	}
	return strconv.FormatUint(atomic.AddUint64(&counter, 1), 10)
}

// Logger returns a logger with the given component name
func Logger(component string) *slog.Logger {
	return slog.Default().With("component", component)
}

// Jitter adds randomness to a duration to prevent thundering herd.
// Returns a duration between d*(1-factor) and d*(1+factor).
// Factor should be between 0 and 1 (e.g., 0.3 for +/-30% jitter).
func Jitter(d time.Duration, factor float64) time.Duration {
	if factor <= 0 || factor > 1 {
		return d
	}
	// Random value between -factor and +factor
	jitter := (rand.Float64()*2 - 1) * factor
	return time.Duration(float64(d) * (1 + jitter))
}

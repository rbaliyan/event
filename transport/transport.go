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

// IsDecodeError checks if a message payload is a decode error
func IsDecodeError(payload any) (*DecodeError, bool) {
	de, ok := payload.(*DecodeError)
	return de, ok
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

	// Subscribe creates a subscription to receive messages for an event
	// mode determines delivery semantics:
	//   - Broadcast: all subscribers receive every message
	//   - WorkerPool: only one subscriber receives each message
	// Returns ErrEventNotRegistered if event not registered
	Subscribe(ctx context.Context, name string, mode DeliveryMode) (Subscription, error)

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
func NewMessage(id, source string, payload any, metadata map[string]string, spanCtx trace.SpanContext) Message {
	return message.New(id, source, payload, metadata, spanCtx)
}

// NewMessageWithRetry creates a new message with retry count
func NewMessageWithRetry(id, source string, payload any, metadata map[string]string, spanCtx trace.SpanContext, retryCount int) Message {
	return message.NewWithRetry(id, source, payload, metadata, spanCtx, retryCount)
}

// NewMessageWithAck creates a new message with retry count and ack function.
// This is used by transports that need custom acknowledgment behavior.
func NewMessageWithAck(id, source string, payload any, metadata map[string]string, retryCount int, ackFn func(error) error) Message {
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

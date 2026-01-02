package event

import (
	"errors"
	"fmt"
)

// Handler result sentinel errors.
// These errors control message acknowledgment behavior when returned from handlers.
// Use errors.Is() to check for these errors as they may be wrapped with additional context.
//
// Example usage:
//
//	func handler(ctx context.Context, ev Event[Order], order Order) error {
//	    if err := validate(order); err != nil {
//	        // Permanent failure - don't retry, send to DLQ
//	        return fmt.Errorf("validation failed: %w", event.ErrReject)
//	    }
//	    if err := process(order); err != nil {
//	        if isTransient(err) {
//	            // Retry immediately
//	            return fmt.Errorf("transient error: %w", event.ErrNack)
//	        }
//	        // Retry with backoff
//	        return fmt.Errorf("processing failed: %w", event.ErrDefer)
//	    }
//	    return nil // Success - acknowledge message
//	}
var (
	// ErrAck indicates successful processing. The message will be acknowledged.
	// This is equivalent to returning nil from the handler.
	// Use when you want to explicitly signal success with additional context.
	ErrAck = errors.New("ack: message processed successfully")

	// ErrNack indicates the message should be retried immediately.
	// The message will NOT be acknowledged and will be redelivered.
	// Use for transient failures that may succeed on immediate retry.
	ErrNack = errors.New("nack: retry message immediately")

	// ErrReject indicates the message should NOT be retried.
	// The message will be acknowledged (removed from queue) and sent to DLQ if configured.
	// Use for permanent failures like validation errors or malformed data.
	ErrReject = errors.New("reject: do not retry, send to dead letter queue")

	// ErrDefer indicates the message should be retried with backoff.
	// The message will NOT be acknowledged and will be redelivered after a delay.
	// Use for failures that need time before retry (rate limits, temporary unavailability).
	ErrDefer = errors.New("defer: retry message with backoff")
)

// HandlerResult represents the outcome of message processing.
// This is used internally to classify handler return values.
type HandlerResult int

const (
	// ResultAck - message processed successfully
	ResultAck HandlerResult = iota
	// ResultNack - retry immediately
	ResultNack
	// ResultReject - don't retry, send to DLQ
	ResultReject
	// ResultDefer - retry with backoff
	ResultDefer
)

// ClassifyError determines the handler result from an error.
// Returns ResultAck if err is nil or wraps ErrAck.
// Returns ResultNack if err wraps ErrNack.
// Returns ResultReject if err wraps ErrReject.
// Returns ResultDefer if err wraps ErrDefer or is any other error.
func ClassifyError(err error) HandlerResult {
	if err == nil {
		return ResultAck
	}
	if errors.Is(err, ErrAck) {
		return ResultAck
	}
	if errors.Is(err, ErrNack) {
		return ResultNack
	}
	if errors.Is(err, ErrReject) {
		return ResultReject
	}
	if errors.Is(err, ErrDefer) {
		return ResultDefer
	}
	// Default: treat unknown errors as defer (retry with backoff)
	return ResultDefer
}

// String returns a string representation of the handler result.
func (r HandlerResult) String() string {
	switch r {
	case ResultAck:
		return "ack"
	case ResultNack:
		return "nack"
	case ResultReject:
		return "reject"
	case ResultDefer:
		return "defer"
	default:
		return fmt.Sprintf("unknown(%d)", r)
	}
}

// Ack wraps an error to indicate successful processing.
// The original error is preserved for logging but the message will be acknowledged.
func Ack(err error) error {
	if err == nil {
		return ErrAck
	}
	return fmt.Errorf("%w: %v", ErrAck, err)
}

// Nack wraps an error to indicate immediate retry is needed.
func Nack(err error) error {
	if err == nil {
		return ErrNack
	}
	return fmt.Errorf("%w: %v", ErrNack, err)
}

// Reject wraps an error to indicate the message should not be retried.
func Reject(err error) error {
	if err == nil {
		return ErrReject
	}
	return fmt.Errorf("%w: %v", ErrReject, err)
}

// Defer wraps an error to indicate retry with backoff is needed.
func Defer(err error) error {
	if err == nil {
		return ErrDefer
	}
	return fmt.Errorf("%w: %v", ErrDefer, err)
}

// RetryExhaustedError indicates all retry attempts have been exhausted.
type RetryExhaustedError struct {
	Attempts int
	LastErr  error
}

func (e *RetryExhaustedError) Error() string {
	return fmt.Sprintf("retry exhausted after %d attempts: %v", e.Attempts, e.LastErr)
}

func (e *RetryExhaustedError) Unwrap() error {
	return e.LastErr
}

// IsRetryExhausted checks if an error indicates retry exhaustion.
func IsRetryExhausted(err error) bool {
	var exhausted *RetryExhaustedError
	return errors.As(err, &exhausted)
}

// CircuitOpenError indicates the circuit breaker is open.
type CircuitOpenError struct {
	Name      string
	OpenUntil string
}

func (e *CircuitOpenError) Error() string {
	return fmt.Sprintf("circuit breaker %q is open until %s", e.Name, e.OpenUntil)
}

// IsCircuitOpen checks if an error indicates an open circuit breaker.
func IsCircuitOpen(err error) bool {
	var circuitErr *CircuitOpenError
	return errors.As(err, &circuitErr)
}

// DuplicateMessageError indicates a duplicate message was detected.
type DuplicateMessageError struct {
	MessageID string
}

func (e *DuplicateMessageError) Error() string {
	return fmt.Sprintf("duplicate message: %s", e.MessageID)
}

// IsDuplicateMessage checks if an error indicates a duplicate message.
func IsDuplicateMessage(err error) bool {
	var dupErr *DuplicateMessageError
	return errors.As(err, &dupErr)
}

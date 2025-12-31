package poison

import (
	"fmt"
)

// Error represents a poison message error.
//
// Use Error to indicate that a message was detected as poison and should
// not be retried. This error type can be checked with IsPoisonError or
// errors.Is.
//
// Example:
//
//	if poisoned, _ := detector.Check(ctx, msg.ID); poisoned {
//	    return poison.NewError(msg.ID, "message is quarantined")
//	}
//
//	// Later, check if error is poison-related
//	if poison.IsPoisonError(err) {
//	    log.Debug("skipping poison message")
//	    return nil // Don't retry
//	}
type Error struct {
	// MessageID is the ID of the poison message.
	MessageID string

	// Reason describes why the message is considered poison.
	Reason string
}

// NewError creates a new poison message error.
//
// Parameters:
//   - messageID: The ID of the poison message
//   - reason: Description of why the message is poison
//
// Example:
//
//	return poison.NewError(msg.ID, "exceeded failure threshold")
func NewError(messageID, reason string) *Error {
	return &Error{
		MessageID: messageID,
		Reason:    reason,
	}
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("poison message %s: %s", e.MessageID, e.Reason)
}

// Is checks if the target error is a poison Error.
//
// This enables use with errors.Is:
//
//	if errors.Is(err, &poison.Error{}) {
//	    // Handle poison error
//	}
func (e *Error) Is(target error) bool {
	_, ok := target.(*Error)
	return ok
}

// IsPoisonError checks if an error is a poison message error.
//
// Returns true if the error is of type *Error.
//
// Example:
//
//	if poison.IsPoisonError(err) {
//	    // Don't retry poison messages
//	    return nil
//	}
func IsPoisonError(err error) bool {
	_, ok := err.(*Error)
	return ok
}

package event

import (
	"context"
	"time"
)

// DLQMessage represents a message to be stored in the dead letter queue.
type DLQMessage struct {
	EventName  string
	MessageID  string
	Payload    []byte
	Metadata   map[string]string
	Error      error
	RetryCount int
	Source     string
	CreatedAt  time.Time
}

// DLQStore stores messages that failed processing permanently.
// The bus calls Store() automatically for rejected messages when configured
// via WithDLQ().
type DLQStore interface {
	Store(ctx context.Context, msg *DLQMessage) error
}

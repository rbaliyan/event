package event

import "context"

// Sender sends raw event messages. This is the canonical interface for
// low-level untyped publishing, used by DLQ replay and distributed recovery.
// *Bus satisfies this interface via its Send method.
type Sender interface {
	Send(ctx context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error
}

var _ Sender = (*Bus)(nil)

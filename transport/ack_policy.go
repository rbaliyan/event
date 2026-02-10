package transport

// AckPolicy controls when messages are acknowledged by the event layer.
// This is orthogonal to DeliveryMode (which controls routing).
type AckPolicy int

const (
	// AckExplicit requires the handler to succeed before acknowledging.
	// Failed handlers trigger retry/DLQ based on error classification.
	// This is the default behavior.
	//
	// For effectively exactly-once processing, combine with
	// IdempotencyMiddleware or a DeduplicationStore.
	AckExplicit AckPolicy = iota

	// AckOnReceive acknowledges messages immediately upon delivery
	// to the handler. Handler errors are logged but never cause
	// redelivery. Equivalent to at-most-once from the handler's
	// perspective.
	//
	// Use for:
	//   - Real-time dashboards where stale retries are worse than gaps
	//   - SSE/WebSocket push where clients will reconnect
	//   - Metrics aggregation where occasional loss is acceptable
	AckOnReceive
)

// String returns a human-readable name for the ack policy.
func (p AckPolicy) String() string {
	switch p {
	case AckExplicit:
		return "explicit"
	case AckOnReceive:
		return "on-receive"
	default:
		return "unknown"
	}
}

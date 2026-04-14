package bridge

import (
	"context"

	"github.com/rbaliyan/event/v3/transport"
)

// Source is the narrow contract the bridge requires of the upstream
// end. A Source supplies messages for forwarding; it is NOT asked to
// publish. Any [transport.Transport] satisfies this interface
// structurally, so existing transports plug in without an adapter.
// Purpose-built sources (e.g. a MongoDB change stream wrapper that
// does not expose the full Transport surface) implement Source
// directly.
//
// A Source MUST deliver messages via [transport.Message] — reusing the
// canonical message type keeps middleware (dedup, DLQ, observe,
// transform) transport-agnostic. Sources with richer native types are
// expected to marshal them into [transport.Message] at the boundary.
type Source interface {
	// RegisterEvent is called once before [Subscribe] to let the source
	// allocate per-event resources. May be a no-op for sources that
	// subscribe globally (change streams, file watchers) — returning
	// [transport.ErrEventAlreadyExists] is benign and treated as success.
	RegisterEvent(ctx context.Context, name string) error

	// UnregisterEvent releases per-event resources. Called when the
	// bridge unregisters name or during shutdown.
	UnregisterEvent(ctx context.Context, name string) error

	// Subscribe returns a subscription that emits messages for name.
	// The subscription's Messages() channel MUST close when the
	// subscription is closed or the source shuts down. Options passed
	// here come from the bridge's [WithPumpSubscribeOptions] and are
	// intended to configure source-specific behaviour (routing, start
	// position).
	Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error)

	// Close shuts down the source entirely. Called once by the bridge
	// during [Transport.Close]. MUST be idempotent.
	Close(ctx context.Context) error
}

// Sink is the narrow contract the bridge requires of the downstream
// end. A Sink accepts published messages and — typically — exposes
// consumer-group or pub-sub semantics so downstream subscribers can
// load-balance. Any [transport.Transport] satisfies this interface
// structurally.
//
// A bridge that uses the same value for both source and sink is
// permitted (and occasionally useful for loopback tests); in that
// case Close is called once per role but concrete implementations
// are expected to be Close-idempotent, so the extra call is harmless.
type Sink interface {
	// RegisterEvent is called before [Publish] to allocate per-event
	// resources (e.g. create a Redis Stream, a Kafka topic, a NATS
	// subject). Errors returned here fail the bridge's
	// [Transport.RegisterEvent].
	RegisterEvent(ctx context.Context, name string) error

	// UnregisterEvent releases per-event resources.
	UnregisterEvent(ctx context.Context, name string) error

	// Publish delivers msg under the named event. Errors bubble up
	// through the middleware pipeline and, unless caught by middleware
	// such as [DLQ], cause the source message to be nacked.
	Publish(ctx context.Context, name string, msg transport.Message) error

	// Subscribe exposes the sink's native subscription surface to the
	// bus. The bridge delegates its own [Transport.Subscribe] here so
	// downstream consumers ride whatever semantics the sink provides
	// (consumer groups, worker pools, broadcast).
	Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error)

	// Close shuts down the sink entirely. MUST be idempotent.
	Close(ctx context.Context) error
}

// Compile-time evidence that any [transport.Transport] can be used as
// a Source or Sink without an adapter.
var (
	_ Source = (transport.Transport)(nil)
	_ Sink   = (transport.Transport)(nil)
)

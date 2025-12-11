// Package event provides mechanism for publishing and subscribing events using abstract transport.
// Default available transport is a channel map with fan-out strategy.
//
// V2 Architecture:
// - Generic events with compile-time type safety: Event[T] ensures publishers and subscribers use the same type
// - Publish() and Subscribe() are fire-and-forget (void returns)
// - Transport owns async behavior, error handling, and graceful shutdown
// - Events are facts that happened - no error handling burden on callers
//
// To handle remote events other transports such as Redis or Nats can also be used.
//
// Basic example with type safety:
//
//	type User struct {
//	    ID   string
//	    Name string
//	}
//
//	e := event.New[User]("user.created")
//	e.Subscribe(context.Background(), func(ctx context.Context, ev event.Event[User], user User) {
//	    fmt.Printf("User created: %s\n", user.Name)
//	})
//
//	e.Publish(context.Background(), User{ID: "123", Name: "John"})
//
// With custom transport:
//
//	transport := event.NewChannelTransport(
//	    event.WithTransportAsync(true),
//	    event.WithTransportBufferSize(100),
//	    event.WithTransportErrorHandler(func(err error) {
//	        log.Printf("transport error: %v", err)
//	    }),
//	)
//	e := event.New[User]("my-event", event.WithTransport(transport))
//
// Event Options:
//   - WithSubscriberTimeout: set handler execution timeout. Default is 0 (no timeout).
//   - WithTracing: enable/disable OpenTelemetry tracing. Default is true.
//   - WithRecovery: enable/disable panic recovery in handlers. Default is true.
//   - WithMetrics: enable/disable Prometheus metrics. Default is true.
//   - WithErrorHandler: set panic recovery error callback.
//   - WithTransport: set custom transport. Default is NewChannelTransport().
//   - WithLogger: set logger for event.
//   - WithChannelBufferSize: set buffer size for default transport. Default is 100.
//   - WithRegistry: set registry. Default is the global defaultRegistry.
//
// Transport Options:
//   - WithTransportAsync: enable/disable async handler execution. Default is true.
//   - WithTransportBufferSize: set channel buffer size. Default is 100.
//   - WithTransportTimeout: set send timeout per subscriber. Default is 0 (no timeout).
//   - WithTransportErrorHandler: set error callback for transport errors.
//   - WithTransportLogger: set logger for transport.
//
// Registry:
// Registry defines the scope of events. Within one registry there can be only one event
// with a given name. Optionally registry also holds the prometheus.Registerer.
// When Registry.Close() is called, all events registered in the registry will
// stop publishing data and transports will gracefully shut down.
//
// Transport:
// Transport defines the delivery layer used by events. Default is a channel-based transport.
// Transports can be shared among events to create event aliases or send data across registries.
// Transport.Close() blocks until all pending messages are delivered to subscriber channels.
//
// Type Safety:
// Events are generic and ensure type safety at compile time:
//
//	// This compiles - correct type
//	userEvent := event.New[User]("user.created")
//	userEvent.Publish(ctx, User{ID: "1"})
//
//	// This won't compile - wrong type
//	userEvent.Publish(ctx, "string")  // compile error!
//
// Event Groups:
// Events with the same type can be grouped:
//
//	events := event.Events[User]{
//	    event.New[User]("user.created"),
//	    event.New[User]("user.updated"),
//	}
//	events.Subscribe(ctx, handler)  // Subscribe to all
//	events.Publish(ctx, user)       // Publish to all
package event

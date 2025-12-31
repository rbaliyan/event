// Package event provides mechanism for publishing and subscribing events using abstract transport.
// Default available transport is a channel-based in-memory transport.
//
// V3 Architecture:
// - Generic events with compile-time type safety: Event[T] ensures publishers and subscribers use the same type
// - Bus owns infrastructure (transport, tracing, metrics, recovery)
// - Events must be registered with a Bus before use
// - Multiple transports: channel (in-memory), Redis Streams, NATS, Kafka
//
// Basic example with type safety:
//
//	type User struct {
//	    ID   string
//	    Name string
//	}
//
//	// Create bus with transport
//	bus, err := event.NewBus("my-app", event.WithBusTransport(channel.New()))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer bus.Close(ctx)
//
//	// Create and register event
//	userEvent, err := event.Register(ctx, bus, event.New[User]("user.created"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Subscribe with type-safe handler
//	userEvent.Subscribe(ctx, func(ctx context.Context, ev event.Event[User], user User) error {
//	    fmt.Printf("User created: %s\n", user.Name)
//	    return nil
//	})
//
//	// Publish
//	userEvent.Publish(ctx, User{ID: "123", Name: "John"})
//
// Bus Options:
//   - WithBusTransport: set transport (required). Use channel.New(), redis.New(), etc.
//   - WithBusTracing: enable/disable OpenTelemetry tracing. Default is true.
//   - WithBusRecovery: enable/disable panic recovery in handlers. Default is true.
//   - WithBusMetrics: enable/disable OpenTelemetry metrics. Default is true.
//   - WithBusLogger: set logger for the bus.
//
// Event Options:
//   - WithSubscriberTimeout: set handler execution timeout. Default is 0 (no timeout).
//   - WithErrorHandler: set panic recovery error callback.
//   - WithMaxRetries: set max retry attempts before sending to DLQ. Default is 0 (unlimited).
//   - WithDeadLetterQueue: set handler for permanently failed messages.
//
// Subscribe Options:
//   - AsWorker: use WorkerPool mode (load balancing - one subscriber receives each message).
//   - AsBroadcast: use Broadcast mode (fan-out - all subscribers receive each message). Default.
//   - WithMiddleware: add custom middleware to the handler chain.
//
// Bus Registry:
// Buses are registered globally by name. Events can be accessed via full name syntax:
//
//	// Get event by full name
//	ev, err := event.Get[User]("my-app://user.created")
//
//	// Publish by full name
//	event.Publish(ctx, "my-app://user.created", User{ID: "1"})
//
// When Bus.Close() is called, all events registered with the bus will
// stop publishing data and the transport will gracefully shut down.
//
// Type Safety:
// Events are generic and ensure type safety at compile time:
//
//	// This compiles - correct type
//	userEvent.Publish(ctx, User{ID: "1"})
//
//	// This won't compile - wrong type
//	userEvent.Publish(ctx, "string")  // compile error!
//
// Event Groups:
// Events with the same type can be grouped:
//
//	events := event.Events[User]{userCreated, userUpdated}
//	events.Subscribe(ctx, handler)  // Subscribe to all
//	events.Publish(ctx, user)       // Publish to all
package event

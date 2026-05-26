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
//	bus, err := event.NewBus("my-app", event.WithTransport(channel.New()))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer bus.Close(ctx)
//
//	// Create and register event
//	userEvent := event.New[User]("user.created")
//	if err := event.Register(ctx, bus, userEvent); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Subscribe with type-safe handler. Subscribe returns error on
//	// transport / registration problems — handle it the same way as
//	// Publish below.
//	if err := userEvent.Subscribe(ctx, func(ctx context.Context, ev event.Event[User], user User) error {
//	    fmt.Printf("User created: %s\n", user.Name)
//	    return nil
//	}); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Publish — returns error on transport failure or unregistered event.
//	if err := userEvent.Publish(ctx, User{ID: "123", Name: "John"}); err != nil {
//	    log.Fatal(err)
//	}
//
// Bus Options Reference. README.md "Options Reference" is the canonical
// surface (defaults, prose, cross-links); bus_options.go has the per-symbol
// godoc that pkg.go.dev renders. This list is a brief inventory; keep it
// in sync with the README table when adding options.
//   - WithTransport: set transport (required). Use channel.New(), redis.New(), etc.
//   - WithTracing: enable/disable OpenTelemetry tracing. Default is true.
//   - WithRecovery: enable/disable panic recovery in handlers. Default is true.
//   - WithMetrics: enable/disable OpenTelemetry metrics. Default is true.
//   - WithLogger: set logger for the bus.
//   - WithIdempotency: set idempotency store for duplicate detection.
//   - WithPoisonDetection: set poison detector for failing message quarantine.
//   - WithMonitor: set monitor store for event processing tracking.
//   - WithSchemaProvider: set schema provider for dynamic event configuration.
//   - WithStrictSchema: fail registration if schema provider returns an error.
//   - WithOutbox: set outbox store for transactional event publishing.
//   - WithDLQ: set DLQ store for automatic dead letter routing.
//   - WithPublishAudit: record every successful transport.Publish for
//     publish-vs-process gap triage; any monitor store satisfies the
//     PublishAuditStore interface.
//   - WithDrainTimeout: maximum time Bus.Close() blocks waiting for
//     in-flight handlers. Default is 0 (no wait).
//   - WithAll: combiner that fans a slice of BusOption values into one
//     option; used by sub-packages such as stack.WithReliabilityStack.
//
// Event Options (option.go):
//   - WithSubscriberTimeout: set handler execution timeout. Default is 0 (no timeout).
//   - WithErrorHandler: set panic recovery error callback.
//   - WithMaxRetries: set max retry attempts before sending to DLQ. Default is 0 (unlimited).
//   - WithPayloadCodec: override codec for this event's payload.
//   - WithMessageFilter: predicate over metadata; subscribers skip false-results.
//   - WithDecodeErrorHandler: decide what to do with a decode failure
//     (nil → ack and drop; ErrReject → route to DLQ).
//
// Subscribe Options (option.go):
//   - AsWorker: use WorkerPool mode (load balancing - one subscriber receives each message).
//   - AsBroadcast: use Broadcast mode (fan-out - all subscribers receive each message). Default.
//   - WithWorkerGroup: name a worker group; multiple groups each receive all messages, workers within compete.
//   - WithMiddleware / WithMiddlewareChain: add custom middleware to the handler chain.
//   - WithLatestOnly / WithMaxAge / WithBufferSize: freshness / backpressure tuning.
//   - WithSubscriberName / WithSubscriberDescription: labels surfaced in topology, monitor entries, and traces.
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
//
// Schema Registry:
// Publishers can define event configuration that subscribers auto-load:
//
//	// Create schema provider
//	provider := schema.NewMemoryProvider()
//
//	// Configure bus with schema provider
//	bus, _ := event.NewBus("my-app",
//	    event.WithTransport(transport),
//	    event.WithSchemaProvider(provider),
//	    event.WithIdempotency(idempStore),     // Required if schema enables idempotency
//	    event.WithPoisonDetection(detector),   // Required if schema enables poison detection
//	)
//
//	// Publisher defines schema
//	provider.Set(ctx, &schema.EventSchema{
//	    Name:              "order.created",
//	    Version:           1,
//	    SubTimeout:        30 * time.Second,
//	    MaxRetries:        3,
//	    EnableIdempotency: true,
//	    EnablePoison:      true,
//	})
//
//	// Subscriber auto-loads schema on Register
//	orderEvent := event.New[Order]("order.created")
//	event.Register(ctx, bus, orderEvent)  // Schema applied automatically
//
// Schema providers: MemoryProvider, PostgresProvider, RedisProvider.
// For MongoDB schema storage, use the separate event-mongodb module (https://github.com/rbaliyan/event-mongodb).
// Use WithStrictSchema(true) to fail registration if schema provider returns an error.
package event

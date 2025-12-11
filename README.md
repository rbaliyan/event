# Event v2

[![CI](https://github.com/rbaliyan/event/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rbaliyan/event/branch/master/graph/badge.svg)](https://codecov.io/gh/rbaliyan/event)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event/v2.svg)](https://pkg.go.dev/github.com/rbaliyan/event/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event/v2)](https://goreportcard.com/report/github.com/rbaliyan/event/v2)

A production-grade event pub-sub library for Go with support for distributed event handling, metrics, tracing, and configurable transports.

## V2 Changes

V2 introduces a cleaner architecture with transport-centric design:

- **Fire-and-forget API**: `Publish()` and `Subscribe()` return void - events are facts that happened
- **Transport owns delivery**: Async behavior, error handling, and graceful shutdown moved to transport layer
- **Simplified event**: Event focuses on routing and middleware (tracing, metrics, recovery)
- **Graceful shutdown**: `Transport.Close()` and `Registry.Close()` block until pending messages are delivered

## Features

- **Pub-Sub Pattern**: Simple publish/subscribe API for event-driven architectures
- **Async Handlers**: Non-blocking event processing (configured at transport level)
- **OpenTelemetry Tracing**: Built-in distributed tracing support
- **Prometheus Metrics**: Out-of-the-box metrics for monitoring
- **Panic Recovery**: Automatic recovery from panics in handlers
- **Context Propagation**: Full support for Go context with metadata
- **Configurable Transports**: Pluggable transport layer (channel-based by default)
- **Registry Scoping**: Namespace events with registries for isolation
- **Graceful Shutdown**: Transport waits for pending deliveries before closing

## Installation

```bash
go get github.com/rbaliyan/event/v2
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/rbaliyan/event/v2"
)

func main() {
    // Create an event
    e := event.New("user.created")

    // Subscribe to the event
    e.Subscribe(context.Background(), func(ctx context.Context, ev event.Event, data event.Data) {
        fmt.Printf("User created: %v\n", data)
    })

    // Publish an event (fire-and-forget)
    e.Publish(context.Background(), map[string]string{"id": "123", "name": "John"})
}
```

## Event Options

Events can be configured with various options:

```go
e := event.New("my.event",
    event.WithSubscriberTimeout(30*time.Second),     // Handler execution timeout (default: 0 = no timeout)
    event.WithTracing(true),                         // Enable OpenTelemetry tracing (default: true)
    event.WithMetrics(true, nil),                    // Enable Prometheus metrics (default: true)
    event.WithRecovery(true),                        // Enable panic recovery (default: true)
    event.WithErrorHandler(func(ev event.Event, err error) {
        log.Printf("Panic in event %s: %v", ev.Name(), err)
    }),
    event.WithChannelBufferSize(100),                // Buffer size for default transport
)
```

## Transport Options

Transport controls delivery behavior:

```go
transport := event.NewChannelTransport(
    event.WithTransportAsync(true),           // Async handler execution (default: true)
    event.WithTransportBufferSize(100),       // Channel buffer size (default: 100)
    event.WithTransportTimeout(time.Second),  // Per-subscriber send timeout (default: 0)
    event.WithTransportErrorHandler(func(err error) {
        log.Printf("Transport error: %v", err)
    }),
)

e := event.New("my.event", event.WithTransport(transport))
```

### Default Values

| Option | Default Value |
|--------|---------------|
| Async (transport) | `false` (synchronous) |
| Buffer Size | `0` (blocking) when sync, `100` when async |
| Subscriber Timeout | `0` (no timeout) |
| Tracing | `true` |
| Metrics | `true` |
| Recovery | `true` |

## Working with Registries

Registries provide namespace isolation for events:

```go
// Create a custom registry
reg := event.NewRegistry("myapp", prometheusRegisterer)

// Create events within the registry
userEvent := event.New("user.created", event.WithRegistry(reg))
orderEvent := event.New("order.placed", event.WithRegistry(reg))

// Get an event by name
e := reg.Get("user.created")

// Close all events in the registry (graceful shutdown)
reg.Close()
```

## Context and Metadata

Pass metadata through event context:

```go
// Add metadata to context
ctx := event.ContextWithMetadata(context.Background(), event.Metadata{
    "request_id": "abc-123",
    "user_id":    "user-456",
})

// Publish with metadata
e.Publish(ctx, data)

// Access metadata in handler
e.Subscribe(ctx, func(ctx context.Context, ev event.Event, data event.Data) {
    meta := event.ContextMetadata(ctx)
    requestID := meta.Get("request_id")

    // Other context helpers
    eventID := event.ContextEventID(ctx)
    source := event.ContextSource(ctx)
    subID := event.ContextSubscriptionID(ctx)
})
```

## Transport Layer

The default transport uses Go channels with a fan-out pattern:

```go
// Fan-out transport (all subscribers receive every message)
transport := event.NewChannelTransport(
    event.WithTransportAsync(true),
    event.WithTransportBufferSize(100),
)

// Single channel transport (load-balancing - one subscriber receives each message)
transport := event.NewSingleTransport(
    event.WithTransportAsync(true),
    event.WithTransportBufferSize(100),
)

// Custom transport (implement the Transport interface)
type Transport interface {
    Send() chan<- Message
    Receive(string) <-chan Message
    Delete(string)
    Close() error  // Blocks until all pending messages delivered
}
```

## Cancellation and Cleanup

Subscriptions can be cancelled using context:

```go
ctx, cancel := context.WithCancel(context.Background())

e.Subscribe(ctx, func(ctx context.Context, ev event.Event, data event.Data) {
    // Handle event
})

// Later, cancel the subscription
cancel()
```

## Multiple Subscribers

Events support multiple subscribers with fan-out delivery:

```go
e := event.New("notifications")

// All subscribers receive every published message
e.Subscribe(ctx, emailHandler)
e.Subscribe(ctx, smsHandler)
e.Subscribe(ctx, pushHandler)

e.Publish(ctx, notification) // Delivered to all three handlers
```

## Event Groups

Work with multiple events as a group:

```go
events := event.Events{
    event.New("event1"),
    event.New("event2"),
    event.New("event3"),
}

// Subscribe to all events
events.Subscribe(ctx, handler)

// Publish to all events
events.Publish(ctx, data)
```

## Metrics

When metrics are enabled, the following Prometheus metrics are exposed:

| Metric | Type | Description |
|--------|------|-------------|
| `event_<name>_published_total` | Counter | Total messages published |
| `event_<name>_subscribed_total` | Counter | Total subscribers added |
| `event_<name>_processed_total` | Counter | Total messages processed |
| `event_<name>_publishing_count` | Gauge | Messages currently being published |
| `event_<name>_processing_count` | Gauge | Messages currently being processed |

## Architecture

### V2 Design

```
Event (routing + middleware)
  └── Transport (delivery semantics)
        ├── Async goroutine spawning
        ├── Error handling via callback
        └── Graceful shutdown
```

### Middleware Chain

Handlers are wrapped in middleware in this order (innermost to outermost):

```
Recovery (panic handling)
  → Tracing (OpenTelemetry spans)
    → Metrics (Prometheus counters)
      → Timeout (context deadline)
```

## Testing

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run benchmarks
go test -bench=.
```

## Migration from V1

### Breaking Changes

1. **Import path**: Change to `github.com/rbaliyan/event/v2`
2. **Publish/Subscribe signatures**: No longer return errors
3. **Async config moved**: Use transport options instead of event options

### Before (V1)

```go
import "github.com/rbaliyan/event"

e := event.New("my.event",
    event.WithAsync(true),
    event.WithWorkerPoolSize(100),
    event.WithPublishTimeout(time.Second),
)

if err := e.Subscribe(ctx, handler); err != nil {
    log.Fatal(err)
}

if err := e.Publish(ctx, data); err != nil {
    log.Fatal(err)
}
```

### After (V2)

```go
import "github.com/rbaliyan/event/v2"

transport := event.NewChannelTransport(
    event.WithTransportAsync(true),
    event.WithTransportErrorHandler(func(err error) {
        log.Printf("error: %v", err)
    }),
)

e := event.New("my.event", event.WithTransport(transport))

e.Subscribe(ctx, handler)  // Fire-and-forget
e.Publish(ctx, data)       // Fire-and-forget
```

## License

MIT License - see [LICENSE](LICENSE) for details.

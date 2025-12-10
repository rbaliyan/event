# Event

[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event.svg)](https://pkg.go.dev/github.com/rbaliyan/event)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event)](https://goreportcard.com/report/github.com/rbaliyan/event)

A production-grade event pub-sub library for Go with support for distributed event handling, metrics, tracing, and configurable transports.

## Features

- **Pub-Sub Pattern**: Simple publish/subscribe API for event-driven architectures
- **Async Handlers**: Non-blocking event processing with configurable worker pools
- **OpenTelemetry Tracing**: Built-in distributed tracing support
- **Prometheus Metrics**: Out-of-the-box metrics for monitoring
- **Panic Recovery**: Automatic recovery from panics in handlers
- **Context Propagation**: Full support for Go context with metadata
- **Configurable Transports**: Pluggable transport layer (channel-based by default)
- **Registry Scoping**: Namespace events with registries for isolation

## Installation

```bash
go get github.com/rbaliyan/event
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/rbaliyan/event"
)

func main() {
    // Create an event
    e := event.New("user.created")

    // Subscribe to the event
    e.Subscribe(context.Background(), func(ctx context.Context, ev event.Event, data event.Data) {
        fmt.Printf("User created: %v\n", data)
    })

    // Publish an event
    e.Publish(context.Background(), map[string]string{"id": "123", "name": "John"})
}
```

## Configuration Options

Events can be configured with various options:

```go
e := event.New("my.event",
    event.WithAsync(true),                           // Enable async handlers (default: true)
    event.WithWorkerPoolSize(50),                    // Limit concurrent handlers (default: 100)
    event.WithPoolTimeout(5*time.Second),            // Timeout for acquiring worker pool slot (default: 5s)
    event.WithPublishTimeout(2*time.Second),         // Timeout for publishing (default: 0 = no timeout)
    event.WithSubscriberTimeout(30*time.Second),     // Timeout for handler execution (default: 0 = no timeout)
    event.WithTracing(true),                         // Enable OpenTelemetry tracing (default: true)
    event.WithMetrics(true, nil),                    // Enable Prometheus metrics (default: true)
    event.WithRecovery(true),                        // Enable panic recovery (default: true)
    event.WithErrorHandler(func(ev event.Event, err error) {
        log.Printf("Error in event %s: %v", ev.Name(), err)
    }),
)
```

### Default Values

| Option | Default Value |
|--------|---------------|
| Async | `true` |
| Worker Pool Size | `100` |
| Pool Timeout | `5000ms` |
| Publish Timeout | `0` (no timeout) |
| Subscriber Timeout | `0` (no timeout) |
| Channel Buffer Size | `100` |
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

// Close all events in the registry
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

The default transport uses Go channels with a fan-out pattern. You can use alternative transports:

```go
// Single channel transport (all subscribers share one channel)
transport := event.NewSingleTransport(time.Second, 100)
e := event.New("my.event", event.WithTransport(transport))

// Custom transport (implement the Transport interface)
type Transport interface {
    Send() chan<- Message
    Receive(string) <-chan Message
    Delete(string)
    Close() error
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

### Middleware Chain

Handlers are wrapped in middleware in this order (innermost to outermost):

```
Recovery (panic handling)
  → Tracing (OpenTelemetry spans)
    → Metrics (Prometheus counters)
      → Timeout (context deadline)
        → Async (goroutine spawning with semaphore)
```

### Worker Pool

The worker pool limits concurrent handler execution:
- Controlled by `WithWorkerPoolSize(n)`
- Uses a semaphore for limiting
- Falls back to unlimited goroutines if pool is exhausted (with warning log)

## Testing

```bash
# Run all tests
go test ./...

# Run tests with race detector
go test -race ./...

# Run benchmarks
go test -bench=.
```

## License

MIT License - see [LICENSE](LICENSE) for details.

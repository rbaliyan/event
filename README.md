# Event v3

[![CI](https://github.com/rbaliyan/event/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event/v3.svg)](https://pkg.go.dev/github.com/rbaliyan/event/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event/v3)](https://goreportcard.com/report/github.com/rbaliyan/event/v3)
[![Release](https://img.shields.io/github/v/release/rbaliyan/event)](https://github.com/rbaliyan/event/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/event/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/event)

A **production-grade event pub-sub library** for Go with support for distributed event handling, exactly-once semantics, and multiple transports. Comparable to MassTransit (.NET), Axon (Java), and Spring Cloud Stream.

## Features

### Core
- **Type-Safe Generics**: `Event[T]` ensures compile-time type safety
- **Multiple Transports**: Channel (in-memory), Redis Streams, NATS JetStream, Kafka
- **Fire-and-Forget API**: `Publish()` and `Subscribe()` are void - events are facts
- **Delivery Modes**: Broadcast (fan-out) or WorkerPool (load balancing)

### Reliability
- **Transactional Outbox**: Atomic publish with database writes (PostgreSQL, Redis; MongoDB via [event-mongodb](https://github.com/rbaliyan/event-mongodb))
- **Idempotency**: Prevent duplicate processing (Redis, PostgreSQL, in-memory)
- **Poison Detection**: Auto-quarantine repeatedly failing messages
- **At-Least-Once Delivery**: Via Redis Streams, NATS, or Kafka

### Advanced
- **Message Routing**: Route events to specific subscribers via metadata routing keys
- **Message Coalescing**: Deduplicate rapid updates, deliver only latest per key
- **Schema Registry**: Publisher-defined event configuration with subscriber auto-sync
- **Backoff Strategies**: Exponential, linear, constant with jitter support

### Observability
- **OpenTelemetry Tracing**: Distributed tracing across services
- **OpenTelemetry Metrics**: Out-of-the-box monitoring
- **Health Checks**: Transport health and consumer lag monitoring
- **Event Monitoring**: Track event processing status, duration, and errors

## Ecosystem

The event library is part of a larger ecosystem of packages:

| Package | Description | Install |
|---------|-------------|---------|
| **event** | Core event bus with transports | `go get github.com/rbaliyan/event/v3` |
| **event-mongodb** | MongoDB Change Stream transport (CDC) | `go get github.com/rbaliyan/event-mongodb` |
| **event-dlq** | Dead Letter Queue management | `go get github.com/rbaliyan/event-dlq` |
| **event-scheduler** | Delayed/scheduled message delivery | `go get github.com/rbaliyan/event-scheduler` |
| **event-extras** | Rate limiting and saga orchestration | `go get github.com/rbaliyan/event-extras` |

All packages share consistent patterns:
- Functional options for configuration
- Health checks via `health.Checker` interface
- OpenTelemetry metrics support
- Multiple backend implementations (PostgreSQL, Redis, and MongoDB via event-mongodb)

> **Note:** MongoDB implementations for outbox, monitor, distributed state manager, schema, idempotency, and checkpoint were moved to the [event-mongodb](https://github.com/rbaliyan/event-mongodb) module. See each section below for migration details.

## Installation

```bash
go get github.com/rbaliyan/event/v3
```

## Quick Start

### Basic Usage with Type Safety

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/transport/channel"
)

type Order struct {
    ID     string
    Amount float64
}

func main() {
    ctx := context.Background()

    // Create a bus with channel transport
    bus, err := event.NewBus("my-app", event.WithTransport(channel.New()))
    if err != nil {
        log.Fatal(err)
    }
    defer bus.Close(ctx)

    // Create and register a type-safe event
    orderEvent := event.New[Order]("order.created")
    if err := event.Register(ctx, bus, orderEvent); err != nil {
        log.Fatal(err)
    }

    // Subscribe with type-safe handler
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        fmt.Printf("Order received: %s, Amount: $%.2f\n", order.ID, order.Amount)
        return nil
    })

    // Publish (fire-and-forget)
    orderEvent.Publish(ctx, Order{ID: "ORD-123", Amount: 99.99})
}
```

## Transports

### Redis Streams (Recommended for Production)

Redis Streams provides at-least-once delivery with consumer groups:

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/transport/redis"
    redisclient "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    rdb := redisclient.NewClient(&redisclient.Options{
        Addr: "localhost:6379",
    })

    transport, _ := redis.New(rdb,
        redis.WithConsumerGroup("order-service"),
        redis.WithMaxLen(10000),
        redis.WithMaxAge(24*time.Hour),
        redis.WithClaimInterval(30*time.Second, time.Minute),
    )

    bus, _ := event.NewBus("order-service", event.WithTransport(transport))
    defer bus.Close(ctx)
}
```

### NATS JetStream

For durable messaging with native broker features:

```go
import (
    "github.com/rbaliyan/event/v3/transport/nats"
    natsgo "github.com/nats-io/nats.go"
)

func main() {
    ctx := context.Background()

    nc, _ := natsgo.Connect("nats://localhost:4222")

    transport, _ := nats.NewJetStream(nc,
        nats.WithDeduplication(time.Hour),
        nats.WithMaxDeliver(5),
        nats.WithAckWait(30*time.Second),
    )

    bus, _ := event.NewBus("my-app", event.WithTransport(transport))
    defer bus.Close(ctx)
}
```

### Kafka

Kafka with native dead letter topic (DLT) support:

```go
import (
    "github.com/rbaliyan/event/v3/transport/kafka"
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()

    config := sarama.NewConfig()
    config.Consumer.Offsets.AutoCommit.Enable = false

    client, _ := sarama.NewClient([]string{"localhost:9092"}, config)

    transport, _ := kafka.New(client,
        kafka.WithConsumerGroup("order-service"),
    )

    bus, _ := event.NewBus("my-app", event.WithTransport(transport))
    defer bus.Close(ctx)
}
```

### MongoDB Change Streams (CDC)

For Change Data Capture scenarios, use the separate **event-mongodb** package:

```go
import (
    "github.com/rbaliyan/event/v3"
    mongodb "github.com/rbaliyan/event-mongodb"
    "go.mongodb.org/mongo-driver/v2/mongo"
)

func main() {
    ctx := context.Background()

    client, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
    db := client.Database("myapp")

    // Watch a specific collection
    transport, _ := mongodb.New(db,
        mongodb.WithCollection("orders"),
        mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    )

    bus, _ := event.NewBus("order-watcher", event.WithTransport(transport))
    defer bus.Close(ctx)

    // Subscribe to changes
    changes := event.New[mongodb.ChangeEvent]("db-changes")
    event.Register(ctx, bus, changes)

    changes.Subscribe(ctx, func(ctx context.Context, e event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
        fmt.Printf("Change: %s on %s.%s\n", change.OperationType, change.Database, change.Collection)
        return nil
    })

    // Publishing via Bus is NOT supported - write directly to MongoDB
    // ordersCol.InsertOne(ctx, order) triggers the subscriber
}
```

**Note:** MongoDB transport is subscribe-only (CDC). Publishing happens via direct MongoDB writes.

### Transport Feature Comparison

| Feature | Redis Streams | NATS JetStream | Kafka | MongoDB CDC |
|---------|:-------------:|:--------------:|:-----:|:-----------:|
| Persistence | ✅ | ✅ | ✅ | ✅ |
| At-Least-Once | ✅ | ✅ | ✅ | ✅ |
| Consumer Groups | ✅ | ✅ | ✅ | ❌ (Broadcast) |
| Native Deduplication | ❌ | ✅ | ❌ | ❌ |
| Native DLQ/DLT | ❌ | ❌ | ✅ | ❌ |
| Publish Support | ✅ | ✅ | ✅ | ❌ |
| WorkerPool Mode | ✅ | ✅ | ✅ | via `distributed`* |

\* MongoDB CDC supports WorkerPool mode through the `distributed` package, which emulates worker semantics using database atomic state transitions. See [Distributed WorkerPool](#distributed-workerpool).

### Circuit Breaker

Protect publish calls from cascading failures when a transport backend is temporarily unavailable. Currently supported on Redis transport:

```go
transport, _ := redis.New(rdb,
    redis.WithCircuitBreaker(5, 30*time.Second), // open after 5 failures, cooldown 30s
)
```

When open, `Publish` returns `transport.ErrCircuitOpen` immediately instead of blocking until timeout. After the cooldown period, one probe call is allowed through — success closes the breaker, failure re-opens it.

The `CircuitBreaker` struct in the `transport` package is reusable and can be embedded by any transport implementation.

### Transport Migration

Bridge an old and new transport during a migration with zero message loss:

```go
import "github.com/rbaliyan/event/v3/transport/migration"

mt, _ := migration.New(oldRedisTransport, newKafkaTransport,
    migration.WithMergedBufferSize(128),
)
bus, _ := event.NewBus("mybus", event.WithTransport(mt))
```

- **Publish** routes to the new transport only
- **Subscribe** fans-in messages from both transports into a single subscription
- Falls back to new-only if the old transport fails to subscribe
- Health reports degraded when old is down, unhealthy when new is down
- Consumer lag is prefixed with `old:`/`new:` for dashboard disambiguation

Once the old transport is fully drained, replace the migration transport with the new one directly.

## Health Checks

Stores implement `health.Checker`; transports implement `transport.HealthChecker`. The bus aggregates both:

```go
// Check bus health (aggregates transport + all configured stores)
status := bus.Status(ctx)
fmt.Printf("Status: %s, Latency: %v\n", status.Code, status.Latency)

// Check individual stores via health.CheckAll
results := health.CheckAll(ctx, map[string]health.Checker{
    "idempotency": idempStore,
    "monitor":     monitorStore,
})
for name, result := range results.Components {
    fmt.Printf("%s: %s\n", name, result.Status)
}
```

Health status levels:
- `StatusHealthy` - Component is fully operational
- `StatusDegraded` - Component is operational but has issues (e.g., high latency, pending messages)
- `StatusUnhealthy` - Component is not operational

## Backoff Strategies

Configure retry behavior with pluggable backoff strategies:

```go
import "github.com/rbaliyan/event/v3/backoff"

// Exponential backoff (recommended)
strategy := &backoff.Exponential{
    Initial:    100 * time.Millisecond,
    Multiplier: 2.0,
    Max:        30 * time.Second,
    Jitter:     0.1, // 10% randomization
}

// Linear backoff
strategy := &backoff.Linear{
    Initial:   100 * time.Millisecond,
    Increment: 100 * time.Millisecond,
    Max:       5 * time.Second,
}

// Constant delay
strategy := &backoff.Constant{
    Delay: 500 * time.Millisecond,
}

// Use with event options
orderEvent := event.New[Order]("order.created",
    event.WithMaxRetries(5),
)
```

## Delivery Modes

### Broadcast (Default)

All subscribers receive every message:

```go
orderEvent.Subscribe(ctx, notifyWarehouse, event.AsBroadcast[Order]())
orderEvent.Subscribe(ctx, notifyShipping, event.AsBroadcast[Order]())
// Both handlers receive every order
```

### Worker Pool

Only one subscriber receives each message (load balancing):

```go
orderEvent.Subscribe(ctx, processOrder, event.AsWorker[Order]())
orderEvent.Subscribe(ctx, processOrder, event.AsWorker[Order]())
// Each order processed by exactly one worker
```

### Worker Groups

Multiple groups, each receiving all messages. Workers within a group compete:

```go
// Group A: Order processors (3 workers compete)
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("order-processors"))

// Group B: Analytics (2 workers compete)
orderEvent.Subscribe(ctx, trackAnalytics,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("analytics"))

// Each order goes to 1 processor AND 1 analytics worker
```

## Distributed WorkerPool

The `distributed` package enables WorkerPool semantics on Broadcast-only transports (like MongoDB Change Streams) using database atomic state transitions. Only one worker processes each message, with automatic failover and payload recovery.

### Basic Usage

```go
import "github.com/rbaliyan/event/v3/distributed"

// Create a coordinator (Redis for distributed deployments)
coord, _ := distributed.NewRedisStateManager(redisClient,
    distributed.WithCompletedTTL(48*time.Hour),
)

// Subscribe with WorkerPool emulation
mongoEvent.Subscribe(ctx, handler,
    event.WithMiddleware(
        distributed.WorkerPoolMiddleware[Order](coord, 5*time.Minute),
    ),
)
```

### Payload Recovery

For transports without re-delivery (e.g., MongoDB Change Streams), the middleware automatically stores message payload so the RecoveryRunner can re-publish stale events if a worker crashes:

```go
// Redis-backed coordinator with payload recovery
coord, _ := distributed.NewRedisStateManager(redisClient,
    distributed.WithCompletedTTL(48*time.Hour),
)

// RecoveryRunner detects PayloadStore capability automatically
runner, _ := distributed.NewRecoveryRunner(coord,
    distributed.WithPublisher(bus),     // enables re-publishing
    distributed.WithStaleTimeout(2*time.Minute),
    distributed.WithCheckInterval(30*time.Second),
)

go runner.Run(ctx)
```

For MongoDB-backed payload recovery, use `distributed.NewMongoStateManager` from the [event-mongodb](https://github.com/rbaliyan/event-mongodb) module.

Recovery is two-phase:
1. **Re-publish**: Stale entries with stored payload are re-published via the bus with a new event ID
2. **Reset**: Remaining stale entries (no payload) are reset for reacquisition

### Worker Groups

Use separate coordinators with different prefixes per group:

```go
smA, _ := distributed.NewRedisStateManager(redis, distributed.WithPrefix("processors:"))
smB, _ := distributed.NewRedisStateManager(redis, distributed.WithPrefix("analytics:"))

orderEvent.Subscribe(ctx, processOrder,
    event.WithMiddleware(distributed.WorkerPoolMiddleware[Order](smA, ttl)))
orderEvent.Subscribe(ctx, collectAnalytics,
    event.WithMiddleware(distributed.WorkerPoolMiddleware[Order](smB, ttl)))
```

### Coordinator Backends

| Backend | Package | Use Case |
|---------|---------|----------|
| Redis | `distributed.NewRedisStateManager` | Distributed deployments (recommended) |
| MongoDB | `distributed.NewMongoStateManager` (event-mongodb) | When MongoDB is already your primary store |
| Memory | `distributed.NewMemoryStateManager` | Single-instance or testing |

All three backends implement both `Coordinator` and `PayloadStore` interfaces.

The MongoDB backend is provided by the [event-mongodb](https://github.com/rbaliyan/event-mongodb) module.

### Worker Observability

Query active and completed worker states using the `WorkerStore` interface
(implemented by `MemoryStateManager` and `MongoStateManager` from event-mongodb):

```go
page, _ := sm.ListWorkers(ctx, distributed.WorkerFilter{
    Status: []distributed.WorkerState{distributed.WorkerStateProcessing},
    Limit:  100,
})

count, _ := sm.CountWorkers(ctx, distributed.WorkerFilter{
    StaleTimeout: 5 * time.Minute,
})
```

**Note**: `RedisStateManager` does not implement `WorkerStore` due to
Redis SCAN's O(N) cost.

## Transactional Outbox Pattern

Ensure atomic publish with database writes:

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/outbox"
)

func main() {
    ctx := context.Background()

    store, _ := outbox.NewPostgresStore(db)

    bus, _ := event.NewBus("order-service",
        event.WithTransport(transport),
        event.WithOutbox(store),
    )
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    // Normal publish - goes directly to transport
    orderEvent.Publish(ctx, Order{ID: "123"})

    // Inside transaction - automatically routes to outbox
    tx, _ := db.BeginTx(ctx, nil)
    ctx = event.WithOutboxTx(ctx, tx)
    _, err := tx.ExecContext(ctx, "INSERT INTO orders ...")
    if err != nil {
        tx.Rollback()
        return
    }
    orderEvent.Publish(ctx, order) // Goes to outbox
    tx.Commit()

    // Start relay to publish from outbox to transport
    relay := outbox.NewRelay(store, transport)
    go relay.Start(ctx)
}
```

For MongoDB outbox support, use the [event-mongodb](https://github.com/rbaliyan/event-mongodb) module.

## Idempotency

Prevent duplicate message processing:

```go
import "github.com/rbaliyan/event/v3/idempotency"

store, _ := idempotency.NewRedisStore(redisClient, time.Hour)

bus, _ := event.NewBus("order-service",
    event.WithTransport(transport),
    event.WithIdempotency(store),
)

// All subscribers automatically get deduplication
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    return processOrder(ctx, order) // Duplicates automatically skipped
})
```

## Poison Message Detection

Auto-quarantine messages that keep failing:

```go
import "github.com/rbaliyan/event/v3/poison"

store, _ := poison.NewRedisStore(redisClient)
detector := poison.NewDetector(store,
    poison.WithThreshold(5),
    poison.WithQuarantineTime(time.Hour),
)

bus, _ := event.NewBus("order-service",
    event.WithTransport(transport),
    event.WithPoisonDetection(detector),
)

// Messages failing 5+ times are automatically quarantined
orderEvent.Subscribe(ctx, processOrder)

// Release a message from quarantine
detector.Release(ctx, messageID)
```

## Event Monitoring

Track event processing status, duration, and errors:

```go
import "github.com/rbaliyan/event/v3/monitor"

store, _ := monitor.NewPostgresStore(db)

bus, _ := event.NewBus("order-service",
    event.WithTransport(transport),
    event.WithMonitor(store),
)

// Query monitoring data
page, _ := store.List(ctx, monitor.Filter{
    Status:    []monitor.Status{monitor.StatusFailed},
    StartTime: time.Now().Add(-time.Hour),
    Limit:     100,
})

for _, entry := range page.Entries {
    fmt.Printf("Event %s: %s (duration: %v)\n",
        entry.EventID, entry.Status, entry.Duration)
}
```

### Monitor HTTP API

```go
import monitorhttp "github.com/rbaliyan/event/v3/monitor/http"

handler := monitorhttp.New(store)
http.Handle("/", handler)
http.ListenAndServe(":8080", nil)
```

Endpoints:
- `GET /v1/monitor/entries` - List entries with filters
- `GET /v1/monitor/entries/{event_id}` - Get entries for an event
- `DELETE /v1/monitor/entries?older_than=1h` - Delete old entries

### Worker Pool State (HTTP)

When using distributed worker pools, expose worker state
via the monitor HTTP handler:

```go
handler := monitorhttp.New(store, monitorhttp.WithWorkerStore(sm))
```

Endpoints:
- `GET /v1/workers` - List workers (filters: status, event_name, stale_timeout, cursor, limit)
- `GET /v1/workers/{message_id}` - Get single worker
- `GET /v1/workers/count` - Count workers matching filter

## Schema Registry

Define event configuration centrally:

```go
import "github.com/rbaliyan/event/v3/schema"

provider, _ := schema.NewPostgresProvider(db, nil)
defer provider.Close()

bus, _ := event.NewBus("order-service",
    event.WithTransport(transport),
    event.WithSchemaProvider(provider),
    event.WithIdempotency(idempStore),
    event.WithMonitor(monitorStore),
)

// Publisher: Define schema
provider.Set(ctx, &schema.EventSchema{
    Name:              "order.created",
    Version:           1,
    SubTimeout:        30 * time.Second,
    MaxRetries:        3,
    EnableMonitor:     true,
    EnableIdempotency: true,
})

// Subscriber: Schema auto-loaded on Register()
orderEvent := event.New[Order]("order.created")
event.Register(ctx, bus, orderEvent) // Loads schema automatically
```

## Error Handling

Use semantic error types to control message acknowledgment:

```go
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    err := processOrder(ctx, order)

    switch {
    case err == nil:
        return nil // ACK - message processed

    case errors.Is(err, ErrTemporary):
        return event.ErrNack // NACK - retry immediately

    case errors.Is(err, ErrTransient):
        return event.ErrDefer // NACK - retry with backoff

    case errors.Is(err, ErrPermanent):
        return event.ErrReject // ACK + send to DLQ

    default:
        return event.Defer(err) // Default: retry with backoff
    }
})
```

## Dead Letter Queue

Use **event-dlq** for failed message management:

```go
import dlq "github.com/rbaliyan/event-dlq"

store := dlq.NewPostgresStore(db)
manager := dlq.NewManager(store, transport)

// Store failed message
manager.Store(ctx, "order.created", msgID, payload, metadata, err, retryCount, "order-service")

// Replay failed messages
replayed, _ := manager.Replay(ctx, dlq.Filter{
    EventName:      "order.created",
    ExcludeRetried: true,
})

// Get statistics
stats, _ := manager.Stats(ctx)
fmt.Printf("Pending: %d\n", stats.PendingMessages)
```

## Scheduled Messages

Use **event-scheduler** for delayed delivery:

```go
import scheduler "github.com/rbaliyan/event-scheduler"

sched := scheduler.NewRedisScheduler(redisClient, transport,
    scheduler.WithPollInterval(100*time.Millisecond),
)
defer sched.Close(ctx)

go sched.Start(ctx)

// Schedule for future delivery
sched.Schedule(ctx, "order.reminder", payload, time.Now().Add(24*time.Hour))

// Schedule with ID for cancellation
sched.ScheduleWithID(ctx, "reminder-123", "order.reminder", payload, deliverAt)
sched.Cancel(ctx, "reminder-123")
```

## Rate Limiting

Use **event-extras/ratelimit** for rate limiting:

```go
import "github.com/rbaliyan/event-extras/ratelimit"

// Local rate limiter
limiter := ratelimit.NewTokenBucket(100, 10) // 100 rps, burst of 10

// Distributed rate limiter
limiter := ratelimit.NewRedisLimiter(redisClient, "api-service", 100, time.Second)

// Use in handler
if limiter.Allow(ctx) {
    processRequest()
} else {
    return errors.New("rate limited")
}

// Or block until allowed
if err := limiter.Wait(ctx); err != nil {
    return err
}
processRequest()
```

## Saga Orchestration

Use **event-extras/saga** for distributed transactions:

```go
import "github.com/rbaliyan/event-extras/saga"

orderSaga := saga.New("order-creation",
    &CreateOrderStep{orderService},
    &ReserveInventoryStep{inventoryService},
    &ProcessPaymentStep{paymentService},
).
    WithStore(saga.NewRedisStore(redisClient)).
    WithBackoff(&backoff.Exponential{Initial: time.Second, Max: 30*time.Second}).
    WithMaxRetries(3)

// Execute saga
sagaID := uuid.New().String()
if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
    // Compensations were automatically run
    log.Error("order creation failed", "saga_id", sagaID, "error", err)
}
```

## Database Support

| Component | PostgreSQL | MongoDB† | Redis | In-Memory |
|-----------|:----------:|:--------:|:-----:|:---------:|
| Outbox | ✅ | ✅ | ✅ | - |
| Idempotency | ✅ | ✅ | ✅ | ✅ |
| Poison | ✅ | - | ✅ | ✅ |
| Monitor | ✅ | ✅ | - | ✅ |
| Schema Registry | ✅ | ✅ | ✅ | ✅ |
| DLQ | ✅ | ✅ | ✅ | ✅ |
| Scheduler | ✅ | ✅ | ✅ | - |
| Saga | ✅ | ✅ | ✅ | ✅ |
| Distributed WP | - | ✅ | ✅ | ✅ |

† MongoDB implementations are provided by the separate [event-mongodb](https://github.com/rbaliyan/event-mongodb) module.

## Testing

Use built-in test utilities:

```go
func TestOrderHandler(t *testing.T) {
    ctx := context.Background()
    bus, _ := event.TestBus(channel.New())
    defer bus.Close(ctx)

    handler := event.NewTestHandler(func(ctx context.Context, e event.Event[Order], order Order) error {
        return nil
    })

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    orderEvent.Subscribe(ctx, handler.Handler())
    orderEvent.Publish(ctx, Order{ID: "test"})

    if !handler.WaitFor(1, 100*time.Millisecond) {
        t.Error("handler not called")
    }

    calls := handler.Received()
    if calls[0].Data.ID != "test" {
        t.Error("wrong order ID")
    }
}
```

## Message Routing

Route messages to specific subscribers based on metadata routing keys:

```go
// Publisher: tag messages with routing keys
ctx = event.ContextWithRoutingKey(ctx, "region", "us-east")
ctx = event.ContextWithRoutingKey(ctx, "priority", "high")
orderEvent.Publish(ctx, order)

// Subscriber: only receive matching messages
orderEvent.Subscribe(ctx, handler,
    event.WithRouteFilter[Order]("region", "us-east"),
)

// Multiple filters (AND semantics)
orderEvent.Subscribe(ctx, handler,
    event.WithRouteFilter[Order]("region", "us-east"),
    event.WithRouteFilter[Order]("priority", "high"),
)

// Custom predicate
orderEvent.Subscribe(ctx, handler,
    event.WithRouteMatch[Order](func(meta map[string]string) bool {
        return meta["X-Route-region"] != "eu-west"
    }),
)
```

For the channel transport, filtering happens at dispatch time — non-matching messages never enter the subscriber's buffer. For other transports, filtering happens at the event layer after receiving.

## Message Coalescing

Deduplicate rapid updates by key, delivering only the latest message per key:

```go
// Post-decode: group by a field in the decoded payload
orderEvent.Subscribe(ctx, handler,
    event.WithCoalesceByKey[Order](func(o Order) string {
        return o.ID // Only latest update per order
    }),
)

// Pre-decode: group by a metadata key (more efficient, no decode overhead)
orderEvent.Subscribe(ctx, handler,
    event.WithCoalesceByMetadata[Order]("document_key"),
)
```

## Consumer Identity (Redis)

For resilient Redis consumers, use stable consumer IDs and orphan claiming:

```go
// Stable consumer ID: reclaim pending messages after restart
orderEvent.Subscribe(ctx, handler,
    event.WithConsumerID[Order]("order-processor-"+hostname),
    event.AsWorker[Order](),
)

// Transport-level: claim orphaned messages from dead consumers
transport, _ := redis.New(client,
    redis.WithClaimInterval(30*time.Second, 2*time.Minute),
    redis.WithClaimBatchSize(500),
)
```

## Topology

Inspect all registered buses, events, and subscriptions at runtime:

```go
// Global topology snapshot
infos := event.Topology()
for _, bus := range infos {
    fmt.Printf("Bus: %s, Events: %d\n", bus.Name, len(bus.Events))
}

// Single bus topology
busInfo := bus.Topology()
```

## System View

The monitor HTTP handler provides a cached system view for dashboards:

```go
import monitorhttp "github.com/rbaliyan/event/v3/monitor/http"

handler := monitorhttp.New(store,
    monitorhttp.WithSystemRefreshInterval(10*time.Second), // default
)
defer handler.Close() // stop background refresh

http.Handle("/", handler)
```

Endpoints:
- `GET /v1/system` — aggregated topology, health, DLQ, scheduler, summary (cached)
- `GET /v1/system/health` — health status (200 or 503)
- `GET /v1/topology` — bus/event/subscription topology

## License

MIT License - see [LICENSE](LICENSE) for details.

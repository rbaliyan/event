# Event v3

[![CI](https://github.com/rbaliyan/event/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rbaliyan/event/branch/development/graph/badge.svg)](https://codecov.io/gh/rbaliyan/event)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event/v3.svg)](https://pkg.go.dev/github.com/rbaliyan/event/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event/v3)](https://goreportcard.com/report/github.com/rbaliyan/event/v3)

A **production-grade event pub-sub library** for Go with support for distributed event handling, exactly-once semantics, sagas, scheduled messages, and multiple transports. Comparable to MassTransit (.NET), Axon (Java), and Spring Cloud Stream.

## Features

### Core
- **Type-Safe Generics**: `Event[T]` ensures compile-time type safety
- **Multiple Transports**: Channel (in-memory), Redis Streams, NATS JetStream, Kafka, MongoDB (CDC)
- **Fire-and-Forget API**: `Publish()` and `Subscribe()` are void - events are facts
- **Delivery Modes**: Broadcast (fan-out) or WorkerPool (load balancing)

### Reliability
- **Transactional Outbox**: Atomic publish with database writes (PostgreSQL, MongoDB, Redis)
- **Dead Letter Queue**: Store, list, and replay failed messages
- **Idempotency**: Prevent duplicate processing (Redis, in-memory)
- **Poison Detection**: Auto-quarantine repeatedly failing messages
- **At-Least-Once Delivery**: Via Redis Streams, NATS, or Kafka

### Advanced
- **Saga Orchestration**: Multi-step workflows with compensation
- **Scheduled Messages**: Delayed/scheduled delivery (Redis, PostgreSQL, MongoDB)
- **Batch Processing**: High-throughput batch handlers
- **Rate Limiting**: Distributed rate limiting (Redis)
- **Circuit Breaker**: Failure isolation pattern
- **Schema Registry**: Publisher-defined event configuration with subscriber auto-sync

### Observability
- **OpenTelemetry Tracing**: Distributed tracing across services
- **Prometheus Metrics**: Out-of-the-box monitoring
- **Health Checks**: Transport health and consumer lag monitoring
- **Event Monitoring**: Track event processing status, duration, and errors

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
    bus, err := event.NewBus("my-app", event.WithBusTransport(channel.New()))
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

Redis Streams provides at-least-once delivery. Since Redis Streams doesn't have native
deduplication or DLQ features, reliability stores can be injected:

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

    // Basic setup
    transport, _ := redis.New(rdb,
        redis.WithConsumerGroup("order-service"),
        redis.WithMaxLen(10000),             // Stream max length
        redis.WithMaxAge(24*time.Hour),      // Message retention
        redis.WithClaimInterval(30*time.Second, time.Minute), // Orphan claiming
    )

    // With reliability store injection
    transport, _ := redis.New(rdb,
        redis.WithConsumerGroup("order-service"),
        redis.WithIdempotencyStore(idempStore),  // Deduplication
        redis.WithDLQHandler(dlqHandler),         // Dead letter handling
        redis.WithPoisonDetector(poisonDetector), // Poison message detection
        redis.WithMaxRetries(3),                  // Retry limit before DLQ
    )

    bus, err := event.NewBus("order-service", event.WithBusTransport(transport))
    if err != nil {
        log.Fatal(err)
    }
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)
}
```

### NATS

The NATS transport provides two modes:

#### NATS Core (At-Most-Once)

For ephemeral events where message loss is acceptable:

```go
import (
    "github.com/rbaliyan/event/v3/transport/nats"
    natsgo "github.com/nats-io/nats.go"
)

func main() {
    ctx := context.Background()

    nc, _ := natsgo.Connect("nats://localhost:4222")

    // NATS Core - simple pub/sub, no persistence
    transport, _ := nats.New(nc,
        nats.WithCoreLogger(logger),
    )

    // Optional: Add library-level reliability stores
    transport, _ := nats.New(nc,
        nats.WithIdempotencyStore(idempStore),  // Deduplication
        nats.WithDLQHandler(dlqHandler),         // Dead letter handling
        nats.WithPoisonDetector(poisonDetector), // Poison message detection
    )

    bus, _ := event.NewBus("my-app", event.WithBusTransport(transport))
    defer bus.Close(ctx)
}
```

#### NATS JetStream (At-Least-Once)

For durable messaging with native broker features:

```go
import (
    "github.com/rbaliyan/event/v3/transport/nats"
    natsgo "github.com/nats-io/nats.go"
)

func main() {
    ctx := context.Background()

    nc, _ := natsgo.Connect("nats://localhost:4222")
    js, _ := nc.JetStream()

    // JetStream with native features - no external stores needed
    transport, _ := nats.NewJetStream(js,
        nats.WithStreamName("ORDERS"),
        nats.WithDeduplication(time.Hour),  // Native dedup via Nats-Msg-Id header
        nats.WithMaxDeliver(5),             // Native retry limit
        nats.WithAckWait(30*time.Second),   // Acknowledgment timeout
    )

    bus, _ := event.NewBus("my-app", event.WithBusTransport(transport))
    defer bus.Close(ctx)
}
```

### Kafka

Kafka provides native dead letter topic (DLT) support:

```go
import (
    "github.com/rbaliyan/event/v3/transport/kafka"
    "github.com/IBM/sarama"
)

func main() {
    ctx := context.Background()

    config := sarama.NewConfig()
    config.Consumer.Offsets.AutoCommit.Enable = false // Required for at-least-once

    // Basic setup
    transport, _ := kafka.New(
        []string{"localhost:9092"},
        config,
        kafka.WithConsumerGroup("order-service"),
    )

    // With native dead letter topic support
    transport, _ := kafka.New(
        []string{"localhost:9092"},
        config,
        kafka.WithConsumerGroup("order-service"),
        kafka.WithDeadLetterTopic("orders.dlq"), // Native DLT routing
        kafka.WithMaxRetries(3),                  // Retry before sending to DLT
        kafka.WithRetention(24*time.Hour),        // Topic retention
    )

    bus, _ := event.NewBus("my-app", event.WithBusTransport(transport))
    defer bus.Close(ctx)
}
```

### MongoDB Change Streams (Subscribe-Only)

MongoDB transport is a **subscribe-only** transport for Change Data Capture (CDC) scenarios. It watches MongoDB for changes and delivers them as events. Publishing is implicit - writing to MongoDB triggers the events.

#### Watch Levels

The transport supports three levels of change stream watching:

| Level | Constructor | Watches | Use Case |
|-------|-------------|---------|----------|
| Collection | `New(db, WithCollection("orders"))` | Single collection | Track specific entity changes |
| Database | `New(db)` | All collections in a database | Multi-tenant per database |
| Cluster | `NewClusterWatch(client)` | All databases | Global audit log, cross-DB sync |

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/transport/mongodb"
    "go.mongodb.org/mongo-driver/mongo"
    mongoopts "go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
    ctx := context.Background()

    // Connect to MongoDB
    client, _ := mongo.Connect(ctx, mongoopts.Client().ApplyURI("mongodb://localhost:27017"))
    db := client.Database("myapp")

    // Option 1: Watch a specific collection
    transport, _ := mongodb.New(db,
        mongodb.WithCollection("orders"),
        mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    )

    // Option 2: Watch all collections in the database
    transport, _ := mongodb.New(db,
        mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    )

    // Option 3: Watch all databases in the cluster
    transport, _ := mongodb.NewClusterWatch(client,
        mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
    )

    // Create bus - works for subscribing only
    bus, _ := event.NewBus("order-watcher", event.WithBusTransport(transport))
    defer bus.Close(ctx)

    // Subscribe to changes
    changes := event.New[mongodb.ChangeEvent]("db-changes")
    event.Register(ctx, bus, changes)

    changes.Subscribe(ctx, func(ctx context.Context, e event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
        // For database/cluster level, check which collection the change came from
        fmt.Printf("Change in %s.%s: %s on %s\n",
            change.Database,
            change.Collection,
            change.OperationType,
            change.DocumentKey.Hex())
        return nil
    })

    // Publishing via Bus is NOT supported - write directly to MongoDB instead
    // This triggers the change stream and notifies subscribers:
    ordersCol := db.Collection("orders")
    ordersCol.InsertOne(ctx, order) // Triggers the subscriber above
}
```

#### ChangeEvent Fields

When watching at database or cluster level, use these fields to identify the source:

| Field | Description | Example |
|-------|-------------|---------|
| `Database` | Database name | `"myapp"` |
| `Collection` | Collection name | `"orders"` |
| `Namespace` | Full namespace | `"myapp.orders"` |
| `OperationType` | Operation type | `"insert"`, `"update"`, `"delete"` |
| `DocumentKey` | Document `_id` | `ObjectID("...")` |
| `FullDocument` | Full document (if requested) | `bson.Raw` |

#### Filtering Changes

Use `WithPipeline()` to filter change events:

```go
// Only watch insert and update in specific collections
pipeline := mongo.Pipeline{
    {{Key: "$match", Value: bson.M{
        "operationType": bson.M{"$in": []string{"insert", "update"}},
        "ns.coll": bson.M{"$in": []string{"orders", "customers"}},
    }}},
}

transport, _ := mongodb.New(db, mongodb.WithPipeline(pipeline))
```

**Key Differences from Other Transports:**
- `Publish()` returns `ErrPublishNotSupported` - use direct MongoDB writes instead
- **Broadcast only** - WorkerPool delivery mode is not supported (all subscribers receive all changes)
- Events are triggered by database changes (insert, update, delete, replace)
- Resume tokens automatically stored in `_event_resume_tokens` collection
- On restart: resumes from where it left off (no missed changes)
- Use `WithoutResume()` to disable (starts from latest on each restart, may miss changes)
- Optional acknowledgment tracking via `WithAckStore()`

**Use Cases:**
- Event sourcing from MongoDB
- Reacting to database changes across services
- Building read models/projections
- Audit logging
- Cache invalidation
- Cross-database synchronization (cluster-level watch)

### Transport Feature Comparison

| Feature | Redis Streams | NATS Core | NATS JetStream | Kafka | MongoDB |
|---------|:-------------:|:---------:|:--------------:|:-----:|:-------:|
| Persistence | ✅ | ❌ | ✅ | ✅ | ✅ |
| At-Least-Once | ✅ | ❌ | ✅ | ✅ | ✅ |
| Native Deduplication | ❌ (inject store) | ❌ (inject store) | ✅ | ❌ | ❌ |
| Native DLQ/DLT | ❌ (inject handler) | ❌ (inject handler) | ❌ | ✅ | ❌ |
| Native Retry Limits | ❌ | ❌ | ✅ (MaxDeliver) | ✅ | ❌ |
| Consumer Groups | ✅ | Queue Groups | ✅ | ✅ | ❌ (Broadcast only) |
| Health Checks | ✅ | ✅ | ✅ | ✅ | ✅ |
| Lag Monitoring | ✅ | ❌ | ❌ | ✅ | ❌ |
| **Publish Support** | ✅ | ✅ | ✅ | ✅ | ❌ (CDC only) |
| **WorkerPool Mode** | ✅ | ✅ | ✅ | ✅ | ❌ (Broadcast only) |

**Native vs Injected Features:**
- **Native features** are handled by the broker (more efficient, no external dependencies)
- **Injected stores** provide library-level features where the broker lacks native support
- **MongoDB** is a special CDC transport - publishing happens via direct database writes

## Distributed Worker Emulation

For transports that only support Broadcast mode (like MongoDB Change Streams), use the `distributed` package to emulate WorkerPool semantics using distributed message claiming.

### How It Works

In Broadcast mode, all subscribers receive every message. The `DistributedWorkerMiddleware` uses a `MessageClaimer` to ensure only one worker processes each message:

1. Message arrives (delivered to all subscribers via Broadcast)
2. Each subscriber calls `TryClaim()` to attempt claiming the message
3. Only the subscriber that succeeds processes the message
4. On success: `Complete()` marks it done; on failure: `Release()` allows retry

### Basic Usage

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/distributed"
    "github.com/rbaliyan/event/v3/transport/mongodb"
)

func main() {
    ctx := context.Background()

    // Create MongoDB transport (Broadcast-only)
    transport, _ := mongodb.New(db, mongodb.WithCollection("orders"))

    bus, _ := event.NewBus("order-watcher", event.WithBusTransport(transport))
    defer bus.Close(ctx)

    // Create Redis claimer for distributed deployments
    claimer := distributed.NewRedisClaimer(redisClient,
        distributed.WithClaimerPrefix("order-workers:"),
        distributed.WithClaimerTTL(5*time.Minute),
    )

    orderChanges := event.New[mongodb.ChangeEvent]("order-changes")
    event.Register(ctx, bus, orderChanges)

    // Subscribe with worker emulation middleware
    orderChanges.Subscribe(ctx, func(ctx context.Context, e event.Event[mongodb.ChangeEvent], change mongodb.ChangeEvent) error {
        // Only one worker processes each change
        return processChange(ctx, change)
    }, event.WithMiddleware(
        distributed.DistributedWorkerMiddleware[mongodb.ChangeEvent](claimer, 5*time.Minute),
    ))
}
```

### Claimer Implementations

| Claimer | Use Case | Notes |
|---------|----------|-------|
| `RedisClaimer` | Production distributed | Uses SETNX with TTL for atomic claiming |
| `MongoClaimer` | MongoDB-only deployments | Uses findOneAndUpdate; supports capped collections |
| `MemoryClaimer` | Single-instance or testing | NOT distributed - each instance has its own state |

```go
// Redis (recommended for high-throughput)
claimer := distributed.NewRedisClaimer(redisClient,
    distributed.WithClaimerPrefix("myapp:claims:"),
    distributed.WithClaimerTTL(5*time.Minute),
    distributed.WithCompletionTTL(24*time.Hour),
)

// MongoDB (ideal when already using MongoDB transport)
claimer := distributed.NewMongoClaimer(db).
    WithCollection("worker_claims").
    WithCompletionTTL(48*time.Hour)
claimer.EnsureIndexes(ctx) // Create TTL index for automatic cleanup

// MongoDB with custom database
claimer := distributed.NewMongoClaimer(appDB).
    WithDatabase(client.Database("claims_db")).
    WithCollection("worker_claims")

// MongoDB with capped collection (high-throughput, size-based cleanup)
claimer := distributed.NewMongoClaimer(db).
    WithCollection("claim_buffer").
    WithCapped(100*1024*1024, 100000) // 100MB max, 100k docs max
claimer.CreateCollection(ctx) // Creates the capped collection

// Memory (single-instance or testing)
claimer := distributed.NewMemoryClaimer()
defer claimer.Close()
```

### Worker Groups with Distributed Middleware

To emulate worker groups on Broadcast-only transports, use separate claimers with different prefixes:

```go
// Group A: Order processors (compete within group)
claimerA := distributed.NewRedisClaimer(redis,
    distributed.WithClaimerPrefix("processors:"))

// Group B: Analytics collectors (compete within group)
claimerB := distributed.NewRedisClaimer(redis,
    distributed.WithClaimerPrefix("analytics:"))

// Both groups receive all messages (like broadcast between groups)
// Workers within each group compete (like WorkerPool within group)
orderChanges.Subscribe(ctx, processOrder,
    event.WithMiddleware(distributed.DistributedWorkerMiddleware[T](claimerA, ttl)))

orderChanges.Subscribe(ctx, collectAnalytics,
    event.WithMiddleware(distributed.DistributedWorkerMiddleware[T](claimerB, ttl)))
```

### Claim TTL Guidelines

The claim TTL should exceed your handler's maximum execution time:

| Handler Timeout | Recommended Claim TTL |
|-----------------|----------------------|
| 30s | 1-2 minutes |
| 1 minute | 3-5 minutes |
| 5 minutes | 10-15 minutes |
| No timeout | Max expected time + buffer |

If a worker crashes, the claim expires after TTL and another worker can claim and process the message.

### Error Handling

- **TryClaim error**: Logs warning and proceeds (fail-open to prevent message loss)
- **Complete error**: Logs warning but doesn't affect handler result
- **Release error**: Logs warning but doesn't affect error propagation

Handlers should be idempotent to handle potential duplicates when claimers fail.

### Distributed Worker vs Idempotency

These solve different problems and can be used together:

| Aspect | DistributedWorkerMiddleware | IdempotencyMiddleware |
|--------|----------------------------|----------------------|
| **Problem** | Multiple workers receive same message simultaneously (Broadcast) | Same message redelivered after processing (retries) |
| **Question** | "Is someone else processing this right now?" | "Was this already processed before?" |
| **Timing** | Concurrent (simultaneous delivery) | Sequential (redelivery over time) |
| **On success** | Mark completed (blocks re-claim during TTL) | Mark processed (blocks reprocessing during TTL) |
| **On failure** | Release claim (another worker can retry) | Don't mark (same worker can retry) |

**When to use each:**

| Transport | Distributed Worker? | Idempotency? |
|-----------|:------------------:|:------------:|
| Redis Streams (WorkerPool) | ❌ (native WorkerPool) | ✅ (retries need dedup) |
| Kafka (WorkerPool) | ❌ (native WorkerPool) | ✅ (retries need dedup) |
| NATS JetStream (WorkerPool) | ❌ (native WorkerPool) | ✅ (retries need dedup) |
| MongoDB Change Streams | ✅ (emulate WorkerPool) | ✅ (if retries possible) |
| Any Broadcast mode | ✅ (if load balancing needed) | ✅ (if retries possible) |

**Using both together (MongoDB example):**

```go
// Both use distributed backends (Redis)
claimer := distributed.NewRedisClaimer(redis)
idempStore := idempotency.NewRedisStore(redis, time.Hour)

// Or both use MongoDB
claimer := distributed.NewMongoClaimer(db)
idempStore := idempotency.NewMongoStore(db) // if available

ev.Subscribe(ctx, handler, event.WithMiddleware(
    // First: Only one worker claims the message (load balancing)
    distributed.DistributedWorkerMiddleware[Order](claimer, 5*time.Minute),
    // Then: Prevent reprocessing if this message was already handled
    event.IdempotencyMiddleware[Order](idempStore),
))
```

**Store distribution:**

Both middleware support distributed stores - they're not limited to single machines:

| Store Type | Distributed? | Use Case |
|------------|:------------:|----------|
| Memory stores | ❌ | Testing, single-instance |
| Redis stores | ✅ | Production multi-instance |
| MongoDB stores | ✅ | Production multi-instance |
| PostgreSQL stores | ✅ | Production multi-instance |

## Transactional Outbox Pattern

Ensure atomic publish with database writes - never lose messages.

### Bus-Level Integration (Recommended)

Configure outbox once at bus level - same `ev.Publish()` API works transparently:

```go
import (
    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/outbox"
    "go.mongodb.org/mongo-driver/mongo"
)

func main() {
    ctx := context.Background()

    // Create outbox store
    store := outbox.NewMongoStore(mongoClient.Database("myapp"))

    // Create bus with outbox support
    bus, _ := event.NewBus("order-service",
        event.WithTransport(transport),
        event.WithOutbox(store),
    )
    defer bus.Close(ctx)

    // Create and register event
    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    // Normal publish - goes directly to transport
    orderEvent.Publish(ctx, Order{ID: "123", Amount: 99.99})

    // Inside transaction - same API, automatically routes to outbox!
    err := outbox.Transaction(ctx, mongoClient, func(ctx context.Context) error {
        // Business logic uses the transaction context
        _, err := ordersCol.InsertOne(ctx, order)
        if err != nil {
            return err
        }

        // This automatically goes to outbox (same transaction)
        return orderEvent.Publish(ctx, order)
    })

    // Start relay to publish messages from outbox to transport
    relay := outbox.NewMongoRelay(store, transport)
    go relay.Start(ctx)
}
```

### Explicit Transaction (PostgreSQL)

For PostgreSQL or when you need explicit control:

```go
import (
    "database/sql"
    "github.com/rbaliyan/event/v3/outbox"
)

func main() {
    ctx := context.Background()

    db, _ := sql.Open("postgres", "postgres://localhost/mydb")

    // Create outbox publisher
    publisher := outbox.NewPostgresPublisher(db)

    // Start relay to publish messages from outbox to transport
    relay := outbox.NewRelay(publisher.Store(), transport,
        outbox.WithPollDelay(100*time.Millisecond),
        outbox.WithBatchSize(100),
    )
    go relay.Start(ctx)

    // In your business logic - atomic with DB transaction
    tx, _ := db.BeginTx(ctx, nil)

    // Update order status
    tx.Exec("UPDATE orders SET status = 'shipped' WHERE id = $1", orderID)

    // Store event in outbox (same transaction)
    publisher.PublishInTransaction(ctx, tx, "order.shipped", order, map[string]string{
        "source": "order-service",
    })

    tx.Commit() // Both succeed or both fail
}
```

**SQL Schema:**
```sql
CREATE TABLE event_outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_name   VARCHAR(255) NOT NULL,
    event_id     VARCHAR(36) NOT NULL,
    payload      BYTEA NOT NULL,
    metadata     JSONB,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP,
    status       VARCHAR(20) NOT NULL DEFAULT 'pending'
);
CREATE INDEX idx_outbox_pending ON event_outbox(status, created_at) WHERE status = 'pending';
```

## Dead Letter Queue (DLQ)

Store and replay failed messages:

```go
import (
    "github.com/rbaliyan/event/v3/dlq"
    "github.com/rbaliyan/event/v3/transport/message"
)

func main() {
    ctx := context.Background()

    // Create DLQ store
    dlqStore := dlq.NewPostgresStore(db)

    // Create DLQ manager
    manager := dlq.NewManager(dlqStore, transport)

    // Configure event with DLQ handler
    orderEvent := event.New[Order]("order.process",
        event.WithMaxRetries(3),
        event.WithDeadLetterQueue(func(ctx context.Context, msg message.Message, err error) error {
            return manager.Store(ctx,
                "order.process",
                msg.ID(),
                msg.Payload().([]byte),
                msg.Metadata(),
                err,
                msg.RetryCount(),
                "order-service",
            )
        }),
    )

    // Later: List failed messages
    messages, _ := manager.List(ctx, dlq.Filter{
        EventName:      "order.process",
        ExcludeRetried: true,
        Limit:          100,
    })

    // Replay failed messages
    replayed, _ := manager.Replay(ctx, dlq.Filter{
        EventName: "order.process",
    })
    fmt.Printf("Replayed %d messages\n", replayed)

    // Get statistics
    stats, _ := manager.Stats(ctx)
    fmt.Printf("Pending: %d, Total: %d\n", stats.PendingMessages, stats.TotalMessages)
}
```

## Saga Orchestration

Coordinate distributed transactions with compensation:

```go
import "github.com/rbaliyan/event/v3/saga"

// Define saga steps
type CreateOrderStep struct {
    orderService *OrderService
}

func (s *CreateOrderStep) Name() string { return "create-order" }

func (s *CreateOrderStep) Execute(ctx context.Context, data any) error {
    order := data.(*Order)
    return s.orderService.Create(ctx, order)
}

func (s *CreateOrderStep) Compensate(ctx context.Context, data any) error {
    order := data.(*Order)
    return s.orderService.Cancel(ctx, order.ID)
}

// Similar for ReserveInventoryStep, ProcessPaymentStep, etc.

func main() {
    ctx := context.Background()

    // Create saga with persistence
    store := saga.NewPostgresStore(db)

    orderSaga := saga.New("order-creation",
        &CreateOrderStep{orderService},
        &ReserveInventoryStep{inventoryService},
        &ProcessPaymentStep{paymentService},
        &SendConfirmationStep{emailService},
    ).WithStore(store)

    // Execute saga
    sagaID := uuid.New().String()
    order := &Order{ID: "ORD-123", Items: items}

    if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
        // Saga failed - compensations were automatically run
        log.Printf("Order saga failed: %v", err)
    }

    // Resume failed sagas after fix
    failedSagas, _ := store.List(ctx, saga.StoreFilter{
        Status: []saga.Status{saga.StatusFailed},
    })

    for _, state := range failedSagas {
        orderSaga.Resume(ctx, state.ID)
    }
}
```

## Scheduled Messages

Schedule messages for future delivery:

```go
import "github.com/rbaliyan/event/v3/scheduler"

func main() {
    ctx := context.Background()

    // Create scheduler with Redis
    sched := scheduler.NewRedisScheduler(redisClient, transport,
        scheduler.WithPollInterval(100*time.Millisecond),
        scheduler.WithBatchSize(100),
    )

    // Start scheduler
    go sched.Start(ctx)

    // Schedule a message for later
    payload, _ := json.Marshal(Order{ID: "ORD-123"})

    // Schedule for specific time
    msgID, _ := sched.ScheduleAt(ctx, "order.reminder", payload, nil,
        time.Now().Add(24*time.Hour))

    // Or schedule after delay
    msgID, _ = sched.ScheduleAfter(ctx, "order.reminder", payload, nil,
        time.Hour)

    // Cancel scheduled message
    sched.Cancel(ctx, msgID)

    // List scheduled messages
    messages, _ := sched.List(ctx, scheduler.Filter{
        EventName: "order.reminder",
        Before:    time.Now().Add(48 * time.Hour),
    })
}
```

## Delivery Modes

Control how messages are distributed to subscribers.

### Broadcast (Default)

All subscribers receive every message (fan-out):

```go
// Default behavior - all handlers receive every message
orderEvent.Subscribe(ctx, notifyWarehouse, event.AsBroadcast[Order]())
orderEvent.Subscribe(ctx, notifyShipping, event.AsBroadcast[Order]())
orderEvent.Subscribe(ctx, updateDashboard, event.AsBroadcast[Order]())
// All three handlers receive every published order
```

### Worker Pool

Only one subscriber receives each message (load balancing):

```go
// Workers compete - each message goes to exactly one handler
orderEvent.Subscribe(ctx, processOrder, event.AsWorker[Order]())
orderEvent.Subscribe(ctx, processOrder, event.AsWorker[Order]())
orderEvent.Subscribe(ctx, processOrder, event.AsWorker[Order]())
// 3 workers, each order processed by exactly one
```

### Worker Groups

Multiple worker groups, each receiving all messages. Workers within a group compete:

```go
// Group A: Order processors (3 workers compete)
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("order-processors"))
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("order-processors"))
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("order-processors"))

// Group B: Inventory updaters (2 workers compete)
orderEvent.Subscribe(ctx, updateInventory,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("inventory-updaters"))
orderEvent.Subscribe(ctx, updateInventory,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("inventory-updaters"))

// Result: Each order is processed by:
// - 1 of 3 order-processors (competing)
// - 1 of 2 inventory-updaters (competing)
// Both groups receive all messages (like broadcast between groups)
```

### Mixing Modes

Combine broadcast, worker pool, and worker groups on the same event:

```go
// Broadcast: All notification services receive every order
orderEvent.Subscribe(ctx, sendEmail, event.AsBroadcast[Order]())
orderEvent.Subscribe(ctx, sendSMS, event.AsBroadcast[Order]())

// Worker Group "processors": 3 workers compete
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("processors"))
orderEvent.Subscribe(ctx, processOrder,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("processors"))

// Worker Group "analytics": 2 workers compete
orderEvent.Subscribe(ctx, trackAnalytics,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("analytics"))
orderEvent.Subscribe(ctx, trackAnalytics,
    event.AsWorker[Order](),
    event.WithWorkerGroup[Order]("analytics"))

// Each order goes to:
// - sendEmail (broadcast)
// - sendSMS (broadcast)
// - 1 of 2 processors (worker group)
// - 1 of 2 analytics workers (worker group)
```

## Batch Processing

Process messages in batches for high throughput:

```go
import "github.com/rbaliyan/event/v3/batch"

func main() {
    ctx := context.Background()

    // Create batch processor
    processor := batch.NewProcessor[Order](
        batch.WithBatchSize(100),
        batch.WithTimeout(time.Second),
        batch.WithMaxRetries(3),
        batch.WithOnError(func(b []any, err error) {
            log.Printf("Batch of %d failed: %v", len(b), err)
        }),
    )

    // Subscribe with batch handler
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event, order Order) error {
        // This is called per-message; use processor for batching
        return nil
    })

    // Or use processor directly with subscription messages
    sub, _ := transport.Subscribe(ctx, "order.process", transport.WorkerPool)

    go processor.Process(ctx, sub.Messages(), func(ctx context.Context, orders []Order) error {
        // Bulk insert all orders at once
        return db.BulkInsert(ctx, orders)
    })
}
```

## Idempotency

Prevent duplicate message processing.

### Bus-Level (Recommended)

Configure once at bus creation - all subscribers automatically get idempotency:

```go
import "github.com/rbaliyan/event/v3/idempotency"

func main() {
    ctx := context.Background()

    // Create idempotency store
    store := idempotency.NewRedisStore(redisClient, time.Hour)

    // Configure at bus level - all events get automatic deduplication
    bus, _ := event.NewBus("order-service",
        event.WithBusTransport(transport),
        event.WithBusIdempotency(store),
    )
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    // Subscriber is simple - no manual idempotency check needed!
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        return processOrder(ctx, order) // Just business logic
    })
}
```

### Manual Approach

For fine-grained control, check idempotency manually in handlers:

```go
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    msgID := event.ContextEventID(ctx)

    if dup, _ := store.IsDuplicate(ctx, msgID); dup {
        return nil // Skip duplicate
    }

    if err := processOrder(ctx, order); err != nil {
        return err
    }

    return store.MarkProcessed(ctx, msgID)
})
```

## Exactly-Once Processing

For true exactly-once semantics, use `TransactionalHandler` which combines idempotency checking with database transactions:

```go
import (
    "github.com/rbaliyan/event/v3/idempotency"
    "github.com/rbaliyan/event/v3/transaction"
)

func main() {
    ctx := context.Background()

    db, _ := sql.Open("postgres", "postgres://localhost/mydb")

    // Create transaction manager and idempotency store
    txManager := transaction.NewSQLManager(db)
    idempStore := idempotency.NewPostgresStore(db,
        idempotency.WithPostgresTTL(24*time.Hour),
    )

    // Create transactional handler - atomic exactly-once processing
    handler := transaction.NewTransactionalHandler(
        func(ctx context.Context, tx transaction.Transaction, order Order) error {
            sqlTx := tx.(transaction.SQLTransactionProvider).Tx()

            // All operations in the same transaction
            _, err := sqlTx.ExecContext(ctx,
                "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
                order.Quantity, order.ProductID)
            if err != nil {
                return err
            }

            _, err = sqlTx.ExecContext(ctx,
                "INSERT INTO orders (id, product_id, quantity) VALUES ($1, $2, $3)",
                order.ID, order.ProductID, order.Quantity)
            return err
        },
        txManager,
        idempStore,
        func(order Order) string { return order.ID },
    )

    // Use in event subscription
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event, order Order) error {
        return handler.Handle(ctx, order)
    })
}
```

**PostgreSQL Schema for Idempotency:**
```sql
CREATE TABLE event_idempotency (
    message_id VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX idx_event_idempotency_expires ON event_idempotency(expires_at);
```

The `TransactionalHandler` guarantees:
- Idempotency check within the transaction (no race conditions)
- Business logic within the same transaction
- Mark-as-processed within the same transaction
- Atomic commit/rollback of all operations

## Poison Message Detection

Automatically quarantine messages that keep failing.

### Bus-Level (Recommended)

Configure once at bus creation - all subscribers automatically get poison detection:

```go
import "github.com/rbaliyan/event/v3/poison"

func main() {
    ctx := context.Background()

    // Create poison detector
    store := poison.NewRedisStore(redisClient)
    detector := poison.NewDetector(store,
        poison.WithThreshold(5),              // Quarantine after 5 failures
        poison.WithQuarantineTime(time.Hour), // Block for 1 hour
    )

    // Configure at bus level - all events get automatic poison detection
    bus, _ := event.NewBus("order-service",
        event.WithBusTransport(transport),
        event.WithBusPoisonDetection(detector),
    )
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    // Subscriber is simple - no manual poison check needed!
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        return processOrder(ctx, order) // Just business logic
    })

    // Release a message from quarantine when needed
    detector.Release(ctx, messageID)
}
```

### Manual Approach

For fine-grained control, check poison status manually:

```go
orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
    msgID := event.ContextEventID(ctx)

    if poisoned, _ := detector.Check(ctx, msgID); poisoned {
        return nil // Skip quarantined message
    }

    if err := processOrder(ctx, order); err != nil {
        quarantined, _ := detector.RecordFailure(ctx, msgID)
        if quarantined {
            log.Printf("Message %s quarantined", msgID)
        }
        return err
    }

    detector.RecordSuccess(ctx, msgID)
    return nil
})
```

**PostgreSQL Schema for Poison Detection:**
```sql
CREATE TABLE poison_failures (
    message_id VARCHAR(255) PRIMARY KEY,
    failure_count INTEGER NOT NULL DEFAULT 1,
    first_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_failure_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE poison_quarantine (
    message_id VARCHAR(255) PRIMARY KEY,
    quarantined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    reason TEXT
);
```

## Resumable Subscriptions (Checkpoints)

Enable "start from latest, resume on reconnect" semantics for subscribers. Perfect for real-time consumers that don't need historical backlog but shouldn't miss messages during restarts.

### How It Works

1. **First connection** (no checkpoint): Starts from latest messages only - no historical backlog
2. **Processing**: Checkpoint saved after each successful message
3. **Disconnect/Restart**: Resumes from last saved checkpoint - no missed messages

### Basic Usage

```go
import "github.com/rbaliyan/event/v3/checkpoint"

func main() {
    ctx := context.Background()

    // Create checkpoint store (Redis or MongoDB)
    store := checkpoint.NewRedisStore(redisClient, "myapp:checkpoints")

    // Subscribe with automatic checkpointing
    orderEvent.Subscribe(ctx, handler,
        event.WithCheckpoint[Order](store, "order-processor-1"),
    )
}
```

### Redis Checkpoint Store

```go
import "github.com/rbaliyan/event/v3/checkpoint"

// Basic setup
store := checkpoint.NewRedisStore(redisClient, "myapp:checkpoints")

// With TTL (checkpoints expire after 7 days of inactivity)
store := checkpoint.NewRedisStore(redisClient, "myapp:checkpoints",
    checkpoint.WithTTL(7*24*time.Hour),
)

// Use with event
orderEvent.Subscribe(ctx, handler,
    event.WithCheckpoint[Order](store, "order-processor-1"),
)
```

### MongoDB Checkpoint Store

```go
import "github.com/rbaliyan/event/v3/checkpoint"

// Create store
collection := mongoClient.Database("myapp").Collection("checkpoints")
store := checkpoint.NewMongoStore(collection)

// With TTL
store := checkpoint.NewMongoStore(collection,
    checkpoint.WithMongoTTL(7*24*time.Hour),
)

// Create indexes (call once at startup)
store.EnsureIndexes(ctx)

// Use with event
orderEvent.Subscribe(ctx, handler,
    event.WithCheckpoint[Order](store, "order-processor-1"),
)
```

### Advanced: Separate Options

For more control, use the resume and middleware options separately:

```go
// Resume from checkpoint (or start from latest if none exists)
orderEvent.Subscribe(ctx, handler,
    event.WithCheckpointResume[Order](store, "order-processor-1"),
    event.WithMiddleware(event.CheckpointMiddleware[Order](store, "order-processor-1")),
)

// Override: Always start from latest (ignore existing checkpoint)
orderEvent.Subscribe(ctx, handler,
    event.FromLatest[Order](),
    event.WithMiddleware(event.CheckpointMiddleware[Order](store, "order-processor-1")),
)
```

### Checkpoint Store Methods

| Method | Description |
|--------|-------------|
| `Save(ctx, id, position)` | Save checkpoint position |
| `Load(ctx, id)` | Load last checkpoint (zero time if none) |
| `Delete(ctx, id)` | Remove a checkpoint |
| `DeleteAll(ctx)` | Remove all checkpoints |
| `List(ctx)` | Get all subscriber IDs |
| `GetAll(ctx)` | Get all checkpoints as map |
| `GetCheckpointInfo(ctx, id)` | Get detailed info including updated_at |
| `Indexes()` | Get index models (MongoDB only) |
| `EnsureIndexes(ctx)` | Create indexes (MongoDB only) |

### When to Use Checkpoints vs Consumer Groups

| Scenario | Solution |
|----------|----------|
| Load balancing across workers | WorkerPool mode (consumer groups) |
| Each instance processes all messages | Broadcast + Checkpoints |
| Resume after restart | Checkpoints or Consumer Groups |
| Real-time dashboard (no history) | `FromLatest()` + Checkpoints |
| Event sourcing (need all history) | `FromBeginning()` (no checkpoint) |

## Event Monitoring

Track event processing status, duration, and errors for observability and debugging.

### Bus-Level (Recommended)

Configure once at bus creation - all subscribers automatically get monitoring:

```go
import "github.com/rbaliyan/event/v3/monitor"

func main() {
    ctx := context.Background()

    // Create monitor store (PostgreSQL, MongoDB, or in-memory)
    store := monitor.NewPostgresStore(db)

    // Configure at bus level - all events get automatic monitoring
    bus, _ := event.NewBus("order-service",
        event.WithTransport(transport),
        event.WithMonitor(store),
    )
    defer bus.Close(ctx)

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    // Subscriber is simple - monitoring happens automatically!
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        return processOrder(ctx, order) // Just business logic
    })

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
}
```

### Monitor HTTP API

Expose monitoring data via REST API:

```go
import (
    "net/http"
    "github.com/rbaliyan/event/v3/monitor"
    monitorhttp "github.com/rbaliyan/event/v3/monitor/http"
)

func main() {
    store := monitor.NewMemoryStore()

    // Create HTTP handler
    handler := monitorhttp.New(store)

    // Mount on your server with your own middleware
    mux := http.NewServeMux()
    mux.Handle("/", handler)

    server := &http.Server{
        Addr:    ":8080",
        Handler: yourAuthMiddleware(mux),
    }
    server.ListenAndServe()
}
```

**REST Endpoints:**

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/v1/monitor/entries` | List entries with query filters |
| GET | `/v1/monitor/entries/{event_id}` | Get all entries for an event |
| GET | `/v1/monitor/entries/{event_id}/{subscription_id}` | Get specific entry |
| GET | `/v1/monitor/entries/count` | Count entries with filters |
| DELETE | `/v1/monitor/entries?older_than=1h` | Delete old entries |

**Query Parameters:**
- `event_id`, `subscription_id`, `event_name`, `bus_id` - Filter by identity
- `status` - Filter by status (can be repeated: `?status=failed&status=pending`)
- `has_error` - Filter by error presence (`true`/`false`)
- `delivery_mode` - Filter by mode (`broadcast`/`worker_pool`)
- `start_time`, `end_time` - Time range (RFC3339 format)
- `min_duration` - Minimum duration (e.g., `100ms`, `1s`)
- `cursor`, `limit`, `order_desc` - Pagination

**Delete Safety:**
- Default: deletes entries older than 24 hours
- To delete newer entries: `?older_than=1h&force=true`

### Monitor gRPC API

Expose monitoring data via gRPC:

```go
import (
    "github.com/rbaliyan/event/v3/monitor"
    monitorgrpc "github.com/rbaliyan/event/v3/monitor/grpc"
    "google.golang.org/grpc"
)

func main() {
    store := monitor.NewMemoryStore()

    // Create gRPC service
    service := monitorgrpc.New(store)

    // Register with your gRPC server
    server := grpc.NewServer(
        grpc.UnaryInterceptor(yourAuthInterceptor),
    )
    service.Register(server)

    lis, _ := net.Listen("tcp", ":9090")
    server.Serve(lis)
}
```

### Manual Approach

For fine-grained control, use the middleware directly:

```go
store := monitor.NewPostgresStore(db)

orderEvent.Subscribe(ctx, handler,
    event.WithMiddleware(monitor.Middleware[Order](store)),
)
```

### Delivery Mode Tracking

Monitor automatically detects and tracks delivery mode:

- **Broadcast (Pub/Sub)**: Tracks per `(EventID, SubscriptionID)` - each subscriber's processing is separate
- **WorkerPool (Queue)**: Tracks per `EventID` only - one worker processes each event

```go
// Get all entries for an event
entries, _ := store.GetByEventID(ctx, "evt-123")

// Broadcast mode: multiple entries (one per subscriber)
// WorkerPool mode: single entry
for _, e := range entries {
    fmt.Printf("Subscriber %s: %s\n", e.SubscriptionID, e.Status)
}
```

**PostgreSQL Schema for Monitoring:**
```sql
CREATE TABLE monitor_entries (
    event_id TEXT NOT NULL,
    subscription_id TEXT NOT NULL DEFAULT '',
    event_name TEXT NOT NULL,
    bus_id TEXT NOT NULL,
    delivery_mode TEXT NOT NULL,
    metadata JSONB,
    status TEXT NOT NULL,
    error TEXT,
    retry_count INT DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    duration_ms BIGINT,
    trace_id TEXT,
    span_id TEXT,
    PRIMARY KEY (event_id, subscription_id)
);
CREATE INDEX idx_monitor_event_name ON monitor_entries(event_name);
CREATE INDEX idx_monitor_status ON monitor_entries(status);
CREATE INDEX idx_monitor_started_at ON monitor_entries(started_at);
CREATE INDEX idx_monitor_delivery_mode ON monitor_entries(delivery_mode);
```

## Schema Registry

Define event processing configuration centrally and ensure all subscribers use consistent settings.

### Overview

The Schema Registry enables **publishers** to define event configuration (timeouts, retries, feature flags) that **subscribers** automatically load when events are registered. This ensures all workers processing the same event have consistent settings across distributed systems.

### Bus-Level Configuration (Recommended)

```go
import "github.com/rbaliyan/event/v3/schema"

func main() {
    ctx := context.Background()

    // Create schema provider (in-memory, PostgreSQL, MongoDB, or Redis)
    provider := schema.NewMemoryProvider()
    defer provider.Close()

    // Configure bus with schema provider and middleware stores
    bus, _ := event.NewBus("order-service",
        event.WithTransport(transport),
        event.WithSchemaProvider(provider),
        event.WithIdempotency(idempStore),     // Required if schema enables idempotency
        event.WithPoisonDetection(detector),   // Required if schema enables poison detection
        event.WithMonitor(monitorStore),       // Required if schema enables monitoring
    )
    defer bus.Close(ctx)

    // Publisher: Register schema before events are created
    provider.Set(ctx, &schema.EventSchema{
        Name:              "order.created",
        Version:           1,
        Description:       "Order creation event",
        SubTimeout:        30 * time.Second,
        MaxRetries:        3,
        EnableMonitor:     true,
        EnableIdempotency: true,
        EnablePoison:      false,
    })

    // Subscriber: Schema is auto-loaded on Register()
    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent) // Loads schema automatically

    // Subscribe - middleware is controlled by schema flags
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        return processOrder(ctx, order) // Just business logic!
    })
}
```

### Schema Providers

#### In-Memory (Testing)

```go
provider := schema.NewMemoryProvider()
defer provider.Close()
```

#### PostgreSQL

```go
import "github.com/rbaliyan/event/v3/schema"

// Create provider with notification callback
provider := schema.NewPostgresProvider(db, func(ctx context.Context, change schema.SchemaChangeEvent) error {
    // Optionally notify other services about schema changes
    return nil
})
defer provider.Close()

// Create table (for development/testing)
provider.CreateTable(ctx)

// Or use custom table name
provider := schema.NewPostgresProvider(db, callback,
    schema.WithTableName("custom_schemas"),
)
```

**PostgreSQL Schema:**
```sql
CREATE TABLE event_schemas (
    name TEXT PRIMARY KEY,
    version INT NOT NULL DEFAULT 1,
    description TEXT,
    sub_timeout_ms BIGINT,
    max_retries INT,
    retry_backoff_ms BIGINT,
    enable_monitor BOOLEAN DEFAULT false,
    enable_idempotency BOOLEAN DEFAULT false,
    enable_poison BOOLEAN DEFAULT false,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_event_schemas_updated ON event_schemas(updated_at);
```

#### MongoDB

```go
import "github.com/rbaliyan/event/v3/schema"

db := mongoClient.Database("myapp")
provider := schema.NewMongoProvider(db, callback)
defer provider.Close()

// Create indexes
provider.EnsureIndexes(ctx)

// Or use custom collection
provider.WithCollection("custom_schemas")
```

#### Redis

```go
import "github.com/rbaliyan/event/v3/schema"

provider := schema.NewRedisProvider(redisClient, callback)
defer provider.Close()

// Or use custom hash key (default: "event:schemas")
provider := schema.NewRedisProvider(redisClient, callback,
    schema.WithKey("myapp:schemas"),
)
```

### How Schema Flags Work

When a schema is loaded, its flags control which middleware is applied:

| Schema Flag | Effect |
|-------------|--------|
| `EnableMonitor: true` | Monitor middleware records processing metrics |
| `EnableIdempotency: true` | Idempotency middleware prevents duplicate processing |
| `EnablePoison: true` | Poison middleware quarantines failing messages |

**Important:** The corresponding store must be configured on the bus for the flag to have effect:
- `EnableMonitor` requires `WithMonitor(store)`
- `EnableIdempotency` requires `WithIdempotency(store)`
- `EnablePoison` requires `WithPoisonDetection(detector)`

### Fallback Behavior

When no schema exists for an event, the bus falls back to its default behavior:
- All configured middleware stores are applied (monitor, idempotency, poison)
- Event-level options (timeout, max retries) are used

### Schema Versioning

Schemas support versioning with automatic validation:

```go
// Version 1
provider.Set(ctx, &schema.EventSchema{
    Name:    "order.created",
    Version: 1,
    // ...
})

// Version 2 (must be >= previous version)
provider.Set(ctx, &schema.EventSchema{
    Name:    "order.created",
    Version: 2,
    // Updated configuration
})

// Downgrade attempt returns error
err := provider.Set(ctx, &schema.EventSchema{
    Name:    "order.created",
    Version: 1, // Error: cannot downgrade
})
// err == schema.ErrVersionDowngrade
```

### Schema Watch (Real-time Updates)

Providers support watching for schema changes:

```go
// Watch for changes
changes, _ := provider.Watch(ctx)

go func() {
    for change := range changes {
        fmt.Printf("Schema %s updated to version %d\n",
            change.EventName, change.Version)
        // Reload event configuration if needed
    }
}()
```

### Publisher vs Subscriber Control

| Configuration | Owner | Rationale |
|---------------|-------|-----------|
| Monitor enable | Publisher | Consistent observability |
| Idempotency enable | Publisher | Consistent dedup behavior |
| Poison detection enable | Publisher | Consistent error handling |
| Max retries | Publisher | Consistent retry policy |
| Handler timeout | Publisher | Consistent SLA |
| **Delivery mode** | **Subscriber** | Subscriber's architectural choice |

## Rate Limiting

Distributed rate limiting for consumers:

```go
import "github.com/rbaliyan/event/v3/ratelimit"

func main() {
    ctx := context.Background()

    // Create rate limiter: 100 requests per second
    limiter := ratelimit.NewRedisLimiter(redisClient, "order-processor", 100, time.Second)

    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event, order Order) error {
        // Wait for rate limit
        if err := limiter.Wait(ctx); err != nil {
            return event.ErrDefer.Wrap(err) // Retry later
        }

        return processOrder(ctx, order)
    })

    // Check remaining capacity
    remaining, _ := limiter.Remaining(ctx)
    fmt.Printf("Remaining: %d requests\n", remaining)
}
```

## Error Handling

Use semantic error types to control message acknowledgment:

```go
import "github.com/rbaliyan/event/v3"

orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event, order Order) error {
    err := processOrder(ctx, order)

    switch {
    case err == nil:
        return nil // ACK - message processed successfully

    case errors.Is(err, ErrTemporary):
        return event.ErrNack // NACK - retry immediately

    case errors.Is(err, ErrTransient):
        return event.ErrDefer // NACK - retry with backoff

    case errors.Is(err, ErrPermanent):
        return event.ErrReject // ACK + send to DLQ

    default:
        return event.ErrDefer.Wrap(err) // Default: retry with backoff
    }
})
```

## Middleware

### Deduplication Middleware

```go
import "github.com/rbaliyan/event/v3"

store := event.NewInMemoryDeduplicationStore(time.Hour, 10000)

orderEvent.Subscribe(ctx, handler,
    event.WithMiddleware(event.DeduplicationMiddleware(store)),
)
```

### Circuit Breaker Middleware

```go
cb := event.NewCircuitBreaker(
    5,              // Open after 5 failures
    2,              // Close after 2 successes
    30*time.Second, // Reset timeout
)

orderEvent.Subscribe(ctx, handler,
    event.WithMiddleware(event.CircuitBreakerMiddleware(cb)),
)
```

## Publisher vs Subscriber Features

| Publisher Side | Subscriber Side | Must Match |
|----------------|-----------------|------------|
| Outbox | DLQ | Event Name |
| Outbox Relay | Idempotency | Codec |
| Scheduler | Deduplication | Schema |
| | Poison Detection | Transport |
| | Checkpoint | Transport Config |
| | Monitor | |
| | Rate Limiting | |
| | Batch Processing | |
| | Circuit Breaker | |

## Database Support

| Component | PostgreSQL | MongoDB | Redis | In-Memory |
|-----------|:----------:|:-------:|:-----:|:---------:|
| Outbox | ✅ | ✅ | ✅ | - |
| DLQ | ✅ | ✅ | ✅ | ✅ |
| Saga | ✅ | ✅ | ✅ | - |
| Scheduler | ✅ | ✅ | ✅ | - |
| Idempotency | ✅ | - | ✅ | ✅ |
| Poison | ✅ | - | ✅ | - |
| Checkpoint | - | ✅ | ✅ | ✅ |
| Monitor | ✅ | ✅ | - | ✅ |
| Schema Registry | ✅ | ✅ | ✅ | ✅ |
| Transaction | ✅ | ✅ | - | - |
| Rate Limit | - | - | ✅ | - |
| Distributed Worker | - | ✅ | ✅ | ✅ |

## Package Structure

The library is organized into focused packages with shared utilities to minimize code duplication.

### Core Packages

| Package | Description |
|---------|-------------|
| `event` | Core bus, event, and middleware types |
| `transport/*` | Transport implementations (channel, redis, nats, kafka) |
| `monitor` | Event processing monitoring with HTTP/gRPC APIs |
| `schema` | Schema registry for publisher-defined event configuration |
| `idempotency` | Exactly-once processing with multiple backends |
| `poison` | Poison message detection and quarantine |
| `dlq` | Dead letter queue management |
| `outbox` | Transactional outbox pattern |
| `saga` | Saga orchestration for distributed transactions |
| `checkpoint` | Resumable subscriptions |
| `scheduler` | Scheduled/delayed message delivery |
| `batch` | High-throughput batch processing |
| `ratelimit` | Distributed rate limiting |
| `distributed` | WorkerPool emulation for Broadcast-only transports |

### Shared Utilities

Internal packages provide common functionality across implementations:

#### store/base

Shared utilities for database store implementations:

```go
import "github.com/rbaliyan/event/v3/store/base"

// Cursor-based pagination
encoded := base.EncodeCursor(cursor{LastID: "123"})
decoded, _ := base.DecodeCursor[cursor](encoded)
result := base.Paginate(items, limit, cursorFn)

// Dynamic SQL query building
qb := base.NewQueryBuilder()
qb.AddIfNotEmpty("name = $%d", filter.Name)
qb.AddIfNotZero("created_at >= $%d", filter.StartTime)
qb.AddIn("status", filter.Statuses)
query, args := qb.Build("SELECT * FROM users %s ORDER BY id")

// Background cleanup with graceful shutdown
go base.SimpleCleanupLoop(interval, stopCh, cleanupFn)

// SQL null helpers
msg.Source = base.NullString(source)      // "" if NULL
msg.RetriedAt = base.NullTime(retriedAt)  // nil if NULL

// Metadata marshaling
data, _ := base.MarshalMetadata(metadata)
metadata, _ := base.UnmarshalMetadata(data)
```

#### transport/base

Shared utilities for transport implementations:

```go
import "github.com/rbaliyan/event/v3/transport/base"

// Event registry for managing subscriptions
registry := base.NewEventRegistry[*MyEvent]()
event, created := registry.Register("order.created", createFn)
event, ok := registry.Get("order.created")
totalSubs := registry.TotalSubscribers()

// Health check builder
result := base.NewHealthCheck().
    WithType("redis").
    WithEvents(10).
    WithSubscribers(25).
    Healthy("connected").
    Build()
```

### Interface Design

The library uses minimal interfaces for flexibility:

| Root Package Interface | Full Interface | Purpose |
|----------------------|----------------|---------|
| `IdempotencyStore` (2 methods) | `idempotency.Store` (4 methods) | Middleware only needs check/mark |
| `PoisonDetector` (3 methods) | `poison.Store` (6 methods) | Detector wraps Store with threshold logic |
| `MonitorStore` (2 methods) | `monitor.Store` (7 methods) | Middleware needs start/complete; Store adds queries |
| `SchemaProvider` | `schema.SchemaProvider` | Type alias for backward compatibility |

This design allows stores to implement both the minimal middleware interface and the full query interface.

## Testing

Use built-in test utilities:

```go
import "github.com/rbaliyan/event/v3"

func TestOrderHandler(t *testing.T) {
    // Create test bus (no tracing, metrics, or recovery)
    bus := event.TestBus(channel.New())
    defer bus.Close(context.Background())

    // Create recording transport to capture messages
    recorder := event.NewRecordingTransport(channel.New())

    // Create test handler to capture calls
    handler := event.NewTestHandler(func(ctx context.Context, e event.Event, order Order) error {
        return nil
    })

    orderEvent := event.New[Order]("order.created")
    event.Register(ctx, bus, orderEvent)

    orderEvent.Subscribe(ctx, handler.Handler())
    orderEvent.Publish(ctx, Order{ID: "test"})

    // Wait for handler to be called
    if !handler.WaitFor(1, 100*time.Millisecond) {
        t.Error("handler not called")
    }

    // Check received data
    orders := handler.Received()
    if orders[0].ID != "test" {
        t.Error("wrong order ID")
    }
}
```

## Full Example: Order Processing System

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/rbaliyan/event/v3"
    "github.com/rbaliyan/event/v3/dlq"
    "github.com/rbaliyan/event/v3/idempotency"
    "github.com/rbaliyan/event/v3/outbox"
    "github.com/rbaliyan/event/v3/poison"
    "github.com/rbaliyan/event/v3/transport/message"
    "github.com/rbaliyan/event/v3/transport/redis"
    redisclient "github.com/redis/go-redis/v9"
)

type Order struct {
    ID     string  `json:"id"`
    Amount float64 `json:"amount"`
    Status string  `json:"status"`
}

func main() {
    ctx := context.Background()

    // Setup infrastructure
    db, _ := sql.Open("postgres", "postgres://localhost/orders")
    rdb := redisclient.NewClient(&redisclient.Options{Addr: "localhost:6379"})

    // Create transport
    transport, _ := redis.New(rdb, redis.WithConsumerGroup("order-service"))

    // Create bus
    bus, _ := event.NewBus("order-service", event.WithBusTransport(transport))
    defer bus.Close(ctx)

    // === PUBLISHER SIDE ===

    // Outbox for atomic publishing
    outboxPublisher := outbox.NewPostgresPublisher(db)
    relay := outbox.NewRelay(outboxPublisher.Store(), transport)
    go relay.Start(ctx)

    // Publish order created event atomically with DB update
    publishOrder := func(ctx context.Context, order Order) error {
        tx, _ := db.BeginTx(ctx, nil)
        tx.Exec("INSERT INTO orders (id, amount) VALUES ($1, $2)", order.ID, order.Amount)
        outboxPublisher.PublishInTransaction(ctx, tx, "order.created", order, nil)
        return tx.Commit()
    }

    // === SUBSCRIBER SIDE ===

    // Create stores (all PostgreSQL for consistency)
    dlqStore := dlq.NewPostgresStore(db)
    dlqManager := dlq.NewManager(dlqStore, transport)
    idempStore := idempotency.NewPostgresStore(db, idempotency.WithPostgresTTL(24*time.Hour))
    poisonStore := poison.NewPostgresStore(db, poison.WithPostgresFailureTTL(24*time.Hour))
    poisonDetector := poison.NewDetector(poisonStore, poison.WithThreshold(5))

    // Create event
    orderEvent := event.New[Order]("order.created",
        event.WithMaxRetries(3),
        event.WithDeadLetterQueue(func(ctx context.Context, msg message.Message, err error) error {
            return dlqManager.Store(ctx, "order.created", msg.ID(),
                msg.Payload().([]byte), msg.Metadata(), err, msg.RetryCount(), "order-service")
        }),
    )
    event.Register(ctx, bus, orderEvent)

    // Subscribe with all protections
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        msgID := event.ContextEventID(ctx)

        // Check poison
        if poisoned, _ := poisonDetector.Check(ctx, msgID); poisoned {
            return nil
        }

        // Check idempotency
        if dup, _ := idempStore.IsDuplicate(ctx, msgID); dup {
            return nil
        }

        // Process order
        if err := processOrder(ctx, order); err != nil {
            poisonDetector.RecordFailure(ctx, msgID)
            return event.ErrDefer.Wrap(err)
        }

        // Mark processed
        idempStore.MarkProcessed(ctx, msgID)
        poisonDetector.RecordSuccess(ctx, msgID)

        log.Printf("Processed order: %s", order.ID)
        return nil
    })

    // Publish a test order
    publishOrder(ctx, Order{ID: "ORD-001", Amount: 99.99})

    // Keep running
    select {}
}

func processOrder(ctx context.Context, order Order) error {
    // Business logic here
    return nil
}
```

## License

MIT License - see [LICENSE](LICENSE) for details.

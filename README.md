# Event v3

[![CI](https://github.com/rbaliyan/event/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rbaliyan/event/branch/development/graph/badge.svg)](https://codecov.io/gh/rbaliyan/event)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event/v3.svg)](https://pkg.go.dev/github.com/rbaliyan/event/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event/v3)](https://goreportcard.com/report/github.com/rbaliyan/event/v3)

A **production-grade event pub-sub library** for Go with support for distributed event handling, exactly-once semantics, and multiple transports. Comparable to MassTransit (.NET), Axon (Java), and Spring Cloud Stream.

## Features

### Core
- **Type-Safe Generics**: `Event[T]` ensures compile-time type safety
- **Multiple Transports**: Channel (in-memory), Redis Streams, NATS JetStream, Kafka, MongoDB (CDC)
- **Fire-and-Forget API**: `Publish()` and `Subscribe()` are void - events are facts
- **Delivery Modes**: Broadcast (fan-out) or WorkerPool (load balancing)

### Reliability
- **Transactional Outbox**: Atomic publish with database writes (PostgreSQL, MongoDB, Redis)
- **Idempotency**: Prevent duplicate processing (Redis, in-memory)
- **Poison Detection**: Auto-quarantine repeatedly failing messages
- **At-Least-Once Delivery**: Via Redis Streams, NATS, or Kafka

### Advanced
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
| Outbox | Idempotency | Event Name |
| Outbox Relay | Deduplication | Codec |
| | Poison Detection | Schema |
| | Monitor | Transport |
| | Circuit Breaker | Transport Config |

## Database Support

| Component | PostgreSQL | MongoDB | Redis | In-Memory |
|-----------|:----------:|:-------:|:-----:|:---------:|
| Outbox | ✅ | ✅ | ✅ | - |
| Idempotency | ✅ | - | ✅ | ✅ |
| Poison | ✅ | - | ✅ | - |
| Monitor | ✅ | ✅ | - | ✅ |
| Schema Registry | ✅ | ✅ | ✅ | ✅ |

## Package Structure

The library is organized into focused packages with shared utilities to minimize code duplication.

### Core Packages

| Package | Description |
|---------|-------------|
| `event` | Core bus, event, and middleware types |
| `transport/*` | Transport implementations (channel, redis, nats, kafka, mongodb) |
| `monitor` | Event processing monitoring with HTTP/gRPC APIs |
| `schema` | Schema registry for publisher-defined event configuration |
| `idempotency` | Exactly-once processing with multiple backends |
| `poison` | Poison message detection and quarantine |
| `outbox` | Transactional outbox pattern |

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
    "github.com/rbaliyan/event/v3/idempotency"
    "github.com/rbaliyan/event/v3/outbox"
    "github.com/rbaliyan/event/v3/poison"
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

    // Create bus with middleware stores
    idempStore := idempotency.NewPostgresStore(db, idempotency.WithPostgresTTL(24*time.Hour))
    poisonStore := poison.NewPostgresStore(db, poison.WithPostgresFailureTTL(24*time.Hour))
    poisonDetector := poison.NewDetector(poisonStore, poison.WithThreshold(5))

    bus, _ := event.NewBus("order-service",
        event.WithBusTransport(transport),
        event.WithBusIdempotency(idempStore),
        event.WithBusPoisonDetection(poisonDetector),
    )
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

    // Create event
    orderEvent := event.New[Order]("order.created",
        event.WithMaxRetries(3),
    )
    event.Register(ctx, bus, orderEvent)

    // Subscribe - idempotency and poison detection are automatic via bus config
    orderEvent.Subscribe(ctx, func(ctx context.Context, e event.Event[Order], order Order) error {
        // Process order - just business logic!
        if err := processOrder(ctx, order); err != nil {
            return event.ErrDefer.Wrap(err)
        }

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

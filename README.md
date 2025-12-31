# Event v3

[![CI](https://github.com/rbaliyan/event/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rbaliyan/event/branch/development/graph/badge.svg)](https://codecov.io/gh/rbaliyan/event)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event/v3.svg)](https://pkg.go.dev/github.com/rbaliyan/event/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event/v3)](https://goreportcard.com/report/github.com/rbaliyan/event/v3)

A **production-grade event pub-sub library** for Go with support for distributed event handling, exactly-once semantics, sagas, scheduled messages, and multiple transports. Comparable to MassTransit (.NET), Axon (Java), and Spring Cloud Stream.

## Features

### Core
- **Type-Safe Generics**: `Event[T]` ensures compile-time type safety
- **Multiple Transports**: Channel (in-memory), Redis Streams, NATS JetStream, Kafka
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
- **Schema Registry**: Payload validation and versioning

### Observability
- **OpenTelemetry Tracing**: Distributed tracing across services
- **Prometheus Metrics**: Out-of-the-box monitoring
- **Health Checks**: Transport health and consumer lag monitoring

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
    orderEvent, err := event.Register(ctx, bus, event.New[Order]("order.created"))
    if err != nil {
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

### Transport Feature Comparison

| Feature | Redis Streams | NATS Core | NATS JetStream | Kafka |
|---------|:-------------:|:---------:|:--------------:|:-----:|
| Persistence | ✅ | ❌ | ✅ | ✅ |
| At-Least-Once | ✅ | ❌ | ✅ | ✅ |
| Native Deduplication | ❌ (inject store) | ❌ (inject store) | ✅ | ❌ |
| Native DLQ/DLT | ❌ (inject handler) | ❌ (inject handler) | ❌ | ✅ |
| Native Retry Limits | ❌ | ❌ | ✅ (MaxDeliver) | ✅ |
| Consumer Groups | ✅ | Queue Groups | ✅ | ✅ |
| Health Checks | ✅ | ✅ | ✅ | ✅ |
| Lag Monitoring | ✅ | ❌ | ❌ | ✅ |

**Native vs Injected Features:**
- **Native features** are handled by the broker (more efficient, no external dependencies)
- **Injected stores** provide library-level features where the broker lacks native support

## Transactional Outbox Pattern

Ensure atomic publish with database writes - never lose messages:

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

    orderEvent, _ := event.Register(ctx, bus, event.New[Order]("order.created"))

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

    orderEvent, _ := event.Register(ctx, bus, event.New[Order]("order.created"))

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
| | Rate Limiting | Transport Config |
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
| Transaction | ✅ | ✅ | - | - |
| Rate Limit | - | - | ✅ | - |

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

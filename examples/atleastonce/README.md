# At-Least-Once Delivery with MongoDB Transport

This document describes how to achieve at-least-once delivery semantics using the MongoDB transport.

## Overview

At-least-once delivery guarantees that every message will be delivered to subscribers at least once, even in the presence of:
- Application crashes
- Network failures
- Database failures
- Transport restarts

The tradeoff is that messages may be delivered more than once, so handlers must be **idempotent**.

## Components

### 1. Transactional Outbox (Publisher Side)

**Purpose:** Ensure events are atomically written with business data.

**How it works:**
```go
err := outbox.Transaction(ctx, client, func(sessCtx context.Context) error {
    // Both operations are in the same MongoDB transaction
    _, err := db.Collection("orders").InsertOne(sessCtx, order)
    if err != nil {
        return err
    }
    return orderCreated.Publish(sessCtx, order) // Goes to outbox table
})
```

**Failure Scenarios:**
| Scenario | What Happens |
|----------|--------------|
| Crash before commit | Transaction rolls back, neither business data nor event is persisted |
| Crash after commit | Both are persisted, relay will publish the event |
| Network failure | Transaction rolls back or retry succeeds |

### 2. Outbox Relay

**Purpose:** Publish outbox messages to the transport.

**Features:**
- Polling mode (works with standalone MongoDB)
- Change stream mode (real-time, requires replica set)
- Stuck message recovery
- Resume tokens for crash recovery

```go
relay := outbox.NewMongoRelay(outboxStore, transport).
    WithMode(outbox.RelayModeChangeStream).
    WithResumeTokenStore(tokenStore).
    WithStuckDuration(5 * time.Minute)
```

**Failure Scenarios:**
| Scenario | What Happens |
|----------|--------------|
| Relay crash during publish | Message stays in outbox, recovered on restart |
| Relay crash after publish | Message marked published, no duplicate |
| Message stuck in "processing" | Recovered after stuck duration |

### 3. Resume Token Store (Transport)

**Purpose:** Allow transport to resume from last position after restart.

```go
transport, _ := mongodb.New(db,
    mongodb.WithResumeTokenStore(resumeTokenStore),
    mongodb.WithResumeTokenID(hostname), // Per-instance
)
```

**Format:** Resume tokens are stored as `namespace:instance_id`

**Failure Scenarios:**
| Scenario | What Happens |
|----------|--------------|
| Transport restart | Resumes from last saved position |
| Resume token lost | Starts from current time (may miss events) |
| Instance failover | New instance uses own resume token |

### 4. Ack Store (Transport)

**Purpose:** Track which events have been acknowledged by handlers.

```go
ackStore := mongodb.NewMongoAckStore(
    db.Collection("_event_acks"),
    24*time.Hour, // TTL for acked events
)

transport, _ := mongodb.New(db,
    mongodb.WithAckStore(ackStore),
)
```

**Flow:**
1. Event received → stored as "pending" in ack store
2. Handler processes event
3. Handler calls `msg.Ack(nil)` → marked as "acknowledged"
4. If handler crashes → event remains pending, redelivered on restart

### 5. Checkpoint Store (Per-Subscriber)

**Purpose:** Track per-subscriber processing position.

```go
checkpointStore := checkpoint.NewMongoStore(
    db.Collection("_checkpoints"),
)

bus, _ := event.NewBus("orders",
    event.WithCheckpointStore(checkpointStore),
)
```

**Failure Scenarios:**
| Scenario | What Happens |
|----------|--------------|
| Subscriber crash | Resumes from last checkpoint |
| Checkpoint save fails | May reprocess some events |

### 6. Dead Letter Queue (DLQ)

**Purpose:** Store messages that fail permanently after all retries.

```go
dlqStore := dlq.NewMongoStore(db.Collection("_dlq"))
dlqManager := dlq.NewManager(dlqStore, transport)

// In handler
if retryCount >= maxRetries {
    dlqManager.Store(ctx, eventName, eventID, payload, metadata, err, retryCount, "source")
    return nil // Ack to prevent infinite retries
}
return err // Nack for redelivery
```

**DLQ Operations:**
- `List(filter)` - View failed messages
- `Replay(filter)` - Reprocess messages after fix
- `Cleanup(age)` - Remove old messages
- `Stats()` - Monitor DLQ health

## Complete Flow

```
Publisher                     Transport                      Subscriber
─────────                     ─────────                      ──────────
    │                             │                              │
    │ 1. Transaction(write+event) │                              │
    │ ─────────────────────────►  │                              │
    │                             │                              │
    │ 2. Commit                   │                              │
    │ ────────────────────────►   │                              │
    │                             │                              │
    │                    3. Relay reads outbox                   │
    │                             │                              │
    │                    4. Publish to transport                 │
    │                             │                              │
    │                    5. Save resume token                    │
    │                             │                              │
    │                             │  6. Change stream delivers   │
    │                             │ ─────────────────────────►   │
    │                             │                              │
    │                             │  7. Store as pending         │
    │                             │                              │
    │                             │  8. Handler processes        │
    │                             │                              │
    │                             │  9. Ack (success)            │
    │                             │ ◄─────────────────────────   │
    │                             │                              │
    │                             │ 10. Mark acked, save checkpoint
    │                             │                              │
```

## Failure Recovery Matrix

| Component | Failure Point | Recovery Mechanism | Data Loss? |
|-----------|--------------|-------------------|------------|
| Publisher | Before commit | Transaction rollback | No |
| Publisher | After commit | Outbox relay | No |
| Outbox Relay | During publish | Stuck message recovery | No |
| Transport | Restart | Resume token | No* |
| Subscriber | Before ack | Redelivery from ack store | No |
| Subscriber | After ack | Already processed | No |
| Handler | Permanent failure | DLQ | No** |

\* May miss events if resume token is very old (beyond oplog window)
\** Messages are preserved in DLQ for manual intervention

## Idempotency Requirement

Since messages may be delivered multiple times, handlers must be idempotent:

```go
func handleOrder(ctx context.Context, e event.Event[Order], order Order) error {
    // Option 1: Check if already processed
    exists, _ := db.Collection("processed").FindOne(ctx, bson.M{"_id": order.ID})
    if exists != nil {
        return nil // Already processed, skip
    }

    // Option 2: Use upsert operations
    _, err := db.Collection("inventory").UpdateOne(ctx,
        bson.M{"_id": order.ProductID},
        bson.M{"$inc": bson.M{"reserved": order.Quantity}},
        options.Update().SetUpsert(true),
    )

    // Option 3: Use idempotency middleware
    // The library provides event.WithIdempotency() middleware

    return err
}
```

## Monitoring

### Key Metrics to Monitor

1. **Outbox Lag:** Messages pending in outbox
2. **DLQ Size:** Failed messages waiting for intervention
3. **Ack Store Pending:** Unacknowledged events
4. **Processing Latency:** Time from publish to ack

### Alerts

- DLQ size > threshold
- Outbox lag > threshold
- Messages stuck > stuck duration
- Resume token age > oplog retention

## Configuration Recommendations

| Setting | Development | Production |
|---------|-------------|------------|
| Stuck Duration | 1 minute | 5 minutes |
| DLQ Retention | 7 days | 30 days |
| Ack TTL | 1 hour | 24 hours |
| Buffer Size | 100 | 1000+ |
| Max Retries | 3 | 5 |

## Running the Example

```bash
# Start MongoDB replica set
mongod --replSet rs0

# Initialize replica set (in mongo shell)
rs.initiate()

# Run the example
go run main.go
```

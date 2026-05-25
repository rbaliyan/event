# Ecosystem Compatibility Matrix

This document describes version compatibility between the event library and its extensions.

## Package Versions

| Package | Current | Go Version | Dependencies |
|---------|---------|------------|--------------|
| event/v3 | 3.x | 1.26.3+ | otel 1.40+ |
| event-scheduler | 1.x | 1.26+ | event/v3, go-redis/v9 |
| event-dlq | 1.x | 1.26+ | event/v3, go-redis/v9 |
| event-extras | 1.x | 1.26+ | event/v3, go-redis/v9 |
| event-mongodb | 0.x | 1.26+ | event/v3, mongo-driver/v2 |

## Shared Packages

The following shared packages provide consistent interfaces across the ecosystem:

### event/v3/errors

Common error types used by all packages:

```go
import eventerrors "github.com/rbaliyan/event/v3/errors"

// Check for common errors
if eventerrors.IsNotFound(err) { ... }
if eventerrors.IsVersionConflict(err) { ... }
```

**Exported Errors:**
- `ErrNotFound` - resource not found
- `ErrVersionConflict` - optimistic locking conflict
- `ErrAlreadyExists` - duplicate resource
- `ErrClosed` - component closed
- `ErrTimeout` - operation timed out
- `ErrInvalidArgument` - validation failed

### event/v3/health

Standardized health check interface:

```go
import "github.com/rbaliyan/event/v3/health"

type MyComponent struct { ... }

func (c *MyComponent) Health(ctx context.Context) *health.Result {
    return &health.Result{
        Status:    health.StatusHealthy,
        CheckedAt: time.Now(),
    }
}

var _ health.Checker = (*MyComponent)(nil)
```

### event/v3/metrics

Common metric types and attribute keys:

```go
import "github.com/rbaliyan/event/v3/metrics"

// Use standard attribute keys
attrs := []attribute.KeyValue{
    metrics.AttrEventName("orders.created"),
    metrics.AttrStatus("success"),
}
```

## Transport Compatibility

All transports implement `transport.Transport`:

| Transport | Package | Delivery Modes | Features |
|-----------|---------|----------------|----------|
| Channel | transport/channel | Broadcast, WorkerPool | In-memory, testing |
| Redis | transport/redis | Broadcast, WorkerPool | Consumer groups, persistence |
| NATS | transport/nats | Broadcast, WorkerPool | JetStream, core |
| Kafka | transport/kafka | Broadcast, WorkerPool | Partitioning, DLT |

## Breaking Changes

See [CHANGELOG.md](CHANGELOG.md) for the running list. Highlights:

### v3.x to v4.x (Future)

No breaking changes planned. All v3.x APIs will remain stable.

### Within v3.x

- **MongoDB store extraction** (mid-v3): MongoDB implementations for outbox,
  monitor, distributed state manager, schema, idempotency, and checkpoint
  moved out of this module into
  [event-mongodb](https://github.com/rbaliyan/event-mongodb). Update imports
  from `github.com/rbaliyan/event/v3/<pkg>` MongoDB constructors to the
  matching `github.com/rbaliyan/event-mongodb/<pkg>` constructors.
- **transport/redis NOGROUP recovery** (`WithAutoRecreateGroup`,
  `WithRecreateHandler`, `RecreateMode`): new opt-in option family added.
  Default behavior is unchanged (`RecreateMode(0)` — no auto-recreate). See
  README "Auto-Recreate Consumer Group (Redis)" for blast-radius guidance.
- **transport/redis broadcast group teardown** (#118): the consume
  goroutine is drained before `XGroupDestroy` runs. Only observable as the
  absence of a previous `read error, retrying with backoff` log line during
  Subscription.Close on the broadcast path.
- **transport/redis retained-stream replay** (#116): a Broadcast subscriber
  no longer replays the retained Redis Stream on restart. If you depend on
  the prior behavior, capture and replay externally.
- **transport/redis deferred group creation** (#120): the base
  consumer group is created on first Subscribe rather than at
  RegisterEvent. Callers that introspected groups between RegisterEvent and
  Subscribe must adjust.
- **Internal `clock.Clock` injection** (test-only, v3.17.x): four stores
  (`distributed.MemoryStateManager`, `idempotency.MemoryStore`,
  `poison.MemoryStore`, `transport/bridge.MemoryCoordinator`) gained an
  unexported `withClock` test hook backed by `internal/clock`. Not a
  breaking change for external consumers — `internal/` paths cannot be
  imported externally — but recorded here so downstream maintainers can
  correlate the test-quality lift with the diff.

### v2.x to v3.x

- Generic types: `Event[T]` replaces `Event`
- Handler signature: `Handler[T]` with typed data parameter
- Subscribe options: `SubscribeOption[T]` requires type parameter

## Store Compatibility

All store implementations follow the same interface patterns:

| Store Type | PostgreSQL | MongoDB† | Redis | Memory |
|------------|:----------:|:--------:|:-----:|:------:|
| Scheduler | ✅ | ✅ | ✅ | - |
| DLQ | ✅ | ✅ | ✅ | ✅ |
| Saga | ✅ | ✅ | ✅ | ✅ |
| Monitor | ✅ | ✅ | - | ✅ |
| Schema | ✅ | ✅ | ✅ | ✅ |
| Idempotency | ✅ | ✅ | ✅ | ✅ |
| Poison | ✅ | - | ✅ | ✅ |

† MongoDB store implementations are provided by the [event-mongodb](https://github.com/rbaliyan/event-mongodb) module, not this package.

## Middleware Chain Order

When a handler is subscribed the bus wraps it in this chain. Execution
flows outermost to handler:

```
Monitor (bus-level, controlled by schema if loaded)
  → Poison detection (bus-level, controlled by schema if loaded)
    → Idempotency (bus-level, controlled by schema if loaded)
      → Custom middleware (via WithMiddleware)
        → Timeout (context deadline)
          → Recovery (panic handling)
            → Handler
```

User-defined chains via `event.NewChain[T]()` follow the standard
outer-to-inner ordering with the first `Use` call as the outermost layer:

```go
chain := event.NewChain[Order]().
    Use(myLoggingMiddleware).    // outermost
    Use(myMetricsMiddleware).
    Use(myValidationMiddleware)  // innermost
```

Schema-controlled middleware:
- When a schema is loaded, each of monitor/poison/idempotency only runs
  if the matching `Enable*` flag is true **and** the bus has a store
  configured for it.
- When no schema is loaded, the bus falls back to applying any configured
  store unconditionally.

This block is canonical — `CLAUDE.md` mirrors it under
"Middleware Chain Execution Order".

## Backoff Strategy Compatibility

All backoff strategies implement `backoff.Strategy`:

```go
type Strategy interface {
    NextDelay(attempt int) time.Duration
}
```

Available strategies:
- `backoff.Constant` - fixed delay
- `backoff.Linear` - linearly increasing delay
- `backoff.Exponential` - exponentially increasing delay with jitter

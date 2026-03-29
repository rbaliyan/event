# Ecosystem Compatibility Matrix

This document describes version compatibility between the event library and its extensions.

## Package Versions

| Package | Current | Go Version | Dependencies |
|---------|---------|------------|--------------|
| event/v3 | 3.x | 1.25.8+ | otel 1.40+ |
| event-scheduler | 1.x | 1.25+ | event/v3, go-redis/v9 |
| event-dlq | 1.x | 1.25+ | event/v3, go-redis/v9 |
| event-extras | 1.x | 1.25+ | event/v3, go-redis/v9 |
| event-mongodb | 0.x | 1.25+ | event/v3, mongo-driver/v2 |

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

### v3.x to v4.x (Future)

No breaking changes planned. All v3.x APIs will remain stable.

### v2.x to v3.x

- Generic types: `Event[T]` replaces `Event`
- Handler signature: `Handler[T]` with typed data parameter
- Subscribe options: `SubscribeOption[T]` requires type parameter

## Store Compatibility

All store implementations follow the same interface patterns:

| Store Type | PostgreSQL | MongoDB | Redis | Memory |
|------------|:----------:|:-------:|:-----:|:------:|
| Scheduler | ✅ | ✅ | ✅ | - |
| DLQ | ✅ | ✅ | ✅ | ✅ |
| Saga | ✅ | ✅ | ✅ | ✅ |
| Monitor | ✅ | ✅ | - | ✅ |
| Schema | ✅ | ✅ | ✅ | ✅ |
| Idempotency | ✅ | ✅ | ✅ | ✅ |
| Poison | ✅ | - | ✅ | ✅ |

## Middleware Chain Order

When middleware is applied, it executes in this order:

```
Outermost (first added)
    → Middle layers
        → Innermost (last added)
            → Handler
        ← Innermost returns
    ← Middle returns
← Outermost returns
```

Example:
```go
chain := event.NewChain[Order]().
    Use(myLoggingMiddleware).    // 1. Outermost - logs before/after (user-defined)
    Use(myMetricsMiddleware).    // 2. Records timing (user-defined)
    Use(myValidationMiddleware)  // 3. Innermost - validates data (user-defined)
```

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

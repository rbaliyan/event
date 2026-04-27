# Event Monitor Debugging Guide

This guide explains how to diagnose event delivery failures, consumer lag, stuck processing, and coverage gaps using the monitoring APIs and raw backend queries.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Monitoring API Reference](#monitoring-api-reference)
3. [Scenario: Subscriber Did Not Process an Event](#scenario-subscriber-did-not-process-an-event)
4. [Scenario: Consumer Lag Is Growing](#scenario-consumer-lag-is-growing)
5. [Scenario: Stuck Pending Entries](#scenario-stuck-pending-entries)
6. [Scenario: Coverage Gaps](#scenario-coverage-gaps)
7. [Scenario: Dead Letter Queue Buildup](#scenario-dead-letter-queue-buildup)
8. [System View Interpretation](#system-view-interpretation)
9. [MongoDB Direct Queries](#mongodb-direct-queries)
10. [Redis Direct Queries](#redis-direct-queries)
11. [Transport-Specific Notes](#transport-specific-notes)

---

## Architecture Overview

### Event Flow

```
Source (MongoDB Change Stream)
  │
  ▼
Bridge dedup (one pod wins via MongoDB CAS on _event_worker_state)
  │
  ▼
Redis Streams (XADD to evt.{busName}.{eventName} stream)
  │
  ▼
Consumer Groups (one per worker group)
  │  ├─ group "workflow"  → competes among pods
  │  ├─ group "signals"   → competes among pods (independent from "workflow")
  │  └─ group "mailbox"   → competes among pods (independent from all others)
  ▼
Monitor middleware (records "pending" BEFORE handler runs)
  │
  ▼
Handler
  │
  ▼
Monitor middleware (updates to "completed" or "failed" AFTER handler returns)
```

### Key Timing Constants

The values below are **example values chosen by the operator** — they are not library defaults. Configure them to match your SLOs and handler runtimes.

| Constant | Example Value | Meaning |
|---|---|---|
| `claimInterval` | 2 minutes | How often a pod sweeps for orphaned PEL messages |
| `claimMinIdle` | 5 minutes | Minimum age before an unclaimed message is stolen |
| `stuckPendingThreshold` | 6 minutes | Age at which a "pending" monitor entry is flagged as stuck |

With example values above, a pod crash means a monitor entry can appear stuck for up to ~5 minutes, then silently recover when another pod claims and processes the message.

### Delivery Modes

| Mode | Behavior | Monitor entry key |
|---|---|---|
| **Broadcast** | Every subscriber receives every message | `(event_id, subscription_id)` |
| **WorkerPool** | Only one subscriber per worker group receives each message | `(event_id, "")` — subscription_id is empty, matched by worker_group |

---

## Monitoring API Reference

All endpoints are served by the handler returned by `monitorhttp.New(store, opts...)`. Mount it on your HTTP server, e.g. at `/events`.

### Monitor Entries

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/monitor/entries` | List entries (paginated, filterable) |
| `GET` | `/v1/monitor/entries/{event_id}` | All entries for a specific event |
| `GET` | `/v1/monitor/entries/{event_id}/{subscription_id}` | Single entry by composite key |
| `GET` | `/v1/monitor/entries/count` | Count entries matching filter |
| `DELETE` | `/v1/monitor/entries?older_than=24h` | Delete entries older than age |

#### Filter Query Parameters

| Parameter | Type | Description |
|---|---|---|
| `event_id` | string | Exact match on event ID |
| `event_name` | string | Exact match on event name |
| `subscription_id` | string | Exact match on subscription ID |
| `bus_id` | string | Filter by bus name |
| `instance_id` | string | Filter by pod/instance ID |
| `worker_group` | string | Filter by worker group |
| `delivery_mode` | string | `broadcast` or `worker_pool` |
| `status` | string (repeatable) | `pending`, `completed`, `failed`, `retrying` |
| `has_error` | bool | `true` to show only entries with errors |
| `start_time` | RFC3339 | Filter entries started after this time |
| `end_time` | RFC3339 | Filter entries started before this time |
| `min_duration` | Go duration | Minimum processing time (e.g. `500ms`) |
| `min_retries` | int | Minimum retry count |
| `limit` | int | Page size (default 100, max 1000) |
| `cursor` | string | Pagination cursor from previous response |
| `order_desc` | bool | Return newest first |

### Summary

```
GET /v1/monitor/summary
GET /v1/monitor/summary?start_time=2025-01-01T00:00:00Z&end_time=2025-01-02T00:00:00Z
```

Returns aggregated statistics: counts by status, by event name (with error rate), by instance ID, average duration, time range of stored data.

### Coverage

```
GET /v1/monitor/coverage/{event_id}
```

Cross-references recorded monitor entries with the **live in-process subscription topology**. For a given event ID, shows which subscriptions have an entry and which do not. Useful for confirming that every expected subscriber processed the event.

**Response fields:**

| Field | Description |
|---|---|
| `event_id` | The queried event ID |
| `topology_as_of` | Timestamp when the topology snapshot was taken |
| `coverage[]` | One entry per subscription in the current topology |
| `coverage[].has_entry` | Whether a monitor entry exists for this subscription |
| `coverage[].entry` | The entry if present (status, duration, error, etc.) |
| `missing_count` | Number of subscriptions with no recorded entry |
| `present_count` | Number of subscriptions with a recorded entry |

**Caveat:** subscription IDs are regenerated on pod restart. Historical entries with old IDs will not match the current topology. Use the `worker_group` to correlate worker pool entries.

### Topology

```
GET /v1/topology                    # All buses and events
GET /v1/topology/{bus_name}         # Single bus
```

Shows the live in-process subscription graph: bus names, event names, subscription IDs, worker groups, delivery modes.

### System View

```
GET /v1/system          # Full system snapshot (cached)
GET /v1/system/health   # Aggregated health status only
```

Returns all system data in one call: topology, DLQ stats, scheduler stats, stuck-pending detection, bus health, consumer lag per stream, and 24-hour summary. Backed by a configurable background refresh (default 10 seconds).

**`stuck_pending` field (if provider configured):**

```json
{
  "stuck_pending": {
    "count": 3,
    "threshold": "6m0s",
    "oldest_at": "2025-01-01T12:00:00Z",
    "samples": [...]
  }
}
```

A non-zero `count` means pods crashed after recording "pending" but before completing processing. Each sample shows the event ID, event name, worker group, and the instance that last owned it.

### Worker Pool State

```
GET /v1/workers          # All worker leases
GET /v1/workers/{id}     # Single worker by message_id (the worker's claim ID)
GET /v1/workers/count    # Count active workers
```

Available when `WithWorkerStore` is configured. Shows which pod holds which worker lease, when it was acquired, and when it expires. Use to diagnose split-brain or stale lease scenarios.

---

## Scenario: Subscriber Did Not Process an Event

**Symptoms:** A downstream effect (e.g., a task created, an email sent) did not happen after a database change.

### Step 1: Find the event ID

If you know when the source document changed, use MongoDB to find the event ID. All events published via the monitor middleware have a unique `event_id`. If you have the trace ID from the originating request, correlate via `trace_id`.

```
GET /v1/monitor/entries?event_name=<name>&start_time=<RFC3339>&end_time=<RFC3339>
```

Or if you have the event ID directly:

```
GET /v1/monitor/entries/{event_id}
```

### Step 2: Check coverage

```
GET /v1/monitor/coverage/{event_id}
```

- `missing_count > 0` → at least one subscriber has no recorded entry.
- Check the `delivery_mode` of missing entries: `worker_pool` entries are matched by worker group, so verify the `worker_group` field matches.

### Step 3: Check the entry status

| Status | Meaning | Action |
|---|---|---|
| `completed` | Handler ran and returned nil | Check downstream logic — event was processed but effect may have failed elsewhere |
| `failed` | Handler returned a non-retryable error | Check `entry.error` field; look at DLQ |
| `retrying` | Handler failed but will retry | Wait or check retry count / DLQ threshold |
| `pending` | Handler started but not completed | Either in-flight, or the pod crashed (see Stuck Pending section) |
| *(missing)* | No entry for this subscription | Handler was never invoked — check transport delivery |

### Step 4: If no entry exists for a subscriber

A missing entry means the monitor middleware never fired for that subscription. Possible causes:

1. **Bridge dedup lost the race** — the event was published to Redis by a different pod, and this pod's bridge dedup saw the deduplicated `ignore`. Check the bridge dedup worker state collection:

   ```javascript
   db._event_worker_state.findOne({_id: "<event_id>"})
   ```

2. **Redis consumer group lag** — the message is in the stream but not yet delivered to this worker group. Check consumer lag:

   ```
   GET /v1/system
   # Look at consumer_lag[] for the relevant stream and group
   ```

3. **Pod was down** — the consumer group had no active consumers when the message arrived. When a pod comes back, it will claim the message from the PEL (after `claimMinIdle`).

4. **Wrong worker group** — the subscriber registered under a different group name. Cross-check `GET /v1/topology` against `GET /v1/workers`.

---

## Scenario: Consumer Lag Is Growing

**Symptoms:** Events are published but subscribers are falling behind. The system view shows increasing `lag` on one or more streams.

### Step 1: Check the system view

```
GET /v1/system
```

Look at `consumer_lag[]`:

```json
{
  "consumer_lag": [
    {
      "event": "case_record.created",
      "consumer_group": "mybus-case_record.created-workflow",
      "lag": 1500,
      "pending_messages": 3,
      "oldest_pending": 270000000000
    }
  ]
}
```

`oldest_pending` is serialized as an int64 nanosecond count (e.g. `270000000000` = 4m30s). The field is omitted from the JSON output when the value is unknown (nil).

| Field | Concern threshold | Meaning |
|---|---|---|
| `lag` | > a few hundred | Unconsumed messages in the stream |
| `pending_messages` | > 0 | Delivered but not ACKed (in-flight or crashed) |
| `oldest_pending` | > operator-configured `claimMinIdle` | A pod claimed this message and has not ACKed it — likely crashed |

### Step 2: Identify the bottleneck

- **High lag, low pending**: handlers are too slow or there aren't enough pods. Scale horizontally — each pod in the worker group is a consumer.
- **High pending, old oldest_pending**: pod crashed and the PEL is not yet reclaimed. Wait for the configured `claimMinIdle` duration and check again. If it persists, check stuck pending (next section).
- **High lag, zero pending**: the consumer group has no active consumers. All pods may be down.

### Step 3: Direct Redis inspection

```bash
# Stream length
XLEN evt.mybus.case_record.created

# Consumer group summary
XPENDING evt.mybus.case_record.created mybus-case_record.created-workflow

# Detailed PEL (messages stuck for > 5 min)
XPENDING evt.mybus.case_record.created mybus-case_record.created-workflow - + 10

# Info on all groups
XINFO GROUPS evt.mybus.case_record.created
```

---

## Scenario: Stuck Pending Entries

**Symptoms:** `GET /v1/system` shows `stuck_pending.count > 0`, or a manual query returns entries in `pending` status with `started_at` much older than expected processing time.

### What "stuck pending" means

The monitor middleware records "pending" **before** the handler runs. If a pod crashes after recording "pending" but before completing the handler:

1. The monitor entry stays `pending` forever (nothing updates it).
2. The Redis PEL still holds the message.
3. After the configured `claimMinIdle` duration, another pod claims the message and processes it.
4. **The original "pending" entry is never updated** because `Record()` uses `$setOnInsert` for WorkerPool mode — a new completion entry cannot overwrite the old pod's pending entry.

This means a stuck pending entry does **not** always mean the event was lost. The new pod's completion is a separate `(event_id, "")` upsert that conflicts with the orphaned entry. Check whether a completion entry exists with the same `event_id` and `worker_group`.

### Step 1: Query stuck pending entries

```
GET /v1/monitor/entries?status=pending&min_duration=6m
```

Or query MongoDB directly (see [MongoDB Direct Queries](#mongodb-direct-queries)).

### Step 2: For each stuck entry

1. Check `instance_id` — which pod last owned it.
2. Check `worker_group` — which subscriber group is affected.
3. Look for a completed entry on the same event: 
   ```
   GET /v1/monitor/entries/{event_id}
   ```
   If a `completed` entry exists with the same `worker_group`, the event **was** processed by another pod — the stuck pending is just the orphaned entry from the crashed pod.
4. If no completed entry exists, the message is still in the PEL waiting to be reclaimed. Verify via Redis:
   ```bash
   XPENDING evt.mybus.{event_name} {consumer_group} - + 1 {consumer_name}
   ```

### Step 3: Resolution

| Situation | Action |
|---|---|
| Completed entry exists for same event + worker_group | No action needed; orphaned pending entry will age out on next `DeleteOlderThan` run |
| No completed entry; message still in Redis PEL | Wait for reclaim, or manually trigger `XCLAIM` |
| No completed entry; message not in Redis PEL | Message was ACKed without monitor update (rare race) — investigate handler logs |
| Pod is still running but slow | Check handler duration via `min_duration` filter; look for resource exhaustion |

---

## Scenario: Coverage Gaps

**Symptoms:** You expect subscriber X to process every message of event Y, but some events have no record for X.

### Understanding coverage

The coverage endpoint cross-references the **live subscription topology** (which subscriptions currently exist in this process) with **historical monitor entries** (which subscriptions recorded processing the event).

A "missing" entry means one of:

1. **The event was published before this subscriber existed** — if the subscription was added in a recent deploy, old events predate it.
2. **The subscriber's consumer group had no active consumers** — the pod was down when the message arrived.
3. **The worker group name changed** — group renames create a new consumer group; the new group starts from `$` (latest) by default.
4. **The event was not forwarded to this transport** — if using bridge mode, verify the bridge dedup pod published to Redis.

### Checking group membership

```bash
XINFO CONSUMERS evt.mybus.{event_name} {consumer_group}
```

This shows all consumers in the group, their pending count, and last interaction time. If there are no consumers, the group is empty.

---

## Scenario: Dead Letter Queue Buildup

**Symptoms:** `GET /v1/system` shows `dlq.pending_messages > 0`, or growing.

### Step 1: Check DLQ stats

```
GET /v1/system
# Look at dlq field:
{
  "dlq": {
    "total_messages": 42,
    "pending_messages": 15,
    "messages_by_event": { "case_record.created": 10, "call.updated": 5 },
    "messages_by_error": { "context deadline exceeded": 12, "connection refused": 3 }
  }
}
```

### Step 2: Investigate the error

The `messages_by_error` map groups messages by the last error string. Common patterns:

| Error pattern | Likely cause |
|---|---|
| `context deadline exceeded` | Handler timeout — increase timeout or optimize handler |
| `connection refused` | Downstream service unavailable during handler |
| `duplicate key` | Idempotency violation — handler not idempotent; fix or add idempotency middleware |
| application-specific | Bug in handler business logic |

### Step 3: Retry or drain

DLQ management depends on the DLQ implementation. Use the DLQ library's own API to retry individual messages or drain the queue after fixing the underlying issue.

---

## System View Interpretation

```json
GET /v1/system

{
  "topology": [...],           // All buses, events, subscriptions in this process
  "health": {                  // Aggregated health: healthy/degraded/unhealthy
    "status": "degraded",
    "components": {
      "bus:mybus": {"status": "healthy"},
      "dlq": {"status": "degraded", "message": "15 pending messages"}
    }
  },
  "bus_health": {              // Per-bus ping latency and status
    "mybus": {"code": "healthy", "latency_ms": 2}
  },
  "consumer_lag": [...],       // Per-stream, per-group lag and PEL info
  "dlq": {...},                // DLQ stats (if DLQProvider configured)
  "scheduler": {...},          // Scheduler stats (if SchedulerProvider configured)
  "stuck_pending": {           // Entries stuck in "pending" beyond threshold
    "count": 0,
    "threshold": "6m0s"
  },
  "summary": {                 // 24h aggregated monitor stats
    "total_entries": 12500,
    "by_status": {"completed": 12400, "failed": 80, "retrying": 20},
    "by_event_name": {
      "case_record.created": {
        "total": 500, "completed": 490, "failed": 10,
        "error_rate": 0.02, "avg_duration_ms": 45
      }
    }
  },
  "collected_at": "2025-01-01T12:00:00Z"
}
```

**Health status rules:**
- `healthy` — all components healthy
- `degraded` — at least one component degraded (system is functional but impaired)
- `unhealthy` — at least one component unhealthy (requests may fail)

`GET /v1/system/health` returns 200 for healthy/degraded and 503 for unhealthy.

---

## MongoDB Direct Queries

The monitor collection is typically named `_event_monitor`. Adjust the collection name to match your configuration.

### Find all entries for an event

```javascript
db._event_monitor.find({ event_id: "<event_id>" }).sort({ started_at: 1 })
```

### Find all failed events in the last hour

```javascript
db._event_monitor.find({
  status: "failed",
  started_at: { $gte: new Date(Date.now() - 3600000) }
}).sort({ started_at: -1 }).limit(20)
```

### Find stuck pending entries (older than 6 minutes)

```javascript
var cutoff = new Date(Date.now() - 6 * 60 * 1000);
db._event_monitor.find({
  status: "pending",
  started_at: { $lt: cutoff }
}).sort({ started_at: 1 })
```

This query uses the `{status: 1, started_at: 1}` index prefix — it does not scan the collection.

### Events with high retry counts

```javascript
db._event_monitor.find({ retry_count: { $gte: 3 } }).sort({ retry_count: -1 }).limit(20)
```

### Error rate by event name (last 24 hours)

```javascript
db._event_monitor.aggregate([
  { $match: { started_at: { $gte: new Date(Date.now() - 86400000) } } },
  { $group: {
      _id: "$event_name",
      total: { $sum: 1 },
      failed: { $sum: { $cond: [{ $eq: ["$status", "failed"] }, 1, 0] } }
  }},
  { $project: {
      total: 1,
      failed: 1,
      error_rate: { $divide: ["$failed", "$total"] }
  }},
  { $sort: { error_rate: -1 } }
])
```

### Slow events (processing time > 5 seconds)

```javascript
db._event_monitor.find({ duration_ms: { $gte: 5000 } }).sort({ duration_ms: -1 }).limit(20)
```

### Coverage check for an event via MongoDB

```javascript
// Find all subscriptions that processed a specific event
db._event_monitor.find(
  { event_id: "<event_id>" },
  { subscription_id: 1, worker_group: 1, status: 1, instance_id: 1, delivery_mode: 1 }
)
```

### Check worker state (bridge dedup)

```javascript
// See which pod won the dedup race for an event
db._event_worker_state.findOne({ _id: "<event_id>" })
// Returns: { _id, instance_id, acquired_at, expires_at }
```

### Check resume tokens

Resume tokens are stored in a separate collection (default: `_event_resume_tokens_{storeName}`):

```javascript
// List all resume tokens
db._event_resume_tokens_GLOBAL.find({})

// Delete token for a specific collection (forces restart from current oplog position)
db._event_resume_tokens_GLOBAL.deleteMany({ _id: /orders/ })

// Delete all tokens (next restart processes from current position for all collections)
db._event_resume_tokens_GLOBAL.deleteMany({})
```

---

## Redis Direct Queries

Streams are named `evt.{busName}.{eventName}`. Consumer groups are named `{deploymentName}-{eventName}-{workerGroup}`.

### List all event streams

```bash
KEYS evt.*
```

### Stream length

```bash
XLEN evt.mybus.case_record.created
```

### Consumer group summary

```bash
# Shows lag (unconsumed messages), pending (delivered but not ACKed), consumers count
XINFO GROUPS evt.mybus.case_record.created
```

### Pending Entry List (PEL) — in-flight or orphaned messages

```bash
# Summary: count, min/max message IDs, consumer breakdown
XPENDING evt.mybus.case_record.created mybus-case_record.created-workflow

# Detailed: messages idle for at least 5 minutes
XPENDING evt.mybus.case_record.created mybus-case_record.created-workflow - + 10
```

The `idle` time in the detailed output shows how long since the message was last delivered. Messages idle longer than the configured `claimMinIdle` duration will be auto-claimed by the next pod that runs a claim sweep.

### Active consumers in a group

```bash
XINFO CONSUMERS evt.mybus.case_record.created mybus-case_record.created-workflow
```

Shows each consumer: name (pod identifier), pending count, idle time, inactive time.

### Manually reclaim a stuck message

```bash
# Claim a stuck message to a specific consumer (use XAUTOCLAIM in production)
XCLAIM evt.mybus.case_record.created mybus-case_record.created-workflow <new_consumer> 300000 <message_id>

# Or use XAUTOCLAIM to claim all messages idle > 5 min
XAUTOCLAIM evt.mybus.case_record.created mybus-case_record.created-workflow <consumer> 300000 0-0 COUNT 10
```

### Read the latest messages

```bash
# Last 10 messages from the stream
XREVRANGE evt.mybus.case_record.created + - COUNT 10
```

### Check stream memory usage

```bash
XINFO STREAM evt.mybus.case_record.created FULL COUNT 0
```

---

## Transport-Specific Notes

### Bridge Mode (MongoDB Change Stream → Redis)

In bridge mode, events originate as MongoDB change stream events and are forwarded to Redis Streams by exactly one pod (the bridge dedup winner). The bridge dedup uses MongoDB compare-and-swap on the `_event_worker_state` collection.

**Key implication:** If the bridge pod crashes immediately after writing to Redis but before the change stream offset advances, the **next restart will re-read the same change** and try to publish again. The dedup worker state (CAS on `event_id`) prevents duplicate processing by downstream consumers, but the stream may have duplicate messages — the idempotency middleware handles this.

**Debugging bridge failures:**

1. Check if the event reached Redis: `XRANGE evt.mybus.{name} - + COUNT 5`
2. If not in Redis, check the bridge dedup state: `db._event_worker_state.findOne({_id: "<event_id>"})`
3. If the dedup state shows a different `instance_id` than expected, that pod won and published to Redis. Check its logs.
4. If the dedup state is missing entirely, the bridge pod crashed before recording the win. The change stream will re-deliver on the next pod's watch loop (up to oplog retention window).

### WorkerPool Mode via Redis Consumer Groups

Each worker group gets its own Redis consumer group. Consumer group names follow the pattern:
```
{deploymentName}-{eventName}-{workerGroup}
```

**Different worker groups are completely independent**: if event `case_record.created` has worker groups `workflow` and `timeline`, every message is delivered to **both** groups (each group gets its own copy). Within each group, only one pod processes the message.

**Debugging: "only some subscribers processed it"**
- Identify which worker groups are registered via `GET /v1/topology`.
- For each group, check `XINFO CONSUMERS` to confirm active consumers.
- Query `GET /v1/monitor/entries/{event_id}` and filter by `worker_group`.

### Monitor Entry for WorkerPool

In WorkerPool mode, the monitor entry `subscription_id` field is stored as an empty string (`""`). Entries are keyed on `(event_id, "")` — meaning only **one** entry per event per worker group. If two pods race to record "pending", the first writer wins (`$setOnInsert`). The losing pod's instance_id is not recorded.

This means:
- You cannot tell from the monitor entry which specific pod ultimately completed the handler.
- The `instance_id` in the entry reflects the pod that first recorded "pending", which may be different from the pod that completed processing (in crash/reclaim scenarios).
- For broadcast mode, `subscription_id` is a UUID regenerated on pod restart, so historical entries may not match the current topology.

### Reading Monitor Entry Fields

| Field | Notes |
|---|---|
| `event_id` | Unique ID for the published event (set by publisher) |
| `subscription_id` | UUID for the subscription (empty for WorkerPool) |
| `subscriber_name` | Handler display name |
| `event_name` | Registered event name |
| `bus_id` | Bus name |
| `instance_id` | Pod hostname or instance identifier |
| `delivery_mode` | `broadcast` or `worker_pool` |
| `worker_group` | Worker group name (worker pool only) |
| `status` | `pending` / `completed` / `failed` / `retrying` |
| `error` | Last error message (if status is failed or retrying) |
| `retry_count` | Number of retries attempted |
| `started_at` | When processing began (when "pending" was recorded) |
| `completed_at` | When processing ended (nil for pending) |
| `duration_ms` | Handler duration in milliseconds |
| `trace_id` | OpenTelemetry trace ID for correlation |
| `span_id` | OpenTelemetry span ID for correlation |

---

## Quick Diagnostic Checklist

Use this checklist when a subscriber appears to have missed an event:

```
□ Find the event_id (from logs, trace, or monitor query by event_name + time range)
□ GET /v1/monitor/coverage/{event_id} — any missing_count?
□ For missing entries:
  □ Is the subscription in GET /v1/topology?
  □ GET /v1/system — any consumer_lag for the affected stream?
  □ GET /v1/system — any stuck_pending?
  □ XINFO CONSUMERS {stream} {group} — are consumers active?
  □ XPENDING {stream} {group} — is the message stuck in PEL?
□ For pending entries older than 6 min:
  □ Does a completed entry exist with the same event_id + worker_group?
  □ If yes: orphaned from crashed pod, event was processed
  □ If no: check Redis PEL and pod health
□ For failed entries:
  □ Check entry.error field
  □ Check GET /v1/system dlq.messages_by_event for DLQ accumulation
□ If event never reached Redis (no PEL, no consumer lag):
  □ Check bridge dedup: db._event_worker_state.findOne({_id: event_id})
  □ Check MongoDB change stream lag: db._event_resume_tokens_* for stale tokens
  □ Check mongodb_stream_reconnections_total metric for history_lost events
```

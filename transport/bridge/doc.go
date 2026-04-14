// Package bridge provides a transport that forwards messages from a
// source transport to a sink transport, optionally transforming or
// filtering them via a composable pipeline.
//
// # Motivation
//
// Some event sources — MongoDB change streams, PostgreSQL logical
// replication, file-system watchers — are inherently receive-only: every
// replica of a horizontally scaled consumer receives every message. This
// forces one of two shapes:
//
//   - Duplicate processing across replicas, relying on handler idempotency.
//   - Per-message distributed coordination in each subscriber's middleware,
//     round-tripping a state manager on every handler invocation.
//
// The bridge solves this by putting a transport that already owns
// consumer-group semantics (Redis Streams, Kafka, NATS JetStream) in
// front of the receive-only source. Every replica reads from the
// source, and the bridge pipeline decides — per replica, per message —
// whether to forward, drop, or transform. Downstream consumers subscribe
// to the sink and inherit its native load balancing.
//
// # Composability
//
// The bridge itself is pure plumbing: it reads messages from the source
// and publishes them to the sink. Everything else — deduplication,
// dead-letter routing, distributed coordination, metrics, tracing — is
// optional middleware composed via [WithMiddleware]:
//
//	t, _ := bridge.New(source, sink,
//	    bridge.WithMiddleware(
//	        bridge.Dedup(coord, mongodb.DedupKeyFromChangeStream(), 24*time.Hour),
//	        bridge.DLQ(dlqSink, "bridge.failed"),
//	        bridge.Observe(metrics),
//	    ),
//	)
//
// Callers pick only the middleware their source/sink pair needs. A
// single-replica bridge with an idempotent sink may need none. A
// multi-replica bridge onto Redis Streams typically wants [Dedup]. A
// bridge that cannot tolerate lost messages when the sink misbehaves
// wants [DLQ]. Users of the distributed package can wrap its state
// manager as a [Middleware] and register it the same way.
//
// # Architecture
//
//	              ┌─ replica 1 ─ bridge ─┐
//	source  ─► ──►├─ replica 2 ─ bridge ─┤──►  sink
//	 (CDC)        └─ replica N ─ bridge ─┘       │
//	                      │                      ▼
//	                 pipeline:             consumers
//	                 [Dedup → DLQ →
//	                  sink.Publish]
//
// # Delivery semantics
//
// The base bridge provides at-least-once delivery to the sink: every
// source message reaches the sink unless a middleware deliberately
// drops it. Middleware can strengthen or weaken this contract:
//
//   - [Dedup] suppresses re-publishes of the same logical event across
//     replicas. Its guarantee depends on the claim TTL vs. source
//     redelivery window — see its documentation.
//   - [DLQ] rescues messages that fail to publish to the sink,
//     preventing source redelivery loops at the cost of moving failed
//     events to a secondary sink.
//
// # Contract
//
// The sink MUST implement [transport.Transport.Publish]. The source
// MAY be receive-only — its Publish is never invoked by the bridge.
// The bridge's own Publish delegates to the sink, so direct publishes
// bypass the source and the middleware pipeline.
package bridge

# Changelog

All notable changes to the v3 module are recorded here. Patch tags pick up
on every functional change; this file groups them by user-visible theme so
the running narrative is easier to scan.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project uses [Semantic Versioning](https://semver.org/). See
[COMPATIBILITY.md](COMPATIBILITY.md) for the cross-module ecosystem matrix.

## Unreleased

### Added
- Documentation: top-level `Sub-packages` table in README; `Deterministic
  time with internal/clock` testing section; running `CHANGELOG.md`.
- `partition/`: package-level godoc.
- `checkpoint/`: package doc now points readers to the MongoDB store in
  `event-mongodb`.
- `schema/`: consolidated package doc that covers both `EventSchema`
  configuration registry and payload-schema evolution.

### Fixed
- README: removed the incorrect "fire-and-forget / void Publish/Subscribe"
  claim; both methods have always returned `error`.
- `CLAUDE.md`: outbox usage example now uses the actual
  `event.WithOutboxTx(ctx, session)` API instead of a non-existent
  `outbox.Transaction` helper.

## v3.17.x — Test-quality lift (2026-Q2)

A multi-PR initiative to make the test suite deterministic, parallel-safe,
and observably faster. No production API changes; production code only
gained internal Clock injection hooks (described below) for testability.

### Added
- `internal/clock` leaf package exposing `Clock` (`Now`, `Since`, `Sleep`),
  `Real{}`, and `Fake` (with `Advance`/`Set`). Re-exported as
  `testutil.Clock` etc. so callers in this repo can share fake-clock
  semantics across packages without an import cycle.
- Unexported `withClock` test hooks on `distributed.MemoryStateManager`,
  `idempotency.MemoryStore`, `poison.MemoryStore`,
  `transport/bridge.MemoryCoordinator`, and the coalescer. Production
  callers always get `clock.Real{}`; tests inject `clock.Fake` to cross
  TTL/stale-timeout boundaries deterministically.
- `coalesce.baseCoalescer.inputsHandled` atomic counter (one deferred
  `Add(1)` per processed message — negligible hot-path cost). Tests use it
  as a barrier between sequential sends and the subsequent `done` signal.
- Integration tests are now blocking in CI (`continue-on-error: true`
  removed); branch protection enforces linear history, strict mode, and
  required checks: Test, Lint, Smoke, CodeQL, Integration (Redis +
  Postgres), Vulnerability Check, GoSec SAST.
- 538 tests across 21 packages now run under `t.Parallel()` (#158–#163).
- Root event package reorganized into per-domain test files:
  `bus_registry_test.go`, `middleware_test.go`,
  `outbox_integration_test.go`, `message_filter_test.go`,
  `decode_error_test.go`, plus `helpers_test.go` consolidating
  `eventuallyEqInt32`, `eventuallyTrue`, `consistentlyEqInt32`, and
  `waitInputsHandled` (#164–#165).

### Fixed
- Coalescer test races: the `run()` goroutine's select between `incoming`
  and `done` is now ordered via `inputsHandled` waits (#149).
- Various test sleeps that masked real timing assumptions; 60+
  `time.Sleep` calls removed in favor of `testutil.Eventually`, fake-clock
  `Advance`, channel signaling, or `consistently`-style negative
  assertions (#136 through #155).

## v3.16.x — transport/redis NOGROUP self-healing

### Added
- `transport/redis.WithAutoRecreateGroup(RecreateMode)` and
  `WithRecreateHandler` (#119). On `NOGROUP` errors, the consume loop
  recreates the consumer group with `XGroupCreateMkStream` at the
  subscription's original start position. Opt-in per delivery mode —
  default behavior is unchanged.
- `transport/redis.RecreateMode` bitmask (`RecreateBroadcast`,
  `RecreateWorkerPool`, `RecreateAll`) with a `String()` method for
  metric/log labels.

### Fixed
- `transport/redis`: broadcast subscription teardown now drains the
  consume goroutine before `XGroupDestroy`, eliminating a stray
  "read error, retrying with backoff" log line during normal shutdown
  (#118).
- `transport/redis`: a Broadcast subscriber no longer replays the
  retained Redis Stream on restart (#116).
- `transport/redis`: the base consumer group is created on first
  Subscribe rather than at RegisterEvent, so callers that never Subscribe
  on a registered event no longer trigger an upstream group write (#120).

## v3.15.x and earlier — MongoDB store extraction

The MongoDB-backed implementations for outbox, monitor, distributed state
manager, schema, idempotency, and checkpoint were moved out of this
module into [event-mongodb](https://github.com/rbaliyan/event-mongodb).
Update affected imports from `github.com/rbaliyan/event/v3/<pkg>` to
`github.com/rbaliyan/event-mongodb/<pkg>`. Constructors and exported
methods on the new module mirror what was in this one.

## v3.0.0 — Generics

- `Event[T]` replaces the untyped `Event`.
- `Handler[T]` exposes a typed `data T` argument.
- Subscribe options become `SubscribeOption[T]`.

See [COMPATIBILITY.md](COMPATIBILITY.md) for the v2→v3 migration table.

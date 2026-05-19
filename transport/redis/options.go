package redis

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// Option configures the Redis transport
type Option func(*Transport)

// RecreateMode is a bitmask selecting which delivery modes auto-recover from
// NOGROUP errors. See WithAutoRecreateGroup for details.
type RecreateMode uint8

const (
	// RecreateBroadcast enables auto-recreate for broadcast subscriptions
	// (per-Subscribe throwaway consumer groups). Low blast radius — broadcast
	// groups have no continuity across pod restarts, so losing the Pending
	// Entries List is moot.
	RecreateBroadcast RecreateMode = 1 << iota

	// RecreateWorkerPool enables auto-recreate for worker-pool subscriptions
	// (shared consumer groups). High blast radius — recreating drops the
	// entire group's Pending Entries List, so every worker in the cluster
	// loses its in-flight messages. Opt in deliberately and only when
	// at-least-once gaps across Redis state loss are acceptable.
	RecreateWorkerPool

	// RecreateAll is a shorthand combining both modes.
	RecreateAll = RecreateBroadcast | RecreateWorkerPool
)

// String returns a human-readable representation of the mode bitmask. Used
// for structured logging. The zero value returns "none"; unknown bits are
// rendered as a hex literal (e.g. "RecreateMode(0x80)") so future flags
// surface as raw bits if a case clause is not added.
func (m RecreateMode) String() string {
	switch m {
	case 0:
		return "none"
	case RecreateBroadcast:
		return "broadcast"
	case RecreateWorkerPool:
		return "worker_pool"
	case RecreateAll:
		return "all"
	default:
		return fmt.Sprintf("RecreateMode(0x%x)", uint8(m))
	}
}

// WithAutoRecreateGroup enables automatic recovery from NOGROUP errors on
// XREADGROUP. When a consumer group (or its stream) is missing — Redis
// restart without persistence, FLUSHDB, manual DEL, failover to an empty
// replica, eviction under maxmemory — the consume loop normally spins with
// exponential backoff and never recovers without a process restart.
//
// With this option set, the group is recreated via XGroupCreateMkStream using
// the subscription's original start position ($ for broadcast/latest, 0 for
// worker-pool/beginning, or the originally-resolved Redis message ID
// "<ms>-<seq>" for StartFromTimestamp).
// A Warn-level "consumer group recreated after NOGROUP" log is emitted on
// each successful recreate. If a WithRecreateHandler is configured, it is
// invoked as well — wire it to a metric counter for alerting.
//
// The Pending Entries List of the destroyed group is unrecoverable —
// at-least-once is best-effort across Redis state loss. Worker-pool groups
// in particular are shared cluster-wide, so enabling RecreateWorkerPool
// means every worker loses its in-flight PEL when the group is recreated.
//
// Messages published between the moment the group was destroyed and the
// moment this consumer recreates it are not delivered to this subscription
// when the original start position was "$" (broadcast / StartFromLatest).
// For worker-pool subscriptions started from "0", any messages still in the
// stream are replayed from the beginning of the retained window.
//
// The two modes are independent so callers can enable broadcast recovery
// (low blast radius) without opting into worker-pool recovery (high blast
// radius). Disabled by default (zero RecreateMode = no recreation).
func WithAutoRecreateGroup(mode RecreateMode) Option {
	return func(t *Transport) {
		t.autoRecreate = mode
	}
}

// WithRecreateHandler sets a callback invoked after each successful auto
// recreate triggered by WithAutoRecreateGroup. Use this to plumb a metric
// counter or alert. The callback runs on the consume goroutine and should
// be non-blocking.
func WithRecreateHandler(fn func(stream, group string, mode RecreateMode)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onRecreate = fn
		}
	}
}

// WithCodec sets the codec for message serialization
func WithCodec(c codec.Codec) Option {
	return func(t *Transport) {
		if c != nil {
			t.codec = c
		}
	}
}

// WithConsumerGroup sets the base consumer group ID
func WithConsumerGroup(groupID string) Option {
	return func(t *Transport) {
		if groupID != "" {
			t.groupID = groupID
		}
	}
}

// WithMaxLen sets the max length for streams (MAXLEN)
func WithMaxLen(n int64) Option {
	return func(t *Transport) {
		if n > 0 {
			t.maxLen = n
		}
	}
}

// WithMaxAge sets the max age for messages in streams (MINID-based trimming).
// Messages older than this duration will be trimmed on each publish.
//
// When used alone (no WithMaxLen), MINID trimming is applied directly on XADD.
// When used together with WithMaxLen, count-based trimming is applied on XADD
// and age-based trimming is applied as a separate XTRIM MINID call after the
// XADD — both constraints are enforced, non-atomically. XTRIM failure is
// non-fatal: the count cap from WithMaxLen already prevents unbounded growth.
//
// Set to 0 (default) for no age-based trimming.
func WithMaxAge(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.maxAge = d
		}
	}
}

// WithBlockTime sets the block time for XREADGROUP
func WithBlockTime(d time.Duration) Option {
	return func(t *Transport) {
		if d > 0 {
			t.blockTime = d
		}
	}
}

// WithLogger sets the logger
func WithLogger(l *slog.Logger) Option {
	return func(t *Transport) {
		if l != nil {
			t.logger = l
		}
	}
}

// WithErrorHandler sets the error handler callback
func WithErrorHandler(fn func(error)) Option {
	return func(t *Transport) {
		if fn != nil {
			t.onError = fn
		}
	}
}

// WithSendTimeout sets the timeout for sending messages to subscriber channels.
// This provides backpressure control when handlers are slow.
//
// Behavior on timeout:
//   - Message is NOT dropped - it stays in the Redis Pending Entries List (PEL)
//   - The consumer continues processing other messages
//   - The timed-out message will be redelivered on consumer restart or via XCLAIM
//
// Set to 0 (default) to block indefinitely until the handler is ready.
// Use a non-zero timeout to prevent slow handlers from blocking the consumer.
func WithSendTimeout(d time.Duration) Option {
	return func(t *Transport) {
		t.sendTimeout = d
	}
}

// WithClaimInterval enables automatic claiming of orphaned messages.
// When a consumer dies without acknowledging messages, those messages remain
// in the Pending Entries List (PEL) forever. This option starts a background
// goroutine that periodically claims and reprocesses orphaned messages.
//
// Parameters:
//   - interval: How often to check for orphaned messages (e.g., 30*time.Second)
//   - minIdle: Minimum time a message must be idle before claiming (e.g., 60*time.Second)
//
// Set interval to 0 to disable (default).
func WithClaimInterval(interval, minIdle time.Duration) Option {
	return func(t *Transport) {
		t.claimInterval = interval
		t.claimMinIdle = minIdle
	}
}

// WithClaimBatchSize sets the maximum number of orphaned messages to claim per cycle.
// Default is 100. Increase for high-throughput systems where many messages may be
// orphaned after a consumer crash.
func WithClaimBatchSize(n int64) Option {
	return func(t *Transport) {
		if n > 0 {
			t.claimBatchSize = n
		}
	}
}

// WithCircuitBreaker enables a circuit breaker on Publish (XAdd) calls.
// After threshold consecutive failures, the breaker opens and Publish returns
// transport.ErrCircuitOpen immediately. After cooldown elapses, one probe call
// is allowed through: success closes the breaker, failure re-opens it.
//
// Disabled by default (zero overhead when not configured).
func WithCircuitBreaker(threshold int, cooldown time.Duration) Option {
	return func(t *Transport) {
		if threshold > 0 {
			t.cb = transport.NewCircuitBreaker(threshold, cooldown)
		}
	}
}

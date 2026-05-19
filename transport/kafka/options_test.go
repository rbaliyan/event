package kafka

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
)

// Options are pure functions over *Transport; no broker is required to
// exercise them. Constructing a bare *Transport and calling the option
// directly hits every branch (set, nil-guard, zero-guard) without standing
// up a sarama client.

func TestOptions_Setters(t *testing.T) {
	t.Parallel()
	jc := codec.Default()
	logger := slog.Default()

	var captured error
	tr := &Transport{
		codec:       codec.Default(),
		groupID:     DefaultBusName,
		partitions:  DefaultPartitions,
		replication: DefaultReplication,
		logger:      slog.Default(),
		onError:     func(error) {},
	}

	WithCodec(jc)(tr)
	WithConsumerGroup("svc-orders")(tr)
	WithPartitions(8)(tr)
	WithReplication(3)(tr)
	WithRetention(48 * time.Hour)(tr)
	WithLogger(logger)(tr)
	WithErrorHandler(func(err error) { captured = err })(tr)
	WithSendTimeout(250 * time.Millisecond)(tr)

	if tr.codec != jc {
		t.Error("WithCodec: codec not set")
	}
	if tr.groupID != "svc-orders" {
		t.Errorf("WithConsumerGroup: got %q, want %q", tr.groupID, "svc-orders")
	}
	if tr.partitions != 8 {
		t.Errorf("WithPartitions: got %d, want 8", tr.partitions)
	}
	if tr.replication != 3 {
		t.Errorf("WithReplication: got %d, want 3", tr.replication)
	}
	if tr.retention != 48*time.Hour {
		t.Errorf("WithRetention: got %v, want 48h", tr.retention)
	}
	if tr.logger != logger {
		t.Error("WithLogger: logger not set")
	}
	sentinel := errors.New("err-test") //nolint:err113 // sentinel for test
	tr.onError(sentinel)
	if !errors.Is(captured, sentinel) {
		t.Errorf("WithErrorHandler: captured=%v, want %v", captured, sentinel)
	}
	if tr.sendTimeout != 250*time.Millisecond {
		t.Errorf("WithSendTimeout: got %v, want 250ms", tr.sendTimeout)
	}
}

func TestOptions_GuardsPreserveDefaults(t *testing.T) {
	t.Parallel()
	// Every option that accepts a "rich" value (codec, logger, error
	// handler, group, count, duration) has a guard against the empty form
	// to avoid silently clobbering a sensible default. Verify each guard.
	defCodec := codec.Default()
	defLogger := slog.Default()
	defOnError := func(error) {}

	tr := &Transport{
		codec:       defCodec,
		groupID:     DefaultBusName,
		partitions:  DefaultPartitions,
		replication: DefaultReplication,
		retention:   24 * time.Hour,
		logger:      defLogger,
		onError:     defOnError,
	}

	WithCodec(nil)(tr)
	if tr.codec != defCodec {
		t.Error("WithCodec(nil) clobbered default codec")
	}

	WithConsumerGroup("")(tr)
	if tr.groupID != DefaultBusName {
		t.Errorf("WithConsumerGroup(\"\") clobbered default group: got %q", tr.groupID)
	}

	WithPartitions(0)(tr)
	WithPartitions(-3)(tr)
	if tr.partitions != DefaultPartitions {
		t.Errorf("WithPartitions zero/negative clobbered defaults: got %d", tr.partitions)
	}

	WithReplication(0)(tr)
	WithReplication(-1)(tr)
	if tr.replication != DefaultReplication {
		t.Errorf("WithReplication zero/negative clobbered defaults: got %d", tr.replication)
	}

	WithRetention(0)(tr)
	WithRetention(-time.Hour)(tr)
	if tr.retention != 24*time.Hour {
		t.Errorf("WithRetention zero/negative clobbered defaults: got %v", tr.retention)
	}

	WithLogger(nil)(tr)
	if tr.logger != defLogger {
		t.Error("WithLogger(nil) clobbered default logger")
	}

	WithErrorHandler(nil)(tr)
	// Existing onError must still be safe to invoke.
	tr.onError(errors.New("post-nil")) //nolint:err113 // sentinel for test
}

func TestOptions_SendTimeoutZeroIsBlocking(t *testing.T) {
	t.Parallel()
	// WithSendTimeout is documented to accept 0 ("block indefinitely").
	// Pin the contract — a future change to add a zero-guard would silently
	// alter delivery semantics and should be a deliberate, visible decision.
	tr := &Transport{sendTimeout: 100 * time.Millisecond}
	WithSendTimeout(0)(tr)
	if tr.sendTimeout != 0 {
		t.Errorf("WithSendTimeout(0): got %v, want 0 (blocking)", tr.sendTimeout)
	}
}

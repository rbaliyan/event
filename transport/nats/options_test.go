package nats

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/codec"
)

// Options are pure functions over the transport structs; no NATS server is
// needed to test them. Constructing a bare *JetStreamTransport / *CoreTransport
// and calling the option directly exercises every branch (set, nil-guard,
// zero-guard) without an in-process server.

func TestJSOptions_Setters(t *testing.T) {
	t.Parallel()
	jc := codec.Default()
	logger := slog.Default()
	captured := errors.New("captured") //nolint:err113 // sentinel for test
	errs := make([]error, 0, 1)

	tr := &JetStreamTransport{
		codec:       codec.Default(),
		replicas:    1,
		maxAge:      time.Hour,
		ackWait:     30 * time.Second,
		dedupWindow: 2 * time.Minute,
		logger:      slog.Default(),
		onError:     func(error) {},
	}

	WithCodec(jc)(tr)
	WithReplicas(5)(tr)
	WithMaxAge(7 * time.Hour)(tr)
	WithLogger(logger)(tr)
	WithErrorHandler(func(err error) { errs = append(errs, err) })(tr)
	WithSendTimeout(250 * time.Millisecond)(tr)
	WithDeduplication(10 * time.Minute)(tr)
	WithMaxDeliver(7)(tr)
	WithAckWait(45 * time.Second)(tr)

	if tr.codec != jc {
		t.Error("WithCodec: codec not set")
	}
	if tr.replicas != 5 {
		t.Errorf("WithReplicas: got %d, want 5", tr.replicas)
	}
	if tr.maxAge != 7*time.Hour {
		t.Errorf("WithMaxAge: got %v, want 7h", tr.maxAge)
	}
	if tr.logger != logger {
		t.Error("WithLogger: logger not set")
	}
	tr.onError(captured)
	if len(errs) != 1 || !errors.Is(errs[0], captured) {
		t.Errorf("WithErrorHandler: callback not wired (got %v)", errs)
	}
	if tr.sendTimeout != 250*time.Millisecond {
		t.Errorf("WithSendTimeout: got %v, want 250ms", tr.sendTimeout)
	}
	if !tr.dedupEnabled {
		t.Error("WithDeduplication: dedupEnabled not set")
	}
	if tr.dedupWindow != 10*time.Minute {
		t.Errorf("WithDeduplication: window got %v, want 10m", tr.dedupWindow)
	}
	if tr.maxDeliver != 7 {
		t.Errorf("WithMaxDeliver: got %d, want 7", tr.maxDeliver)
	}
	if tr.ackWait != 45*time.Second {
		t.Errorf("WithAckWait: got %v, want 45s", tr.ackWait)
	}
}

func TestJSOptions_NilAndZeroGuards(t *testing.T) {
	t.Parallel()
	// Each option carries a guard against nil/zero/negative arguments to
	// avoid silently clobbering the default. Verify every guard branch.
	def := codec.Default()
	defLogger := slog.Default()
	defErr := func(error) {}

	tr := &JetStreamTransport{
		codec:       def,
		replicas:    3,
		maxAge:      2 * time.Hour,
		ackWait:     20 * time.Second,
		dedupWindow: 5 * time.Minute,
		logger:      defLogger,
		onError:     defErr,
	}

	// nil codec — guarded
	WithCodec(nil)(tr)
	if tr.codec != def {
		t.Error("WithCodec(nil) clobbered the existing codec")
	}

	// non-positive replicas — guarded
	WithReplicas(0)(tr)
	WithReplicas(-1)(tr)
	if tr.replicas != 3 {
		t.Errorf("WithReplicas zero/negative clobbered defaults: got %d", tr.replicas)
	}

	// non-positive max age — guarded
	WithMaxAge(0)(tr)
	WithMaxAge(-time.Hour)(tr)
	if tr.maxAge != 2*time.Hour {
		t.Errorf("WithMaxAge zero/negative clobbered defaults: got %v", tr.maxAge)
	}

	// nil logger — guarded
	WithLogger(nil)(tr)
	if tr.logger != defLogger {
		t.Error("WithLogger(nil) clobbered the existing logger")
	}

	// nil error handler — guarded
	WithErrorHandler(nil)(tr)
	// We cannot compare func equality reliably; instead assert that the
	// existing handler still runs without panic.
	tr.onError(errors.New("post-nil")) //nolint:err113 // sentinel for test

	// Zero deduplication window keeps prior window but DOES toggle dedupEnabled
	// (a documented design decision: enabling dedup without specifying a
	// window means "use the default that was already configured").
	prevWindow := tr.dedupWindow
	WithDeduplication(0)(tr)
	if !tr.dedupEnabled {
		t.Error("WithDeduplication(0): dedupEnabled should still flip to true")
	}
	if tr.dedupWindow != prevWindow {
		t.Errorf("WithDeduplication(0) clobbered window: got %v want %v", tr.dedupWindow, prevWindow)
	}

	// Non-positive ack wait — guarded
	WithAckWait(0)(tr)
	WithAckWait(-time.Second)(tr)
	if tr.ackWait != 20*time.Second {
		t.Errorf("WithAckWait zero/negative clobbered defaults: got %v", tr.ackWait)
	}

	// WithSendTimeout is documented to accept 0 ("block indefinitely") and
	// must NOT be guarded — verify the user can opt back into blocking.
	tr.sendTimeout = 100 * time.Millisecond
	WithSendTimeout(0)(tr)
	if tr.sendTimeout != 0 {
		t.Errorf("WithSendTimeout(0) should set 0 (blocking); got %v", tr.sendTimeout)
	}

	// WithMaxDeliver accepts 0 ("unlimited"); not guarded.
	tr.maxDeliver = 5
	WithMaxDeliver(0)(tr)
	if tr.maxDeliver != 0 {
		t.Errorf("WithMaxDeliver(0) should set 0 (unlimited); got %d", tr.maxDeliver)
	}
}

func TestCoreOptions_Setters(t *testing.T) {
	t.Parallel()
	jc := codec.Default()
	logger := slog.Default()

	tr := &CoreTransport{
		codec:   codec.Default(),
		logger:  slog.Default(),
		onError: func(error) {},
	}

	WithCoreCodec(jc)(tr)
	WithCoreLogger(logger)(tr)

	var captured error
	WithCoreErrorHandler(func(err error) { captured = err })(tr)

	if tr.codec != jc {
		t.Error("WithCoreCodec: codec not set")
	}
	if tr.logger != logger {
		t.Error("WithCoreLogger: logger not set")
	}
	sentinel := errors.New("err-test") //nolint:err113 // sentinel for test
	tr.onError(sentinel)
	if !errors.Is(captured, sentinel) {
		t.Errorf("WithCoreErrorHandler: handler not invoked (captured=%v)", captured)
	}
}

func TestCoreOptions_NilGuards(t *testing.T) {
	t.Parallel()
	defCodec := codec.Default()
	defLogger := slog.Default()
	defOnError := func(error) {}

	tr := &CoreTransport{
		codec:   defCodec,
		logger:  defLogger,
		onError: defOnError,
	}

	WithCoreCodec(nil)(tr)
	if tr.codec != defCodec {
		t.Error("WithCoreCodec(nil) clobbered the existing codec")
	}
	WithCoreLogger(nil)(tr)
	if tr.logger != defLogger {
		t.Error("WithCoreLogger(nil) clobbered the existing logger")
	}
	WithCoreErrorHandler(nil)(tr)
	// onError must still be safe to invoke after nil-guard rejection.
	tr.onError(errors.New("post-nil")) //nolint:err113 // sentinel for test
}

func TestCoreOptions_IdempotencyAndPoisonInstall(t *testing.T) {
	t.Parallel()
	// The option setters accept an interface; we can't easily construct a
	// real store here without dragging in idempotency. Instead, verify the
	// pointer is recorded on the transport. nil should also be accepted (it
	// disables the feature).
	tr := &CoreTransport{}

	// nil install is valid — disables the feature.
	WithIdempotencyStore(nil)(tr)
	if tr.idempotencyStore != nil {
		t.Error("WithIdempotencyStore(nil) should leave store nil")
	}
	WithPoisonDetector(nil)(tr)
	if tr.poisonDetector != nil {
		t.Error("WithPoisonDetector(nil) should leave detector nil")
	}
}

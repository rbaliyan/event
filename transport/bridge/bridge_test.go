package bridge_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/bridge"
)

// -----------------------------------------------------------------------------
// Fakes
// -----------------------------------------------------------------------------

// fakeTransport is a minimal test double that implements transport.Transport.
// It records published messages and exposes a channel for injecting
// messages into subscribers (simulating a source).
type fakeTransport struct {
	mu            sync.Mutex
	events        map[string]*fakeEvent
	publishErr    error
	publishErrMap map[string]error // per-event override
	published     []publishRecord  // every successful Publish call
	publishHook   func(name string, msg transport.Message)
	unregCount    int
	closed        bool
}

type fakeEvent struct {
	name        string
	subscribers []*fakeSub
}

type fakeSub struct {
	id     string
	ch     chan transport.Message
	closed atomic.Bool
	done   chan struct{}
}

type publishRecord struct {
	Event string
	Msg   transport.Message
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		events:        make(map[string]*fakeEvent),
		publishErrMap: make(map[string]error),
	}
}

func (f *fakeTransport) RegisterEvent(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.events[name]; ok {
		return transport.ErrEventAlreadyExists
	}
	f.events[name] = &fakeEvent{name: name}
	return nil
}

func (f *fakeTransport) UnregisterEvent(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ev, ok := f.events[name]
	if !ok {
		return transport.ErrEventNotRegistered
	}
	for _, s := range ev.subscribers {
		s.closeOnce()
	}
	delete(f.events, name)
	f.unregCount++
	return nil
}

func (f *fakeTransport) Publish(_ context.Context, name string, msg transport.Message) error {
	f.mu.Lock()
	if err, ok := f.publishErrMap[name]; ok && err != nil {
		f.mu.Unlock()
		return err
	}
	if f.publishErr != nil {
		err := f.publishErr
		f.mu.Unlock()
		return err
	}
	f.published = append(f.published, publishRecord{Event: name, Msg: msg})
	hook := f.publishHook
	f.mu.Unlock()

	if hook != nil {
		hook(name, msg)
	}

	// Also deliver to any subscribers of this event so the fake can
	// act as a sink that clients subscribe to.
	f.mu.Lock()
	ev := f.events[name]
	var subs []*fakeSub
	if ev != nil {
		subs = append(subs, ev.subscribers...)
	}
	f.mu.Unlock()

	for _, s := range subs {
		if s.closed.Load() {
			continue
		}
		select {
		case s.ch <- msg:
		default:
		}
	}
	return nil
}

func (f *fakeTransport) Subscribe(_ context.Context, name string, _ ...transport.SubscribeOption) (transport.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ev, ok := f.events[name]
	if !ok {
		return nil, transport.ErrEventNotRegistered
	}
	s := &fakeSub{
		id:   transport.NewID(),
		ch:   make(chan transport.Message, 64),
		done: make(chan struct{}),
	}
	ev.subscribers = append(ev.subscribers, s)
	return s, nil
}

func (f *fakeTransport) Close(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	for _, ev := range f.events {
		for _, s := range ev.subscribers {
			s.closeOnce()
		}
	}
	return nil
}

// inject pushes a message to every subscriber of name. Simulates a
// receive-only source emitting an event.
func (f *fakeTransport) inject(name string, msg transport.Message) {
	f.mu.Lock()
	ev := f.events[name]
	var subs []*fakeSub
	if ev != nil {
		subs = append(subs, ev.subscribers...)
	}
	f.mu.Unlock()
	for _, s := range subs {
		if s.closed.Load() {
			continue
		}
		s.ch <- msg
	}
}

func (f *fakeTransport) publishedEvents() []publishRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishRecord, len(f.published))
	copy(out, f.published)
	return out
}

func (f *fakeTransport) setPublishErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishErr = err
}

func (s *fakeSub) ID() string                              { return s.id }
func (s *fakeSub) Messages() <-chan transport.Message      { return s.ch }
func (s *fakeSub) Close(_ context.Context) error           { s.closeOnce(); return nil }
func (s *fakeSub) closeOnce() {
	if s.closed.CompareAndSwap(false, true) {
		close(s.done)
		close(s.ch)
	}
}

// testMessage is a minimal transport.Message for tests.
type testMessage struct {
	id      string
	payload []byte
	meta    map[string]string
	ackFn   func(error) error
}

func newMsg(id string) *testMessage {
	return &testMessage{id: id, meta: map[string]string{}}
}

func (m *testMessage) ID() string                  { return m.id }
func (m *testMessage) Source() string              { return "test" }
func (m *testMessage) Payload() []byte             { return m.payload }
func (m *testMessage) Metadata() map[string]string { return m.meta }
func (m *testMessage) Timestamp() time.Time        { return time.Unix(0, 0) }
func (m *testMessage) RetryCount() int             { return 0 }
func (m *testMessage) Context() context.Context    { return context.Background() }
func (m *testMessage) Ack(err error) error {
	if m.ackFn != nil {
		return m.ackFn(err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------

func TestNew_errors(t *testing.T) {
	sink := newFakeTransport()
	src := newFakeTransport()

	if _, err := bridge.New(nil, sink); !errors.Is(err, bridge.ErrSourceRequired) {
		t.Errorf("nil source: got %v, want ErrSourceRequired", err)
	}
	if _, err := bridge.New(src, nil); !errors.Is(err, bridge.ErrSinkRequired) {
		t.Errorf("nil sink: got %v, want ErrSinkRequired", err)
	}
}

// -----------------------------------------------------------------------------
// Basic pump: source → sink
// -----------------------------------------------------------------------------

func TestPump_forwardsMessages(t *testing.T) {
	src := newFakeTransport()
	sink := newFakeTransport()
	ctx := context.Background()

	// Pre-register on source so pump can subscribe.
	if err := src.RegisterEvent(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	b, err := bridge.New(src, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(ctx) })

	if err := b.RegisterEvent(ctx, "orders"); err != nil {
		t.Fatal(err)
	}

	src.inject("orders", newMsg("m1"))
	src.inject("orders", newMsg("m2"))

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 2 }, time.Second)

	got := sink.publishedEvents()
	if got[0].Event != "orders" || got[0].Msg.ID() != "m1" {
		t.Errorf("msg[0] = %+v, want event=orders id=m1", got[0])
	}
	if got[1].Msg.ID() != "m2" {
		t.Errorf("msg[1].ID = %s, want m2", got[1].Msg.ID())
	}
}

// -----------------------------------------------------------------------------
// Publish / Subscribe delegate to sink
// -----------------------------------------------------------------------------

func TestPublishDelegatesToSink(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })

	if err := b.RegisterEvent(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(ctx, "x", newMsg("direct")); err != nil {
		t.Fatal(err)
	}

	// The direct publish should appear on the sink (once), and NOT be
	// fed back through the source pump.
	got := sink.publishedEvents()
	if len(got) != 1 || got[0].Msg.ID() != "direct" {
		t.Errorf("sink got %+v, want 1 direct publish", got)
	}
}

func TestSubscribeDelegatesToSink(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })

	_ = b.RegisterEvent(ctx, "x")

	sub, err := b.Subscribe(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close(ctx) })

	src.inject("x", newMsg("through"))

	select {
	case msg := <-sub.Messages():
		if msg.ID() != "through" {
			t.Errorf("got %s, want through", msg.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("no message delivered to subscriber")
	}
}

// -----------------------------------------------------------------------------
// Middleware: Dedup
// -----------------------------------------------------------------------------

func TestDedupMiddleware_suppressesDuplicates(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	coord := bridge.NewMemoryCoordinator()
	var skipped int32
	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(
			bridge.Dedup(coord, bridge.DefaultDedupKey, time.Minute,
				bridge.WithDedupOnSkip(func(_ string, _ transport.Message) {
					atomic.AddInt32(&skipped, 1)
				}),
			),
		),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	// Same ID injected three times — only the first should publish.
	src.inject("x", newMsg("dup"))
	src.inject("x", newMsg("dup"))
	src.inject("x", newMsg("dup"))

	waitFor(t, func() bool {
		return len(sink.publishedEvents()) == 1 && atomic.LoadInt32(&skipped) == 2
	}, time.Second)

	if got := len(sink.publishedEvents()); got != 1 {
		t.Errorf("sink publishes = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&skipped); got != 2 {
		t.Errorf("skipped = %d, want 2", got)
	}
}

func TestDedupMiddleware_differentIDsPassThrough(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	coord := bridge.NewMemoryCoordinator()
	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Dedup(coord, bridge.DefaultDedupKey, time.Minute)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	for i := range 5 {
		src.inject("x", newMsg(fmt.Sprintf("m%d", i)))
	}

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 5 }, time.Second)
}

func TestDedup_failClosedOnCoordinatorError(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	sentinel := errors.New("coord down")
	coord := &erroringCoord{err: sentinel}

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Dedup(coord, bridge.DefaultDedupKey, time.Minute)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	var acked sync.WaitGroup
	acked.Add(1)
	msg := newMsg("m1")
	var ackErr error
	msg.ackFn = func(err error) error { ackErr = err; acked.Done(); return nil }

	src.inject("x", msg)
	acked.Wait()

	if len(sink.publishedEvents()) != 0 {
		t.Errorf("sink should not have received message, got %d", len(sink.publishedEvents()))
	}
	if !errors.Is(ackErr, sentinel) {
		t.Errorf("ack err = %v, want coord down (nack for redelivery)", ackErr)
	}
}

func TestDedup_failOpenOnCoordinatorError(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	coord := &erroringCoord{err: errors.New("down")}

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Dedup(coord, bridge.DefaultDedupKey, time.Minute,
			bridge.WithDedupFailOpen(true),
		)),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("m1"))
	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)
}

// -----------------------------------------------------------------------------
// Middleware: DLQ
// -----------------------------------------------------------------------------

func TestDLQMiddleware_catchesSinkErrors(t *testing.T) {
	src, sink, dlq := newFakeTransport(), newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")
	_ = dlq.RegisterEvent(ctx, "x.failed")

	sentinel := errors.New("sink unavailable")
	sink.setPublishErr(sentinel)

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.DLQ(dlq, "x.failed")),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	var ackErr error
	var acked sync.WaitGroup
	acked.Add(1)
	msg := newMsg("failing")
	msg.ackFn = func(err error) error { ackErr = err; acked.Done(); return nil }

	src.inject("x", msg)
	acked.Wait()

	if got := len(dlq.publishedEvents()); got != 1 {
		t.Errorf("DLQ publishes = %d, want 1", got)
	}
	if ackErr != nil {
		t.Errorf("ack err = %v, want nil (DLQ swallowed the error)", ackErr)
	}
}

// -----------------------------------------------------------------------------
// Middleware: Filter / Transform / Observe
// -----------------------------------------------------------------------------

func TestFilterMiddleware(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Filter(func(_ string, m transport.Message) bool {
			return m.ID() != "skip"
		})),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("keep1"))
	src.inject("x", newMsg("skip"))
	src.inject("x", newMsg("keep2"))

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 2 }, time.Second)
	ids := []string{sink.publishedEvents()[0].Msg.ID(), sink.publishedEvents()[1].Msg.ID()}
	if ids[0] != "keep1" || ids[1] != "keep2" {
		t.Errorf("got %v, want [keep1 keep2]", ids)
	}
}

func TestTransformMiddleware(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Transform(func(_ string, m transport.Message) transport.Message {
			rewritten := newMsg(m.ID() + "-rewritten")
			return rewritten
		})),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("a"))
	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)

	if got := sink.publishedEvents()[0].Msg.ID(); got != "a-rewritten" {
		t.Errorf("got %s, want a-rewritten", got)
	}
}

func TestObserveMiddleware_hooksFire(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	var receive, publish, errHook int32
	b, _ := bridge.New(src, sink,
		bridge.WithMiddleware(bridge.Observe(bridge.Hooks{
			OnReceive: func(_ string, _ transport.Message) { atomic.AddInt32(&receive, 1) },
			OnPublish: func(_ string, _ transport.Message) { atomic.AddInt32(&publish, 1) },
			OnError:   func(_ string, _ transport.Message, _ error) { atomic.AddInt32(&errHook, 1) },
		})),
	)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	src.inject("x", newMsg("ok"))
	// Wait for the success path to complete before flipping the sink
	// into an error state — otherwise a fast test can observe the ok
	// message after the setPublishErr took effect.
	waitFor(t, func() bool { return atomic.LoadInt32(&publish) == 1 }, time.Second)

	sink.setPublishErr(errors.New("boom"))
	src.inject("x", newMsg("bad"))

	waitFor(t, func() bool { return atomic.LoadInt32(&errHook) == 1 }, time.Second)
	if got := atomic.LoadInt32(&receive); got != 2 {
		t.Errorf("OnReceive fired %d times, want 2", got)
	}
	if got := atomic.LoadInt32(&publish); got != 1 {
		t.Errorf("OnPublish fired %d times, want 1", got)
	}
}

// -----------------------------------------------------------------------------
// Panic recovery
// -----------------------------------------------------------------------------

func TestPump_recoversFromPanic(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	panicMW := func(next bridge.Handler) bridge.Handler {
		return func(ctx context.Context, ev string, m transport.Message) error {
			if m.ID() == "boom" {
				panic("explode")
			}
			return next(ctx, ev, m)
		}
	}

	b, _ := bridge.New(src, sink, bridge.WithMiddleware(panicMW))
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	// Panic first, then a normal message — pump must still deliver.
	src.inject("x", newMsg("boom"))
	src.inject("x", newMsg("ok"))

	waitFor(t, func() bool { return len(sink.publishedEvents()) == 1 }, time.Second)
	if got := sink.publishedEvents()[0].Msg.ID(); got != "ok" {
		t.Errorf("recovered pump delivered %s, want ok", got)
	}
}

// -----------------------------------------------------------------------------
// Close / Unregister lifecycle
// -----------------------------------------------------------------------------

func TestClose_stopsPumps(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	_ = b.RegisterEvent(ctx, "x")

	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Second close is a no-op and must not panic.
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if err := b.RegisterEvent(ctx, "y"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("RegisterEvent after close = %v, want ErrTransportClosed", err)
	}
}

func TestUnregisterEvent(t *testing.T) {
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	if err := b.UnregisterEvent(ctx, "x"); err != nil {
		t.Fatal(err)
	}
	if err := b.UnregisterEvent(ctx, "x"); !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("double unregister = %v, want ErrEventNotRegistered", err)
	}
}

// -----------------------------------------------------------------------------
// Coordinator implementations
// -----------------------------------------------------------------------------

func TestMemoryCoordinator_claimsAndExpires(t *testing.T) {
	ctx := context.Background()
	c := bridge.NewMemoryCoordinator()

	ok, err := c.Claim(ctx, "k", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("first claim: ok=%v err=%v", ok, err)
	}
	ok, err = c.Claim(ctx, "k", 50*time.Millisecond)
	if err != nil || ok {
		t.Errorf("second claim: ok=%v err=%v, want false", ok, err)
	}

	time.Sleep(70 * time.Millisecond)

	ok, err = c.Claim(ctx, "k", 50*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("claim after expiry: ok=%v err=%v", ok, err)
	}
}

func TestNoopCoordinator_alwaysClaims(t *testing.T) {
	ctx := context.Background()
	c := bridge.NoopCoordinator{}
	for range 3 {
		ok, err := c.Claim(ctx, "same", time.Minute)
		if err != nil || !ok {
			t.Errorf("noop claim: ok=%v err=%v", ok, err)
		}
	}
}

// -----------------------------------------------------------------------------
// Health
// -----------------------------------------------------------------------------

// healthyFakeTransport wraps fakeTransport and implements HealthChecker.
type healthyFakeTransport struct {
	*fakeTransport
	healthStatus transport.HealthStatus
	healthMsg    string
}

func newHealthFake(status transport.HealthStatus, msg string) *healthyFakeTransport {
	return &healthyFakeTransport{
		fakeTransport: newFakeTransport(),
		healthStatus:  status,
		healthMsg:     msg,
	}
}

func (h *healthyFakeTransport) Health(_ context.Context) *transport.HealthCheckResult {
	return &transport.HealthCheckResult{
		Status:  h.healthStatus,
		Message: h.healthMsg,
	}
}

func TestHealth_BothHealthy(t *testing.T) {
	src := newHealthFake(transport.HealthStatusHealthy, "ok")
	sink := newHealthFake(transport.HealthStatusHealthy, "ok")
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })
	_ = b.RegisterEvent(ctx, "x")

	h := b.Health(ctx)
	if h.Status != transport.HealthStatusHealthy {
		t.Errorf("status = %v, want Healthy", h.Status)
	}
	if h.Components["source"] == nil || h.Components["sink"] == nil {
		t.Error("expected source and sink components in health result")
	}
}

func TestHealth_SourceUnhealthy(t *testing.T) {
	src := newHealthFake(transport.HealthStatusUnhealthy, "source down")
	sink := newHealthFake(transport.HealthStatusHealthy, "ok")
	ctx := context.Background()
	_ = src.RegisterEvent(ctx, "x")

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })

	h := b.Health(ctx)
	if h.Status != transport.HealthStatusUnhealthy {
		t.Errorf("status = %v, want Unhealthy", h.Status)
	}
}

func TestHealth_SinkDegraded(t *testing.T) {
	src := newHealthFake(transport.HealthStatusHealthy, "ok")
	sink := newHealthFake(transport.HealthStatusDegraded, "slow")
	ctx := context.Background()

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })

	h := b.Health(ctx)
	if h.Status != transport.HealthStatusDegraded {
		t.Errorf("status = %v, want Degraded", h.Status)
	}
}

func TestHealth_AfterClose(t *testing.T) {
	src := newHealthFake(transport.HealthStatusHealthy, "ok")
	sink := newHealthFake(transport.HealthStatusHealthy, "ok")
	ctx := context.Background()

	b, _ := bridge.New(src, sink)
	_ = b.Close(ctx)

	h := b.Health(ctx)
	if h.Status != transport.HealthStatusUnhealthy {
		t.Errorf("status = %v, want Unhealthy (closed)", h.Status)
	}
}

func TestHealth_NonHealthCheckerTransports(t *testing.T) {
	// Plain fakeTransport does NOT implement HealthChecker.
	src, sink := newFakeTransport(), newFakeTransport()
	ctx := context.Background()

	b, _ := bridge.New(src, sink)
	t.Cleanup(func() { _ = b.Close(ctx) })

	h := b.Health(ctx)
	if h.Status != transport.HealthStatusHealthy {
		t.Errorf("status = %v, want Healthy (non-checker assumed healthy)", h.Status)
	}
	if len(h.Components) != 0 {
		t.Errorf("components = %v, want none for non-checker transports", h.Components)
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

type erroringCoord struct{ err error }

func (e *erroringCoord) Claim(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return false, e.err
}

// waitFor polls cond until it returns true or timeout expires.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

package bridge

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// Errors returned by [New].
var (
	ErrSourceRequired = errors.New("bridge: source transport is required")
	ErrSinkRequired   = errors.New("bridge: sink transport is required")
)

// defaultPumpBuffer is the source-subscription buffer size when the
// caller does not override it with [WithPumpBuffer].
const defaultPumpBuffer = 256

// Transport forwards messages from a [Source] into a [Sink] through a
// composable middleware pipeline.
//
// From the bus's perspective Transport behaves like the sink:
// [Publish], [Subscribe], [RegisterEvent] and [UnregisterEvent] all
// delegate to the sink. The source participates only inside the pump
// that runs per registered event.
//
// The base Transport performs no deduplication, no dead-letter
// handling, and no distributed coordination. All such behaviours are
// supplied by middleware passed to [WithMiddleware] so each caller
// enables only what their source/sink pair requires.
type Transport struct {
	status int32 // 1 = open, 0 = closed

	source Source
	sink   Sink

	middleware        []Middleware
	pumpBuffer        int
	pumpSubscribeOpts []transport.SubscribeOption

	logger  *slog.Logger
	onError func(error)

	mu    sync.Mutex
	pumps map[string]*pump
}

// New constructs a bridge. Both source and sink MUST be non-nil.
//
// source supplies messages (typically a receive-only transport such as
// a MongoDB change stream). Its Publish is never called — the [Source]
// interface does not even require it.
//
// sink receives forwarded messages and exposes the subscription
// surface consumed by the bus. It MUST support Publish.
//
// Any [transport.Transport] satisfies both [Source] and [Sink]
// structurally, so existing transports are valid arguments without
// wrapping.
func New(source Source, sink Sink, opts ...Option) (*Transport, error) {
	if source == nil {
		return nil, ErrSourceRequired
	}
	if sink == nil {
		return nil, ErrSinkRequired
	}

	t := &Transport{
		status:     1,
		source:     source,
		sink:       sink,
		pumpBuffer: defaultPumpBuffer,
		logger:     transport.Logger("transport>bridge"),
		onError:    func(error) {},
		pumps:      make(map[string]*pump),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// RegisterEvent registers the event on the sink, tolerates the same
// on the source (sources with global subscriptions often return
// ErrEventAlreadyExists), then starts a pump that forwards source
// messages for name through the middleware pipeline.
//
// A successful RegisterEvent guarantees that a pump is running and
// Subscribe on the sink will see the forwarded messages.
// pumpRegistering is a sentinel stored in the pumps map while a
// RegisterEvent is in progress. This prevents a concurrent call for
// the same name from starting a second pump.
var pumpRegistering = &pump{}

func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Acquire a per-name registration slot under the lock. If the
	// name already has a pump (or another goroutine is registering it)
	// we return immediately. This eliminates the race where two
	// concurrent RegisterEvent("same") both pass an existence check.
	t.mu.Lock()
	if t.pumps == nil {
		t.mu.Unlock()
		return transport.ErrTransportClosed
	}
	if _, exists := t.pumps[name]; exists {
		t.mu.Unlock()
		return transport.ErrEventAlreadyExists
	}
	t.pumps[name] = pumpRegistering // sentinel: slot reserved
	t.mu.Unlock()

	// On any failure path, release the slot so a retry can succeed.
	cleanup := func() {
		t.mu.Lock()
		if t.pumps != nil && t.pumps[name] == pumpRegistering {
			delete(t.pumps, name)
		}
		t.mu.Unlock()
	}

	if err := t.sink.RegisterEvent(ctx, name); err != nil {
		cleanup()
		return err
	}

	// Sources often register lazily or at the pipeline level; treat
	// ErrEventAlreadyExists as benign.
	if err := t.source.RegisterEvent(ctx, name); err != nil &&
		!errors.Is(err, transport.ErrEventAlreadyExists) {
		t.logger.Warn("source register failed; continuing",
			"event", name, "error", err)
	}

	p, err := startPump(ctx, pumpConfig{
		name:              name,
		source:            t.source,
		sink:              t.sink,
		middleware:        t.middleware,
		bufferSize:        t.pumpBuffer,
		logger:            t.logger.With("event", name),
		onError:           t.onError,
		pumpSubscribeOpts: t.pumpSubscribeOpts,
	})
	if err != nil {
		cleanup()
		return err
	}

	t.mu.Lock()
	if t.pumps == nil || !t.isOpen() {
		t.mu.Unlock()
		p.stop(ctx)
		return transport.ErrTransportClosed
	}
	t.pumps[name] = p // replace sentinel with real pump
	t.mu.Unlock()

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent stops the pump for name and unregisters the event on
// both underlying transports. Returns [transport.ErrEventNotRegistered]
// if name was not registered via this bridge.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	t.mu.Lock()
	if t.pumps == nil {
		t.mu.Unlock()
		return transport.ErrTransportClosed
	}
	p, ok := t.pumps[name]
	if !ok || p == pumpRegistering {
		t.mu.Unlock()
		return transport.ErrEventNotRegistered
	}
	delete(t.pumps, name)
	t.mu.Unlock()

	p.stop(ctx)

	if err := t.source.UnregisterEvent(ctx, name); err != nil {
		t.logger.Warn("source unregister failed",
			"event", name, "error", err)
	}
	if err := t.sink.UnregisterEvent(ctx, name); err != nil {
		t.logger.Warn("sink unregister failed",
			"event", name, "error", err)
	}

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish delegates to the sink. Direct publishes bypass the source
// and the middleware pipeline — they are normal sink publishes. Use
// this for synthetic events originating in the application rather
// than from the source.
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}
	return t.sink.Publish(ctx, name, msg)
}

// Subscribe delegates to the sink. Consumers read from the sink, which
// supplies the consumer-group / load-balancing semantics the bridge
// exists to provide.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}
	return t.sink.Subscribe(ctx, name, opts...)
}

// Close stops every pump and closes both underlying transports. The
// first error is returned; others are logged.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	t.mu.Lock()
	pumps := t.pumps
	t.pumps = nil
	t.mu.Unlock()

	for _, p := range pumps {
		p.stop(ctx)
	}

	var firstErr error
	if err := t.source.Close(ctx); err != nil {
		t.logger.Warn("source close failed", "error", err)
		firstErr = err
	}
	if err := t.sink.Close(ctx); err != nil {
		t.logger.Warn("sink close failed", "error", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	t.logger.Debug("transport closed")
	return firstErr
}

// Health reports the composed health of source and sink. The bridge is
// Healthy only when both are healthy. Transports that do not implement
// [transport.HealthChecker] are assumed healthy.
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()
	result := &transport.HealthCheckResult{
		CheckedAt:  start,
		Details:    make(map[string]any),
		Components: make(map[string]*transport.HealthCheckResult),
	}

	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	result.Status = transport.HealthStatusHealthy
	result.Message = "bridge transport is healthy"

	if checker, ok := t.source.(transport.HealthChecker); ok {
		h := checker.Health(ctx)
		result.Components["source"] = h
		switch h.Status {
		case transport.HealthStatusUnhealthy:
			result.Status = transport.HealthStatusUnhealthy
			result.Message = "source unhealthy — no events flowing"
		case transport.HealthStatusDegraded:
			if result.Status == transport.HealthStatusHealthy {
				result.Status = transport.HealthStatusDegraded
				result.Message = "source degraded"
			}
		}
	}
	if checker, ok := t.sink.(transport.HealthChecker); ok {
		h := checker.Health(ctx)
		result.Components["sink"] = h
		switch h.Status {
		case transport.HealthStatusUnhealthy:
			result.Status = transport.HealthStatusUnhealthy
			result.Message = "sink unhealthy — events cannot be forwarded"
		case transport.HealthStatusDegraded:
			if result.Status == transport.HealthStatusHealthy {
				result.Status = transport.HealthStatusDegraded
				result.Message = "sink degraded"
			}
		}
	}

	t.mu.Lock()
	pumpCount := len(t.pumps)
	t.mu.Unlock()

	result.Latency = time.Since(start)
	result.Details["type"] = "bridge"
	result.Details["pumps"] = pumpCount
	result.Details["middleware"] = len(t.middleware)
	return result
}

// Name identifies this transport in topology reports.
func (t *Transport) Name() string { return "bridge" }

// Compile-time interface checks.
var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
	_ transport.Named         = (*Transport)(nil)
)

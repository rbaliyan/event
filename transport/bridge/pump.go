package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// pump drives a single registered event: it subscribes to the source,
// runs each message through the handler pipeline, and acks/nacks the
// source based on the pipeline's return value.
//
// One pump runs per (bridge, event name). It owns a goroutine and a
// source subscription. Stop drains both.
type pump struct {
	name    string
	sub     transport.Subscription
	handler Handler
	logger  *slog.Logger
	onError func(error)

	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// pumpConfig bundles the arguments to [startPump] for readability.
type pumpConfig struct {
	name              string
	source            Source
	sink              Sink
	middleware        []Middleware
	bufferSize        int
	logger            *slog.Logger
	onError           func(error)
	pumpSubscribeOpts []transport.SubscribeOption
}

// startPump subscribes to the source for cfg.name and starts the pump
// goroutine. Returns an error if the source subscription cannot be
// established — the caller is expected to surface this as a failed
// RegisterEvent.
func startPump(parentCtx context.Context, cfg pumpConfig) (*pump, error) {
	handler := chain(publishTo(cfg.sink), cfg.middleware...)

	opts := make([]transport.SubscribeOption, 0, len(cfg.pumpSubscribeOpts)+2)
	// Prefer broadcast semantics: every replica needs to see every
	// source message so dedup (if any) can decide ownership. Callers
	// who want different semantics can override via
	// WithPumpSubscribeOptions.
	opts = append(opts, transport.WithDeliveryMode(transport.Broadcast))
	if cfg.bufferSize > 0 {
		opts = append(opts, transport.WithBufferSize(cfg.bufferSize))
	}
	opts = append(opts, cfg.pumpSubscribeOpts...)

	// The subscription inherits a cancellable context so stop() can
	// unwind cleanly even if the parent context is long-lived.
	subCtx, cancel := context.WithCancel(parentCtx)
	sub, err := cfg.source.Subscribe(subCtx, cfg.name, opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("bridge: source subscribe %q: %w", cfg.name, err)
	}

	p := &pump{
		name:    cfg.name,
		sub:     sub,
		handler: handler,
		logger:  cfg.logger,
		onError: cfg.onError,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	go p.run(subCtx)
	return p, nil
}

// run is the pump loop. Exits when the subscription closes or the
// context is cancelled.
func (p *pump) run(ctx context.Context) {
	defer close(p.done)

	msgs := p.sub.Messages()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			p.handle(ctx, msg)
		}
	}
}

// handle runs a single message through the handler pipeline and acks
// or nacks the source based on the outcome. Panics are recovered and
// treated as handler errors so one bad middleware cannot take down
// the pump goroutine.
func (p *pump) handle(ctx context.Context, msg transport.Message) {
	err := safeCall(ctx, p.name, msg, p.handler)
	if err != nil {
		p.logger.Warn("pipeline error",
			"msg_id", msg.ID(), "error", err)
		if p.onError != nil {
			p.onError(err)
		}
	}

	// Ack the source with the pipeline's outcome. Transports that do
	// not implement redelivery will still observe the nack (they can
	// log or no-op). Errors from Ack itself (already-closed sub, etc.)
	// are logged but not escalated.
	if ackErr := msg.Ack(err); ackErr != nil &&
		!errors.Is(ackErr, transport.ErrSubscriptionClosed) {
		p.logger.Warn("ack failed",
			"msg_id", msg.ID(), "error", ackErr)
	}
}

// safeCall invokes h with panic recovery. A recovered panic is
// converted to an error so the pump behaves as if the middleware had
// returned it.
func safeCall(ctx context.Context, event string, msg transport.Message, h Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
		}
	}()
	return h(ctx, event, msg)
}

// panicAsError converts a recovered panic value to an error suitable
// for the normal handler return path.
func panicAsError(r any) error {
	switch v := r.(type) {
	case error:
		return fmt.Errorf("bridge: handler panic: %w", v)
	default:
		return fmt.Errorf("bridge: handler panic: %v", v)
	}
}

// stopTimeout bounds how long stop waits for the pump goroutine to
// exit after cancellation. Long enough for in-flight middleware to
// finish, short enough to not block a graceful shutdown indefinitely.
const stopTimeout = 10 * time.Second

// stop cancels the pump's context, closes its source subscription, and
// waits for the goroutine to exit. If the goroutine does not exit
// within [stopTimeout] the wait is abandoned (the goroutine will still
// exit once its cancelled context propagates). Safe to call multiple
// times; subsequent calls are no-ops.
func (p *pump) stop(_ context.Context) {
	p.stopOnce.Do(func() {
		p.cancel()
		// Close the subscription so the source stops delivering and
		// the Messages() channel eventually drains.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), stopTimeout)
		defer closeCancel()
		_ = p.sub.Close(closeCtx)
		select {
		case <-p.done:
		case <-time.After(stopTimeout):
			p.logger.Warn("pump goroutine did not exit within timeout")
		}
	})
}

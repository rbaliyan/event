package bridge

import (
	"context"

	"github.com/rbaliyan/event/v3/transport"
)

// Handler processes a single source message. The terminal handler in
// the pipeline publishes to the sink; intermediate handlers come from
// [Middleware].
//
// Return values are interpreted by the pump:
//
//   - nil:   the message is considered handled and is acked on the source.
//   - error: the message is nacked on the source (and redelivered if
//     the source supports redelivery).
//
// A middleware that wants to "drop" a message (e.g. [Dedup] when the
// key is already claimed) should return nil WITHOUT calling next.
type Handler func(ctx context.Context, event string, msg transport.Message) error

// Middleware decorates a [Handler]. Middleware composes in declaration
// order: the first middleware passed to [WithMiddleware] is the
// outermost wrapper (runs first and sees the returned error last).
//
// Typical shape:
//
//	func MyMiddleware(cfg Cfg) Middleware {
//	    return func(next Handler) Handler {
//	        return func(ctx context.Context, event string, msg transport.Message) error {
//	            // pre-processing (may return early to drop or short-circuit)
//	            err := next(ctx, event, msg)
//	            // post-processing (e.g. record metrics, route errors)
//	            return err
//	        }
//	    }
//	}
type Middleware func(Handler) Handler

// chain composes middleware around a terminal handler. The first
// middleware in the slice becomes the outermost wrapper.
func chain(terminal Handler, mws ...Middleware) Handler {
	h := terminal
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// publishTo returns the terminal handler that publishes to the given sink.
func publishTo(sink Sink) Handler {
	return func(ctx context.Context, event string, msg transport.Message) error {
		return sink.Publish(ctx, event, msg)
	}
}

// DLQ returns middleware that diverts messages whose downstream handler
// (typically the sink publish) returns an error to a dead-letter sink.
// The source message is acked after a successful DLQ write, preventing
// the source from redelivering the failure indefinitely.
//
// dlqEvent is the event name to publish under on dlqSink. A single
// event name is typical ("bridge.failed") since DLQ messages usually
// go to a shared inspection pipeline; callers who want per-event DLQs
// can supply a function instead of a fixed name via [DLQFunc].
//
// If the DLQ publish itself fails, the original error is returned so
// the source redelivers and the next bridge replica can retry.
func DLQ(dlqSink Sink, dlqEvent string) Middleware {
	return DLQFunc(dlqSink, func(event string, _ transport.Message) string {
		if dlqEvent != "" {
			return dlqEvent
		}
		return event + ".failed"
	})
}

// DLQFunc is the generalised form of [DLQ] that lets callers compute
// the DLQ event name dynamically from the source event name and
// message. Typical use is to preserve the source event name:
//
//	bridge.DLQFunc(dlqSink, func(ev string, _ transport.Message) string {
//	    return ev + ".failed"
//	})
func DLQFunc(dlqSink Sink, name func(event string, msg transport.Message) string) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			if err := next(ctx, event, msg); err != nil {
				dlqName := name(event, msg)
				if dlqErr := dlqSink.Publish(ctx, dlqName, msg); dlqErr != nil {
					// DLQ write failed — surface the ORIGINAL error so
					// the source redelivers. We lose the DLQ attempt
					// but don't mask the real fault.
					return err
				}
				return nil
			}
			return nil
		}
	}
}

// Observe returns middleware that invokes the supplied callbacks at
// well-defined points in the pipeline. All callbacks are optional and
// MUST NOT block. Any callback being nil disables that signal.
//
// The callbacks run synchronously on the pump goroutine; do heavy work
// (metric emission, trace span creation) with non-blocking primitives.
func Observe(hooks Hooks) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			if hooks.OnReceive != nil {
				hooks.OnReceive(event, msg)
			}
			err := next(ctx, event, msg)
			switch {
			case err != nil && hooks.OnError != nil:
				hooks.OnError(event, msg, err)
			case err == nil && hooks.OnPublish != nil:
				hooks.OnPublish(event, msg)
			}
			return err
		}
	}
}

// Hooks is the callback set used by [Observe].
type Hooks struct {
	// OnReceive fires as soon as the message enters the pipeline,
	// before any further middleware or the sink publish.
	OnReceive func(event string, msg transport.Message)
	// OnPublish fires after a successful terminal handler (the sink
	// accepted the message).
	OnPublish func(event string, msg transport.Message)
	// OnError fires after any middleware or the terminal handler
	// returned an error. The error bubbles up to the pump regardless.
	OnError func(event string, msg transport.Message, err error)
}

// Filter returns middleware that drops messages for which keep returns
// false. Dropped messages are acked on the source — they are treated
// as successfully handled, just not forwarded.
//
// Use for messages that the sink does not need (e.g. operations the
// consumer doesn't care about) that the source cannot filter itself.
func Filter(keep func(event string, msg transport.Message) bool) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			if !keep(event, msg) {
				return nil
			}
			return next(ctx, event, msg)
		}
	}
}

// Transform returns middleware that replaces each message with the
// result of fn before it reaches the next handler. Returning a nil
// message drops the event (same semantics as [Filter] returning false).
//
// Use to rewrite payload encoding (BSON → JSON), redact fields, or
// enrich metadata with bridge-level context.
func Transform(fn func(event string, msg transport.Message) transport.Message) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event string, msg transport.Message) error {
			out := fn(event, msg)
			if out == nil {
				return nil
			}
			return next(ctx, event, out)
		}
	}
}

package event

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	spanKeyEventID             = "event.id"
	spanKeyEventName           = "event.name"
	spanKeyEventSource         = "event.source"
	spanKeyEventBus            = "event.bus"
	spanKeyEventSubscriptionID = "subscription.id"
)

// Sanitize strings and remove special chars
func Sanitize(s string) string {
	var result strings.Builder
	result.Grow(len(s))
	for i := 0; i < len(s); i++ {
		b := s[i]
		if ('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') {
			result.WriteByte(b)
		} else {
			result.WriteByte(byte('_'))
		}
	}
	return result.String()
}

// AsyncHandler convert event handler to async
// This wraps a typed handler to run in a goroutine with panic recovery.
// Returns nil immediately since the actual handler runs asynchronously.
// Errors from the async handler are logged but cannot trigger retries.
func AsyncHandler[T any](handler Handler[T], copyContextFns ...func(to, from context.Context) context.Context) Handler[T] {
	return func(ctx context.Context, ev Event[T], data T) error {
		// Call handler with go routine
		go func() {
			defer func() {
				_, file, l, _ := runtime.Caller(1)
				if err := recover(); err != nil {
					slog.Error("async handler panic recovered",
						"event", ev.Name(),
						"line", l,
						"file", file,
						"error", err,
						"stack", string(debug.Stack()),
					)
				}
			}()
			// Create a new copy of context
			spanCtx := trace.SpanContextFromContext(ctx)

			// Create a new context
			newCtx := NewContext(ctx)
			for _, fn := range copyContextFns {
				// Copy other data
				newCtx = fn(newCtx, ctx)
			}
			// enable tracing
			if tracer := otel.Tracer("event"); tracer != nil {
				var span trace.Span
				newCtx, span = tracer.Start(newCtx, fmt.Sprintf("%s.subscribe.async", ev.Name()),
					trace.WithAttributes(attribute.String(spanKeyEventID, ContextEventID(ctx)),
						attribute.String(spanKeyEventSource, ContextSource(ctx)),
						attribute.String(spanKeyEventName, ev.Name())),
					trace.WithSpanKind(trace.SpanKindInternal),
					trace.WithLinks(trace.Link{
						SpanContext: spanCtx,
					}))
				defer span.End()
			}
			if err := handler(newCtx, ev, data); err != nil {
				slog.Error("async handler error",
					"event", ev.Name(),
					"error", err,
				)
			}
		}()
		// Async handler always acks immediately - errors are logged but can't retry
		return nil
	}
}

// Caller get caller function name
func Caller(depth int) string {
	pc, _, _, ok := runtime.Caller(depth)
	if !ok {
		return ""
	}
	details := runtime.FuncForPC(pc)
	if details != nil {
		return details.Name()
	}
	return ""
}

// Jitter adds randomness to a duration to prevent thundering herd.
// Returns a duration between d*(1-factor) and d*(1+factor).
// Factor should be between 0 and 1 (e.g., 0.3 for +/-30% jitter).
func Jitter(d time.Duration, factor float64) time.Duration {
	if factor <= 0 || factor > 1 {
		return d
	}
	// Random value between -factor and +factor
	jitter := (rand.Float64()*2 - 1) * factor
	return time.Duration(float64(d) * (1 + jitter))
}

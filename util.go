package event

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	spanKeyEventID             = "event.id"
	spanKeyEventName           = "event.name"
	spanKeyEventSource         = "event.source"
	spanKeyEventRegistry       = "event.registry"
	spanKeyEventSubscriptionID = "subscription.id"
)

var (
	counter uint64
	// DefaultLoggerFlags default flags for logger
	DefaultLoggerFlags = log.LstdFlags | log.Lshortfile | log.Lmsgprefix
)

// NewID generate new event id
func NewID() string {
	u, err := uuid.NewRandom()
	if err == nil {
		return u.String()
	}
	return strconv.FormatUint(atomic.AddUint64(&counter, 1), 10)
}

// CloneMetadata clone metadata
func CloneMetadata(m Metadata) Metadata {
	if len(m) == 0 {
		return nil
	}
	m1 := make([]byte, len(m))
	copy(m1, []byte(m))
	return m1
}

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

// Logger get logger
func Logger(prefix string) *log.Logger {
	return log.New(os.Stdout, prefix, DefaultLoggerFlags)
}

// AsyncHandler convert event handler to async
func AsyncHandler(handler Handler, copyContextFns ...func(to, from context.Context) context.Context) Handler {
	return func(ctx context.Context, ev Event, data Data) {
		// Call handler with go routine
		go func() {
			defer func() {
				_, _, l, _ := runtime.Caller(1)
				if err := recover(); err != nil {
					flag := ev.Name()
					logger.Printf("Event[%s] Recover panic line => %v\n", flag, l)
					logger.Printf("Event[%s] Recover err => %v\n", flag, err)
					debug.PrintStack()
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
			handler(newCtx, ev, data)
		}()
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

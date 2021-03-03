package event

import (
	"context"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

var (
	// DefaultNamespace ...
	DefaultNamespace = ""
	// DefaultNamespaceSep ...
	DefaultNamespaceSep = "/"

	defaultManager *Manager
)

func init() {
	defaultManager = NewManager()
}

type eventImpl struct {
	name     string
	handlers []Handler
	sync.Mutex
}

func (e *eventImpl) String() string {
	return e.name
}

// Name ...
func (e *eventImpl) Name() string {
	return e.name
}

// Publish ...
func (e *eventImpl) Publish(ctx context.Context, data Data) {
	e.Lock()
	defer e.Unlock()
	for _, h := range e.handlers {
		go h(ctx, e, data)
	}
}

func (e *eventImpl) wrapRecover(handler Handler) Handler {
	return func(ctx context.Context, e Event, data Data) {
		defer func() {
			_, _, l, _ := runtime.Caller(1)
			if err := recover(); err != nil {
				flag := e.Name()
				logger.Printf("Event[%s] Recover panic line => %v\n", flag, l)
				logger.Printf("Event[%s] Recover err => %v\n", flag, err)
				debug.PrintStack()
			}
		}()
		handler(ctx, e, data)
	}
}

// Subscribe ...
func (e *eventImpl) Subscribe(ctx context.Context, handler Handler) int {
	e.Lock()
	defer e.Unlock()
	handler = e.wrapRecover(handler)
	for i, h := range e.handlers {
		if h == nil {
			e.handlers[i] = handler
			return i
		}
	}
	e.handlers = append(e.handlers, handler)
	return len(e.handlers)
}

// Stop ...
func (e *eventImpl) Stop(index int) error {
	e.Lock()
	defer e.Unlock()
	if index >= len(e.handlers) {
		return ErrEventDisabled
	}
	if e.handlers[index] == nil {
		return ErrEventDisabled
	}
	e.handlers[index] = nil
	return nil
}

// FullName ...
func FullName(names ...string) string {
	name := ""
	if len(names) > 1 {
		name = strings.Join(names, DefaultNamespaceSep)
	}
	return name
}

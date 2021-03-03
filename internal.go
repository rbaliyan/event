package event

import (
	"context"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

var (
	// DefaultNamespaceSep ...
	DefaultNamespaceSep = "/"

	// MessageBusSize ...
	MessageBusSize = 100

	defaultManager *Manager
)

func init() {
	defaultManager = NewManager()
}

type eventImpl struct {
	name     string
	channels []chan Data
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
	for _, ch := range e.channels {
		ch <- data
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
func (e *eventImpl) Subscribe(ctx context.Context, handler Handler) {
	e.Lock()
	defer e.Unlock()
	handler = e.wrapRecover(handler)
	ch := make(chan Data, MessageBusSize)
	e.channels = append(e.channels, ch)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-ch:
				if !ok {
					return
				}
				handler(ctx, e, data)
			}
		}
	}()
}

// Name ...
func Name(names ...string) string {
	name := ""
	if len(names) > 1 {
		name = strings.Join(names, DefaultNamespaceSep)
	}
	return name
}

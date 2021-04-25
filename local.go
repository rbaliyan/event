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

	defaultRegistry *Registry
)

func init() {
	defaultRegistry = NewRegistry()
}

// localImpl ...
type localImpl struct {
	name     string
	channels []chan Data
	sync.RWMutex
}

func (e *localImpl) String() string {
	return e.name
}

// Name ...
func (e *localImpl) Name() string {
	return e.name
}

// Default ...
func Local(name string) Event {
	return &localImpl{name: name}
}

// Publish ...
func (e *localImpl) Publish(ctx context.Context, data Data) {
	e.RLock()
	defer e.RUnlock()
	for _, ch := range e.channels {
		ch <- data
	}
}

func (e *localImpl) wrapRecover(handler Handler) Handler {
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
func (e *localImpl) Subscribe(ctx context.Context, handler Handler) {
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

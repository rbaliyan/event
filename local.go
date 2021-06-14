package event

import (
	"context"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

var (
	// DefaultNamespaceSep ...
	DefaultNamespaceSep = "/"

	// DefaultPublishTimeout default publish timeout in milliseconds if no timeout specified
	DefaultPublishTimeout = 1000

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
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Millisecond*time.Duration(DefaultPublishTimeout))
		defer cancel()
	}
	e.RLock()
	defer e.RUnlock()
	for _, ch := range e.channels {
		if ch == nil {
			continue
		}
		select {
		case ch <- data:
		case <-ctx.Done():
		}
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
	index := len(e.channels)
	for i := 0; i < len(e.channels); i++ {
		if e.channels[i] == nil {
			index = i
			e.channels[i] = ch
		}
	}
	if index >= len(e.channels) {
		e.channels = append(e.channels, ch)
	}
	closed := false
	go func() {
		defer func() {
			e.Lock()
			if e.channels[index] == ch {
				e.channels[index] = nil
			}
			e.Unlock()
			if !closed {
				close(ch)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-ch:
				if !ok {
					closed = true
					return
				}
				// Call handler
				go handler(ctx, e, data)
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

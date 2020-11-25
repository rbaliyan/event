package event

import (
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
func (e *eventImpl) Publish(data Data) {
	e.Lock()
	defer e.Unlock()
	for _, h := range e.handlers {
		go h(e, data)
	}
}

// Subscribe ...
func (e *eventImpl) Subscribe(handler Handler) int {
	e.Lock()
	defer e.Unlock()
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

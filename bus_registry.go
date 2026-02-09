package event

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// FullNameSeparator is the separator between bus name and event name in full event names.
// Full name format: "<bus_name>://<event_name>"
const FullNameSeparator = "://"

// Global bus registry
var busRegistry sync.Map // map[string]*Bus

// GetBus returns a registered bus by name.
// Returns nil if no bus with that name exists.
//
// Note: The returned bus may be closed. Use GetBusOrError() if you need
// to ensure the bus is running, or check bus.Running() after retrieval.
func GetBus(name string) *Bus {
	if v, ok := busRegistry.Load(name); ok {
		return v.(*Bus)
	}
	return nil
}

// GetBusOrError returns a registered bus by name, or an error if the bus
// doesn't exist or was closed at the time of the check. This is safer than
// GetBus() when you need to verify the bus exists and is running.
//
// Note: The bus could be closed between this check and subsequent use (TOCTOU).
// All Bus methods (Send, Recv, etc.) return ErrBusClosed if called after close,
// so callers are safe from crashes but should handle ErrBusClosed on use.
//
// Example:
//
//	bus, err := event.GetBusOrError("my-bus")
//	if err != nil {
//	    return err // Bus doesn't exist or is closed
//	}
//	// Bus was running at check time; handle ErrBusClosed on subsequent calls
func GetBusOrError(name string) (*Bus, error) {
	v, ok := busRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrBusNotFound, name)
	}
	bus := v.(*Bus)
	if !bus.Running() {
		return nil, fmt.Errorf("%w: %q", ErrBusClosed, name)
	}
	return bus, nil
}

// ListBuses returns the names of all registered buses.
func ListBuses() []string {
	var names []string
	busRegistry.Range(func(key, value any) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

// parseFullName splits a full event name into bus name and event name.
// Format: "<bus_name>://<event_name>"
// Returns error if format is invalid.
func parseFullName(fullName string) (busName, eventName string, err error) {
	idx := strings.Index(fullName, FullNameSeparator)
	if idx == -1 {
		return "", "", fmt.Errorf("%w: missing separator %q in %q", ErrInvalidFullName, FullNameSeparator, fullName)
	}
	busName = fullName[:idx]
	eventName = fullName[idx+len(FullNameSeparator):]
	if busName == "" {
		return "", "", fmt.Errorf("%w: empty bus name in %q", ErrInvalidFullName, fullName)
	}
	if eventName == "" {
		return "", "", fmt.Errorf("%w: empty event name in %q", ErrInvalidFullName, fullName)
	}
	return busName, eventName, nil
}

// Get retrieves a typed event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
// Returns ErrTypeMismatch if the types don't match.
//
// Example:
//
//	event, err := event.Get[Order]("mybus://order.created")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	event.Publish(ctx, Order{ID: "123"})
func Get[T any](fullName string) (Event[T], error) {
	busName, eventName, err := parseFullName(fullName)
	if err != nil {
		return nil, err
	}

	bus := GetBus(busName)
	if bus == nil {
		return nil, fmt.Errorf("%w: %q", ErrBusNotFound, busName)
	}

	// Get the type for T to validate
	var zero T
	eventType := reflect.TypeOf(zero)

	ev, err := bus.getTyped(eventName, eventType)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, fmt.Errorf("%w: %q", ErrEventNotFound, eventName)
	}

	typed, ok := ev.(Event[T])
	if !ok {
		return nil, fmt.Errorf("%w: cannot cast event %q to requested type", ErrTypeMismatch, eventName)
	}

	return typed, nil
}

// Publish sends data to an event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
//
// Example:
//
//	err := event.Publish(ctx, "mybus://order.created", Order{ID: "123"})
func Publish[T any](ctx context.Context, fullName string, data T) error {
	ev, err := Get[T](fullName)
	if err != nil {
		return err
	}
	return ev.Publish(ctx, data)
}

// Subscribe registers a handler for an event by its full name.
// Full name format: "<bus_name>://<event_name>"
//
// The type parameter T must match the type used when the event was registered.
//
// Example:
//
//	err := event.Subscribe(ctx, "mybus://order.created", func(ctx context.Context, e event.Event[Order], order Order) error {
//	    fmt.Println("Received order:", order.ID)
//	    return nil
//	})
func Subscribe[T any](ctx context.Context, fullName string, handler Handler[T], opts ...SubscribeOption[T]) error {
	ev, err := Get[T](fullName)
	if err != nil {
		return err
	}
	return ev.Subscribe(ctx, handler, opts...)
}

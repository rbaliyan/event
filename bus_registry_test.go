package event

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/channel"
)

// TestGetBus verifies GetBus returns registered buses
func TestGetBus(t *testing.T) {
	t.Parallel()
	busName := "test-getbus-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// GetBus should return the bus
	got := GetBus(busName)
	if got == nil {
		t.Fatal("GetBus returned nil for registered bus")
	}
	if got.Name() != busName {
		t.Errorf("expected bus name %q, got %q", busName, got.Name())
	}

	// GetBus for non-existent should return nil
	if got := GetBus("non-existent-bus"); got != nil {
		t.Errorf("expected nil for non-existent bus, got %v", got)
	}
}

// TestListBuses verifies ListBuses returns all registered bus names
func TestListBuses(t *testing.T) {
	t.Parallel()
	busName1 := "test-list-1-" + NewID()
	busName2 := "test-list-2-" + NewID()

	bus1 := mustNewBus(t, busName1, WithTransport(channel.New()))
	defer bus1.Close(context.Background())

	bus2 := mustNewBus(t, busName2, WithTransport(channel.New()))
	defer bus2.Close(context.Background())

	names := ListBuses()

	found1, found2 := false, false
	for _, name := range names {
		if name == busName1 {
			found1 = true
		}
		if name == busName2 {
			found2 = true
		}
	}

	if !found1 {
		t.Errorf("ListBuses did not include %q", busName1)
	}
	if !found2 {
		t.Errorf("ListBuses did not include %q", busName2)
	}
}

// TestDuplicateBusError verifies NewBus returns error for duplicate name
func TestDuplicateBusError(t *testing.T) {
	t.Parallel()
	busName := "test-duplicate-" + NewID()
	bus1 := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus1.Close(context.Background())

	// Try to create another bus with same name
	_, err := NewBus(busName, WithTransport(channel.New()))
	if err == nil {
		t.Fatal("expected error for duplicate bus name")
	}
	if !errors.Is(err, ErrBusExists) {
		t.Errorf("expected ErrBusExists, got %v", err)
	}
}

// TestBusUnregisteredOnClose verifies bus is removed from registry on Close
func TestBusUnregisteredOnClose(t *testing.T) {
	t.Parallel()
	busName := "test-unregister-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))

	// Bus should be registered
	if GetBus(busName) == nil {
		t.Fatal("bus not registered after creation")
	}

	// Close the bus
	bus.Close(context.Background())

	// Bus should no longer be registered
	if GetBus(busName) != nil {
		t.Fatal("bus still registered after Close")
	}

	// Should be able to create a new bus with same name
	bus2, err := NewBus(busName, WithTransport(channel.New()))
	if err != nil {
		t.Fatalf("could not create bus after Close: %v", err)
	}
	defer bus2.Close(context.Background())
}

// TestGetEventByFullName verifies Get[T] works with full name
func TestGetEventByFullName(t *testing.T) {
	t.Parallel()
	type Order struct {
		ID string
	}

	busName := "test-fullname-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register event
	ev := New[Order]("order.created")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	// Get event by full name
	fullName := busName + "://order.created"
	event, err := Get[Order](fullName)
	if err != nil {
		t.Fatalf("Get[Order] failed: %v", err)
	}
	if event.Name() != "order.created" {
		t.Errorf("expected event name 'order.created', got %q", event.Name())
	}
}

// TestGetEventTypeMismatch verifies Get returns error for type mismatch
func TestGetEventTypeMismatch(t *testing.T) {
	t.Parallel()
	type Order struct {
		ID string
	}
	type User struct {
		Name string
	}

	busName := "test-mismatch-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register event as Order
	ev := New[Order]("order.created")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	// Try to get as User - should fail
	fullName := busName + "://order.created"
	_, err := Get[User](fullName)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("expected ErrTypeMismatch, got %v", err)
	}
}

// TestPublishByFullName verifies Publish works with full name
func TestPublishByFullName(t *testing.T) {
	t.Parallel()
	type Order struct {
		ID string
	}

	busName := "test-publish-fn-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register and subscribe
	ev := New[Order]("order.created")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan Order)
	ev.Subscribe(context.Background(), func(ctx context.Context, e Event[Order], order Order) error {
		ch <- order
		return nil
	})

	// Publish using full name
	fullName := busName + "://order.created"
	if err := Publish(context.Background(), fullName, Order{ID: "test-123"}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify received
	select {
	case order := <-ch:
		if order.ID != "test-123" {
			t.Errorf("expected ID 'test-123', got %q", order.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestSubscribeByFullName verifies Subscribe works with full name
func TestSubscribeByFullName(t *testing.T) {
	t.Parallel()
	type Order struct {
		ID string
	}

	busName := "test-subscribe-fn-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register event
	ev := New[Order]("order.created")
	if err := Register(context.Background(), bus, ev); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	// Subscribe using full name
	ch := make(chan Order)
	fullName := busName + "://order.created"
	if err := Subscribe(context.Background(), fullName, func(ctx context.Context, e Event[Order], order Order) error {
		ch <- order
		return nil
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Publish and verify
	ev.Publish(context.Background(), Order{ID: "test-456"})

	select {
	case order := <-ch:
		if order.ID != "test-456" {
			t.Errorf("expected ID 'test-456', got %q", order.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestInvalidFullName verifies error handling for invalid full names
func TestInvalidFullName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		fullName string
	}{
		{"missing separator", "busname-eventname"},
		{"empty bus name", "://eventname"},
		{"empty event name", "busname://"},
		{"no separator at all", "justsomestring"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Get[any](tt.fullName)
			if err == nil {
				t.Errorf("expected error for full name %q", tt.fullName)
			}
			if !errors.Is(err, ErrInvalidFullName) && !errors.Is(err, ErrBusNotFound) {
				t.Errorf("expected ErrInvalidFullName or ErrBusNotFound, got %v", err)
			}
		})
	}
}

// TestGetEventNotFound verifies error for non-existent event
func TestGetEventNotFound(t *testing.T) {
	t.Parallel()
	busName := "test-notfound-" + NewID()
	bus := mustNewBus(t, busName, WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Try to get non-existent event
	fullName := busName + "://nonexistent.event"
	_, err := Get[any](fullName)
	if err == nil {
		t.Fatal("expected error for non-existent event")
	}
	if !errors.Is(err, ErrEventNotFound) {
		t.Errorf("expected ErrEventNotFound, got %v", err)
	}
}

// TestGetBusNotFound verifies error for non-existent bus in full name
func TestGetBusNotFound(t *testing.T) {
	t.Parallel()
	fullName := "nonexistent-bus-12345://some.event"
	_, err := Get[any](fullName)
	if err == nil {
		t.Fatal("expected error for non-existent bus")
	}
	if !errors.Is(err, ErrBusNotFound) {
		t.Errorf("expected ErrBusNotFound, got %v", err)
	}
}

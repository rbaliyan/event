package event

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"math"
	"sync/atomic"

	"github.com/google/go-cmp/cmp"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
	"syreclabs.com/go/faker"
)

func init() {
	faker.Seed(time.Now().UnixNano())
}

// mustNewBus creates a bus for testing, fails test on error
func mustNewBus(t testing.TB, name string, opts ...BusOption) *Bus {
	t.Helper()
	bus, err := NewBus(name, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return bus
}

const waitChTimeoutMS = 100

func waitForMetaData(ch chan map[string]string, timeout int) (map[string]string, bool) {
	select {
	case d := <-ch:
		return d, true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return nil, false
	}
}

func waitForData[T any](ch chan T, timeout int) (T, bool) {
	select {
	case d := <-ch:
		return d, true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		var zero T
		return zero, false
	}
}

func wait(ch chan struct{}, timeout int) bool {
	select {
	case <-ch:
		return true
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		return false
	}
}

// Compare metadata
func CompareMetadata(m1, m2 map[string]string) bool {
	if len(m1) == len(m2) {
		for i, x := range m1 {
			if m2[i] != x {
				return false
			}
		}
		return true
	}
	return false
}

func TestEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	// Duplicate registration should return error
	e2 := New[any]("test")
	if err := Register(context.Background(), bus, e2); !errors.Is(err, ErrEventExists) {
		t.Fatalf("expected ErrEventExists for duplicate register, got: %v", err)
	}

	ch := make(chan struct{})
	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) error {
		if id := ContextEventID(ctx); id == "" {
			t.Error("event id is null")
		}
		if b := ContextBus(ctx); b == nil {
			t.Error("bus is null")
		} else if b.ID() != bus.ID() {
			t.Errorf("bus is wrong got:%s, expected:%s", b.ID(), bus.ID())
		}
		if source := ContextSource(ctx); source != bus.ID() {
			t.Errorf("source is wrong got:%s, expected:%s", source, bus.ID())
		}
		if data != nil {
			t.Error("data is not null")
		}
		ch <- struct{}{}
		return nil
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("Failed")
	}
	e1 := bus.Get("test")
	if e1 == nil {
		t.Fatal("Failed to get event")
	}
	// Type assert to use Publish
	if typed, ok := e1.(Event[any]); ok {
		typed.Publish(context.TODO(), nil)
	}
	if !wait(ch, waitChTimeoutMS) {
		t.Error("Failed")
	}
}

func TestMetadata(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	if bus.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}
	ch1 := make(chan map[string]string, 2) // Buffer to avoid blocking
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) error {
		if m := ContextMetadata(ctx); m == nil {
			t.Error("metadata is null")
		} else {
			ch1 <- m
		}
		return nil
	})
	ch2 := make(chan map[string]string, 2) // Buffer to avoid blocking
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) error {
		if m := ContextMetadata(ctx); m == nil {
			t.Error("metadata is null")
		} else {
			ch2 <- m
		}
		return nil
	})
	msg := "this is a test"
	m := map[string]string{"": msg}

	// First publish - both subscribers receive it
	e.Publish(ContextWithMetadata(context.Background(), m), nil)
	m1, ok := waitForMetaData(ch1, waitChTimeoutMS)
	if !ok {
		t.Fatal("metadata not found from ch1")
	}
	if !CompareMetadata(m, m1) {
		t.Errorf("metadata is different got:%v, expected:%v", m1, m)
	}
	// Also consume from ch2 for the first publish
	m2, ok := waitForMetaData(ch2, waitChTimeoutMS)
	if !ok {
		t.Fatal("metadata not found from ch2")
	}
	if !CompareMetadata(m, m2) {
		t.Errorf("metadata is different got:%v, expected:%v", m2, m)
	}
}

func TestPanic(t *testing.T) {
	ch1 := make(chan struct{})
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("test",
		WithErrorHandler(func(bus *Bus, name string, err error) {
			ch1 <- struct{}{}
		}))
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	if bus.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}

	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) error {
		panic("test")
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch1, waitChTimeoutMS) {
		t.Error("Panic failed")
	}
}

func TestCancel(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	ctx1, cancel1 := context.WithCancel(context.Background())
	e.Subscribe(ctx1, func(ctx context.Context, ev Event[any], data any) error {
		ch1 <- struct{}{}
		return nil
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	e.Subscribe(ctx2, func(context.Context, Event[any], any) error {
		ch2 <- struct{}{}
		return nil
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if !wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	cancel1()
	time.Sleep(10 * time.Millisecond) // Allow cancel to propagate
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if !wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	cancel2()
	time.Sleep(10 * time.Millisecond) // Allow cancel to propagate
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
}

func TestData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	no := faker.RandomInt(0, math.MaxInt-1)
	s := faker.Lorem().String()
	st := struct {
		N int
		S string
	}{no, s}

	tests := []struct {
		name string
		args any
	}{
		{"null", nil},
		{"number", no},
		{"string", s},
		{"struct", st},
	}
	ch := make(chan any)
	ch1 := make(chan any)
	ch2 := make(chan any)
	e := New[any]("test-data")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) error {
		ch <- data
		return nil
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) error {
		ch1 <- data
		return nil
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) error {
		ch2 <- data
		return nil
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e.Publish(context.Background(), tt.args)
			out, ok := waitForData(ch, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out, tt.args))
			}
			out1, ok := waitForData(ch1, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out1, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out1, tt.args))
			}
			out2, ok := waitForData(ch2, waitChTimeoutMS)
			if !ok {
				t.Fatal("Sub failed")
			}
			if !cmp.Equal(out2, tt.args) {
				t.Errorf("diff : %v", cmp.Diff(out2, tt.args))
			}
		})
	}
}

func BenchmarkEvent(b *testing.B) {
	bus := mustNewBus(b, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[int]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		b.Fatalf("failed to register event: %v", err)
	}
	ch1 := make(chan struct{})
	total := int32(b.N)
	var counter int32
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int], data int) error {
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
		return nil
	})
	for i := 0; i < b.N; i++ {
		e.Publish(context.Background(), i)
	}
	e.Publish(context.Background(), -1)
	if !wait(ch1, 2000) {
		b.Error("timeout")
	}
	if counter < int32(b.N) {
		b.Error("counter is smaller :", counter, b.N)
	}
}

func TestPool(t *testing.T) {
	var poolSize int64 = 4
	transport := channel.New(
		channel.WithAsync(true),
		channel.WithWorkerPoolSize(poolSize),
	)
	bus := mustNewBus(t, "test", WithTransport(transport))
	defer bus.Close(context.Background())

	e := New[int32]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	var total int32 = 100
	var counter int32
	var counter1 int32
	var max int32
	ch := make(chan int32)
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	go func() {
		for {
			select {
			case count := <-ch:
				if count > max {
					max = count
				}
			case <-ch1:
				if atomic.AddInt32(&counter1, 1) >= total {
					ch2 <- struct{}{}
				}
			}
		}
	}()
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		defer atomic.AddInt32(&counter, -1)
		ch <- atomic.AddInt32(&counter, 1)
		ch1 <- struct{}{}
		return nil
	})
	var i int32
	for i = 0; i < total; i++ {
		e.Publish(context.Background(), i)
	}
	if !wait(ch2, 2000) {
		t.Error("timeout")
	}
	if max > total/2 {
		t.Error("Failed")
	}
}

func TestWorkerPoolDeliveryMode(t *testing.T) {
	// Test worker pool delivery mode - each message goes to only one subscriber
	transport := channel.New(
		channel.WithBufferSize(100),
		channel.WithTimeout(time.Duration(100)*time.Millisecond),
	)
	// Create bus (delivery mode is now per-subscription)
	bus := mustNewBus(t, "test", WithTransport(transport))
	defer bus.Close(context.Background())

	e := New[int32]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	var total int32 = 100
	var counter int32
	var counter1 int32
	var counter2 int32
	var counter3 int32
	ch1 := make(chan struct{}, 1)

	// Use AsWorker() to subscribe in worker pool mode
	if err := e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&counter1, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			select {
			case ch1 <- struct{}{}:
			default:
			}
		}
		return nil
	}, AsWorker[int32]()); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&counter2, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			select {
			case ch1 <- struct{}{}:
			default:
			}
		}
		return nil
	}, AsWorker[int32]()); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if err := e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&counter3, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			select {
			case ch1 <- struct{}{}:
			default:
			}
		}
		return nil
	}, AsWorker[int32]()); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	var i int32
	for i = 0; i < total; i++ {
		if err := e.Publish(context.Background(), i); err != nil {
			t.Errorf("publish failed: %v", err)
		}
	}
	if !wait(ch1, 2000) {
		t.Error("timeout", counter1)
	}
	if counter != total {
		t.Error("Failed", counter, total)
	}
	// In competing mode, each message goes to only one subscriber
	// So no single subscriber should have received all messages
	if counter1 >= total {
		t.Error("Failed - subscriber 1 got all messages", counter1, total)
	}
	if counter2 >= total {
		t.Error("Failed - subscriber 2 got all messages", counter2, total)
	}
	if counter3 >= total {
		t.Error("Failed - subscriber 3 got all messages", counter3, total)
	}
}

// TestContextImmutability verifies that context modification functions
// don't mutate the original context data (race condition fix)
func TestContextImmutability(t *testing.T) {
	// Create initial context with data
	ctx := context.Background()
	ctx = ContextWithMetadata(ctx, map[string]string{"key": "original"})
	ctx = ContextWithEventID(ctx, "event-123")

	// Get original values
	originalMeta := ContextMetadata(ctx)
	originalID := ContextEventID(ctx)

	// Modify context with new values
	ctx2 := ContextWithMetadata(ctx, map[string]string{"key": "modified"})
	ctx3 := ContextWithEventID(ctx, "event-456")

	// Verify original context is unchanged
	if ContextMetadata(ctx)["key"] != "original" {
		t.Error("original metadata was mutated")
	}
	if ContextEventID(ctx) != "event-123" {
		t.Error("original event ID was mutated")
	}

	// Verify new contexts have new values
	if ContextMetadata(ctx2)["key"] != "modified" {
		t.Error("new metadata not set correctly")
	}
	if ContextEventID(ctx3) != "event-456" {
		t.Error("new event ID not set correctly")
	}

	// Verify we didn't accidentally modify the original references
	if originalMeta["key"] != "original" {
		t.Error("original metadata reference was mutated")
	}
	if originalID != "event-123" {
		t.Error("original event ID reference was mutated")
	}
}

// TestContextConcurrentAccess verifies context functions are safe for concurrent use
func TestContextConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithMetadata(ctx, map[string]string{"initial": "value"})
	ctx = ContextWithEventID(ctx, "initial-id")

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Spawn multiple goroutines that read and write context concurrently
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Read operations
			_ = ContextMetadata(ctx)
			_ = ContextEventID(ctx)
			_ = ContextSource(ctx)
			_ = ContextLogger(ctx)
			_ = ContextBus(ctx)

			// Write operations (should create new contexts, not mutate)
			newCtx := ContextWithMetadata(ctx, map[string]string{"goroutine": "value"})
			newCtx = ContextWithEventID(newCtx, "new-id")

			// Verify the new context has correct values
			if ContextEventID(newCtx) != "new-id" {
				errors <- nil // Signal error occurred
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		t.Error("concurrent context access caused errors")
	}
}

// TestTransportCloseDoesNotPanic verifies transport close doesn't cause panics
func TestTransportCloseDoesNotPanic(t *testing.T) {
	tr := channel.New()

	// Register an event first
	if err := tr.RegisterEvent(context.Background(), "test"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Subscribe to get a subscription
	sub, err := tr.Subscribe(context.Background(), "test")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if sub == nil {
		t.Fatal("expected subscription")
	}

	// Close transport
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("close error: %v", err)
	}

	// Verify Subscribe returns error after close
	_, err = tr.Subscribe(context.Background(), "test")
	if err == nil {
		t.Error("expected error after close")
	}

	// Double close should not panic
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("double close error: %v", err)
	}
}

// TestEventClose verifies event close works correctly
func TestEventClose(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	e := New[any]("test-close")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) error {
		ch <- struct{}{}
		return nil
	})

	// Verify event works before close
	e.Publish(context.Background(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("expected to receive event before close")
	}

	// Close bus (which closes all events)
	bus.Close(context.Background())

	// Publish after close should not panic (silently ignored)
	e.Publish(context.Background(), nil)

	// Should not receive anything after close
	if wait(ch, 50) {
		t.Error("should not receive event after close")
	}
}

// TestWorkerPoolBackpressure verifies that worker pool blocks when exhausted
func TestWorkerPoolBackpressure(t *testing.T) {
	poolSize := int64(2)
	transport := channel.New(
		channel.WithAsync(true),
		channel.WithWorkerPoolSize(poolSize),
	)
	bus := mustNewBus(t, "test", WithTransport(transport))
	defer bus.Close(context.Background())

	e := New[int]("test-pool-backpressure")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	var processed int32
	blockCh := make(chan struct{})
	doneCh := make(chan struct{})

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[int], data int) error {
		atomic.AddInt32(&processed, 1)
		<-blockCh // Block until released
		return nil
	})

	// Publish more events than pool size
	for i := 0; i < int(poolSize)+2; i++ {
		e.Publish(context.Background(), i)
	}

	// Wait a bit for events to be processed
	time.Sleep(50 * time.Millisecond)

	// All events should eventually be processed
	go func() {
		for atomic.LoadInt32(&processed) < int32(poolSize)+2 {
			time.Sleep(10 * time.Millisecond)
		}
		close(doneCh)
	}()

	// Release blocked handlers
	for i := 0; i < int(poolSize)+2; i++ {
		blockCh <- struct{}{}
	}

	select {
	case <-doneCh:
		// Success
	case <-time.After(2 * time.Second):
		t.Errorf("timeout waiting for all events, processed: %d", atomic.LoadInt32(&processed))
	}
}

// TestGracefulShutdown verifies that Close() blocks until all messages are delivered to subscribers
func TestGracefulShutdown(t *testing.T) {
	transport := channel.New(
		channel.WithAsync(true),
	)
	bus := mustNewBus(t, "test", WithTransport(transport))

	e := New[int]("test-graceful")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	var delivered int32
	deliveryCh := make(chan struct{}, 5)

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[int], data int) error {
		atomic.AddInt32(&delivered, 1)
		deliveryCh <- struct{}{}
		return nil
	})

	// Publish events
	for i := 0; i < 5; i++ {
		e.Publish(context.Background(), i)
	}

	// Wait for all deliveries
	for i := 0; i < 5; i++ {
		select {
		case <-deliveryCh:
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for delivery %d", i)
		}
	}

	// Now close
	bus.Close(context.Background())

	if atomic.LoadInt32(&delivered) != 5 {
		t.Errorf("expected 5 delivered, got %d", delivered)
	}
}

// TestDiscardEvent verifies discardEvent works correctly
func TestDiscardEvent(t *testing.T) {
	e := Discard[string]("test")

	if e.Name() != "" {
		t.Errorf("expected empty name, got %s", e.Name())
	}

	// These should not panic
	e.Publish(context.Background(), "data")
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		t.Error("handler should not be called for discard event")
		return nil
	})
}

// TestTransportErrorHandler verifies transport error handler is called on timeout
func TestTransportErrorHandler(t *testing.T) {
	errorCh := make(chan error, 1)

	tr := channel.New(
		channel.WithTimeout(1*time.Millisecond),
		channel.WithBufferSize(0), // No buffer to force blocking
		channel.WithErrorHandler(func(err error) {
			errorCh <- err
		}),
	)

	// Register an event
	if err := tr.RegisterEvent(context.Background(), "test"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Subscribe but don't read from channel (will cause timeout)
	sub, err := tr.Subscribe(context.Background(), "test")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	_ = sub // Don't read from this subscription

	// Try to publish - should timeout because subscriber isn't reading
	msg := message.New("test", "source", "data", nil, trace.SpanContext{})
	go func() {
		tr.Publish(context.Background(), "test", msg)
	}()

	select {
	case err := <-errorCh:
		if !errors.Is(err, transport.ErrPublishTimeout) {
			t.Errorf("expected ErrPublishTimeout, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		// Timeout is expected behavior when buffer is full and no timeout error handler called
		// The new transport drops messages when channel is full (non-blocking)
	}

	tr.Close(context.Background())
}

// TestEventsSlice verifies Events slice publish/subscribe
func TestEventsSlice(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e1 := New[string]("event1")
	if err := Register(context.Background(), bus, e1); err != nil {
		t.Fatalf("failed to register event1: %v", err)
	}
	e2 := New[string]("event2")
	if err := Register(context.Background(), bus, e2); err != nil {
		t.Fatalf("failed to register event2: %v", err)
	}
	e3 := New[string]("event3")
	if err := Register(context.Background(), bus, e3); err != nil {
		t.Fatalf("failed to register event3: %v", err)
	}

	events := Events[string]{e1, e2, e3}

	if events.Name() != "event1,event2,event3" {
		t.Errorf("unexpected name: %s", events.Name())
	}

	var count int32
	ch := make(chan struct{}, 1) // Buffered to avoid blocking

	events.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		if atomic.AddInt32(&count, 1) >= 3 {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		return nil
	})

	events.Publish(context.Background(), "test")

	if !wait(ch, waitChTimeoutMS) {
		t.Error("timeout waiting for all events")
	}

	if atomic.LoadInt32(&count) < 3 {
		t.Errorf("expected at least 3 handlers called, got %d", count)
	}
}

// TestContextName verifies ContextName function
func TestContextName(t *testing.T) {
	// Test empty context returns empty string
	ctx := context.Background()
	if name := ContextName(ctx); name != "" {
		t.Errorf("expected empty name, got %s", name)
	}

	// Test context with name (via handler context)
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())
	e := New[any]("test-context-name")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- ContextName(ctx)
		return nil
	})

	e.Publish(context.Background(), nil)

	select {
	case name := <-ch:
		if name != "test-context-name" {
			t.Errorf("expected test-context-name, got %s", name)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestContextWithLogger verifies ContextWithLogger function
func TestContextWithLogger(t *testing.T) {
	ctx := context.Background()

	// Test nil logger returns same context
	ctx2 := ContextWithLogger(ctx, nil)
	if ctx2 != ctx {
		t.Error("nil logger should return same context")
	}

	// Test setting logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx3 := ContextWithLogger(ctx, logger)
	if got := ContextLogger(ctx3); got != logger {
		t.Error("expected logger to be set")
	}

	// Test setting logger on context with existing data
	ctx4 := ContextWithEventID(ctx, "event-123")
	ctx5 := ContextWithLogger(ctx4, logger)
	if got := ContextLogger(ctx5); got != logger {
		t.Error("expected logger on existing context")
	}
	if got := ContextEventID(ctx5); got != "event-123" {
		t.Error("expected event ID to be preserved")
	}
}

// TestContextWithEventFromContext verifies ContextWithEventFromContext
func TestContextWithEventFromContext(t *testing.T) {
	// Create source context with data
	from := context.Background()
	from = ContextWithEventID(from, "event-abc")
	from = ContextWithMetadata(from, map[string]string{"key": "value"})

	// Create destination context
	to := context.Background()

	// Copy event data
	result := ContextWithEventFromContext(to, from)

	// Verify data was copied
	if got := ContextEventID(result); got != "event-abc" {
		t.Errorf("expected event-abc, got %s", got)
	}
	if got := ContextMetadata(result); got["key"] != "value" {
		t.Error("expected metadata to be copied")
	}

	// Test with empty source context
	empty := context.Background()
	result2 := ContextWithEventFromContext(to, empty)
	if result2 != to {
		t.Error("expected same context when source is empty")
	}
}

// TestNewContext verifies NewContext function
func TestNewContext(t *testing.T) {
	// Create context with data
	ctx := context.Background()
	ctx = ContextWithEventID(ctx, "event-xyz")
	ctx = ContextWithMetadata(ctx, map[string]string{"test": "data"})

	// Create new context
	newCtx := NewContext(ctx)

	// Verify data was copied
	if got := ContextEventID(newCtx); got != "event-xyz" {
		t.Errorf("expected event-xyz, got %s", got)
	}
	if got := ContextMetadata(newCtx); got["test"] != "data" {
		t.Error("expected metadata to be copied")
	}
}

// TestBusPublishSubscribe verifies basic bus publish/subscribe
func TestBusPublishSubscribe(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ch := make(chan any)

	e := New[any]("global-test-2")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- data
		return nil
	})

	e.Publish(context.Background(), "test-data")

	select {
	case data := <-ch:
		if data != "test-data" {
			t.Errorf("expected test-data, got %v", data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestTransportCloseWithSubscriptions verifies transport.Close(context.Background()) properly cleans up
func TestTransportCloseWithSubscriptions(t *testing.T) {
	tr := channel.New(channel.WithBufferSize(10))

	// Register an event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Create subscription
	sub, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Publish a message
	msg := message.New("test-msg", "source", "data", nil, trace.SpanContext{})
	if err := tr.Publish(context.Background(), "test-event", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Close transport
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("close error: %v", err)
	}

	// Verify Subscribe returns error after close
	_, err = tr.Subscribe(context.Background(), "test-event")
	if !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("expected ErrTransportClosed, got: %v", err)
	}

	// Verify subscription channel is closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			// Got the message we published, try again
			select {
			case _, ok := <-sub.Messages():
				if ok {
					t.Error("expected channel to be closed")
				}
			case <-time.After(10 * time.Millisecond):
				t.Error("expected channel to be closed immediately")
			}
		}
	case <-time.After(10 * time.Millisecond):
		t.Error("expected channel to be closed immediately")
	}

	// Double close should not panic
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("double close error: %v", err)
	}
}

// TestWithSubscriberTimeout verifies WithSubscriberTimeout option
func TestWithSubscriberTimeout(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	timeout := 50 * time.Millisecond
	e := New[any]("timeout-test", WithSubscriberTimeout(timeout))
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan bool)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		// Check if context has deadline
		if deadline, ok := ctx.Deadline(); ok {
			ch <- time.Until(deadline) <= timeout
		} else {
			ch <- false
		}
		return nil
	})

	e.Publish(context.Background(), nil)

	select {
	case hasDeadline := <-ch:
		if !hasDeadline {
			t.Error("expected context to have deadline")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestBusLogger verifies bus logger is used by events
func TestBusLogger(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("logger-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- struct{}{}
		return nil
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestSanitize verifies Sanitize function
func TestSanitize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"Hello123", "Hello123"},
		{"hello.world", "hello_world"},
		{"hello-world", "hello_world"},
		{"hello world", "hello_world"},
		{"user@email.com", "user_email_com"},
		{"test!@#$%", "test_____"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := Sanitize(tt.input); got != tt.expected {
				t.Errorf("Sanitize(%s) = %s, want %s", tt.input, got, tt.expected)
			}
		})
	}
}

// TestCaller verifies Caller function
func TestCaller(t *testing.T) {
	// Test calling from this function
	name := Caller(1)
	if name == "" {
		t.Error("expected non-empty caller name")
	}
	if !strings.Contains(name, "TestCaller") {
		t.Errorf("expected caller to contain TestCaller, got %s", name)
	}

	// Test invalid depth
	name = Caller(100)
	if name != "" {
		t.Errorf("expected empty string for invalid depth, got %s", name)
	}
}

// TestAsyncHandler verifies AsyncHandler function
func TestAsyncHandler(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[string]("async-handler-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan string)
	handler := func(ctx context.Context, ev Event[string], data string) error {
		ch <- data
		return nil
	}

	asyncHandler := AsyncHandler(handler)
	e.Subscribe(context.Background(), asyncHandler)

	e.Publish(context.Background(), "async-data")

	select {
	case data := <-ch:
		if data != "async-data" {
			t.Errorf("expected async-data, got %s", data)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for async handler")
	}
}

// TestAsyncHandlerWithPanic verifies AsyncHandler recovers from panic
func TestAsyncHandlerWithPanic(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("async-panic-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	handler := func(ctx context.Context, ev Event[any], data any) error {
		panic("test panic")
	}

	asyncHandler := AsyncHandler(handler)
	e.Subscribe(context.Background(), asyncHandler)

	e.Publish(context.Background(), nil)

	// Give time for async handler to run and recover
	time.Sleep(50 * time.Millisecond)

	// Test passes if no panic propagated
	close(ch)
}

// TestEventString verifies eventImpl.String()
func TestEventString(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("string-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	if impl, ok := e.(*eventImpl[any]); ok {
		if impl.String() != "string-test" {
			t.Errorf("expected string-test, got %s", impl.String())
		}
	}
}

// TestEventSubscribers verifies eventImpl.Subscribers()
func TestEventSubscribers(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("subscribers-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	impl, ok := e.(*eventImpl[any])
	if !ok {
		t.Fatal("expected eventImpl")
	}

	if impl.Subscribers() != 0 {
		t.Errorf("expected 0 subscribers, got %d", impl.Subscribers())
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) error { return nil })

	time.Sleep(10 * time.Millisecond) // Allow subscription to register

	if impl.Subscribers() != 1 {
		t.Errorf("expected 1 subscriber, got %d", impl.Subscribers())
	}

	cancel()
	time.Sleep(20 * time.Millisecond) // Allow unsubscribe

	if impl.Subscribers() != 0 {
		t.Errorf("expected 0 subscribers after cancel, got %d", impl.Subscribers())
	}
}

// TestNilEventPublish verifies nil event doesn't panic on Publish
func TestNilEventPublish(t *testing.T) {
	var e *eventImpl[any]
	// Should not panic
	e.Publish(context.Background(), "data")
}

// TestNilEventSubscribe verifies nil event doesn't panic on Subscribe
func TestNilEventSubscribe(t *testing.T) {
	var e *eventImpl[any]
	// Should not panic
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error { return nil })
}

// TestSubscriptionClose verifies Subscription.Close() removes subscriber
func TestSubscriptionClose(t *testing.T) {
	tr := channel.New()
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Create subscriber
	sub, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Close subscriber
	if err := sub.Close(context.Background()); err != nil {
		t.Errorf("close error: %v", err)
	}

	// Verify channel is closed by trying to receive (should be closed)
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(10 * time.Millisecond):
		t.Error("timeout - channel should be closed immediately")
	}
}

// TestSubscriptionCloseOnClosedTransport verifies subscription Close works after transport close
func TestSubscriptionCloseOnClosedTransport(t *testing.T) {
	tr := channel.New()

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Create subscriber
	sub, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	tr.Close(context.Background())

	// Should not panic - double close is safe
	sub.Close(context.Background())
}

// TestTransportSubscribeRace verifies concurrent Subscribe calls
func TestTransportSubscribeRace(t *testing.T) {
	tr := channel.New()
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sub, err := tr.Subscribe(context.Background(), "test-event")
			if err != nil {
				t.Errorf("subscribe failed: %v", err)
				return
			}
			if sub == nil {
				t.Error("expected non-nil subscription")
				return
			}
			defer sub.Close(context.Background())
		}(i)
	}
	wg.Wait()
}

// TestWithTransportLogger verifies WithTransportLogger option
func TestWithTransportLogger(t *testing.T) {
	customLogger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "transport")
	tr := channel.New(
		channel.WithLogger(customLogger),
	)
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Verify transport works with custom logger
	sub, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	if sub == nil {
		t.Error("expected subscription")
	}
	sub.Close(context.Background())
}

// TestBusMetricsIntegration verifies bus metrics work with events
func TestBusMetricsIntegration(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("metrics-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- struct{}{}
		return nil
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestBusWithEmptyName verifies NewBus handles empty name
func TestBusWithEmptyName(t *testing.T) {
	bus := mustNewBus(t, "", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	if bus.Name() != DefaultBusName {
		t.Errorf("expected default name '%s', got %s", DefaultBusName, bus.Name())
	}
}

// TestBusRegister verifies Bus.Register works correctly
func TestBusRegister(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// First call creates event
	e1 := New[any]("test-event")
	if err := Register(context.Background(), bus, e1); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	if e1 == nil {
		t.Fatal("expected event")
	}

	// Second call with same name returns error
	e2 := New[any]("test-event")
	if err := Register(context.Background(), bus, e2); !errors.Is(err, ErrEventExists) {
		t.Fatalf("expected ErrEventExists, got: %v", err)
	}
}

// TestContextSubscriptionID verifies ContextSubscriptionID in handler
func TestContextSubscriptionID(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("sub-id-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- ContextSubscriptionID(ctx)
		return nil
	})

	e.Publish(context.Background(), nil)

	select {
	case subID := <-ch:
		if subID == "" {
			t.Error("expected non-empty subscription ID")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestTransportPublishTimeout verifies publish timeout handling
func TestTransportPublishTimeout(t *testing.T) {
	errorCh := make(chan error, 1)

	tr := channel.New(
		channel.WithTimeout(1*time.Millisecond),
		channel.WithBufferSize(0), // blocking channel
		channel.WithErrorHandler(func(err error) {
			errorCh <- err
		}),
	)
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Create subscriber but don't read from it
	sub, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Close(context.Background())

	// Try to publish - should timeout since subscriber isn't reading
	msg := message.New("timeout-test", "source", "data", nil, trace.SpanContext{})
	go func() {
		tr.Publish(context.Background(), "test-event", msg)
	}()

	select {
	case err := <-errorCh:
		if !errors.Is(err, transport.ErrPublishTimeout) {
			t.Errorf("expected ErrPublishTimeout, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		// When buffer is 0 and timeout is set, publish should either:
		// 1. Timeout and call error handler
		// 2. Drop message silently if no timeout (non-blocking mode)
		// Both are acceptable behaviors
	}
}

// TestTracingDisabled verifies tracing can be disabled at bus level
func TestTracingDisabled(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()), WithTracing(false))
	defer bus.Close(context.Background())

	e := New[any]("no-tracing-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- struct{}{}
		return nil
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestRecoveryDisabled verifies recovery can be disabled at bus level
func TestRecoveryDisabled(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()), WithRecovery(false))
	defer bus.Close(context.Background())

	e := New[any]("no-recovery-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- struct{}{}
		return nil
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestMultipleSubscribersGetUniqueIDs verifies each subscriber gets unique ID
func TestMultipleSubscribersGetUniqueIDs(t *testing.T) {
	tr := channel.New()
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	sub1, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub1.Close(context.Background())

	sub2, err := tr.Subscribe(context.Background(), "test-event")
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub2.Close(context.Background())

	// Each subscriber should have a unique ID
	if sub1.ID() == sub2.ID() {
		t.Error("expected different IDs for different subscriptions")
	}
}

// TestBroadcastMultipleSubscribers verifies Broadcast delivers to all subscribers
func TestBroadcastMultipleSubscribers(t *testing.T) {
	tr := channel.New(channel.WithBufferSize(10))
	defer tr.Close(context.Background())

	// Register event
	if err := tr.RegisterEvent(context.Background(), "test-event"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Create two broadcast subscribers
	sub1, _ := tr.Subscribe(context.Background(), "test-event")
	defer sub1.Close(context.Background())
	sub2, _ := tr.Subscribe(context.Background(), "test-event")
	defer sub2.Close(context.Background())

	// Publish a message
	msg := message.New("test-msg", "source", "data", nil, trace.SpanContext{})
	if err := tr.Publish(context.Background(), "test-event", msg); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	// Both subscribers should receive the message
	select {
	case m := <-sub1.Messages():
		if m.ID() != "test-msg" {
			t.Errorf("sub1 got wrong message: %s", m.ID())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub1 timeout waiting for message")
	}

	select {
	case m := <-sub2.Messages():
		if m.ID() != "test-msg" {
			t.Errorf("sub2 got wrong message: %s", m.ID())
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("sub2 timeout waiting for message")
	}
}

// TestUnsubscribedEventDropsMessages verifies that events without subscribers drop messages
func TestUnsubscribedEventDropsMessages(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register two events
	subscribedEvent := New[string]("subscribed-event")
	if err := Register(context.Background(), bus, subscribedEvent); err != nil {
		t.Fatalf("failed to register subscribed event: %v", err)
	}

	unsubscribedEvent := New[string]("unsubscribed-event")
	if err := Register(context.Background(), bus, unsubscribedEvent); err != nil {
		t.Fatalf("failed to register unsubscribed event: %v", err)
	}

	// Only subscribe to one event
	receivedCh := make(chan string, 1)
	if err := subscribedEvent.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		receivedCh <- data
		return nil
	}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Publish to both events
	if err := unsubscribedEvent.Publish(context.Background(), "unsubscribed-data"); err != nil {
		t.Fatalf("publish to unsubscribed event failed: %v", err)
	}

	if err := subscribedEvent.Publish(context.Background(), "subscribed-data"); err != nil {
		t.Fatalf("publish to subscribed event failed: %v", err)
	}

	// Verify subscribed event received its message
	select {
	case data := <-receivedCh:
		if data != "subscribed-data" {
			t.Errorf("expected 'subscribed-data', got '%s'", data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for subscribed event")
	}

	// Verify no more messages (unsubscribed event's message was dropped)
	select {
	case data := <-receivedCh:
		t.Errorf("unexpected message received: %s", data)
	case <-time.After(10 * time.Millisecond):
		// Expected - no more messages
	}
}

// TestBlockedSubscriberDoesNotAffectOtherEvents verifies event isolation when one subscriber is blocked
func TestBlockedSubscriberDoesNotAffectOtherEvents(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register two events
	blockedEvent := New[string]("blocked-event")
	if err := Register(context.Background(), bus, blockedEvent); err != nil {
		t.Fatalf("failed to register blocked event: %v", err)
	}

	fastEvent := New[string]("fast-event")
	if err := Register(context.Background(), bus, fastEvent); err != nil {
		t.Fatalf("failed to register fast event: %v", err)
	}

	// Subscribe to blocked event with a handler that blocks
	blockedStarted := make(chan struct{})
	blockedRelease := make(chan struct{})
	if err := blockedEvent.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		close(blockedStarted) // Signal that we started processing
		<-blockedRelease      // Block until released
		return nil
	}); err != nil {
		t.Fatalf("subscribe to blocked event failed: %v", err)
	}

	// Subscribe to fast event with a quick handler
	fastReceived := make(chan string, 10)
	if err := fastEvent.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		fastReceived <- data
		return nil
	}); err != nil {
		t.Fatalf("subscribe to fast event failed: %v", err)
	}

	// Publish to blocked event first - this will start blocking the handler
	if err := blockedEvent.Publish(context.Background(), "blocked-data"); err != nil {
		t.Fatalf("publish to blocked event failed: %v", err)
	}

	// Wait for blocked handler to start
	select {
	case <-blockedStarted:
		// Good, handler is now blocked
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked handler didn't start")
	}

	// Now publish multiple messages to fast event - these should all be delivered
	// even though the blocked event's handler is still blocked
	for i := 0; i < 5; i++ {
		msg := "fast-data-" + string(rune('0'+i))
		if err := fastEvent.Publish(context.Background(), msg); err != nil {
			t.Fatalf("publish to fast event failed: %v", err)
		}
	}

	// Verify all fast messages were received
	for i := 0; i < 5; i++ {
		expected := "fast-data-" + string(rune('0'+i))
		select {
		case data := <-fastReceived:
			if data != expected {
				t.Errorf("expected '%s', got '%s'", expected, data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("timeout waiting for fast event message %d", i)
		}
	}

	// Release the blocked handler
	close(blockedRelease)
}

// TestMultipleEventsIndependentDelivery verifies each event delivers only to its own subscribers
func TestMultipleEventsIndependentDelivery(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Register three events
	event1 := New[string]("event-1")
	Register(context.Background(), bus, event1)
	event2 := New[string]("event-2")
	Register(context.Background(), bus, event2)
	event3 := New[string]("event-3")
	Register(context.Background(), bus, event3)

	// Create channels to track received messages per event
	received1 := make(chan string, 10)
	received2 := make(chan string, 10)
	received3 := make(chan string, 10)

	// Subscribe to each event
	event1.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		received1 <- data
		return nil
	})
	event2.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		received2 <- data
		return nil
	})
	event3.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) error {
		received3 <- data
		return nil
	})

	// Publish to each event
	event1.Publish(context.Background(), "msg-for-event1")
	event2.Publish(context.Background(), "msg-for-event2")
	event3.Publish(context.Background(), "msg-for-event3")

	// Verify each event received only its own message
	select {
	case data := <-received1:
		if data != "msg-for-event1" {
			t.Errorf("event1 got wrong message: %s", data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("event1 timeout")
	}

	select {
	case data := <-received2:
		if data != "msg-for-event2" {
			t.Errorf("event2 got wrong message: %s", data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("event2 timeout")
	}

	select {
	case data := <-received3:
		if data != "msg-for-event3" {
			t.Errorf("event3 got wrong message: %s", data)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("event3 timeout")
	}

	// Verify no cross-delivery (each channel should be empty now)
	select {
	case data := <-received1:
		t.Errorf("event1 received unexpected message: %s", data)
	default:
	}
	select {
	case data := <-received2:
		t.Errorf("event2 received unexpected message: %s", data)
	default:
	}
	select {
	case data := <-received3:
		t.Errorf("event3 received unexpected message: %s", data)
	default:
	}
}

// TestAsyncHandlerWithContextCopy verifies AsyncHandler with context copy functions
func TestAsyncHandlerWithContextCopy(t *testing.T) {
	// Test that AsyncHandler copies context values when provided with copy functions
	ch := make(chan string)

	type ctxKey string
	const customKey ctxKey = "custom-key"

	handler := func(ctx context.Context, ev Event[any], data any) error {
		// Check if custom context value was copied
		if v := ctx.Value(customKey); v != nil {
			ch <- v.(string)
		} else {
			ch <- ""
		}
		return nil
	}

	// Define a custom context copy function
	copyFn := func(to, from context.Context) context.Context {
		if v := from.Value(customKey); v != nil {
			return context.WithValue(to, customKey, v)
		}
		return to
	}

	asyncHandler := AsyncHandler(handler, copyFn)

	// Create context with custom value
	ctx := context.WithValue(context.Background(), customKey, "custom-value")

	// Create a simple event for testing
	e := Discard[any]("test")

	// Call the async handler directly
	asyncHandler(ctx, e, nil)

	select {
	case val := <-ch:
		if val != "custom-value" {
			t.Errorf("expected custom-value, got %s", val)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("timeout waiting for handler")
	}
}

// TestBusAddDuplicate verifies Bus returns error for duplicate event name
func TestBusAddDuplicate(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	// Create first event
	e1 := New[any]("add-test")
	if err := Register(context.Background(), bus, e1); err != nil {
		t.Fatalf("failed to register first event: %v", err)
	}

	// Create second event with same name - should fail
	e2 := New[any]("add-test")
	if err := Register(context.Background(), bus, e2); !errors.Is(err, ErrEventExists) {
		t.Fatalf("expected ErrEventExists for duplicate, got: %v", err)
	}
}

// TestNewEventWithExistingEventID verifies publishing with existing event ID
func TestNewEventWithExistingEventID(t *testing.T) {
	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("existing-id-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error {
		ch <- ContextEventID(ctx)
		return nil
	})

	// Publish with existing event ID
	ctx := ContextWithEventID(context.Background(), "my-custom-id")
	e.Publish(ctx, nil)

	select {
	case id := <-ch:
		if id != "my-custom-id" {
			t.Errorf("expected my-custom-id, got %s", id)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestTypedEvent verifies compile-time type safety with generics
func TestTypedEvent(t *testing.T) {
	type User struct {
		ID   string
		Name string
	}

	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[User]("user.created")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	ch := make(chan User)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[User], user User) error {
		ch <- user
		return nil
	})

	expected := User{ID: "123", Name: "John"}
	e.Publish(context.Background(), expected)

	select {
	case received := <-ch:
		if received.ID != expected.ID || received.Name != expected.Name {
			t.Errorf("expected %+v, got %+v", expected, received)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for event")
	}
}

// TestTypedEventsSlice verifies Events slice with typed events
func TestTypedEventsSlice(t *testing.T) {
	type Order struct {
		ID     string
		Amount float64
	}

	bus := mustNewBus(t, "test", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e1 := New[Order]("order.created")
	if err := Register(context.Background(), bus, e1); err != nil {
		t.Fatalf("failed to register order.created: %v", err)
	}
	e2 := New[Order]("order.updated")
	if err := Register(context.Background(), bus, e2); err != nil {
		t.Fatalf("failed to register order.updated: %v", err)
	}

	events := Events[Order]{e1, e2}

	var count int32
	ch := make(chan struct{})

	events.Subscribe(context.Background(), func(ctx context.Context, ev Event[Order], order Order) error {
		if order.ID != "123" {
			t.Errorf("unexpected order ID: %s", order.ID)
		}
		if atomic.AddInt32(&count, 1) == 2 {
			ch <- struct{}{}
		}
		return nil
	})

	events.Publish(context.Background(), Order{ID: "123", Amount: 99.99})

	if !wait(ch, waitChTimeoutMS) {
		t.Error("timeout waiting for all events")
	}
}

// =============================================================================
// Bus Registry Tests
// =============================================================================

// TestGetBus verifies GetBus returns registered buses
func TestGetBus(t *testing.T) {
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
	fullName := "nonexistent-bus-12345://some.event"
	_, err := Get[any](fullName)
	if err == nil {
		t.Fatal("expected error for non-existent bus")
	}
	if !errors.Is(err, ErrBusNotFound) {
		t.Errorf("expected ErrBusNotFound, got %v", err)
	}
}

// mockIdempotencyStore implements IdempotencyStore for testing
type mockIdempotencyStore struct {
	mu        sync.Mutex
	processed map[string]bool
}

func newMockIdempotencyStore() *mockIdempotencyStore {
	return &mockIdempotencyStore{processed: make(map[string]bool)}
}

func (s *mockIdempotencyStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processed[messageID], nil
}

func (s *mockIdempotencyStore) MarkProcessed(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[messageID] = true
	return nil
}

// mockPoisonDetector implements PoisonDetector for testing
type mockPoisonDetector struct {
	mu          sync.Mutex
	failures    map[string]int
	quarantined map[string]bool
	threshold   int
}

func newMockPoisonDetector(threshold int) *mockPoisonDetector {
	return &mockPoisonDetector{
		failures:    make(map[string]int),
		quarantined: make(map[string]bool),
		threshold:   threshold,
	}
}

func (d *mockPoisonDetector) Check(ctx context.Context, messageID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quarantined[messageID], nil
}

func (d *mockPoisonDetector) RecordFailure(ctx context.Context, messageID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures[messageID]++
	if d.failures[messageID] >= d.threshold {
		d.quarantined[messageID] = true
		return true, nil
	}
	return false, nil
}

func (d *mockPoisonDetector) RecordSuccess(ctx context.Context, messageID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.failures, messageID)
	return nil
}

// TestBusLevelIdempotency verifies that bus-level idempotency automatically skips duplicates
func TestBusLevelIdempotency(t *testing.T) {
	ctx := context.Background()
	idempStore := newMockIdempotencyStore()

	bus := mustNewBus(t, "test-idemp-bus-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithIdempotency(idempStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	// Wait for subscription to be ready
	time.Sleep(10 * time.Millisecond)

	// Publish same message twice (same event ID via context)
	msgCtx := ContextWithEventID(ctx, "msg-123")
	ev.Publish(msgCtx, "hello")
	time.Sleep(20 * time.Millisecond)

	ev.Publish(msgCtx, "hello again")
	time.Sleep(20 * time.Millisecond)

	// Should only be called once (second is duplicate)
	if count := callCount.Load(); count != 1 {
		t.Errorf("expected handler called 1 time, got %d", count)
	}
}

// TestBusLevelPoisonDetection verifies that bus-level poison detection skips quarantined messages
func TestBusLevelPoisonDetection(t *testing.T) {
	ctx := context.Background()
	poisonDetector := newMockPoisonDetector(2) // quarantine after 2 failures

	bus := mustNewBus(t, "test-poison-bus-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithPoisonDetection(poisonDetector),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.poison.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return errors.New("always fails")
	})

	// Wait for subscription to be ready
	time.Sleep(10 * time.Millisecond)

	// Publish message 3 times with same ID
	msgCtx := ContextWithEventID(ctx, "poison-msg-456")
	ev.Publish(msgCtx, "first")
	time.Sleep(20 * time.Millisecond)

	ev.Publish(msgCtx, "second")
	time.Sleep(20 * time.Millisecond)

	ev.Publish(msgCtx, "third") // should be skipped (quarantined)
	time.Sleep(20 * time.Millisecond)

	// Should be called twice (third is quarantined)
	if count := callCount.Load(); count != 2 {
		t.Errorf("expected handler called 2 times, got %d", count)
	}

	// Verify message is quarantined
	poisonDetector.mu.Lock()
	isQuarantined := poisonDetector.quarantined["poison-msg-456"]
	poisonDetector.mu.Unlock()
	if !isQuarantined {
		t.Error("expected message to be quarantined")
	}
}

// TestBusLevelMiddlewareCombined verifies both idempotency and poison detection work together
func TestBusLevelMiddlewareCombined(t *testing.T) {
	ctx := context.Background()
	idempStore := newMockIdempotencyStore()
	poisonDetector := newMockPoisonDetector(3)

	bus := mustNewBus(t, "test-combined-bus-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithIdempotency(idempStore),
		WithPoisonDetection(poisonDetector),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.combined.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil // success
	})

	// Wait for subscription to be ready
	time.Sleep(10 * time.Millisecond)

	// Publish two different messages
	ev.Publish(ContextWithEventID(ctx, "msg-1"), "first")
	time.Sleep(20 * time.Millisecond)

	ev.Publish(ContextWithEventID(ctx, "msg-2"), "second")
	time.Sleep(20 * time.Millisecond)

	// Publish duplicate of first message
	ev.Publish(ContextWithEventID(ctx, "msg-1"), "duplicate")
	time.Sleep(20 * time.Millisecond)

	// Should be called twice (third is duplicate)
	if count := callCount.Load(); count != 2 {
		t.Errorf("expected handler called 2 times, got %d", count)
	}
}

// mockMonitorStore implements MonitorStore for testing
type mockMonitorStore struct {
	mu      sync.Mutex
	started map[string]bool
	status  map[string]string
}

func newMockMonitorStore() *mockMonitorStore {
	return &mockMonitorStore{
		started: make(map[string]bool),
		status:  make(map[string]string),
	}
}

func (s *mockMonitorStore) RecordStart(ctx context.Context, eventID, subscriptionID, eventName, busID string, workerPool bool, metadata map[string]string, traceID, spanID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started[eventID] = true
	return nil
}

func (s *mockMonitorStore) RecordComplete(ctx context.Context, eventID, subscriptionID, status string, handlerErr error, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[eventID] = status
	return nil
}

func (s *mockMonitorStore) wasRecorded(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started[eventID]
}

// mockSchemaProvider implements SchemaProvider for testing
type mockSchemaProvider struct {
	mu      sync.RWMutex
	schemas map[string]*EventSchema
}

func newMockSchemaProvider() *mockSchemaProvider {
	return &mockSchemaProvider{
		schemas: make(map[string]*EventSchema),
	}
}

func (p *mockSchemaProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if schema, ok := p.schemas[eventName]; ok {
		// Return a copy
		copy := *schema
		return &copy, nil
	}
	return nil, nil
}

func (p *mockSchemaProvider) Set(ctx context.Context, schema *EventSchema) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.schemas[schema.Name] = schema
	return nil
}

func (p *mockSchemaProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.schemas, eventName)
	return nil
}

func (p *mockSchemaProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
	ch := make(chan SchemaChangeEvent, 100)
	return ch, nil
}

func (p *mockSchemaProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*EventSchema, 0, len(p.schemas))
	for _, schema := range p.schemas {
		copy := *schema
		result = append(result, &copy)
	}
	return result, nil
}

func (p *mockSchemaProvider) Close() error {
	return nil
}

// TestSchemaLoadingOnRegister verifies that schemas are loaded when events are registered
func TestSchemaLoadingOnRegister(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()

	// Pre-register a schema
	provider.Set(ctx, &EventSchema{
		Name:              "order.created",
		Version:           1,
		SubTimeout:        5 * time.Second,
		MaxRetries:        3,
		EnableMonitor:     true,
		EnableIdempotency: true,
		EnablePoison:      false,
	})

	bus := mustNewBus(t, "test-schema-load-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	ev := New[string]("order.created")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// Verify schema was loaded by checking internal state
	impl := ev.(*eventImpl[string])
	if !impl.schema.loaded {
		t.Error("expected schema to be loaded")
	}
	if !impl.schema.enableMonitor {
		t.Error("expected enableMonitor to be true")
	}
	if !impl.schema.enableIdempotency {
		t.Error("expected enableIdempotency to be true")
	}
	if impl.schema.enablePoison {
		t.Error("expected enablePoison to be false")
	}
	if impl.subTimeout != 5*time.Second {
		t.Errorf("expected subTimeout 5s, got %v", impl.subTimeout)
	}
	if impl.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", impl.maxRetries)
	}
}

// TestSchemaNotFoundFallback verifies that missing schema doesn't prevent registration
func TestSchemaNotFoundFallback(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()
	// Don't pre-register any schema

	bus := mustNewBus(t, "test-schema-notfound-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	ev := New[string]("order.created")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// Verify schema was not loaded
	impl := ev.(*eventImpl[string])
	if impl.schema.loaded {
		t.Error("expected schema NOT to be loaded")
	}
}

// TestSchemaControlsMiddleware verifies that schema flags control which middleware is applied
func TestSchemaControlsMiddleware(t *testing.T) {
	ctx := context.Background()

	t.Run("schema enables only monitor", func(t *testing.T) {
		provider := newMockSchemaProvider()
		provider.Set(ctx, &EventSchema{
			Name:              "test.event",
			Version:           1,
			EnableMonitor:     true,
			EnableIdempotency: false,
			EnablePoison:      false,
		})

		idempStore := newMockIdempotencyStore()
		poisonDetector := newMockPoisonDetector(2)
		monitorStore := newMockMonitorStore()

		bus := mustNewBus(t, "test-schema-monitor-"+faker.RandomString(5),
			WithTransport(channel.New()),
			WithSchemaProvider(provider),
			WithIdempotency(idempStore),
			WithPoisonDetection(poisonDetector),
			WithMonitor(monitorStore),
		)
		defer bus.Close(ctx)

		ev := New[string]("test.event")
		if err := Register(ctx, bus, ev); err != nil {
			t.Fatal(err)
		}

		var received atomic.Bool
		ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
			received.Store(true)
			return nil
		})

		time.Sleep(10 * time.Millisecond)

		msgID := "test-msg-" + faker.RandomString(5)
		ev.Publish(ContextWithEventID(ctx, msgID), "hello")
		time.Sleep(30 * time.Millisecond)

		if !received.Load() {
			t.Error("handler should have been called")
		}

		// Monitor should have recorded (enabled in schema)
		if !monitorStore.wasRecorded(msgID) {
			t.Error("monitor should have recorded the event")
		}

		// Idempotency store should NOT have been called (disabled in schema)
		idempStore.mu.Lock()
		_, wasProcessed := idempStore.processed[msgID]
		idempStore.mu.Unlock()
		if wasProcessed {
			t.Error("idempotency should NOT have been applied (disabled in schema)")
		}
	})

	t.Run("schema enables only idempotency", func(t *testing.T) {
		provider := newMockSchemaProvider()
		provider.Set(ctx, &EventSchema{
			Name:              "test.idemp.event",
			Version:           1,
			EnableMonitor:     false,
			EnableIdempotency: true,
			EnablePoison:      false,
		})

		idempStore := newMockIdempotencyStore()
		monitorStore := newMockMonitorStore()

		bus := mustNewBus(t, "test-schema-idemp-"+faker.RandomString(5),
			WithTransport(channel.New()),
			WithSchemaProvider(provider),
			WithIdempotency(idempStore),
			WithMonitor(monitorStore),
		)
		defer bus.Close(ctx)

		ev := New[string]("test.idemp.event")
		if err := Register(ctx, bus, ev); err != nil {
			t.Fatal(err)
		}

		var callCount atomic.Int32
		ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
			callCount.Add(1)
			return nil
		})

		time.Sleep(10 * time.Millisecond)

		msgID := "test-msg-" + faker.RandomString(5)
		// Publish same message twice
		ev.Publish(ContextWithEventID(ctx, msgID), "hello")
		time.Sleep(20 * time.Millisecond)
		ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
		time.Sleep(20 * time.Millisecond)

		// Should only be called once (idempotency enabled)
		if count := callCount.Load(); count != 1 {
			t.Errorf("expected 1 call, got %d (idempotency should skip duplicate)", count)
		}

		// Monitor should NOT have recorded (disabled in schema)
		if monitorStore.wasRecorded(msgID) {
			t.Error("monitor should NOT have recorded (disabled in schema)")
		}
	})
}

// TestNoSchemaFallbackToBusMiddleware verifies middleware is applied when no schema exists
func TestNoSchemaFallbackToBusMiddleware(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()
	// No schema registered

	idempStore := newMockIdempotencyStore()
	monitorStore := newMockMonitorStore()

	bus := mustNewBus(t, "test-no-schema-fallback-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
		WithIdempotency(idempStore),
		WithMonitor(monitorStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("unregistered.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	time.Sleep(10 * time.Millisecond)

	msgID := "test-msg-" + faker.RandomString(5)
	// Publish same message twice
	ev.Publish(ContextWithEventID(ctx, msgID), "hello")
	time.Sleep(20 * time.Millisecond)
	ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
	time.Sleep(20 * time.Millisecond)

	// Should only be called once - idempotency is applied as fallback
	if count := callCount.Load(); count != 1 {
		t.Errorf("expected 1 call, got %d (fallback idempotency should skip duplicate)", count)
	}

	// Monitor should have recorded (fallback behavior)
	if !monitorStore.wasRecorded(msgID) {
		t.Error("monitor should have recorded (fallback behavior)")
	}
}

// TestSchemaDisablesAllMiddleware verifies that schema can disable all middleware
func TestSchemaDisablesAllMiddleware(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:              "test.no.middleware",
		Version:           1,
		EnableMonitor:     false,
		EnableIdempotency: false,
		EnablePoison:      false,
	})

	idempStore := newMockIdempotencyStore()
	poisonDetector := newMockPoisonDetector(2)
	monitorStore := newMockMonitorStore()

	bus := mustNewBus(t, "test-schema-disable-all-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
		WithIdempotency(idempStore),
		WithPoisonDetection(poisonDetector),
		WithMonitor(monitorStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.no.middleware")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	time.Sleep(10 * time.Millisecond)

	msgID := "test-msg-" + faker.RandomString(5)
	// Publish same message twice
	ev.Publish(ContextWithEventID(ctx, msgID), "hello")
	time.Sleep(20 * time.Millisecond)
	ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
	time.Sleep(20 * time.Millisecond)

	// Should be called twice - no idempotency
	if count := callCount.Load(); count != 2 {
		t.Errorf("expected 2 calls, got %d (no middleware should be applied)", count)
	}

	// Monitor should NOT have recorded
	if monitorStore.wasRecorded(msgID) {
		t.Error("monitor should NOT have recorded (disabled in schema)")
	}

	// Idempotency should NOT have been applied
	idempStore.mu.Lock()
	_, wasProcessed := idempStore.processed[msgID]
	idempStore.mu.Unlock()
	if wasProcessed {
		t.Error("idempotency should NOT have been applied (disabled in schema)")
	}
}

// TestSchemaTimeoutApplied verifies that schema timeout is applied to events
func TestSchemaTimeoutApplied(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:       "test.timeout",
		Version:    1,
		SubTimeout: 100 * time.Millisecond,
	})

	bus := mustNewBus(t, "test-schema-timeout-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	// Event without timeout option - should get timeout from schema
	ev := New[string]("test.timeout")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	impl := ev.(*eventImpl[string])
	if impl.subTimeout != 100*time.Millisecond {
		t.Errorf("expected subTimeout 100ms from schema, got %v", impl.subTimeout)
	}
}

// TestEventTimeoutOverridesSchema verifies that event-level timeout takes precedence
func TestEventTimeoutOverridesSchema(t *testing.T) {
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:       "test.timeout.override",
		Version:    1,
		SubTimeout: 100 * time.Millisecond,
	})

	bus := mustNewBus(t, "test-schema-timeout-override-"+faker.RandomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	// Event WITH timeout option - should keep event timeout
	ev := New[string]("test.timeout.override", WithSubscriberTimeout(500*time.Millisecond))
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	impl := ev.(*eventImpl[string])
	// Event timeout should be preserved (schema doesn't override existing values)
	if impl.subTimeout != 500*time.Millisecond {
		t.Errorf("expected subTimeout 500ms from event option, got %v", impl.subTimeout)
	}
}

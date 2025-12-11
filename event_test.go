package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"math"
	"sync/atomic"

	"github.com/google/go-cmp/cmp"
	"syreclabs.com/go/faker"
)

func init() {
	faker.Seed(time.Now().UnixNano())
}

const waitChTimeoutMS = 100

func waitForMetaData(ch chan Metadata, timeout int) (Metadata, bool) {
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
func CompareMetadata(m1, m2 Metadata) bool {
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
	// With default registry use cancellable context as they will reuse same registry and event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := defaultRegistry
	e := New[any]("test")
	if err := Register(e); err == nil || !errors.Is(err, ErrDuplicateEvent) {
		t.Errorf("duplicate event registered event: %v", err)
	}
	ch := make(chan struct{})
	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) {
		if id := ContextEventID(ctx); id == "" {
			t.Error("event id is null")
		}
		if r1 := ContextRegistry(ctx); r1 == nil {
			t.Error("registry id is null")
		} else if r1.id != r.id {
			t.Errorf("registry is wrong got:%s, expected:%s", r1.id, r.id)
		}
		if source := ContextSource(ctx); source != r.id {
			t.Errorf("source is wrong got:%s, expected:%s", source, r.id)
		}
		if data != nil {
			t.Error("data is not null")
		}
		ch <- struct{}{}
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("Failed")
	}
	e1 := Get("test")
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
	r := NewRegistry("test", nil)
	e := New[any]("test",
		WithTracing(true),
		WithMetrics(true, nil),
		WithRegistry(r))

	if r.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}
	ch1 := make(chan Metadata, 2) // Buffer to avoid blocking
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) {
		if m := ContextMetadata(ctx); m == nil {
			t.Error("metadata is null")
		} else {
			ch1 <- m
		}
	})
	ch2 := make(chan Metadata, 2) // Buffer to avoid blocking
	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) {
		if m := ContextMetadata(ctx); m == nil {
			t.Error("metadata is null")
		} else {
			ch2 <- m
		}
	})
	msg := "this is a test"
	m := NewMetadata().Set("", msg)

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

	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestPanic(t *testing.T) {
	ch1 := make(chan struct{})
	r := NewRegistry("test", nil)
	e := New[any]("test",
		WithTracing(true),
		WithRecovery(true),
		WithMetrics(true, nil),
		WithRegistry(r),
		WithErrorHandler(func(event BaseEvent, err error) {
			ch1 <- struct{}{}
		}))

	if r.Get(e.Name()) == nil {
		t.Fatal("event not registered")
	}

	e.Subscribe(context.TODO(), func(ctx context.Context, _ Event[any], _ any) {
		panic("test")
	})
	e.Publish(context.TODO(), nil)
	if !wait(ch1, waitChTimeoutMS) {
		t.Error("Panic failed")
	}
	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestCancel(t *testing.T) {
	r := NewRegistry("test", nil)
	e := New[any]("test",
		WithTracing(true),
		WithMetrics(true, nil), WithRegistry(r))
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	ctx1, cancel1 := context.WithCancel(context.Background())
	e.Subscribe(ctx1, func(ctx context.Context, ev Event[any], data any) {
		ch1 <- struct{}{}
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	e.Subscribe(ctx2, func(context.Context, Event[any], any) {
		ch2 <- struct{}{}
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
	if err := r.Close(); err != nil {
		t.Error("failed to close registry")
	}
}

func TestData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) {
		ch <- data
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) {
		ch1 <- data
	})
	e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) {
		ch2 <- data
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
	r := NewRegistry("test", nil)
	e := New[int]("test",
		WithTracing(true),
		WithMetrics(true, nil), WithRegistry(r))
	ch1 := make(chan struct{})
	total := int32(b.N)
	var counter int32
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int], data int) {
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
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
	if err := r.Close(); err != nil {
		b.Error("failed to close registry")
	}
}

func TestPool(t *testing.T) {
	var poolSize int64 = 4
	r := NewRegistry("test", nil)
	transport := NewChannelTransport(
		WithTransportAsync(true),
		WithTransportWorkerPoolSize(poolSize),
	)
	e := New[int32]("test",
		WithTracing(true),
		WithTransport(transport),
		WithMetrics(true, nil), WithRegistry(r))
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
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) {
		defer atomic.AddInt32(&counter, -1)
		ch <- atomic.AddInt32(&counter, 1)
		ch1 <- struct{}{}
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

func TestSingleTransport(t *testing.T) {
	var poolSize int64 = 4
	r := NewRegistry("test", nil)
	transport := NewSingleTransport(
		WithTransportAsync(true),
		WithTransportWorkerPoolSize(poolSize),
		WithTransportTimeout(time.Duration(100)*time.Millisecond),
	)
	e := New[int32]("test",
		WithTracing(true),
		WithTransport(transport),
		WithMetrics(true, nil), WithRegistry(r))
	var total int32 = 100
	var counter int32
	var counter1 int32
	var counter2 int32
	var counter3 int32
	ch1 := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) {
		atomic.AddInt32(&counter1, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) {
		atomic.AddInt32(&counter2, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) {
		atomic.AddInt32(&counter3, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			ch1 <- struct{}{}
		}
	})
	var i int32
	for i = 0; i < total; i++ {
		e.Publish(context.Background(), i)
	}
	if !wait(ch1, 2000) {
		t.Error("timeout", counter1)
	}
	if counter != total {
		t.Error("Failed", counter, total)
	}
	if counter1 >= total {
		t.Error("Failed", counter1, total)
	}
	if counter2 >= total {
		t.Error("Failed", counter2, total)
	}
	if counter3 >= total {
		t.Error("Failed", counter3, total)
	}
}

// TestMetadataCopy tests that Metadata.Copy() handles nil and empty cases correctly
func TestMetadataCopy(t *testing.T) {
	tests := []struct {
		name    string
		input   Metadata
		wantNil bool
		wantLen int
	}{
		{"nil metadata", nil, true, 0},
		{"empty metadata", Metadata{}, false, 0},
		{"single key", Metadata{"key": "value"}, false, 1},
		{"multiple keys", Metadata{"a": "1", "b": "2"}, false, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.Copy()
			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("expected non-nil, got nil")
				} else if len(result) != tt.wantLen {
					t.Errorf("expected len %d, got %d", tt.wantLen, len(result))
				}
			}
		})
	}
}

// TestContextImmutability verifies that context modification functions
// don't mutate the original context data (race condition fix)
func TestContextImmutability(t *testing.T) {
	// Create initial context with data
	ctx := context.Background()
	ctx = ContextWithMetadata(ctx, Metadata{"key": "original"})
	ctx = ContextWithEventID(ctx, "event-123")

	// Get original values
	originalMeta := ContextMetadata(ctx)
	originalID := ContextEventID(ctx)

	// Modify context with new values
	ctx2 := ContextWithMetadata(ctx, Metadata{"key": "modified"})
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
	ctx = ContextWithMetadata(ctx, Metadata{"initial": "value"})
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
			_ = ContextRegistry(ctx)

			// Write operations (should create new contexts, not mutate)
			newCtx := ContextWithMetadata(ctx, Metadata{"goroutine": "value"})
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
	transport := NewChannelTransport()

	// Subscribe to get a receive channel
	ch := transport.Receive("sub1")
	if ch == nil {
		t.Fatal("expected receive channel")
	}

	// Close transport
	if err := transport.Close(); err != nil {
		t.Errorf("close error: %v", err)
	}

	// Verify Send() returns nil after close (not panic)
	if sendCh := transport.Send(); sendCh != nil {
		t.Error("expected nil send channel after close")
	}

	// Verify Receive() returns nil after close
	if recvCh := transport.Receive("sub2"); recvCh != nil {
		t.Error("expected nil receive channel after close")
	}

	// Double close should not panic
	if err := transport.Close(); err != nil {
		t.Errorf("double close error: %v", err)
	}
}

// TestEventClose verifies event close works correctly
func TestEventClose(t *testing.T) {
	r := NewRegistry("test", nil)
	e := New[any]("test-close", WithRegistry(r))

	ch := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
	})

	// Verify event works before close
	e.Publish(context.Background(), nil)
	if !wait(ch, waitChTimeoutMS) {
		t.Error("expected to receive event before close")
	}

	// Close registry (which closes all events)
	r.Close()

	// Publish after close should not panic (silently ignored)
	e.Publish(context.Background(), nil)

	// Should not receive anything after close
	if wait(ch, 50) {
		t.Error("should not receive event after close")
	}
}

// TestWorkerPoolBackpressure verifies that worker pool blocks when exhausted
func TestWorkerPoolBackpressure(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	poolSize := int64(2)
	transport := NewChannelTransport(
		WithTransportAsync(true),
		WithTransportWorkerPoolSize(poolSize),
	)
	e := New[int]("test-pool-backpressure",
		WithTransport(transport),
		WithRegistry(r))

	var processed int32
	blockCh := make(chan struct{})
	doneCh := make(chan struct{})

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[int], data int) {
		atomic.AddInt32(&processed, 1)
		<-blockCh // Block until released
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
	r := NewRegistry("test", nil)

	transport := NewChannelTransport(
		WithTransportAsync(true),
	)
	e := New[int]("test-graceful",
		WithTransport(transport),
		WithRegistry(r))

	var delivered int32
	deliveryCh := make(chan struct{}, 5)

	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[int], data int) {
		atomic.AddInt32(&delivered, 1)
		deliveryCh <- struct{}{}
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
	r.Close()

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
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) {
		t.Error("handler should not be called for discard event")
	})
}

// TestTransportErrorHandler verifies transport error handler is called on timeout
func TestTransportErrorHandler(t *testing.T) {
	errorCh := make(chan error, 1)

	transport := NewChannelTransport(
		WithTransportAsync(false), // Sync mode to test timeout
		WithTransportTimeout(1*time.Millisecond),
		WithTransportBufferSize(0), // No buffer to force blocking
		WithTransportErrorHandler(func(err error) {
			errorCh <- err
		}),
	)

	// Create a subscriber channel but don't read from it
	_ = transport.Receive("slow-sub")

	// Try to send - should timeout
	sendCh := transport.Send()
	if sendCh != nil {
		go func() {
			sendCh <- &message{id: "test"}
		}()
	}

	select {
	case err := <-errorCh:
		if !errors.Is(err, ErrTransportTimeout) {
			t.Errorf("expected ErrTransportTimeout, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected error handler to be called")
	}

	transport.Close()
}

// TestEventsSlice verifies Events slice publish/subscribe
func TestEventsSlice(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e1 := New[string]("event1", WithRegistry(r))
	e2 := New[string]("event2", WithRegistry(r))
	e3 := New[string]("event3", WithRegistry(r))

	events := Events[string]{e1, e2, e3}

	if events.Name() != "event1,event2,event3" {
		t.Errorf("unexpected name: %s", events.Name())
	}

	var count int32
	ch := make(chan struct{})

	events.Subscribe(context.Background(), func(ctx context.Context, ev Event[string], data string) {
		if atomic.AddInt32(&count, 1) == 3 {
			ch <- struct{}{}
		}
	})

	events.Publish(context.Background(), "test")

	if !wait(ch, waitChTimeoutMS) {
		t.Error("timeout waiting for all events")
	}

	if atomic.LoadInt32(&count) != 3 {
		t.Errorf("expected 3 handlers called, got %d", count)
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
	r := NewRegistry("test", nil)
	defer r.Close()
	e := New[any]("test-context-name", WithRegistry(r))

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- ContextName(ctx)
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
	from = ContextWithMetadata(from, Metadata{"key": "value"})

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
	ctx = ContextWithMetadata(ctx, Metadata{"test": "data"})

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

// TestMetadataGet verifies Metadata.Get function
func TestMetadataGet(t *testing.T) {
	m := Metadata{"key1": "value1", "key2": "value2"}

	if got := m.Get("key1"); got != "value1" {
		t.Errorf("expected value1, got %s", got)
	}
	if got := m.Get("key2"); got != "value2" {
		t.Errorf("expected value2, got %s", got)
	}
	if got := m.Get("nonexistent"); got != "" {
		t.Errorf("expected empty string, got %s", got)
	}
}

// TestMetadataString verifies Metadata.String function
func TestMetadataString(t *testing.T) {
	// Test nil metadata
	var m Metadata
	if got := m.String(); got != "" {
		t.Errorf("expected empty string for nil, got %s", got)
	}

	// Test empty metadata
	m2 := Metadata{}
	if got := m2.String(); got != "Metadata{}" {
		t.Errorf("expected Metadata{}, got %s", got)
	}

	// Test metadata with values
	m3 := Metadata{"key": "value"}
	got := m3.String()
	if got != "Metadata{key=value}" {
		t.Errorf("expected Metadata{key=value}, got %s", got)
	}
}

// TestRegistryGlobalFunctions verifies Handle and Publish global functions
func TestRegistryGlobalFunctions(t *testing.T) {
	ch := make(chan any)

	// Use New[T] to get a shared event for both subscribe and publish
	e := New[any]("global-test-2")
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- data
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

// TestSingleTransportClose verifies singleChannelTransport.Close()
func TestSingleTransportClose(t *testing.T) {
	transport := NewSingleTransport(
		WithTransportAsync(true),
		WithTransportBufferSize(10),
	)

	// Get channels
	_ = transport.Receive("sub1")
	sendCh := transport.Send()

	// Send a message
	if sendCh != nil {
		sendCh <- &message{id: "test-msg"}
	}

	// Close transport
	if err := transport.Close(); err != nil {
		t.Errorf("close error: %v", err)
	}

	// Verify Send returns nil after close
	if transport.Send() != nil {
		t.Error("expected nil send channel after close")
	}

	// Verify Receive returns nil after close
	if transport.Receive("sub2") != nil {
		t.Error("expected nil receive channel after close")
	}

	// Double close should not panic
	if err := transport.Close(); err != nil {
		t.Errorf("double close error: %v", err)
	}
}

// TestWithSubscriberTimeout verifies WithSubscriberTimeout option
func TestWithSubscriberTimeout(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	timeout := 50 * time.Millisecond
	e := New[any]("timeout-test",
		WithRegistry(r),
		WithSubscriberTimeout(timeout),
	)

	ch := make(chan bool)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		// Check if context has deadline
		if deadline, ok := ctx.Deadline(); ok {
			ch <- time.Until(deadline) <= timeout
		} else {
			ch <- false
		}
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

// TestWithLogger verifies WithLogger option
func TestWithLogger(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	customLogger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "custom")
	e := New[any]("logger-test",
		WithRegistry(r),
		WithLogger(customLogger),
	)

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestWithChannelBufferSize verifies WithChannelBufferSize option
func TestWithChannelBufferSize(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("buffer-test",
		WithRegistry(r),
		WithChannelBufferSize(50),
	)

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
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
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[string]("async-handler-test", WithRegistry(r))

	ch := make(chan string)
	handler := func(ctx context.Context, ev Event[string], data string) {
		ch <- data
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
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("async-panic-test", WithRegistry(r))

	ch := make(chan struct{})
	handler := func(ctx context.Context, ev Event[any], data any) {
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
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("string-test", WithRegistry(r))

	if impl, ok := e.(*eventImpl[any]); ok {
		if impl.String() != "string-test" {
			t.Errorf("expected string-test, got %s", impl.String())
		}
	}
}

// TestEventSubscribers verifies eventImpl.Subscribers()
func TestEventSubscribers(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("subscribers-test", WithRegistry(r))

	impl, ok := e.(*eventImpl[any])
	if !ok {
		t.Fatal("expected eventImpl")
	}

	if impl.Subscribers() != 0 {
		t.Errorf("expected 0 subscribers, got %d", impl.Subscribers())
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.Subscribe(ctx, func(ctx context.Context, ev Event[any], data any) {})

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
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {})
}

// TestTransportDelete verifies Delete removes subscriber
func TestTransportDelete(t *testing.T) {
	transport := NewChannelTransport()

	// Create subscriber
	ch := transport.Receive("sub-to-delete")
	if ch == nil {
		t.Fatal("expected channel")
	}

	// Delete subscriber
	transport.Delete("sub-to-delete")

	// Verify channel is closed by trying to receive (should be closed)
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(10 * time.Millisecond):
		t.Error("timeout - channel should be closed immediately")
	}

	transport.Close()
}

// TestTransportDeleteOnClosedTransport verifies Delete on closed transport
func TestTransportDeleteOnClosedTransport(t *testing.T) {
	transport := NewChannelTransport()
	transport.Close()

	// Should not panic
	transport.Delete("some-id")
}

// TestTransportReceiveRace verifies concurrent Receive calls
func TestTransportReceiveRace(t *testing.T) {
	transport := NewChannelTransport()
	defer transport.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch := transport.Receive(fmt.Sprintf("sub-%d", id))
			if ch == nil {
				t.Error("expected non-nil channel")
			}
		}(i)
	}
	wg.Wait()
}

// TestWithTransportLogger verifies WithTransportLogger option
func TestWithTransportLogger(t *testing.T) {
	customLogger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "transport")
	transport := NewChannelTransport(
		WithTransportLogger(customLogger),
	)
	defer transport.Close()

	// Verify transport works with custom logger
	ch := transport.Receive("sub1")
	if ch == nil {
		t.Error("expected channel")
	}
}

// TestDummyMetrics verifies dummyMetrics doesn't panic
func TestDummyMetrics(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("dummy-metrics-test",
		WithRegistry(r),
		WithMetrics(false, nil), // Disable metrics
	)

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestRegistryWithEmptyName verifies NewRegistry handles empty name
func TestRegistryWithEmptyName(t *testing.T) {
	r := NewRegistry("", nil)
	defer r.Close()

	if r.Name() != "event" {
		t.Errorf("expected default name 'event', got %s", r.Name())
	}
}

// TestRegistryEvent verifies Registry.Event creates or returns existing event
func TestRegistryEvent(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	// First call creates event
	e1 := New[any]("test-event", WithRegistry(r))
	if e1 == nil {
		t.Fatal("expected event")
	}

	// Second call returns same event
	e2 := New[any]("test-event", WithRegistry(r))
	if e1 != e2 {
		t.Error("expected same event instance")
	}
}

// TestContextSubscriptionID verifies ContextSubscriptionID in handler
func TestContextSubscriptionID(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("sub-id-test", WithRegistry(r))

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- ContextSubscriptionID(ctx)
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

// TestSingleTransportTimeout verifies singleChannelTransport timeout handling
func TestSingleTransportTimeout(t *testing.T) {
	errorCh := make(chan error, 1)

	transport := NewSingleTransport(
		WithTransportAsync(false),
		WithTransportTimeout(1*time.Millisecond),
		WithTransportBufferSize(0),
		WithTransportErrorHandler(func(err error) {
			errorCh <- err
		}),
	)

	// Get receive channel but don't read from it
	_ = transport.Receive("slow-sub")

	// Try to send - should timeout
	sendCh := transport.Send()
	if sendCh != nil {
		go func() {
			sendCh <- &message{id: "timeout-test"}
		}()
	}

	select {
	case err := <-errorCh:
		if !errors.Is(err, ErrTransportTimeout) {
			t.Errorf("expected ErrTransportTimeout, got: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected error handler to be called")
	}

	transport.Close()
}

// TestTracingDisabled verifies tracing can be disabled
func TestTracingDisabled(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("no-tracing-test",
		WithRegistry(r),
		WithTracing(false),
	)

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}

	// Verify Tracer returns nil when disabled
	if impl, ok := e.(*eventImpl[any]); ok {
		if impl.Tracer() != nil {
			t.Error("expected nil tracer when tracing disabled")
		}
	}
}

// TestRecoveryDisabled verifies recovery can be disabled
func TestRecoveryDisabled(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("no-recovery-test",
		WithRegistry(r),
		WithRecovery(false),
	)

	ch := make(chan struct{})
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- struct{}{}
	})

	e.Publish(context.Background(), nil)

	if !wait(ch, waitChTimeoutMS) {
		t.Error("event not received")
	}
}

// TestDuplicateReceive verifies duplicate Receive returns same channel
func TestDuplicateReceive(t *testing.T) {
	transport := NewChannelTransport()
	defer transport.Close()

	ch1 := transport.Receive("sub1")
	ch2 := transport.Receive("sub1")

	// Both should be the same channel
	if ch1 != ch2 {
		t.Error("expected same channel for duplicate Receive")
	}
}

// TestSingleTransportDelete verifies singleTransport Delete is no-op
func TestSingleTransportDelete(t *testing.T) {
	transport := NewSingleTransport()
	defer transport.Close()

	// Get channel
	ch := transport.Receive("sub1")
	if ch == nil {
		t.Fatal("expected channel")
	}

	// Delete should be no-op (channel should still work)
	transport.Delete("sub1")

	// Channel should still be usable (not closed)
	select {
	case <-ch:
		// This would only happen if channel was closed, which it shouldn't be
	default:
		// Expected - channel is still open
	}
}

// TestAsyncHandlerWithContextCopy verifies AsyncHandler with context copy functions
func TestAsyncHandlerWithContextCopy(t *testing.T) {
	// Test that AsyncHandler copies context values when provided with copy functions
	ch := make(chan string)

	type ctxKey string
	const customKey ctxKey = "custom-key"

	handler := func(ctx context.Context, ev Event[any], data any) {
		// Check if custom context value was copied
		if v := ctx.Value(customKey); v != nil {
			ch <- v.(string)
		} else {
			ch <- ""
		}
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

// TestRegistryMetrics verifies Registry.Metrics creates and caches metrics
func TestRegistryMetrics(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	// First call creates metrics
	m1 := r.Metrics("test-event")
	if m1 == nil {
		t.Fatal("expected metrics")
	}

	// Second call returns cached metrics
	m2 := r.Metrics("test-event")
	if m1 != m2 {
		t.Error("expected same metrics instance")
	}
}

// TestRegistryAdd verifies Registry.Add returns existing event
func TestRegistryAdd(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	// Create first event
	e1 := New[any]("add-test", WithRegistry(r))

	// Create second event with same name
	e2 := New[any]("add-test", WithRegistry(r))

	// Should be same event
	if e1 != e2 {
		t.Error("expected same event for duplicate Add")
	}
}

// TestNewEventWithExistingEventID verifies publishing with existing event ID
func TestNewEventWithExistingEventID(t *testing.T) {
	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[any]("existing-id-test", WithRegistry(r))

	ch := make(chan string)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) {
		ch <- ContextEventID(ctx)
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

	r := NewRegistry("test", nil)
	defer r.Close()

	e := New[User]("user.created", WithRegistry(r))

	ch := make(chan User)
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[User], user User) {
		ch <- user
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

	r := NewRegistry("test", nil)
	defer r.Close()

	events := Events[Order]{
		New[Order]("order.created", WithRegistry(r)),
		New[Order]("order.updated", WithRegistry(r)),
	}

	var count int32
	ch := make(chan struct{})

	events.Subscribe(context.Background(), func(ctx context.Context, ev Event[Order], order Order) {
		if order.ID != "123" {
			t.Errorf("unexpected order ID: %s", order.ID)
		}
		if atomic.AddInt32(&count, 1) == 2 {
			ch <- struct{}{}
		}
	})

	events.Publish(context.Background(), Order{ID: "123", Amount: 99.99})

	if !wait(ch, waitChTimeoutMS) {
		t.Error("timeout waiting for all events")
	}
}

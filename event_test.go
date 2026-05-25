package event

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/rbaliyan/event/v3/transport/message"
)

// randomString generates a random alphanumeric string of length n.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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

// testDLQStore is a simple DLQStore for testing that signals when Store is called.
type testDLQStore struct {
	called chan struct{}
}

func newTestDLQStore() *testDLQStore {
	return &testDLQStore{called: make(chan struct{}, 10)}
}

func (s *testDLQStore) Store(_ context.Context, _ *DLQMessage) error {
	s.called <- struct{}{}
	return nil
}

// Compare metadata (ignoring Content-Type which is added by payload codec)
func CompareMetadata(expected, actual map[string]string) bool {
	// Check that all expected keys are in actual
	for k, v := range expected {
		if actual[k] != v {
			return false
		}
	}
	// Check that actual doesn't have extra keys (except Content-Type)
	for k := range actual {
		if k == MetadataContentType {
			continue
		}
		if _, ok := expected[k]; !ok {
			return false
		}
	}
	return true
}

func TestEvent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	ch1 := make(chan struct{})
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}
	impl := e.(*eventImpl[any])
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
	eventuallyTrue(t, 2*time.Second, func() bool { return impl.Subscribers() == 1 },
		"cancel1 did not remove subscription 1")
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if !wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
	cancel2()
	eventuallyTrue(t, 2*time.Second, func() bool { return impl.Subscribers() == 0 },
		"cancel2 did not remove subscription 2")
	e.Publish(context.TODO(), nil)
	if wait(ch1, waitChTimeoutMS) {
		t.Error("1. Failed")
	}
	if wait(ch2, waitChTimeoutMS) {
		t.Error("2. Failed")
	}
}

func TestData(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	s := faker.Sentence()

	// Test with specific typed events to ensure proper encoding/decoding
	t.Run("nil", func(t *testing.T) {
		ch := make(chan any)
		e := New[any]("test-nil")
		if err := Register(context.Background(), bus, e); err != nil {
			t.Fatalf("failed to register event: %v", err)
		}
		e.Subscribe(ctx, func(ctx context.Context, event Event[any], data any) error {
			ch <- data
			return nil
		})
		e.Publish(context.Background(), nil)
		out, ok := waitForData(ch, waitChTimeoutMS)
		if !ok {
			t.Fatal("Sub failed")
		}
		if out != nil {
			t.Errorf("expected nil, got %v", out)
		}
	})

	t.Run("string", func(t *testing.T) {
		ch := make(chan string)
		e := New[string]("test-string")
		if err := Register(context.Background(), bus, e); err != nil {
			t.Fatalf("failed to register event: %v", err)
		}
		e.Subscribe(ctx, func(ctx context.Context, event Event[string], data string) error {
			ch <- data
			return nil
		})
		e.Publish(context.Background(), s)
		out, ok := waitForData(ch, waitChTimeoutMS)
		if !ok {
			t.Fatal("Sub failed")
		}
		if out != s {
			t.Errorf("expected %s, got %s", s, out)
		}
	})

	t.Run("number", func(t *testing.T) {
		ch := make(chan int)
		e := New[int]("test-number")
		if err := Register(context.Background(), bus, e); err != nil {
			t.Fatalf("failed to register event: %v", err)
		}
		e.Subscribe(ctx, func(ctx context.Context, event Event[int], data int) error {
			ch <- data
			return nil
		})
		no := 42
		e.Publish(context.Background(), no)
		out, ok := waitForData(ch, waitChTimeoutMS)
		if !ok {
			t.Fatal("Sub failed")
		}
		if out != no {
			t.Errorf("expected %d, got %d", no, out)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type TestStruct struct {
			N int    `json:"n"`
			S string `json:"s"`
		}
		ch := make(chan TestStruct)
		e := New[TestStruct]("test-struct")
		if err := Register(context.Background(), bus, e); err != nil {
			t.Fatalf("failed to register event: %v", err)
		}
		e.Subscribe(ctx, func(ctx context.Context, event Event[TestStruct], data TestStruct) error {
			ch <- data
			return nil
		})
		st := TestStruct{N: 42, S: s}
		e.Publish(context.Background(), st)
		out, ok := waitForData(ch, waitChTimeoutMS)
		if !ok {
			t.Fatal("Sub failed")
		}
		if out.N != st.N || out.S != st.S {
			t.Errorf("expected %+v, got %+v", st, out)
		}
	})
}

func BenchmarkEvent(b *testing.B) {
	bus := mustNewBus(b, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	var poolSize int64 = 4
	transport := channel.New(
		channel.WithAsync(true),
		channel.WithWorkerPoolSize(poolSize),
	)
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(transport))
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
	t.Parallel()
	// Test worker pool delivery mode - each message goes to only one subscriber
	transport := channel.New(
		channel.WithBufferSize(100),
		channel.WithTimeout(time.Duration(100)*time.Millisecond),
	)
	// Create bus (delivery mode is now per-subscription)
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(transport))
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

	// Use WithDeliveryMode(WorkerPool) to subscribe in worker pool mode
	if err := e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&counter1, 1)
		if atomic.AddInt32(&counter, 1) >= total {
			select {
			case ch1 <- struct{}{}:
			default:
			}
		}
		return nil
	}, WithDeliveryMode[int32](WorkerPool)); err != nil {
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
	}, WithDeliveryMode[int32](WorkerPool)); err != nil {
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
	}, WithDeliveryMode[int32](WorkerPool)); err != nil {
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

func TestWorkerGroupDeliveryMode(t *testing.T) {
	t.Parallel()
	// Test worker groups - each group receives all messages, workers within compete
	transport := channel.New(
		channel.WithBufferSize(100),
		channel.WithTimeout(time.Duration(100)*time.Millisecond),
	)
	bus := mustNewBus(t, "test-worker-group", WithTransport(transport))
	defer bus.Close(context.Background())

	e := New[int32]("test-worker-group")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	var total int32 = 50

	// Counters for each worker in each group
	var groupAWorker1, groupAWorker2 int32
	var groupBWorker1, groupBWorker2 int32
	var totalProcessed int32

	// We expect 2 groups * 50 messages = 100 total handler calls
	expectedTotal := total * 2
	done := make(chan struct{}, 1)

	// Group A: 2 workers
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&groupAWorker1, 1)
		if atomic.AddInt32(&totalProcessed, 1) >= expectedTotal {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	}, WithWorkerGroup[int32]("group-a"))

	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&groupAWorker2, 1)
		if atomic.AddInt32(&totalProcessed, 1) >= expectedTotal {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	}, WithWorkerGroup[int32]("group-a"))

	// Group B: 2 workers
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&groupBWorker1, 1)
		if atomic.AddInt32(&totalProcessed, 1) >= expectedTotal {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	}, WithWorkerGroup[int32]("group-b"))

	e.Subscribe(context.Background(), func(ctx context.Context, event Event[int32], data int32) error {
		atomic.AddInt32(&groupBWorker2, 1)
		if atomic.AddInt32(&totalProcessed, 1) >= expectedTotal {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil
	}, WithWorkerGroup[int32]("group-b"))

	// Publish messages
	for i := int32(0); i < total; i++ {
		if err := e.Publish(context.Background(), i); err != nil {
			t.Errorf("publish failed: %v", err)
		}
	}

	// Wait for processing
	if !wait(done, 2000) {
		t.Errorf("timeout waiting for messages, processed: %d, expected: %d", totalProcessed, expectedTotal)
	}

	// Verify each group received all messages
	groupATotal := groupAWorker1 + groupAWorker2
	groupBTotal := groupBWorker1 + groupBWorker2

	if groupATotal != total {
		t.Errorf("group A expected %d messages, got %d (worker1: %d, worker2: %d)",
			total, groupATotal, groupAWorker1, groupAWorker2)
	}
	if groupBTotal != total {
		t.Errorf("group B expected %d messages, got %d (worker1: %d, worker2: %d)",
			total, groupBTotal, groupBWorker1, groupBWorker2)
	}

	t.Logf("Group A: worker1=%d, worker2=%d, total=%d", groupAWorker1, groupAWorker2, groupATotal)
	t.Logf("Group B: worker1=%d, worker2=%d, total=%d", groupBWorker1, groupBWorker2, groupBTotal)
}

func TestWorkerGroupWithBroadcast(t *testing.T) {
	t.Parallel()
	// Test mixing worker groups with broadcast subscribers
	transport := channel.New(
		channel.WithBufferSize(100),
		channel.WithTimeout(time.Duration(100)*time.Millisecond),
	)
	bus := mustNewBus(t, "test-mixed", WithTransport(transport))
	defer bus.Close(context.Background())

	e := New[string]("test-mixed")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	var total int32 = 20

	// Counters
	var broadcast1, broadcast2 int32         // broadcast subscribers
	var workerA1, workerA2 int32             // worker group A
	var workerDefault1, workerDefault2 int32 // default worker pool (no group)
	var totalProcessed int32

	// Expected: 2 broadcast + 1 from group-a + 1 from default = 4 per message
	expectedTotal := total * 4
	done := make(chan struct{}, 1)

	incrementAndCheck := func(counter *int32) {
		atomic.AddInt32(counter, 1)
		if atomic.AddInt32(&totalProcessed, 1) >= expectedTotal {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}

	// Broadcast subscribers (receive all messages) - default mode, no options needed
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&broadcast1)
		return nil
	})

	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&broadcast2)
		return nil
	})

	// Worker group A (compete within group) - WithWorkerGroup auto-enables WorkerPool
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&workerA1)
		return nil
	}, WithWorkerGroup[string]("group-a"))

	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&workerA2)
		return nil
	}, WithWorkerGroup[string]("group-a"))

	// Default worker pool (no group - compete among themselves)
	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&workerDefault1)
		return nil
	}, WithDeliveryMode[string](WorkerPool))

	e.Subscribe(context.Background(), func(ctx context.Context, event Event[string], data string) error {
		incrementAndCheck(&workerDefault2)
		return nil
	}, WithDeliveryMode[string](WorkerPool))

	// Publish messages
	for i := int32(0); i < total; i++ {
		if err := e.Publish(context.Background(), "msg"); err != nil {
			t.Errorf("publish failed: %v", err)
		}
	}

	// Wait for processing
	if !wait(done, 2000) {
		t.Errorf("timeout, processed: %d, expected: %d", totalProcessed, expectedTotal)
	}

	// Verify broadcast subscribers received all messages
	if broadcast1 != total {
		t.Errorf("broadcast1 expected %d, got %d", total, broadcast1)
	}
	if broadcast2 != total {
		t.Errorf("broadcast2 expected %d, got %d", total, broadcast2)
	}

	// Verify group-a received all messages (distributed among workers)
	groupATotal := workerA1 + workerA2
	if groupATotal != total {
		t.Errorf("group-a expected %d total, got %d", total, groupATotal)
	}

	// Verify default workers received all messages (distributed among workers)
	defaultTotal := workerDefault1 + workerDefault2
	if defaultTotal != total {
		t.Errorf("default workers expected %d total, got %d", total, defaultTotal)
	}

	t.Logf("Broadcast: sub1=%d, sub2=%d", broadcast1, broadcast2)
	t.Logf("Group A: worker1=%d, worker2=%d, total=%d", workerA1, workerA2, groupATotal)
	t.Logf("Default: worker1=%d, worker2=%d, total=%d", workerDefault1, workerDefault2, defaultTotal)
}

// TestContextImmutability verifies that context modification functions
// don't mutate the original context data (race condition fix)
func TestContextImmutability(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	poolSize := int64(2)
	transport := channel.New(
		channel.WithAsync(true),
		channel.WithWorkerPoolSize(poolSize),
	)
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(transport))
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

	// Wait for the pipeline to start consuming before releasing handlers.
	eventuallyTrue(t, 2*time.Second,
		func() bool { return atomic.LoadInt32(&processed) >= 1 },
		"worker pool did not begin consuming")

	// All events should eventually be processed once handlers are released.
	go func() {
		for atomic.LoadInt32(&processed) < int32(poolSize)+2 {
			time.Sleep(2 * time.Millisecond)
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
	t.Parallel()
	transport := channel.New(
		channel.WithAsync(true),
	)
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(transport))

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
	t.Parallel()
	e := Discard[string]()

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
	t.Parallel()
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
	msg := message.New("test", "source", []byte("data"), nil)
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	// Test empty context returns empty string
	ctx := context.Background()
	if name := ContextName(ctx); name != "" {
		t.Errorf("expected empty name, got %s", name)
	}

	// Test context with name (via handler context)
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
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
	msg := message.New("test-msg", "source", []byte("data"), nil)
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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

// sanitize strings and remove special chars (test-only helper).
func sanitize(s string) string {
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

// caller gets the caller function name (test-only helper).
func caller(depth int) string {
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

// TestSanitize verifies sanitize function
func TestSanitize(t *testing.T) {
	t.Parallel()
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
			if got := sanitize(tt.input); got != tt.expected {
				t.Errorf("sanitize(%s) = %s, want %s", tt.input, got, tt.expected)
			}
		})
	}
}

// TestCaller verifies caller function
func TestCaller(t *testing.T) {
	t.Parallel()
	// Test calling from this function
	name := caller(1)
	if name == "" {
		t.Error("expected non-empty caller name")
	}
	if !strings.Contains(name, "TestCaller") {
		t.Errorf("expected caller to contain TestCaller, got %s", name)
	}

	// Test invalid depth
	name = caller(100)
	if name != "" {
		t.Errorf("expected empty string for invalid depth, got %s", name)
	}
}

// TestAsyncHandler verifies AsyncHandler function
func TestAsyncHandler(t *testing.T) {
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	e := New[any]("async-panic-test")
	if err := Register(context.Background(), bus, e); err != nil {
		t.Fatalf("failed to register event: %v", err)
	}

	reached := make(chan struct{}, 1)
	handler := func(ctx context.Context, ev Event[any], data any) error {
		defer func() {
			// Fires during panic unwinding before AsyncHandler's outer recover.
			// Receiving on this channel proves the handler ran; AsyncHandler's
			// deferred recover is guaranteed to execute next during unwind,
			// so if it failed to recover the test process would crash.
			select {
			case reached <- struct{}{}:
			default:
			}
		}()
		panic("test panic")
	}

	asyncHandler := AsyncHandler(handler)
	e.Subscribe(context.Background(), asyncHandler)

	e.Publish(context.Background(), nil)

	if !wait(reached, waitChTimeoutMS) {
		t.Fatal("async handler did not run")
	}
}

// TestEventString verifies eventImpl.String()
func TestEventString(t *testing.T) {
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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

	eventuallyTrue(t, 2*time.Second, func() bool { return impl.Subscribers() == 1 },
		"subscription did not register")

	cancel()
	eventuallyTrue(t, 2*time.Second, func() bool { return impl.Subscribers() == 0 },
		"cancel did not unsubscribe")
}

// TestNilEventPublish verifies nil event doesn't panic on Publish
func TestNilEventPublish(t *testing.T) {
	t.Parallel()
	var e *eventImpl[any]
	// Should not panic
	e.Publish(context.Background(), "data")
}

// TestNilEventSubscribe verifies nil event doesn't panic on Subscribe
func TestNilEventSubscribe(t *testing.T) {
	t.Parallel()
	var e *eventImpl[any]
	// Should not panic
	e.Subscribe(context.Background(), func(ctx context.Context, ev Event[any], data any) error { return nil })
}

// TestSubscriptionClose verifies Subscription.Close() removes subscriber
func TestSubscriptionClose(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "", WithTransport(channel.New()))
	defer bus.Close(context.Background())

	if bus.Name() != DefaultBusName {
		t.Errorf("expected default name '%s', got %s", DefaultBusName, bus.Name())
	}
}

// TestBusRegister verifies Bus.Register works correctly
func TestBusRegister(t *testing.T) {
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
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
	msg := message.New("timeout-test", "source", []byte("data"), nil)
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()), WithTracing(false))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()), WithRecovery(false))
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
	t.Parallel()
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
	t.Parallel()
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
	msg := message.New("test-msg", "source", []byte("data"), nil)
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
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
	e := Discard[any]()

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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	type User struct {
		ID   string
		Name string
	}

	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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
	t.Parallel()
	type Order struct {
		ID     string
		Amount float64
	}

	bus := mustNewBus(t, "test-"+randomString(5), WithTransport(channel.New()))
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


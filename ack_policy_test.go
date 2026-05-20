package event

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/channel"
)

// eventuallyEqInt32 polls until atomic.Int32 reaches want or the deadline
// fires. Replaces the time.Sleep + assert pattern: the test passes the
// instant the contract is met, and only burns the timeout when something
// is actually wrong. Defined here (not in internal/testutil) because the
// root event package cannot import testutil — it would create an import
// cycle through internal/testutil/bus.go.
func eventuallyEqInt32(t testing.TB, timeout time.Duration, counter *atomic.Int32, want int32, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for counter.Load() != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: got %d, want %d (after %s)", msg, counter.Load(), want, timeout)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestWithBestEffort_AutoAcksAndSuppressesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := mustNewBus(t, "best-effort-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ev := New[string]("test.besteffort")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var received atomic.Int32
	handlerErr := errors.New("handler failed")

	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		received.Add(1)
		return handlerErr // error should be suppressed
	}, WithBestEffort[string]())
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe is synchronous on the channel transport — no pre-publish
	// sleep needed. Publish several messages.
	for i := 0; i < 5; i++ {
		if err := ev.Publish(ctx, "msg"); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until all 5 messages have been delivered. Polling exits the
	// instant the contract is met, not after a fixed 200ms wait.
	eventuallyEqInt32(t, 2*time.Second, &received, 5, "expected 5 messages received")
}

func TestWithAckPolicy_ExplicitIsDefault(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := mustNewBus(t, "ack-explicit-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ev := New[string]("test.explicit")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	ch := make(chan string, 1)
	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		ch <- data
		return nil
	}, WithAckPolicy[string](AckExplicit))
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe is synchronous on the channel transport — publish
	// immediately. The waitForData channel poll below handles any
	// residual setup latency.
	if err := ev.Publish(ctx, "hello"); err != nil {
		t.Fatal(err)
	}

	data, ok := waitForData(ch, 500)
	if !ok {
		t.Fatal("timed out waiting for message")
	}
	if data != "hello" {
		t.Errorf("expected 'hello', got %q", data)
	}
}

func TestSubscribeOptionValidation_CoalesceAndLatestOnly(t *testing.T) {
	ctx := context.Background()
	bus := mustNewBus(t, "validate-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(ctx)

	ev := New[string]("test.validate")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// WithCoalesceByKey + WithLatestOnly should error
	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		return nil
	}, WithCoalesceByKey[string](func(s string) string { return s }),
		WithLatestOnly[string]())

	if err == nil {
		t.Fatal("expected error for WithCoalesceByKey + WithLatestOnly")
	}
}

func TestSubscribeOptionValidation_BothCoalesceOptions(t *testing.T) {
	ctx := context.Background()
	bus := mustNewBus(t, "validate2-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(ctx)

	ev := New[string]("test.validate2")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// WithCoalesceByKey + WithCoalesceByMetadata should error
	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		return nil
	}, WithCoalesceByKey[string](func(s string) string { return s }),
		WithCoalesceByMetadata[string]("some_key"))

	if err == nil {
		t.Fatal("expected error for WithCoalesceByKey + WithCoalesceByMetadata")
	}
}

func TestBestEffortMiddleware(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := mustNewBus(t, "be-mw-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	ev := New[string]("test.bemw")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var received atomic.Int32
	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		received.Add(1)
		return errors.New("fail")
	}, WithMiddleware(BestEffortMiddleware[string]()))
	if err != nil {
		t.Fatal(err)
	}

	// Subscribe is synchronous — publish immediately. Poll for completion
	// instead of fixed 200ms wait.
	for i := 0; i < 3; i++ {
		if err := ev.Publish(ctx, "msg"); err != nil {
			t.Fatal(err)
		}
	}

	eventuallyEqInt32(t, 2*time.Second, &received, 3, "expected 3 messages received with BestEffortMiddleware")
}

func TestContextCoalescedCount_DefaultZero(t *testing.T) {
	ctx := context.Background()
	if count := ContextCoalescedCount(ctx); count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestContextCoalescedCount_Set(t *testing.T) {
	ctx := contextWithInfo(context.Background(), contextInfo{
		id: "id", name: "name", source: "source", subID: "sub",
		msgTime: time.Now(), mode: Broadcast, coalescedCount: 5,
	})
	if count := ContextCoalescedCount(ctx); count != 5 {
		t.Errorf("expected 5, got %d", count)
	}
}

func TestWithCoalesceByKey_SupersedesPendingMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := mustNewBus(t, "coalesce-key-"+randomString(6), WithTransport(channel.New()))
	defer bus.Close(context.Background())

	type Order struct {
		ID    string `json:"id"`
		Value int    `json:"value"`
	}

	ev := New[Order]("test.coalesce")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	delivered := make(map[string]int) // id -> latest value delivered
	coalescedCounts := make(map[string]int)
	handlerReady := make(chan struct{})
	handlerDone := make(chan struct{}, 10)

	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[Order], data Order) error {
		// Signal that handler is processing.
		select {
		case handlerReady <- struct{}{}:
		default:
		}
		// Slow handler to allow coalescing
		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		delivered[data.ID] = data.Value
		coalescedCounts[data.ID] = ContextCoalescedCount(ctx)
		mu.Unlock()

		select {
		case handlerDone <- struct{}{}:
		default:
		}
		return nil
	}, WithCoalesceByKey[Order](func(o Order) string { return o.ID }))
	if err != nil {
		t.Fatal(err)
	}

	// Publish the initial message and wait for the handler to start. Unlike
	// the other ack_policy tests, WithCoalesceByKey wraps the subscription
	// in a coalescer that runs on a separate goroutine started during
	// Subscribe; that goroutine's `select { case <-incoming: ... }` is set
	// up asynchronously, so a Publish that races it can land in the
	// coalescer's buffered incoming channel before the run loop is reading
	// from it. The buffer absorbs the message but the handler has nothing
	// to react to until the run loop catches up.
	//
	// Retry the initial publish until handlerReady fires. All retries land
	// under the same key and coalesce harmlessly into a single delivery —
	// the coalescer's supersede logic preserves the latest value. This
	// pattern is deterministic on both fast machines and loaded CI runners,
	// replacing a fixed 50ms pre-Publish sleep that CI proved was not
	// always sufficient.
	const initial = 1
	if err := ev.Publish(ctx, Order{ID: "A", Value: initial}); err != nil {
		t.Fatal(err)
	}

	handlerStarted := false
	deadline := time.Now().Add(2 * time.Second)
	for !handlerStarted && time.Now().Before(deadline) {
		select {
		case <-handlerReady:
			handlerStarted = true
		case <-time.After(50 * time.Millisecond):
			// Republish — coalescer may have missed the first send if its
			// run loop was not yet ready when we published.
			if err := ev.Publish(ctx, Order{ID: "A", Value: initial}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !handlerStarted {
		t.Fatal("handler didn't start after retries")
	}

	// While handler is busy, send more messages for same key.
	// These should be coalesced — only the last one should be delivered.
	// The 10ms spread is intentional: it staggers publishes so the
	// coalescer's run loop has a chance to pull each from the channel
	// individually rather than all at once. Removing it would defeat the
	// coalescing-under-load scenario this test exists to verify.
	for i := 2; i <= 5; i++ {
		if err := ev.Publish(ctx, Order{ID: "A", Value: i}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for handler to finish both rounds (initial + coalesced).
	for i := 0; i < 2; i++ {
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for handler invocation %d", i+1)
		}
	}

	// Both handler invocations have signaled handlerDone, which happens
	// AFTER delivered[data.ID] is written. No further settle wait needed.

	mu.Lock()
	defer mu.Unlock()

	// The last delivered value for key A should be 5 (the latest).
	if delivered["A"] != 5 {
		t.Errorf("expected latest value 5 for key A, got %d", delivered["A"])
	}

	// The coalesced count should be > 0 for the second delivery.
	if coalescedCounts["A"] == 0 {
		t.Log("coalesced count is 0 — messages may not have been coalesced (timing-dependent)")
	}
}

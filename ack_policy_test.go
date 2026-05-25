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
	// handlerStarted is an idempotent signal that the handler has begun
	// processing. atomic.Bool — NOT a channel — because the retry loop
	// below may not yet be in a select when the handler signals, and a
	// dropped channel signal would cause the loop to keep republishing
	// Value=1 indefinitely. With atomic.Bool the polling loop sees the
	// signal regardless of when it was set.
	var handlerStarted atomic.Bool
	handlerDone := make(chan struct{}, 10)

	err := ev.Subscribe(ctx, func(ctx context.Context, e Event[Order], data Order) error {
		handlerStarted.Store(true)
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
	// from it.
	//
	// Retry the initial publish until handlerStarted flips. All retries
	// land under the same key and coalesce harmlessly into a single pending
	// delivery — the coalescer's supersede logic preserves the latest
	// value, and since all retries publish the same Value=1, the pending
	// entry stays at Value=1 until the subsequent for-loop bumps it up.
	const initial = 1
	if err := ev.Publish(ctx, Order{ID: "A", Value: initial}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !handlerStarted.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		if !handlerStarted.Load() {
			// Republish — coalescer may have missed the first send if its
			// run loop was not yet ready when we published.
			if err := ev.Publish(ctx, Order{ID: "A", Value: initial}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !handlerStarted.Load() {
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

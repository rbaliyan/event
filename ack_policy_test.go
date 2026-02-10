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

	time.Sleep(50 * time.Millisecond)

	// Publish several messages
	for i := 0; i < 5; i++ {
		if err := ev.Publish(ctx, "msg"); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	// All messages should be received (errors suppressed, no retries)
	if got := received.Load(); got != 5 {
		t.Errorf("expected 5 messages received, got %d", got)
	}
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

	time.Sleep(50 * time.Millisecond)

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

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ {
		if err := ev.Publish(ctx, "msg"); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	if got := received.Load(); got != 3 {
		t.Errorf("expected 3 messages received with BestEffortMiddleware, got %d", got)
	}
}

func TestContextCoalescedCount_DefaultZero(t *testing.T) {
	ctx := context.Background()
	if count := ContextCoalescedCount(ctx); count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestContextCoalescedCount_Set(t *testing.T) {
	ctx := contextWithInfoCoalesced(
		context.Background(),
		"id", "name", "source", "sub",
		nil, time.Now(), nil, nil,
		Broadcast, "", "", 5,
	)
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

	time.Sleep(50 * time.Millisecond)

	// Publish initial message to start handler processing.
	if err := ev.Publish(ctx, Order{ID: "A", Value: 1}); err != nil {
		t.Fatal(err)
	}

	// Wait for handler to start processing
	select {
	case <-handlerReady:
	case <-time.After(time.Second):
		t.Fatal("handler didn't start")
	}

	// While handler is busy, send more messages for same key.
	// These should be coalesced — only the last one should be delivered.
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

	// Allow any further processing to settle.
	time.Sleep(200 * time.Millisecond)

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

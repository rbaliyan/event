package outbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// Tests

func TestStatusConstants(t *testing.T) {
	t.Parallel()
	if StatusPending != "pending" {
		t.Errorf("expected pending, got %s", StatusPending)
	}
	if StatusPublished != "published" {
		t.Errorf("expected published, got %s", StatusPublished)
	}
	if StatusFailed != "failed" {
		t.Errorf("expected failed, got %s", StatusFailed)
	}
	if StatusProcessing != "processing" {
		t.Errorf("expected processing, got %s", StatusProcessing)
	}
}

func TestNewRelay(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	tr := channel.New()
	relay := NewRelay(store, tr)

	if relay.store != store {
		t.Error("store not set")
	}
	if relay.transport != tr {
		t.Error("transport not set")
	}
	if relay.pollDelay != 100*time.Millisecond {
		t.Errorf("expected 100ms poll delay, got %v", relay.pollDelay)
	}
	if relay.batchSize != 100 {
		t.Errorf("expected batch size 100, got %d", relay.batchSize)
	}
	if relay.cleanupAge != 24*time.Hour {
		t.Errorf("expected 24h cleanup age, got %v", relay.cleanupAge)
	}
}

func TestRelayOptions(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	tr := channel.New()

	relay := NewRelay(store, tr,
		WithPollDelay(50*time.Millisecond),
		WithBatchSize(50),
		WithCleanupAge(48*time.Hour),
	)

	if relay.pollDelay != 50*time.Millisecond {
		t.Errorf("expected 50ms, got %v", relay.pollDelay)
	}
	if relay.batchSize != 50 {
		t.Errorf("expected 50, got %d", relay.batchSize)
	}
	if relay.cleanupAge != 48*time.Hour {
		t.Errorf("expected 48h, got %v", relay.cleanupAge)
	}
}

func TestRelayPublishOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newFakeStore(Message{
		EventName: "test.event",
		EventID:   "evt-123",
		Payload:   []byte(`{"order_id":"abc"}`),
		Metadata:  map[string]string{"source": "test"},
	})
	tr := channel.New()

	// Register event so transport accepts publish
	if err := tr.RegisterEvent(ctx, "test.event"); err != nil {
		t.Fatal(err)
	}

	// Subscribe with buffer to prevent blocking
	sub, err := tr.Subscribe(ctx, "test.event",
		transport.WithStartFrom(transport.StartFromLatest),
		transport.WithBufferSize(10),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)

	if len(store.pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(store.pending))
	}

	// Run relay once
	relay := NewRelay(store, tr)
	if err := relay.PublishOnce(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify message was published
	select {
	case received := <-sub.Messages():
		if received.ID() != "evt-123" {
			t.Errorf("expected event ID evt-123, got %s", received.ID())
		}
		_ = received.Ack(nil)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Verify store was updated
	if len(store.published) != 1 {
		t.Errorf("expected 1 published, got %d", len(store.published))
	}
}

func TestRelayPublishFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Don't register event - publish will fail with ErrEventNotRegistered
	store := newFakeStore(Message{
		EventName: "unregistered.event",
		EventID:   "evt-fail",
		Payload:   []byte(`{}`),
	})
	tr := channel.New()

	relay := NewRelay(store, tr)
	err := relay.PublishOnce(ctx)

	// PublishOnce should return an error
	if err == nil {
		t.Error("expected PublishOnce to return an error")
	}

	// Message should be marked as failed
	store.mu.Lock()
	_, failed := store.failed["evt-fail"]
	store.mu.Unlock()

	if !failed {
		t.Error("expected message to be marked as failed")
	}
}

func TestRelayStartStop(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	tr := channel.New()

	relay := NewRelay(store, tr,
		WithPollDelay(10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- relay.Start(ctx)
	}()

	// Start blocks on ctx; cancel interrupts it. No warm-up sleep needed.
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not stop")
	}
}

func TestRelayMultipleMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tr := channel.New()

	if err := tr.RegisterEvent(ctx, "batch.event"); err != nil {
		t.Fatal(err)
	}

	// Subscribe with buffer to prevent blocking
	sub, err := tr.Subscribe(ctx, "batch.event",
		transport.WithStartFrom(transport.StartFromLatest),
		transport.WithBufferSize(10),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)

	// Insert multiple messages
	var msgs []Message
	for i := 0; i < 5; i++ {
		msgs = append(msgs, Message{
			EventName: "batch.event",
			EventID:   "evt-" + string(rune('a'+i)),
			Payload:   []byte(`{}`),
		})
	}
	store := newFakeStore(msgs...)

	if len(store.pending) != 5 {
		t.Fatalf("expected 5 pending, got %d", len(store.pending))
	}

	relay := NewRelay(store, tr, WithBatchSize(10))
	if err := relay.PublishOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if len(store.published) != 5 {
		t.Errorf("expected 5 published, got %d", len(store.published))
	}
}

func TestFakeStoreClaimPendingError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newFakeStore()
	store.claimErr = errors.New("db connection failed")

	tr := channel.New()
	relay := NewRelay(store, tr)

	// Should not panic
	_ = relay.PublishOnce(ctx)
}

func TestRelayCleanup(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.cleaned = 5 // simulate 5 deleted
	tr := channel.New()

	relay := NewRelay(store, tr, WithCleanupAge(12*time.Hour))
	relay.cleanup(context.Background())

	// cleanup calls Cleanup — verify it doesn't error
	// We can't observe the call directly but verify no panic
}

func TestRelayCleanupError(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	store.cleanupErr = errors.New("cleanup failed")
	tr := channel.New()

	relay := NewRelay(store, tr)
	// Should not panic
	relay.cleanup(context.Background())
}

func TestRelayMarkFailedOnPublishError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Don't register event — publish will fail
	store := newFakeStore(Message{EventName: "bad.event", EventID: "evt-mf", Payload: []byte(`{}`)})
	tr := channel.New()

	relay := NewRelay(store, tr)
	_ = relay.PublishOnce(ctx)

	store.mu.Lock()
	failErr, isFailed := store.failed["evt-mf"]
	store.mu.Unlock()

	if !isFailed {
		t.Error("expected message to be marked as failed")
	}
	if failErr == "" {
		t.Error("expected non-empty error in failed record")
	}
}

func TestRelayEmptyStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newFakeStore()
	tr := channel.New()

	relay := NewRelay(store, tr)
	err := relay.PublishOnce(ctx)

	// No messages, no error
	if err != nil {
		t.Errorf("expected no error for empty store, got %v", err)
	}
}

func TestMessageStruct(t *testing.T) {
	t.Parallel()
	now := time.Now()
	msg := Message{
		EventName:  "order.created",
		EventID:    "evt-1",
		Payload:    []byte(`{"id":"123"}`),
		Metadata:   map[string]string{"key": "val"},
		CreatedAt:  now,
		Status:     StatusPending,
		RetryCount: 0,
	}

	if msg.EventName != "order.created" {
		t.Errorf("unexpected event name: %s", msg.EventName)
	}
	if msg.Status != StatusPending {
		t.Errorf("unexpected status: %s", msg.Status)
	}
	if msg.PublishedAt != nil {
		t.Error("expected nil PublishedAt")
	}
}

func TestRelayMaxRetries(t *testing.T) {
	t.Parallel()
	// Don't register event — publish always fails, so the message keeps
	// getting re-queued with an increasing retry count.
	store := newFakeStore(Message{
		EventName: "test.event",
		EventID:   "evt-1",
		Payload:   []byte(`{}`),
	})
	tr := channel.New(channel.WithBufferSize(100))
	relay := NewRelay(store, tr,
		WithPollDelay(50*time.Millisecond),
		WithMaxRetries(3),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go relay.Start(ctx)

	testutil.Eventually(t, 2*time.Second, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.pending) == 0 {
			return false
		}
		return store.pending[len(store.pending)-1].RetryCount >= 3
	}, "message never reached RetryCount >= 3")

	store.mu.Lock()
	_, failed := store.failed["evt-1"]
	store.mu.Unlock()

	if !failed {
		t.Fatal("expected message to be marked as failed")
	}
}

func TestRelayAdaptiveBackpressure(t *testing.T) {
	t.Parallel()
	// Don't register event — publish always fails, driving the backoff path.
	store := newFakeStore(Message{
		EventName: "test.event",
		EventID:   "evt-1",
		Payload:   []byte(`{}`),
	})
	tr := channel.New(channel.WithBufferSize(100))

	var called atomic.Bool
	strategy := &testBackoff{called: &called}
	relay := NewRelay(store, tr,
		WithPollDelay(50*time.Millisecond),
		WithRetryBackoff(strategy),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go relay.Start(ctx)

	testutil.Eventually(t, 2*time.Second, called.Load,
		"backoff strategy never called on failures")
}

type testBackoff struct {
	called *atomic.Bool
}

func (b *testBackoff) NextDelay(attempt int) time.Duration {
	b.called.Store(true)
	return 100 * time.Millisecond
}

func TestPostgresTransaction_PiggyBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Simulate already being inside a Postgres transaction.
	// db is nil — if piggy-back works correctly, db.BeginTx is never called,
	// so nil is safe here and proves the short-circuit path was taken.
	txCtx := event.WithOutboxTx(ctx, &sql.Tx{})

	called := false
	err := PostgresTransaction(txCtx, nil, func(innerCtx context.Context) error {
		called = true
		if !event.InOutboxTx(innerCtx) {
			t.Error("expected InOutboxTx to be true inside piggy-backed call")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("fn should have been called")
	}
}

func TestPostgresTransaction_PiggyBack_PropagatesError(t *testing.T) {
	t.Parallel()
	ctx := event.WithOutboxTx(context.Background(), &sql.Tx{})

	testErr := errors.New("business logic failed")
	err := PostgresTransaction(ctx, nil, func(_ context.Context) error {
		return testErr
	})
	if !errors.Is(err, testErr) {
		t.Fatalf("expected %v, got %v", testErr, err)
	}
}

func TestPostgresTransaction_NoPiggyBackOnMongoSession(t *testing.T) {
	t.Parallel()
	// Set a non-*sql.Tx session (simulating a Mongo context.Context session).
	// PostgresTransaction should NOT piggy-back — it should try to start a new tx.
	ctx := event.WithOutboxTx(context.Background(), context.Background())

	// Since db is nil, BeginTx will panic — this proves piggy-back was skipped.
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		PostgresTransaction(ctx, nil, func(_ context.Context) error {
			t.Fatal("fn should not have been called")
			return nil
		})
	}()
	if !panicked {
		t.Fatal("expected panic when db is nil and piggy-back is skipped")
	}
}

func TestPostgresStoreImplementsOutboxStore(t *testing.T) {
	t.Parallel()
	var s interface{} = &PostgresStore{}
	if _, ok := s.(event.OutboxStore); !ok {
		t.Fatal("PostgresStore should implement event.OutboxStore")
	}
}

func TestPostgresStore_Store_WrongSessionType(t *testing.T) {
	t.Parallel()
	store := &PostgresStore{tableName: "event_outbox"}
	// Put a non-*sql.Tx session in context.
	ctx := event.WithOutboxTx(context.Background(), "not-a-sql-tx")

	err := store.Store(ctx, "test.event", "evt-1", []byte(`{}`), nil)
	if err == nil {
		t.Fatal("expected error for wrong session type")
	}
	if !strings.Contains(err.Error(), "expected *sql.Tx") {
		t.Fatalf("expected error about *sql.Tx, got: %v", err)
	}
}

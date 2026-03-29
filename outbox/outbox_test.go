package outbox

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/channel"
)

// mockStore implements Store for testing
type mockStore struct {
	mu        sync.Mutex
	messages  []*Message
	published map[int64]bool
	failed    map[int64]error
	deleted   int64
	nextID    int64

	// Control behavior
	insertErr     error
	getPendingErr error
	markPubErr    error
	markFailErr   error
	deleteErr     error
}

func newMockStore() *mockStore {
	return &mockStore{
		published: make(map[int64]bool),
		failed:    make(map[int64]error),
	}
}

func (s *mockStore) Insert(_ context.Context, _ *sql.Tx, msg *Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.insertErr != nil {
		return s.insertErr
	}
	s.nextID++
	msg.ID = s.nextID
	msg.Status = StatusPending
	msg.CreatedAt = time.Now()
	s.messages = append(s.messages, msg)
	return nil
}

func (s *mockStore) GetPending(_ context.Context, limit int) ([]*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getPendingErr != nil {
		return nil, s.getPendingErr
	}
	var pending []*Message
	for _, msg := range s.messages {
		if (msg.Status == StatusPending || msg.Status == StatusFailed) && !s.published[msg.ID] {
			pending = append(pending, msg)
		}
	}
	// Sort by priority DESC, then created_at ASC (matches real store behavior)
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Priority != pending[j].Priority {
			return pending[i].Priority > pending[j].Priority
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	if len(pending) > limit {
		pending = pending[:limit]
	}
	return pending, nil
}

func (s *mockStore) MarkPublished(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markPubErr != nil {
		return s.markPubErr
	}
	s.published[id] = true
	for _, msg := range s.messages {
		if msg.ID == id {
			msg.Status = StatusPublished
			now := time.Now()
			msg.PublishedAt = &now
		}
	}
	return nil
}

func (s *mockStore) MarkFailed(_ context.Context, id int64, err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markFailErr != nil {
		return s.markFailErr
	}
	s.failed[id] = err
	for _, msg := range s.messages {
		if msg.ID == id {
			msg.Status = StatusFailed
			msg.RetryCount++
			msg.LastError = err.Error()
		}
	}
	return nil
}

func (s *mockStore) Delete(_ context.Context, _ time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return 0, s.deleteErr
	}
	count := s.deleted
	s.deleted = 0
	return count, nil
}

func (s *mockStore) pendingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, msg := range s.messages {
		if msg.Status == StatusPending {
			count++
		}
	}
	return count
}

func (s *mockStore) publishedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, msg := range s.messages {
		if msg.Status == StatusPublished {
			count++
		}
	}
	return count
}

// Tests

func TestStatusConstants(t *testing.T) {
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
	store := newMockStore()
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
	store := newMockStore()
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
	ctx := context.Background()
	store := newMockStore()
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

	// Insert a message into the mock store
	msg := &Message{
		EventName: "test.event",
		EventID:   "evt-123",
		Payload:   []byte(`{"order_id":"abc"}`),
		Metadata:  map[string]string{"source": "test"},
	}
	if err := store.Insert(ctx, nil, msg); err != nil {
		t.Fatal(err)
	}

	if store.pendingCount() != 1 {
		t.Fatalf("expected 1 pending, got %d", store.pendingCount())
	}

	// Run relay once
	relay := NewRelay(store, tr)
	relay.PublishOnce(ctx)

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
	if store.publishedCount() != 1 {
		t.Errorf("expected 1 published, got %d", store.publishedCount())
	}
}

func TestRelayPublishFailure(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	tr := channel.New()

	// Don't register event - publish will fail with ErrEventNotRegistered

	msg := &Message{
		EventName: "unregistered.event",
		EventID:   "evt-fail",
		Payload:   []byte(`{}`),
	}
	if err := store.Insert(ctx, nil, msg); err != nil {
		t.Fatal(err)
	}

	relay := NewRelay(store, tr)
	err := relay.PublishOnce(ctx)

	// PublishOnce should return an error
	if err == nil {
		t.Error("expected PublishOnce to return an error")
	}

	// Message should be marked as failed
	store.mu.Lock()
	_, failed := store.failed[msg.ID]
	store.mu.Unlock()

	if !failed {
		t.Error("expected message to be marked as failed")
	}
}

func TestRelayStartStop(t *testing.T) {
	store := newMockStore()
	tr := channel.New()

	relay := NewRelay(store, tr,
		WithPollDelay(10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- relay.Start(ctx)
	}()

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)
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
	ctx := context.Background()
	store := newMockStore()
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
	for i := 0; i < 5; i++ {
		msg := &Message{
			EventName: "batch.event",
			EventID:   "evt-" + string(rune('a'+i)),
			Payload:   []byte(`{}`),
		}
		if err := store.Insert(ctx, nil, msg); err != nil {
			t.Fatal(err)
		}
	}

	if store.pendingCount() != 5 {
		t.Fatalf("expected 5 pending, got %d", store.pendingCount())
	}

	relay := NewRelay(store, tr, WithBatchSize(10))
	relay.PublishOnce(ctx)

	if store.publishedCount() != 5 {
		t.Errorf("expected 5 published, got %d", store.publishedCount())
	}
}

func TestMockStoreGetPendingError(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	store.getPendingErr = errors.New("db connection failed")

	tr := channel.New()
	relay := NewRelay(store, tr)

	// Should not panic
	relay.PublishOnce(ctx)
}

// mockProcessPendingStore wraps mockStore and adds ProcessPending support.
type mockProcessPendingStore struct {
	*mockStore
	processCalled bool
	processErr    error
}

func (s *mockProcessPendingStore) ProcessPending(ctx context.Context, limit int, fn func(msg *Message) error) error {
	s.processCalled = true
	if s.processErr != nil {
		return s.processErr
	}
	msgs, err := s.mockStore.GetPending(ctx, limit)
	if err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := fn(msg); err != nil {
			return nil // ProcessPending stops on first error but doesn't fail
		}
		_ = s.mockStore.MarkPublished(ctx, msg.ID)
	}
	return nil
}

func TestRelayProcessPendingPreferred(t *testing.T) {
	ctx := context.Background()
	inner := newMockStore()
	store := &mockProcessPendingStore{mockStore: inner}
	tr := channel.New()

	if err := tr.RegisterEvent(ctx, "test.event"); err != nil {
		t.Fatal(err)
	}
	sub, err := tr.Subscribe(ctx, "test.event",
		transport.WithStartFrom(transport.StartFromLatest),
		transport.WithBufferSize(10),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close(ctx)

	msg := &Message{EventName: "test.event", EventID: "evt-pp", Payload: []byte(`{}`)}
	store.Insert(ctx, nil, msg)

	relay := NewRelay(store, tr)
	relay.PublishOnce(ctx)

	if !store.processCalled {
		t.Error("expected ProcessPending to be called when available")
	}
}

func TestRelayCleanup(t *testing.T) {
	store := newMockStore()
	store.deleted = 5 // simulate 5 deleted
	tr := channel.New()

	relay := NewRelay(store, tr, WithCleanupAge(12*time.Hour))
	relay.cleanup(context.Background())

	// cleanup calls Delete — verify it doesn't error
	// We can't observe the call directly but verify no panic
}

func TestRelayMarkFailedOnPublishError(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	tr := channel.New()
	// Don't register event — publish will fail

	msg := &Message{EventName: "bad.event", EventID: "evt-mf", Payload: []byte(`{}`)}
	store.Insert(ctx, nil, msg)

	relay := NewRelay(store, tr)
	relay.PublishOnce(ctx)

	store.mu.Lock()
	failErr, isFailed := store.failed[msg.ID]
	store.mu.Unlock()

	if !isFailed {
		t.Error("expected MarkFailed to be called")
	}
	if failErr == nil {
		t.Error("expected non-nil error in MarkFailed")
	}
}

func TestRelayEmptyStore(t *testing.T) {
	ctx := context.Background()
	store := newMockStore()
	tr := channel.New()

	relay := NewRelay(store, tr)
	err := relay.PublishOnce(ctx)

	// No messages, no error
	if err != nil {
		t.Errorf("expected no error for empty store, got %v", err)
	}
}

func TestRelayDeleteError(t *testing.T) {
	store := newMockStore()
	store.deleteErr = errors.New("delete failed")
	tr := channel.New()

	relay := NewRelay(store, tr)
	// Should not panic
	relay.cleanup(context.Background())
}

func TestMessageStruct(t *testing.T) {
	now := time.Now()
	msg := Message{
		ID:         1,
		EventName:  "order.created",
		EventID:    "evt-1",
		Payload:    []byte(`{"id":"123"}`),
		Metadata:   map[string]string{"key": "val"},
		CreatedAt:  now,
		Status:     StatusPending,
		RetryCount: 0,
	}

	if msg.ID != 1 {
		t.Errorf("unexpected ID: %d", msg.ID)
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
	store := newMockStore()
	tr := channel.New(channel.WithBufferSize(100))
	relay := NewRelay(store, tr,
		WithPollDelay(50*time.Millisecond),
		WithMaxRetries(3),
	)

	store.Insert(context.Background(), nil, &Message{
		EventName: "test.event",
		EventID:   "evt-1",
		Payload:   []byte(`{}`),
	})

	ctx, cancel := context.WithCancel(context.Background())
	go relay.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	cancel()

	store.mu.Lock()
	status := store.messages[0].Status
	retryCount := store.messages[0].RetryCount
	store.mu.Unlock()

	if status != StatusFailed {
		t.Fatalf("expected StatusFailed, got %s", status)
	}
	if retryCount < 3 {
		t.Fatalf("expected retry count >= 3, got %d", retryCount)
	}
}

func TestRelayAdaptiveBackpressure(t *testing.T) {
	store := newMockStore()
	tr := channel.New(channel.WithBufferSize(100))

	var called atomic.Bool
	strategy := &testBackoff{called: &called}
	relay := NewRelay(store, tr,
		WithPollDelay(50*time.Millisecond),
		WithRetryBackoff(strategy),
	)

	store.Insert(context.Background(), nil, &Message{
		EventName: "test.event",
		EventID:   "evt-1",
		Payload:   []byte(`{}`),
	})

	ctx, cancel := context.WithCancel(context.Background())
	go relay.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()

	if !called.Load() {
		t.Fatal("expected backoff strategy to be called on failures")
	}
}

type testBackoff struct {
	called *atomic.Bool
}

func (b *testBackoff) NextDelay(attempt int) time.Duration {
	b.called.Store(true)
	return 100 * time.Millisecond
}

func TestRelayPriority(t *testing.T) {
	store := newMockStore()
	ctx := context.Background()

	// Insert low priority first, then high priority
	store.Insert(ctx, nil, &Message{
		EventName: "test.event",
		EventID:   "low",
		Payload:   []byte(`{}`),
		Priority:  1,
	})
	store.Insert(ctx, nil, &Message{
		EventName: "test.event",
		EventID:   "high",
		Payload:   []byte(`{}`),
		Priority:  10,
	})

	// Verify priority is stored correctly
	store.mu.Lock()
	if store.messages[0].Priority != 1 {
		t.Fatalf("expected priority 1, got %d", store.messages[0].Priority)
	}
	if store.messages[1].Priority != 10 {
		t.Fatalf("expected priority 10, got %d", store.messages[1].Priority)
	}
	store.mu.Unlock()

	// Verify GetPending returns high priority first
	msgs, err := store.GetPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].EventID != "high" {
		t.Fatalf("expected high-priority message first, got %s", msgs[0].EventID)
	}
	if msgs[1].EventID != "low" {
		t.Fatalf("expected low-priority message second, got %s", msgs[1].EventID)
	}
}

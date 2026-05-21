package stream

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
)

// mockStore implements monitor.Store for testing.
type mockStore struct {
	entries []*monitor.Entry
	err     error
}

func (m *mockStore) Record(ctx context.Context, entry *monitor.Entry) error {
	m.entries = append(m.entries, entry)
	return nil
}

func (m *mockStore) List(ctx context.Context, filter monitor.Filter) (*monitor.Page, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &monitor.Page{
		Entries: m.entries,
	}, nil
}

func (m *mockStore) Get(ctx context.Context, eventID, subscriptionID string) (*monitor.Entry, error) {
	return nil, nil
}

func (m *mockStore) GetByEventID(ctx context.Context, eventID string) ([]*monitor.Entry, error) {
	return nil, nil
}

func (m *mockStore) Count(ctx context.Context, filter monitor.Filter) (int64, error) {
	return int64(len(m.entries)), nil
}

func (m *mockStore) UpdateStatus(ctx context.Context, eventID, subscriptionID string, status monitor.Status, err error, duration time.Duration) error {
	return nil
}

func (m *mockStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	return 0, nil
}

func TestMatchesFilter_Empty(t *testing.T) {
	entry := &monitor.Entry{
		EventID:   "e1",
		EventName: "orders.created",
		Status:    monitor.StatusCompleted,
	}
	if !matchesFilter(entry, monitor.Filter{}) {
		t.Error("empty filter should match all entries")
	}
}

func TestMatchesFilter_EventName(t *testing.T) {
	entry := &monitor.Entry{EventName: "orders.created"}

	if !matchesFilter(entry, monitor.Filter{EventName: "orders.created"}) {
		t.Error("matching event name should match")
	}
	if matchesFilter(entry, monitor.Filter{EventName: "orders.updated"}) {
		t.Error("non-matching event name should not match")
	}
}

func TestMatchesFilter_Status(t *testing.T) {
	entry := &monitor.Entry{Status: monitor.StatusFailed}

	f := monitor.Filter{
		Status: []monitor.Status{monitor.StatusFailed, monitor.StatusRetrying},
	}
	if !matchesFilter(entry, f) {
		t.Error("entry status in filter list should match")
	}

	f.Status = []monitor.Status{monitor.StatusCompleted}
	if matchesFilter(entry, f) {
		t.Error("entry status not in filter list should not match")
	}
}

func TestMatchesFilter_HasError(t *testing.T) {
	withErr := &monitor.Entry{Error: "something failed"}
	noErr := &monitor.Entry{}

	hasErrTrue := true
	hasErrFalse := false

	if !matchesFilter(withErr, monitor.Filter{HasError: &hasErrTrue}) {
		t.Error("entry with error should match HasError=true")
	}
	if matchesFilter(noErr, monitor.Filter{HasError: &hasErrTrue}) {
		t.Error("entry without error should not match HasError=true")
	}
	if !matchesFilter(noErr, monitor.Filter{HasError: &hasErrFalse}) {
		t.Error("entry without error should match HasError=false")
	}
}

func TestMatchesFilter_DeliveryMode(t *testing.T) {
	entry := &monitor.Entry{DeliveryMode: monitor.WorkerPool}

	wp := monitor.WorkerPool
	bc := monitor.Broadcast

	if !matchesFilter(entry, monitor.Filter{DeliveryMode: &wp}) {
		t.Error("matching delivery mode should match")
	}
	if matchesFilter(entry, monitor.Filter{DeliveryMode: &bc}) {
		t.Error("non-matching delivery mode should not match")
	}
}

func TestMatchesFilter_TimeRange(t *testing.T) {
	now := time.Now()
	entry := &monitor.Entry{StartedAt: now}

	if !matchesFilter(entry, monitor.Filter{StartTime: now.Add(-time.Second)}) {
		t.Error("entry after start time should match")
	}
	if matchesFilter(entry, monitor.Filter{StartTime: now.Add(time.Second)}) {
		t.Error("entry before start time should not match")
	}
}

func TestMatchesFilter_MinDuration(t *testing.T) {
	entry := &monitor.Entry{Duration: 5 * time.Second}

	if !matchesFilter(entry, monitor.Filter{MinDuration: 3 * time.Second}) {
		t.Error("entry with longer duration should match")
	}
	if matchesFilter(entry, monitor.Filter{MinDuration: 10 * time.Second}) {
		t.Error("entry with shorter duration should not match")
	}
}

func TestMatchesFilter_MinRetries(t *testing.T) {
	entry := &monitor.Entry{RetryCount: 3}

	if !matchesFilter(entry, monitor.Filter{MinRetries: 2}) {
		t.Error("entry with more retries should match")
	}
	if matchesFilter(entry, monitor.Filter{MinRetries: 5}) {
		t.Error("entry with fewer retries should not match")
	}
}

func TestSubscriber_Close(t *testing.T) {
	sub := &Subscriber{
		entries: make(chan *monitor.Entry, 10),
		done:    make(chan struct{}),
	}

	sub.Close()
	// Second close should not panic
	sub.Close()
}

func TestSubscriber_Entries(t *testing.T) {
	sub := &Subscriber{
		entries: make(chan *monitor.Entry, 10),
		done:    make(chan struct{}),
	}
	ch := sub.Entries()
	if ch == nil {
		t.Error("Entries() should return non-nil channel")
	}
}

func TestNewBroadcaster(t *testing.T) {
	store := &mockStore{}
	b := NewBroadcaster(store, 0)
	if b.pollInterval != DefaultPollInterval {
		t.Errorf("default pollInterval = %v, want %v", b.pollInterval, DefaultPollInterval)
	}

	b = NewBroadcaster(store, 500*time.Millisecond)
	if b.pollInterval != 500*time.Millisecond {
		t.Errorf("pollInterval = %v, want 500ms", b.pollInterval)
	}
}

func TestBroadcaster_SubscribeUnsubscribe(t *testing.T) {
	store := &mockStore{}
	b := NewBroadcaster(store, time.Second)

	sub := b.Subscribe(monitor.Filter{EventName: "test"})
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}

	b.mu.RLock()
	count := len(b.subscribers)
	b.mu.RUnlock()
	if count != 1 {
		t.Errorf("subscriber count = %d, want 1", count)
	}

	b.Unsubscribe(sub)
	b.mu.RLock()
	count = len(b.subscribers)
	b.mu.RUnlock()
	if count != 0 {
		t.Errorf("subscriber count after unsubscribe = %d, want 0", count)
	}
}

func TestBroadcaster_UnsubscribeNil(t *testing.T) {
	store := &mockStore{}
	b := NewBroadcaster(store, time.Second)
	// Should not panic
	b.Unsubscribe(nil)
}

func TestBroadcaster_StartStop(t *testing.T) {
	store := &mockStore{}
	b := NewBroadcaster(store, 50*time.Millisecond)

	ctx := context.Background()
	b.Start(ctx)
	// Double start should be idempotent
	b.Start(ctx)

	// Stop blocks on the internal wait group until the poll goroutine exits,
	// so no warm-up sleep is needed for start/stop to be observably ordered.
	b.Stop()
}

func TestBroadcaster_StopWithoutStart(t *testing.T) {
	store := &mockStore{}
	b := NewBroadcaster(store, time.Second)
	// Should not panic
	b.Stop()
}

func TestBroadcaster_BroadcastEntries(t *testing.T) {
	entry := &monitor.Entry{
		EventID:   "e1",
		EventName: "orders.created",
		Status:    monitor.StatusCompleted,
		StartedAt: time.Now(),
	}
	store := &mockStore{entries: []*monitor.Entry{entry}}

	b := NewBroadcaster(store, 50*time.Millisecond)

	sub := b.Subscribe(monitor.Filter{EventName: "orders.created"})
	defer b.Unsubscribe(sub)

	ctx := context.Background()
	b.Start(ctx)
	defer b.Stop()

	select {
	case got := <-sub.Entries():
		if got.EventID != "e1" {
			t.Errorf("EventID = %q, want %q", got.EventID, "e1")
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for entry")
	}
}

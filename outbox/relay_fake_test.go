package outbox

import (
	"context"
	"sync"
	"time"
)

// fakeStore is an in-memory Store for engine unit tests.
type fakeStore struct {
	mu         sync.Mutex
	pending    []Message // FIFO; token is the index identity via EventID
	published  []string  // EventIDs acked
	failed     map[string]string
	claimErr   error
	closeErr   error
	ackErr     error
	cleanupErr error
	recovered  int64
	cleaned    int64
	notifyCh   chan struct{}
}

func newFakeStore(msgs ...Message) *fakeStore {
	return &fakeStore{pending: msgs, failed: map[string]string{}}
}

func (f *fakeStore) Store(context.Context, string, string, []byte, map[string]string) error {
	return nil
}

func (f *fakeStore) Cleanup(context.Context, time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cleanupErr != nil {
		return 0, f.cleanupErr
	}
	n := f.cleaned
	f.cleaned = 0
	return n, nil
}

func (f *fakeStore) ClaimPending(_ context.Context, limit int) (Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	n := limit
	if n > len(f.pending) {
		n = len(f.pending)
	}
	claimed := make([]Message, n)
	copy(claimed, f.pending[:n])
	for i := range claimed {
		claimed[i].token = claimed[i].EventID // opaque token = EventID for the fake
	}
	f.pending = f.pending[n:]
	return &fakeBatch{store: f, msgs: claimed}, nil
}

type fakeBatch struct {
	store *fakeStore
	msgs  []Message
}

func (b *fakeBatch) Messages() []Message { return b.msgs }

func (b *fakeBatch) Ack(_ context.Context, msg Message) error {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	if b.store.ackErr != nil {
		return b.store.ackErr
	}
	b.store.published = append(b.store.published, msg.token.(string))
	return nil
}

func (b *fakeBatch) Fail(_ context.Context, msg Message, cause error) error {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	m := msg
	m.RetryCount++
	// re-queue for the next claim, simulating status=failed being re-fetchable
	b.store.failed[msg.token.(string)] = cause.Error()
	b.store.pending = append(b.store.pending, m)
	return nil
}

func (b *fakeBatch) Close(context.Context) error { return b.store.closeErr }

// StuckRecoverer + Waker are opt-in via embedding in specific tests.
type fakeStoreWithRecovery struct{ *fakeStore }

func (f fakeStoreWithRecovery) RecoverStuck(context.Context, time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.recovered, nil
}

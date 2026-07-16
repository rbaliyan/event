package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

// stubTransport implements the full transport.Transport interface and records
// publishes; failNext forces one publish error for the failure-path test.
type stubTransport struct {
	published []string
	failNext  bool
}

func (s *stubTransport) Publish(_ context.Context, name string, _ message.Message) error {
	if s.failNext {
		s.failNext = false
		return errors.New("transport down")
	}
	s.published = append(s.published, name)
	return nil
}
func (s *stubTransport) Subscribe(context.Context, string, ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, nil
}
func (s *stubTransport) RegisterEvent(context.Context, string) error   { return nil }
func (s *stubTransport) UnregisterEvent(context.Context, string) error { return nil }
func (s *stubTransport) Close(context.Context) error                   { return nil }

var _ transport.Transport = (*stubTransport)(nil)

func msg(eventID, name string) Message { return Message{EventID: eventID, EventName: name} }

func TestDrainOnce_PublishesAndAcks(t *testing.T) {
	store := newFakeStore(msg("a", "order.created"), msg("b", "order.created"))
	tr := &stubTransport{}
	r := NewRelay(store, tr)
	if failures := r.drainOnce(context.Background()); failures != 0 {
		t.Fatalf("expected 0 failures, got %d", failures)
	}
	if len(tr.published) != 2 {
		t.Fatalf("expected 2 published, got %d", len(tr.published))
	}
	if len(store.published) != 2 {
		t.Fatalf("expected 2 acked, got %d", len(store.published))
	}
}

func TestDrainOnce_PublishFailureMarksFailed(t *testing.T) {
	store := newFakeStore(msg("a", "e"))
	tr := &stubTransport{failNext: true}
	r := NewRelay(store, tr)
	if failures := r.drainOnce(context.Background()); failures != 1 {
		t.Fatalf("expected 1 failure, got %d", failures)
	}
	if _, ok := store.failed["a"]; !ok {
		t.Fatalf("message 'a' should be marked failed")
	}
}

func TestDrainOnce_CloseErrorCountsAsFailure(t *testing.T) {
	store := newFakeStore(msg("a", "e"))
	store.closeErr = errors.New("commit failed")
	r := NewRelay(store, &stubTransport{})
	if failures := r.drainOnce(context.Background()); failures != 1 {
		t.Fatalf("Close error must count as a failure, got %d", failures)
	}
}

func TestShouldSkip_ExhaustedRetries(t *testing.T) {
	store := newFakeStore()
	r := NewRelay(store, &stubTransport{}, WithMaxRetries(3))
	if !r.shouldSkip(Message{RetryCount: 3}) {
		t.Fatal("RetryCount==maxRetries must skip")
	}
	if r.shouldSkip(Message{RetryCount: 2}) {
		t.Fatal("RetryCount<maxRetries must not skip")
	}
}

func TestPublishOnce_ReturnsErrorOnFailure(t *testing.T) {
	store := newFakeStore(msg("a", "e"))
	store.closeErr = errors.New("commit failed")
	r := NewRelay(store, &stubTransport{})
	if err := r.PublishOnce(context.Background()); err == nil {
		t.Fatal("PublishOnce must return error when a batch fails to close")
	}
}

package outbox

import (
	"context"
	"testing"
	"time"
)

// compile-time proof the interfaces are shaped as the engine expects.
type typesProbe struct{}

func (typesProbe) Store(context.Context, string, string, []byte, map[string]string) error {
	return nil
}
func (typesProbe) ClaimPending(context.Context, int) (Batch, error) { return nil, nil }
func (typesProbe) Cleanup(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ Store = typesProbe{}

func TestMessageCarriesUnexportedToken(t *testing.T) {
	m := Message{EventName: "x"}
	m.token = int64(42) // must compile: token is settable within the package
	if got, ok := m.token.(int64); !ok || got != 42 {
		t.Fatalf("token round-trip failed: %v", m.token)
	}
	if m.RetryCount != 0 {
		t.Fatalf("zero value RetryCount expected 0, got %d", m.RetryCount)
	}
}

func TestNewClaimedMessageRoundTrip(t *testing.T) {
	m := NewClaimedMessage(Message{EventID: "x"}, int64(7))
	if Token(m) != int64(7) {
		t.Fatalf("token round-trip failed: %v", Token(m))
	}
}

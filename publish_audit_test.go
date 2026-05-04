package event

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/channel"
)

// fakePublishAuditStore captures every RecordPublish call for assertions.
type fakePublishAuditStore struct {
	mu      sync.Mutex
	entries []RecordPublishParams
}

func (f *fakePublishAuditStore) RecordPublish(ctx context.Context, params RecordPublishParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries = append(f.entries, params)
	return nil
}

func (f *fakePublishAuditStore) Get(eventID string) (RecordPublishParams, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if e.EventID == eventID {
			return e, true
		}
	}
	return RecordPublishParams{}, false
}

func (f *fakePublishAuditStore) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

func TestPublishAudit_RecordsOnSuccessfulPublish(t *testing.T) {
	ctx := context.Background()
	audit := &fakePublishAuditStore{}

	bus := mustNewBus(t, "audit-bus-"+randomString(5),
		WithTransport(channel.New()),
		WithPublishAudit(audit),
	)
	defer bus.Close(ctx)

	ev := New[string]("audit.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	eventID := "evt-" + randomString(8)
	if err := ev.Publish(ContextWithEventID(ctx, eventID), "payload-data"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if audit.Len() != 1 {
		t.Fatalf("expected 1 audit entry, got %d", audit.Len())
	}
	entry, ok := audit.Get(eventID)
	if !ok {
		t.Fatalf("audit entry for %s not found", eventID)
	}
	if entry.EventName != "audit.event" {
		t.Errorf("EventName = %q, want %q", entry.EventName, "audit.event")
	}
	if entry.BusName == "" {
		t.Error("BusName should not be empty")
	}
	if entry.BusID == "" {
		t.Error("BusID should not be empty")
	}
	if entry.PayloadSize == 0 {
		t.Error("PayloadSize should not be zero")
	}
}

func TestPublishAudit_NotCalledWhenStoreNil(t *testing.T) {
	ctx := context.Background()
	bus := mustNewBus(t, "audit-nil-"+randomString(5),
		WithTransport(channel.New()),
	)
	defer bus.Close(ctx)
	if bus.PublishAuditStore() != nil {
		t.Error("expected PublishAuditStore to be nil")
	}

	ev := New[string]("audit.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}
	if err := ev.Publish(ctx, "data"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// No assertion needed — the test verifies that Publish doesn't panic
	// or fail when no audit store is configured.
}

func TestPublishAudit_AccessorReturnsConfiguredStore(t *testing.T) {
	ctx := context.Background()
	audit := &fakePublishAuditStore{}

	bus := mustNewBus(t, "audit-accessor-"+randomString(5),
		WithTransport(channel.New()),
		WithPublishAudit(audit),
	)
	defer bus.Close(ctx)

	got := bus.PublishAuditStore()
	if got == nil {
		t.Fatal("expected PublishAuditStore to be non-nil")
	}
	if got != PublishAuditStore(audit) {
		t.Error("PublishAuditStore should return the configured store")
	}
}

func TestPublishAudit_NotCalledOnClosedBus(t *testing.T) {
	// Verifies that audit recording happens only on successful transport.Publish:
	// publishes against a closed bus short-circuit before transport.Publish and
	// so should not produce audit entries.
	ctx := context.Background()
	audit := &fakePublishAuditStore{}

	bus := mustNewBus(t, "audit-closed-"+randomString(5),
		WithTransport(channel.New()),
		WithPublishAudit(audit),
	)

	ev := New[string]("audit.event.closed")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// Close the bus. The error from event unregistration (transport closed
	// first) is a known warning in the existing bus.Close implementation
	// and is not relevant to this test's assertions.
	_ = bus.Close(ctx)

	// Publish should be rejected with ErrBusClosed before reaching the transport.
	if err := ev.Publish(ctx, "after-close"); err == nil {
		t.Error("expected publish on closed bus to fail")
	}

	// Allow any in-flight goroutines to settle
	time.Sleep(20 * time.Millisecond)

	if audit.Len() != 0 {
		t.Errorf("expected 0 audit entries after closed-bus publish, got %d", audit.Len())
	}
}

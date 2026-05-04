package monitor

import (
	"context"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
)

// TestMemoryStore_RecordPublishCreatesEntry verifies that RecordPublish writes
// an Entry with Status=StatusPublished, SubscriptionID=PublishMarker, and the
// supplied bus/event metadata.
func TestMemoryStore_RecordPublishCreatesEntry(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	params := event.RecordPublishParams{
		EventID:   "evt-1",
		EventName: "order.created",
		BusID:     "bus-abc",
		BusName:   "orders",
		TraceID:   "trace-1",
		SpanID:    "span-1",
	}
	if err := s.RecordPublish(ctx, params); err != nil {
		t.Fatalf("RecordPublish: %v", err)
	}

	entry, err := s.Get(ctx, "evt-1", PublishMarker)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if entry == nil {
		t.Fatal("expected publish entry, got nil")
	}
	if entry.Status != StatusPublished {
		t.Errorf("Status = %q, want %q", entry.Status, StatusPublished)
	}
	if entry.SubscriptionID != PublishMarker {
		t.Errorf("SubscriptionID = %q, want %q", entry.SubscriptionID, PublishMarker)
	}
	if entry.EventName != "order.created" {
		t.Errorf("EventName = %q, want %q", entry.EventName, "order.created")
	}
	if entry.BusID != "bus-abc" {
		t.Errorf("BusID = %q, want bus-abc", entry.BusID)
	}
	if entry.InstanceID != "orders" {
		t.Errorf("InstanceID = %q, want orders (bus name)", entry.InstanceID)
	}
	if entry.TraceID != "trace-1" {
		t.Errorf("TraceID = %q, want trace-1", entry.TraceID)
	}
	if entry.CompletedAt == nil {
		t.Error("CompletedAt should be set for publish entries")
	}
}

// TestMemoryStore_GetByEventIDIncludesPublishAndHandlers verifies the unified
// diagnostic flow: a single GetByEventID returns the publish entry plus all
// subscriber entries, so callers can answer "was it fired and processed?".
func TestMemoryStore_GetByEventIDIncludesPublishAndHandlers(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	// Publish (producer-side milestone).
	if err := s.RecordPublish(ctx, event.RecordPublishParams{
		EventID:   "evt-x",
		EventName: "order.created",
		BusID:     "bus-1",
	}); err != nil {
		t.Fatalf("RecordPublish: %v", err)
	}

	// Subscriber A starts and completes.
	if err := s.RecordStart(ctx, event.RecordStartParams{
		EventID:        "evt-x",
		SubscriptionID: "sub-a",
		EventName:      "order.created",
		BusID:          "bus-1",
	}); err != nil {
		t.Fatalf("RecordStart A: %v", err)
	}
	if err := s.RecordComplete(ctx, event.RecordCompleteParams{
		EventID:        "evt-x",
		SubscriptionID: "sub-a",
		Status:         "completed",
		Duration:       2 * time.Millisecond,
	}); err != nil {
		t.Fatalf("RecordComplete A: %v", err)
	}

	// Subscriber B starts and fails.
	if err := s.RecordStart(ctx, event.RecordStartParams{
		EventID:        "evt-x",
		SubscriptionID: "sub-b",
		EventName:      "order.created",
		BusID:          "bus-1",
	}); err != nil {
		t.Fatalf("RecordStart B: %v", err)
	}

	entries, err := s.GetByEventID(ctx, "evt-x")
	if err != nil {
		t.Fatalf("GetByEventID: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries (publish + 2 subscribers), got %d", len(entries))
	}

	var hasPublish, hasA, hasB bool
	for _, e := range entries {
		switch e.SubscriptionID {
		case PublishMarker:
			hasPublish = true
			if e.Status != StatusPublished {
				t.Errorf("publish entry status = %q, want %q", e.Status, StatusPublished)
			}
		case "sub-a":
			hasA = true
			if e.Status != StatusCompleted {
				t.Errorf("sub-a status = %q, want %q", e.Status, StatusCompleted)
			}
		case "sub-b":
			hasB = true
			if e.Status != StatusPending {
				t.Errorf("sub-b status = %q, want %q", e.Status, StatusPending)
			}
		}
	}
	if !hasPublish || !hasA || !hasB {
		t.Errorf("missing entries: publish=%v sub-a=%v sub-b=%v", hasPublish, hasA, hasB)
	}
}

// TestMemoryStore_FilterByStatusPublished verifies the existing List/Filter
// surface can return only publish entries via Status=StatusPublished.
func TestMemoryStore_FilterByStatusPublished(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	for i := 0; i < 3; i++ {
		if err := s.RecordPublish(ctx, event.RecordPublishParams{
			EventID:   "p" + string(rune('1'+i)),
			EventName: "order.created",
		}); err != nil {
			t.Fatalf("RecordPublish: %v", err)
		}
	}
	if err := s.RecordStart(ctx, event.RecordStartParams{
		EventID:        "p1",
		SubscriptionID: "sub",
		EventName:      "order.created",
	}); err != nil {
		t.Fatalf("RecordStart: %v", err)
	}

	page, err := s.List(ctx, Filter{Status: []Status{StatusPublished}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Entries) != 3 {
		t.Errorf("expected 3 publish entries, got %d", len(page.Entries))
	}
	for _, e := range page.Entries {
		if e.Status != StatusPublished {
			t.Errorf("entry %s has status %q, want %q", e.EventID, e.Status, StatusPublished)
		}
	}
}

// TestMemoryStore_RecordPublishOverwrites verifies idempotent semantics:
// republishing the same event ID overwrites without error.
func TestMemoryStore_RecordPublishOverwrites(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	defer s.Close()

	for i := 0; i < 3; i++ {
		if err := s.RecordPublish(ctx, event.RecordPublishParams{
			EventID:   "evt-same",
			EventName: "order.created",
		}); err != nil {
			t.Fatalf("RecordPublish %d: %v", i, err)
		}
	}

	count, err := s.Count(ctx, Filter{EventID: "evt-same"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 entry after 3 republishes, got %d", count)
	}
}

// Compile-time check: monitor.MemoryStore satisfies event.PublishAuditStore
// alongside event.MonitorStore.
var _ event.PublishAuditStore = (*MemoryStore)(nil)

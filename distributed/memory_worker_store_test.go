package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/internal/clock"
)

func newTestMemoryWorkerStore(t *testing.T) *MemoryStateManager {
	t.Helper()
	sm := NewMemoryStateManager(WithCleanup(false, 0))
	t.Cleanup(sm.Close)
	return sm
}

// newTestMemoryWorkerStoreWithClock returns a state manager wired to a
// clock.Fake pinned at the Unix epoch. Tests that need to establish a
// time-ordering between Acquire calls (e.g., to assert "msg-stale was
// created before msg-fresh") use clk.Advance to bump the fake clock
// deterministically instead of time.Sleep.
func newTestMemoryWorkerStoreWithClock(t *testing.T) (*MemoryStateManager, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Time{})
	sm := NewMemoryStateManager(WithCleanup(false, 0), withClock(clk))
	t.Cleanup(sm.Close)
	return sm, clk
}

func TestMemoryListWorkers(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.Acquire(ctx, "msg-3", 5*time.Minute)

	page, err := sm.ListWorkers(ctx, WorkerFilter{})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(page.Entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(page.Entries))
	}
}

func TestMemoryListWorkersFilterStatus(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.MarkProcessed(ctx, "msg-2")

	t.Run("processing only", func(t *testing.T) {
		page, err := sm.ListWorkers(ctx, WorkerFilter{
			Status: []WorkerState{WorkerStateProcessing},
		})
		if err != nil {
			t.Fatalf("ListWorkers: %v", err)
		}
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 processing entry, got %d", len(page.Entries))
		}
		if len(page.Entries) > 0 && page.Entries[0].Status != WorkerStateProcessing {
			t.Errorf("expected processing, got %s", page.Entries[0].Status)
		}
	})

	t.Run("completed only", func(t *testing.T) {
		page, err := sm.ListWorkers(ctx, WorkerFilter{
			Status: []WorkerState{WorkerStateCompleted},
		})
		if err != nil {
			t.Fatalf("ListWorkers: %v", err)
		}
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 completed entry, got %d", len(page.Entries))
		}
		if len(page.Entries) > 0 && page.Entries[0].Status != WorkerStateCompleted {
			t.Errorf("expected completed, got %s", page.Entries[0].Status)
		}
	})
}

func TestMemoryListWorkersStaleTimeout(t *testing.T) {
	ctx := context.Background()
	sm, clk := newTestMemoryWorkerStoreWithClock(t)

	sm.Acquire(ctx, "msg-stale", 5*time.Minute)
	clk.Advance(15 * time.Millisecond) // msg-stale ages past the 10ms threshold
	sm.Acquire(ctx, "msg-fresh", 5*time.Minute)

	page, err := sm.ListWorkers(ctx, WorkerFilter{
		StaleTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(page.Entries) != 1 {
		t.Errorf("expected 1 stale entry, got %d", len(page.Entries))
	}
	if len(page.Entries) > 0 && page.Entries[0].MessageID != "msg-stale" {
		t.Errorf("expected msg-stale, got %s", page.Entries[0].MessageID)
	}
}

func TestMemoryListWorkersCreatedRange(t *testing.T) {
	ctx := context.Background()
	sm, clk := newTestMemoryWorkerStoreWithClock(t)

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	clk.Advance(5 * time.Millisecond)
	midpoint := clk.Now() // capture from the fake clock so filter comparisons use the same timeline
	clk.Advance(5 * time.Millisecond)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)

	t.Run("created after midpoint", func(t *testing.T) {
		page, err := sm.ListWorkers(ctx, WorkerFilter{
			CreatedAfter: midpoint,
		})
		if err != nil {
			t.Fatalf("ListWorkers: %v", err)
		}
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 entry after midpoint, got %d", len(page.Entries))
		}
	})

	t.Run("created before midpoint", func(t *testing.T) {
		page, err := sm.ListWorkers(ctx, WorkerFilter{
			CreatedBefore: midpoint,
		})
		if err != nil {
			t.Fatalf("ListWorkers: %v", err)
		}
		if len(page.Entries) != 1 {
			t.Errorf("expected 1 entry before midpoint, got %d", len(page.Entries))
		}
	})
}

func TestMemoryListWorkersOrderDesc(t *testing.T) {
	ctx := context.Background()
	sm, clk := newTestMemoryWorkerStoreWithClock(t)

	sm.Acquire(ctx, "msg-a", 5*time.Minute)
	clk.Advance(2 * time.Millisecond)
	sm.Acquire(ctx, "msg-b", 5*time.Minute)
	clk.Advance(2 * time.Millisecond)
	sm.Acquire(ctx, "msg-c", 5*time.Minute)

	page, err := sm.ListWorkers(ctx, WorkerFilter{OrderDesc: true})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(page.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(page.Entries))
	}
	// Descending: msg-c (newest) should be first
	if page.Entries[0].MessageID != "msg-c" {
		t.Errorf("expected msg-c first (newest), got %s", page.Entries[0].MessageID)
	}
	if page.Entries[2].MessageID != "msg-a" {
		t.Errorf("expected msg-a last (oldest), got %s", page.Entries[2].MessageID)
	}
}

func TestMemoryListWorkersPagination(t *testing.T) {
	ctx := context.Background()
	sm, clk := newTestMemoryWorkerStoreWithClock(t)

	for i := 0; i < 5; i++ {
		sm.Acquire(ctx, "msg-"+string(rune('a'+i)), 5*time.Minute)
		clk.Advance(2 * time.Millisecond)
	}

	// First page
	page1, err := sm.ListWorkers(ctx, WorkerFilter{Limit: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Entries) != 2 {
		t.Fatalf("expected 2 entries on page 1, got %d", len(page1.Entries))
	}
	if !page1.HasMore {
		t.Fatal("expected HasMore on page 1")
	}
	if page1.NextCursor == "" {
		t.Fatal("expected non-empty cursor on page 1")
	}

	// Second page
	page2, err := sm.ListWorkers(ctx, WorkerFilter{Limit: 2, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Entries) != 2 {
		t.Fatalf("expected 2 entries on page 2, got %d", len(page2.Entries))
	}
	if !page2.HasMore {
		t.Fatal("expected HasMore on page 2")
	}

	// Third page (last)
	page3, err := sm.ListWorkers(ctx, WorkerFilter{Limit: 2, Cursor: page2.NextCursor})
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if len(page3.Entries) != 1 {
		t.Fatalf("expected 1 entry on page 3, got %d", len(page3.Entries))
	}
	if page3.HasMore {
		t.Fatal("expected no more pages")
	}

	// Verify no overlap between pages
	seen := make(map[string]bool)
	for _, e := range page1.Entries {
		seen[e.MessageID] = true
	}
	for _, e := range page2.Entries {
		if seen[e.MessageID] {
			t.Errorf("duplicate entry %s on page 2", e.MessageID)
		}
		seen[e.MessageID] = true
	}
	for _, e := range page3.Entries {
		if seen[e.MessageID] {
			t.Errorf("duplicate entry %s on page 3", e.MessageID)
		}
	}
}

func TestMemoryListWorkersCursorPastEnd(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	sm.Acquire(ctx, "msg-1", 5*time.Minute)

	// Get cursor from first page
	page1, err := sm.ListWorkers(ctx, WorkerFilter{Limit: 1})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(page1.Entries))
	}

	// Create a cursor pointing past all entries
	futureCursor := encodeWorkerCursor(workerCursor{
		UpdatedAt: time.Now().Add(time.Hour),
		ID:        "zzz",
	})

	page2, err := sm.ListWorkers(ctx, WorkerFilter{Cursor: futureCursor})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Entries) != 0 {
		t.Errorf("expected 0 entries for cursor past end, got %d", len(page2.Entries))
	}
	if page2.HasMore {
		t.Error("expected HasMore=false for cursor past end")
	}
}

func TestMemoryListWorkersInvalidCursor(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	_, err := sm.ListWorkers(ctx, WorkerFilter{Cursor: "not-valid-base64!"})
	if err == nil {
		t.Error("expected error for invalid cursor")
	}
}

func TestMemoryCountWorkers(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	sm.Acquire(ctx, "msg-1", 5*time.Minute)
	sm.Acquire(ctx, "msg-2", 5*time.Minute)
	sm.Acquire(ctx, "msg-3", 5*time.Minute)
	sm.MarkProcessed(ctx, "msg-3")

	t.Run("total count", func(t *testing.T) {
		count, err := sm.CountWorkers(ctx, WorkerFilter{})
		if err != nil {
			t.Fatalf("CountWorkers: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3, got %d", count)
		}
	})

	t.Run("count processing", func(t *testing.T) {
		count, err := sm.CountWorkers(ctx, WorkerFilter{
			Status: []WorkerState{WorkerStateProcessing},
		})
		if err != nil {
			t.Fatalf("CountWorkers: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 processing, got %d", count)
		}
	})

	t.Run("count completed", func(t *testing.T) {
		count, err := sm.CountWorkers(ctx, WorkerFilter{
			Status: []WorkerState{WorkerStateCompleted},
		})
		if err != nil {
			t.Fatalf("CountWorkers: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 completed, got %d", count)
		}
	})
}

func TestMemoryGetWorker(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	sm.Acquire(ctx, "msg-42", 5*time.Minute)

	entry, err := sm.GetWorker(ctx, "msg-42")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if entry.MessageID != "msg-42" {
		t.Errorf("expected msg-42, got %s", entry.MessageID)
	}
	if entry.Status != WorkerStateProcessing {
		t.Errorf("expected processing, got %s", entry.Status)
	}
}

func TestMemoryGetWorkerNotFound(t *testing.T) {
	ctx := context.Background()
	sm := newTestMemoryWorkerStore(t)

	entry, err := sm.GetWorker(ctx, "non-existent")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if entry != nil {
		t.Errorf("expected nil for non-existent, got %+v", entry)
	}
}

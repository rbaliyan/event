package checkpoint

import (
	"context"
	"testing"
	"time"
)

func TestMemoryCheckpointStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	t.Run("Save and Load", func(t *testing.T) {
		now := time.Now()
		err := store.Save(ctx, "sub-1", now)
		if err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		loaded, err := store.Load(ctx, "sub-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if !loaded.Equal(now) {
			t.Errorf("expected %v, got %v", now, loaded)
		}
	})

	t.Run("Load non-existent returns zero time", func(t *testing.T) {
		loaded, err := store.Load(ctx, "non-existent")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if !loaded.IsZero() {
			t.Errorf("expected zero time, got %v", loaded)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		now := time.Now()
		store.Save(ctx, "to-delete", now)

		err := store.Delete(ctx, "to-delete")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		loaded, _ := store.Load(ctx, "to-delete")
		if !loaded.IsZero() {
			t.Errorf("expected zero time after delete, got %v", loaded)
		}
	})

	t.Run("Delete non-existent does not error", func(t *testing.T) {
		err := store.Delete(ctx, "never-existed")
		if err != nil {
			t.Errorf("Delete non-existent should not error: %v", err)
		}
	})

	t.Run("Overwrite existing checkpoint", func(t *testing.T) {
		first := time.Now()
		store.Save(ctx, "overwrite", first)

		second := first.Add(time.Hour)
		store.Save(ctx, "overwrite", second)

		loaded, _ := store.Load(ctx, "overwrite")
		if !loaded.Equal(second) {
			t.Errorf("expected %v, got %v", second, loaded)
		}
	})
}

func TestCheckpointInfo(t *testing.T) {
	info := &CheckpointInfo{
		SubscriberID: "test-sub",
		Position:     time.Now(),
		UpdatedAt:    time.Now(),
	}

	if info.SubscriberID != "test-sub" {
		t.Errorf("expected test-sub, got %s", info.SubscriberID)
	}
}

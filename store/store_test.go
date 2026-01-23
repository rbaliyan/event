package store

import (
	"context"
	"testing"
	"time"
)

// testItem implements TimestampedItem for testing.
type testItem struct {
	ID        string    `bson:"_id"`
	Name      string    `bson:"name"`
	CreatedAt time.Time `bson:"created_at"`
}

func (t *testItem) GetID() string           { return t.ID }
func (t *testItem) GetCreatedAt() time.Time { return t.CreatedAt }

func TestMemoryStore_CRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore[*testItem]()

	// Create
	item := &testItem{ID: "1", Name: "test", CreatedAt: time.Now()}
	if err := store.Create(ctx, item); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Create duplicate should fail
	if err := store.Create(ctx, item); err != ErrAlreadyExists {
		t.Fatalf("Expected ErrAlreadyExists, got: %v", err)
	}

	// Get
	got, err := store.Get(ctx, "1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Name != "test" {
		t.Fatalf("Expected name 'test', got '%s'", got.Name)
	}

	// Get non-existent
	_, err = store.Get(ctx, "nonexistent")
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, got: %v", err)
	}

	// Update
	item.Name = "updated"
	if err := store.Update(ctx, item); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, _ = store.Get(ctx, "1")
	if got.Name != "updated" {
		t.Fatalf("Expected name 'updated', got '%s'", got.Name)
	}

	// Update non-existent
	nonexistent := &testItem{ID: "nonexistent", Name: "test", CreatedAt: time.Now()}
	if err := store.Update(ctx, nonexistent); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, got: %v", err)
	}

	// Delete
	if err := store.Delete(ctx, "1"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = store.Get(ctx, "1")
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound after delete, got: %v", err)
	}
}

func TestMemoryStore_List(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore[*testItem](WithDefaultLimit[*testItem](2))

	// Create items with different timestamps
	now := time.Now()
	items := []*testItem{
		{ID: "1", Name: "first", CreatedAt: now.Add(-3 * time.Hour)},
		{ID: "2", Name: "second", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "3", Name: "third", CreatedAt: now.Add(-1 * time.Hour)},
		{ID: "4", Name: "fourth", CreatedAt: now},
	}
	for _, item := range items {
		store.Create(ctx, item)
	}

	// List first page (ascending)
	page, err := store.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(page.Items))
	}
	if page.Items[0].ID != "1" {
		t.Fatalf("Expected first item ID '1', got '%s'", page.Items[0].ID)
	}
	if !page.HasMore() {
		t.Fatal("Expected HasMore() to be true")
	}

	// List second page
	page2, err := store.List(ctx, Filter{Cursor: page.NextCursor})
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(page2.Items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(page2.Items))
	}
	if page2.Items[0].ID != "3" {
		t.Fatalf("Expected first item ID '3', got '%s'", page2.Items[0].ID)
	}

	// List descending
	page, err = store.List(ctx, Filter{OrderDesc: true})
	if err != nil {
		t.Fatalf("List descending failed: %v", err)
	}
	if page.Items[0].ID != "4" {
		t.Fatalf("Expected first item ID '4' (newest), got '%s'", page.Items[0].ID)
	}
}

func TestMemoryStore_TimeFilter(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore[*testItem]()

	now := time.Now()
	items := []*testItem{
		{ID: "1", Name: "old", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "2", Name: "recent", CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "3", Name: "new", CreatedAt: now},
	}
	for _, item := range items {
		store.Create(ctx, item)
	}

	// Filter by time range
	page, err := store.List(ctx, Filter{
		StartTime: now.Add(-1 * time.Hour),
		EndTime:   now.Add(1 * time.Minute),
	})
	if err != nil {
		t.Fatalf("List with time filter failed: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("Expected 2 items in time range, got %d", len(page.Items))
	}
}

func TestMemoryStore_Count(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore[*testItem]()

	now := time.Now()
	for i := 0; i < 5; i++ {
		store.Create(ctx, &testItem{
			ID:        string(rune('a' + i)),
			Name:      "test",
			CreatedAt: now.Add(time.Duration(i) * time.Hour),
		})
	}

	count, err := store.Count(ctx, Filter{})
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 5 {
		t.Fatalf("Expected count 5, got %d", count)
	}
}

func TestMemoryStore_DeleteOlderThan(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore[*testItem]()

	now := time.Now()
	items := []*testItem{
		{ID: "1", Name: "old", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "2", Name: "recent", CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "3", Name: "new", CreatedAt: now},
	}
	for _, item := range items {
		store.Create(ctx, item)
	}

	deleted, err := store.DeleteOlderThan(ctx, 1*time.Hour)
	if err != nil {
		t.Fatalf("DeleteOlderThan failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("Expected 1 deleted, got %d", deleted)
	}
	if store.Len() != 2 {
		t.Fatalf("Expected 2 remaining items, got %d", store.Len())
	}
}

func TestCursor_EncodeDecode(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond) // JSON loses nanoseconds
	c := NewCursor(now, "test-id")

	encoded := EncodeCursor(c)
	if encoded == "" {
		t.Fatal("Expected non-empty encoded cursor")
	}

	decoded := DecodeCursor(encoded)
	if !decoded.Timestamp.Equal(now) {
		t.Fatalf("Timestamp mismatch: %v != %v", decoded.Timestamp, now)
	}
	if decoded.ID != "test-id" {
		t.Fatalf("ID mismatch: %s != test-id", decoded.ID)
	}
}

func TestCursor_EmptyString(t *testing.T) {
	c := DecodeCursor("")
	if !c.IsZero() {
		t.Fatal("Expected zero cursor for empty string")
	}

	encoded := EncodeCursor(Cursor{})
	if encoded != "" {
		t.Fatalf("Expected empty string for zero cursor, got '%s'", encoded)
	}
}

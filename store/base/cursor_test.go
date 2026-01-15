package base

import (
	"testing"
	"time"
)

func TestEncodeDecode(t *testing.T) {
	type testCursor struct {
		StartedAt time.Time `json:"s"`
		EventID   string    `json:"e"`
		SubID     string    `json:"u"`
	}

	original := testCursor{
		StartedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		EventID:   "evt-123",
		SubID:     "sub-456",
	}

	// Encode
	encoded := EncodeCursor(original)
	if encoded == "" {
		t.Error("expected non-empty encoded cursor")
	}

	// Decode
	decoded, err := DecodeCursor[testCursor](encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if !decoded.StartedAt.Equal(original.StartedAt) {
		t.Errorf("StartedAt mismatch: got %v, want %v", decoded.StartedAt, original.StartedAt)
	}
	if decoded.EventID != original.EventID {
		t.Errorf("EventID mismatch: got %v, want %v", decoded.EventID, original.EventID)
	}
	if decoded.SubID != original.SubID {
		t.Errorf("SubID mismatch: got %v, want %v", decoded.SubID, original.SubID)
	}
}

func TestDecodeEmptyString(t *testing.T) {
	type testCursor struct {
		ID string `json:"id"`
	}

	decoded, err := DecodeCursor[testCursor]("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decoded.ID != "" {
		t.Errorf("expected zero value for empty string, got %v", decoded.ID)
	}
}

func TestDecodeInvalidBase64(t *testing.T) {
	type testCursor struct {
		ID string `json:"id"`
	}

	_, err := DecodeCursor[testCursor]("not-valid-base64!!!")
	if err == nil {
		t.Error("expected error for invalid base64")
	}
}

func TestPaginate(t *testing.T) {
	type item struct {
		ID   int
		Name string
	}

	type cursor struct {
		LastID int `json:"id"`
	}

	t.Run("no more pages", func(t *testing.T) {
		items := []item{{ID: 1}, {ID: 2}, {ID: 3}}
		result := Paginate(items, 5, func(i item) cursor { return cursor{LastID: i.ID} })

		if result.HasMore {
			t.Error("expected HasMore to be false")
		}
		if result.NextCursor != "" {
			t.Error("expected empty NextCursor")
		}
		if len(result.Items) != 3 {
			t.Errorf("expected 3 items, got %d", len(result.Items))
		}
	})

	t.Run("has more pages", func(t *testing.T) {
		// Fetch with limit+1 pattern
		items := []item{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}, {ID: 6}}
		result := Paginate(items, 5, func(i item) cursor { return cursor{LastID: i.ID} })

		if !result.HasMore {
			t.Error("expected HasMore to be true")
		}
		if result.NextCursor == "" {
			t.Error("expected non-empty NextCursor")
		}
		if len(result.Items) != 5 {
			t.Errorf("expected 5 items, got %d", len(result.Items))
		}

		// Verify cursor contains last item's ID
		decoded, _ := DecodeCursor[cursor](result.NextCursor)
		if decoded.LastID != 5 {
			t.Errorf("expected cursor LastID to be 5, got %d", decoded.LastID)
		}
	})

	t.Run("empty items", func(t *testing.T) {
		var items []item
		result := Paginate(items, 5, func(i item) cursor { return cursor{LastID: i.ID} })

		if result.HasMore {
			t.Error("expected HasMore to be false")
		}
		if result.NextCursor != "" {
			t.Error("expected empty NextCursor")
		}
	})
}

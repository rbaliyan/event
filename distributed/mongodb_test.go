package distributed

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestGenerateWorkerID(t *testing.T) {
	// Without instance ID: generates 24-character hex strings
	sm := &MongoStateManager{}
	id1 := sm.generateWorkerID()
	id2 := sm.generateWorkerID()

	if id1 == "" {
		t.Fatal("expected non-empty worker ID")
	}
	if id2 == "" {
		t.Fatal("expected non-empty worker ID")
	}
	// 12 random bytes → 24 hex characters
	if len(id1) != 24 {
		t.Fatalf("expected worker ID length 24, got %d", len(id1))
	}
	if id1 == id2 {
		t.Fatal("expected unique worker IDs")
	}
}

func TestGenerateWorkerID_WithInstanceID(t *testing.T) {
	sm := &MongoStateManager{instanceID: "compass-7d8f9b6c4-x2k9p"}
	id := sm.generateWorkerID()

	// Format: "instanceID:randomHex"
	if len(id) != len("compass-7d8f9b6c4-x2k9p")+1+24 {
		t.Fatalf("expected worker ID length %d, got %d: %s",
			len("compass-7d8f9b6c4-x2k9p")+1+24, len(id), id)
	}
	if id[:len("compass-7d8f9b6c4-x2k9p")+1] != "compass-7d8f9b6c4-x2k9p:" {
		t.Fatalf("expected prefix 'compass-7d8f9b6c4-x2k9p:', got %s", id)
	}

	// Two calls should produce different IDs (different random nonce)
	id2 := sm.generateWorkerID()
	if id == id2 {
		t.Fatal("expected unique worker IDs")
	}
}

func TestIsNamespaceExistsError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "namespace exists error (code 48)",
			err:      mongo.CommandError{Code: 48, Message: "collection already exists"},
			expected: true,
		},
		{
			name:     "different command error",
			err:      mongo.CommandError{Code: 11000, Message: "duplicate key"},
			expected: false,
		},
		{
			name:     "non-command error",
			err:      mongo.ErrNoDocuments,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNamespaceExistsError(tt.err); got != tt.expected {
				t.Errorf("isNamespaceExistsError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMongoStateManager_Indexes_Regular(t *testing.T) {
	// nil db is fine - we're only testing Indexes() which doesn't call MongoDB
	sm := &MongoStateManager{capped: false}
	indexes := sm.Indexes()

	if len(indexes) != 4 {
		t.Fatalf("expected 4 indexes for regular collection, got %d", len(indexes))
	}

	// First index: TTL index on expires_at
	if indexes[0].Options == nil {
		t.Fatal("expected TTL index to have options")
	}

	// Remaining indexes: compound indexes without TTL options
	for i := 1; i < len(indexes); i++ {
		if indexes[i].Options != nil {
			t.Fatalf("expected index %d to have no options (not TTL)", i)
		}
	}
}

func TestMongoStateManager_Indexes_Capped(t *testing.T) {
	sm := &MongoStateManager{capped: true}
	indexes := sm.Indexes()

	if len(indexes) != 4 {
		t.Fatalf("expected 4 indexes for capped collection, got %d", len(indexes))
	}

	// Capped collections don't support TTL indexes
	for i, idx := range indexes {
		if idx.Options != nil {
			t.Fatalf("expected capped index %d to have no TTL options", i)
		}
	}
}

func TestMongoStatusConstants(t *testing.T) {
	// Verify constants have expected values (guards against accidental changes)
	if statusProcessing != "processing" {
		t.Errorf("statusProcessing = %q, want %q", statusProcessing, "processing")
	}
	if statusCompleted != "completed" {
		t.Errorf("statusCompleted = %q, want %q", statusCompleted, "completed")
	}
	if statusReleased != "released" {
		t.Errorf("statusReleased = %q, want %q", statusReleased, "released")
	}
	if defaultStateCollection != "_message_state" {
		t.Errorf("defaultStateCollection = %q, want %q", defaultStateCollection, "_message_state")
	}
}

func TestMongoStateManager_CreateCollection_NonCapped(t *testing.T) {
	// Non-capped CreateCollection is a no-op
	sm := &MongoStateManager{capped: false}
	err := sm.CreateCollection(t.Context())
	if err != nil {
		t.Fatalf("expected nil error for non-capped CreateCollection, got %v", err)
	}
}

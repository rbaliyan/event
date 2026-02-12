//go:build integration

package distributed

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27017"
}

func setupMongoStateManager(t *testing.T) (*MongoStateManager, func()) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(getMongoURI()))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("MongoDB not reachable: %v", err)
	}

	dbName := "event_test_distributed"
	collName := "state_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	sm := NewMongoStateManager(db,
		WithCollection(collName),
		WithCompletedTTL(time.Hour),
	)

	if err := sm.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("failed to ensure indexes: %v", err)
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sm.collection.Drop(ctx)
		_ = client.Disconnect(ctx)
	}

	return sm, cleanup
}

func TestMongoStateManager_Integration_Acquire(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("first acquisition succeeds", func(t *testing.T) {
		acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Error("expected acquisition to succeed")
		}
	})

	t.Run("second acquisition for same message fails", func(t *testing.T) {
		msgID := "msg-2"
		acquired, err := sm.Acquire(ctx, msgID, time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Error("expected first acquisition to succeed")
		}

		acquired, err = sm.Acquire(ctx, msgID, time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acquired {
			t.Error("expected second acquisition to fail")
		}
	})

	t.Run("different messages can be acquired", func(t *testing.T) {
		acquired1, _ := sm.Acquire(ctx, "msg-diff-1", time.Minute)
		acquired2, _ := sm.Acquire(ctx, "msg-diff-2", time.Minute)

		if !acquired1 || !acquired2 {
			t.Error("expected both different messages to be acquired")
		}
	})
}

func TestMongoStateManager_Integration_MarkProcessed(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("mark processed prevents reacquisition", func(t *testing.T) {
		msgID := "msg-processed-1"

		acquired, _ := sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		if err := sm.MarkProcessed(ctx, msgID); err != nil {
			t.Fatalf("MarkProcessed failed: %v", err)
		}

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if acquired {
			t.Error("expected acquisition to fail after mark processed")
		}
	})
}

func TestMongoStateManager_Integration_Reset(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("reset allows reacquisition", func(t *testing.T) {
		msgID := "msg-reset-1"

		acquired, _ := sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		if err := sm.Reset(ctx, msgID); err != nil {
			t.Fatalf("Reset failed: %v", err)
		}

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Error("expected acquisition to succeed after reset")
		}
	})
}

func TestMongoStateManager_Integration_TTLExpiry(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("expired state allows reacquisition", func(t *testing.T) {
		msgID := "msg-expiry-1"

		acquired, _ := sm.Acquire(ctx, msgID, 50*time.Millisecond)
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}

		// Wait for expiry
		time.Sleep(100 * time.Millisecond)

		acquired, _ = sm.Acquire(ctx, msgID, time.Minute)
		if !acquired {
			t.Error("expected acquisition to succeed after expiry")
		}
	})
}

func TestMongoStateManager_Integration_ListStale(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "stale-1", time.Hour)
	sm.Acquire(ctx, "stale-2", time.Hour)
	sm.Acquire(ctx, "stale-3", time.Hour)

	// Mark one as processed
	sm.MarkProcessed(ctx, "stale-2")

	// Wait a bit so they become stale
	time.Sleep(100 * time.Millisecond)

	// List stale with 50ms stale timeout
	stale, err := sm.ListStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}

	// Should find stale-1 and stale-3 (processing and stale), not stale-2 (completed)
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale, got %d", len(stale))
	}

	// Test limit
	stale, err = sm.ListStale(ctx, 50*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("ListStale with limit failed: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale with limit, got %d", len(stale))
	}
}

func TestMongoStateManager_Integration_ResetStale(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "reset-stale-1", time.Hour)
	sm.Acquire(ctx, "reset-stale-2", time.Hour)

	// Wait a bit so they become stale
	time.Sleep(100 * time.Millisecond)

	// Reset stale states
	reset, err := sm.ResetStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ResetStale failed: %v", err)
	}
	if reset != 2 {
		t.Fatalf("expected 2 reset, got %d", reset)
	}

	// Should be acquirable again
	acquired, _ := sm.Acquire(ctx, "reset-stale-1", time.Hour)
	if !acquired {
		t.Error("expected acquisition to succeed after stale reset")
	}
}

func TestMongoStateManager_Integration_ConcurrentAcquire(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()
	msgID := "msg-concurrent-1"

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// Multiple goroutines try to acquire the same message
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			acquired, err := sm.Acquire(ctx, msgID, time.Minute)
			if err != nil {
				return
			}

			if acquired {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only one goroutine should have successfully acquired the message
	if successCount != 1 {
		t.Errorf("expected exactly 1 successful acquisition, got %d", successCount)
	}
}

func TestMongoStateManager_Integration_RecoveryRunner(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Acquire a message
	sm.Acquire(ctx, "recovery-1", time.Hour)

	// Not stale yet
	reset, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 0 {
		t.Fatalf("expected 0 reset (not stale yet), got %d", reset)
	}

	// Wait for it to become stale
	time.Sleep(100 * time.Millisecond)

	// Now should be reset
	reset, err = runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 1 {
		t.Fatalf("expected 1 reset, got %d", reset)
	}

	// Message should be acquirable again
	acquired, _ := sm.Acquire(ctx, "recovery-1", time.Hour)
	if !acquired {
		t.Error("expected acquisition to succeed after recovery")
	}
}

func TestMongoStateManager_Integration_PayloadRecovery(t *testing.T) {
	sm, cleanup := setupMongoStateManager(t)
	defer cleanup()

	ctx := context.Background()

	// Track re-published events
	pub := &integrationMockPublisher{}

	runner, err := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
		WithPublisher(pub),
	)
	if err != nil {
		t.Fatalf("NewRecoveryRunner: %v", err)
	}

	// Acquire and store payload (simulates middleware with payload storage)
	acquired, err := sm.Acquire(ctx, "payload-1", time.Hour)
	if err != nil || !acquired {
		t.Fatalf("expected acquisition to succeed: acquired=%v err=%v", acquired, err)
	}
	err = sm.StorePayload(ctx, "payload-1", &MessageData{
		Payload:   []byte(`{"order":"abc"}`),
		Metadata:  map[string]string{"source": "integration-test"},
		EventName: "order.created",
	})
	if err != nil {
		t.Fatalf("StorePayload failed: %v", err)
	}

	// Acquire another message without payload (should be reset, not re-published)
	sm.Acquire(ctx, "no-payload-1", time.Hour)

	// Wait for both to become stale
	time.Sleep(100 * time.Millisecond)

	// Phase 1: re-publish payload entry, Phase 2: reset no-payload entry
	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered, got %d", recovered)
	}

	// Publisher should have been called once (for payload-1)
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	if pub.calls[0].eventName != "order.created" {
		t.Errorf("expected event name 'order.created', got %q", pub.calls[0].eventName)
	}
	if string(pub.calls[0].payload) != `{"order":"abc"}` {
		t.Errorf("unexpected payload: %s", pub.calls[0].payload)
	}

	// payload-1 should be marked as processed (not acquirable)
	acquired, _ = sm.Acquire(ctx, "payload-1", time.Hour)
	if acquired {
		t.Error("expected payload-1 to NOT be acquirable after recovery (marked processed)")
	}

	// no-payload-1 should be acquirable after reset
	acquired, _ = sm.Acquire(ctx, "no-payload-1", time.Hour)
	if !acquired {
		t.Error("expected no-payload-1 to be acquirable after reset")
	}
}

// integrationMockPublisher records published events for test verification.
// Uses mockPublishCall to avoid collision with payload_test.go types
// (both compile together under -tags=integration).
type integrationMockPublisher struct {
	calls []mockPublishCall
}

type mockPublishCall struct {
	eventName string
	eventID   string
	payload   []byte
	metadata  map[string]string
}

func (p *integrationMockPublisher) Send(_ context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error {
	p.calls = append(p.calls, mockPublishCall{
		eventName: eventName,
		eventID:   eventID,
		payload:   payload,
		metadata:  metadata,
	})
	return nil
}

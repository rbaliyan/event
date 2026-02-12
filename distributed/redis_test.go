package distributed

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedisStateManager(t *testing.T, opts ...Option) (*RedisStateManager, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisStateManager(client, opts...), mr
}

func TestRedisStateManager_Acquire(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	t.Run("first acquisition succeeds", func(t *testing.T) {
		acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Fatal("expected acquisition to succeed")
		}
	})

	t.Run("second acquisition for same message fails", func(t *testing.T) {
		acquired, err := sm.Acquire(ctx, "msg-1", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if acquired {
			t.Fatal("expected second acquisition to fail")
		}
	})

	t.Run("different messages can be acquired", func(t *testing.T) {
		acquired, err := sm.Acquire(ctx, "msg-2", time.Minute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !acquired {
			t.Fatal("expected different message acquisition to succeed")
		}
	})
}

func TestRedisStateManager_Acquire_Expiry(t *testing.T) {
	sm, mr := setupRedisStateManager(t)
	ctx := context.Background()

	acquired, _ := sm.Acquire(ctx, "msg-1", time.Second)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Fast-forward past expiry
	mr.FastForward(2 * time.Second)

	// Should be acquirable again after expiry
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed after expiry")
	}
}

func TestRedisStateManager_MarkProcessed(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Mark processed
	if err := sm.MarkProcessed(ctx, "msg-1"); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	// Should not be acquirable (completed state blocks)
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if acquired {
		t.Fatal("expected acquisition to fail after mark processed")
	}
}

func TestRedisStateManager_MarkProcessed_AtomicNoRecreate(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire then reset (simulate another process deleting the key)
	sm.Acquire(ctx, "msg-1", time.Minute)
	sm.Reset(ctx, "msg-1")

	// MarkProcessed should be a no-op on a deleted key (Lua script checks existence)
	if err := sm.MarkProcessed(ctx, "msg-1"); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	// Key should NOT exist (Lua script does not recreate deleted keys)
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed (MarkProcessed should not have recreated deleted key)")
	}
}

func TestRedisStateManager_Reset(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed")
	}

	// Reset
	if err := sm.Reset(ctx, "msg-1"); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// Should be acquirable again
	acquired, _ = sm.Acquire(ctx, "msg-1", time.Minute)
	if !acquired {
		t.Fatal("expected acquisition to succeed after reset")
	}
}

func TestRedisStateManager_ListStale(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.Acquire(ctx, "msg-3", time.Hour)

	// Mark one as processed
	sm.MarkProcessed(ctx, "msg-2")

	// Wait for stale timeout (stale = UpdatedAt older than staleTimeout)
	time.Sleep(60 * time.Millisecond)

	// List stale with 50ms timeout
	stale, err := sm.ListStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ListStale failed: %v", err)
	}

	// Should find msg-1 and msg-3 (processing and stale), not msg-2 (completed)
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

func TestRedisStateManager_ResetStale(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)

	time.Sleep(60 * time.Millisecond)

	reset, err := sm.ResetStale(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("ResetStale failed: %v", err)
	}
	if reset != 2 {
		t.Fatalf("expected 2 reset, got %d", reset)
	}

	// Should be acquirable again
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected acquisition to succeed after stale reset")
	}
}

func TestRedisStateManager_ResetStale_Limit(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.Acquire(ctx, "msg-3", time.Hour)

	time.Sleep(60 * time.Millisecond)

	// Reset with limit=1
	reset, err := sm.ResetStale(ctx, 50*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("ResetStale failed: %v", err)
	}
	if reset != 1 {
		t.Fatalf("expected 1 reset with limit, got %d", reset)
	}
}

func TestRedisStateManager_StorePayload(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire first (payload key TTL matches state key TTL)
	sm.Acquire(ctx, "msg-1", time.Minute)

	err := sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		Metadata:  map[string]string{"source": "test"},
		EventName: "order.created",
	})
	if err != nil {
		t.Fatalf("StorePayload failed: %v", err)
	}
}

func TestRedisStateManager_StorePayload_SkipsWhenNoState(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// No Acquire — state key doesn't exist, TTL <= 0
	err := sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})
	if err != nil {
		t.Fatalf("StorePayload failed: %v", err)
	}

	// Payload key should NOT exist (skipped to avoid orphaned keys)
	stale, _ := sm.LoadStalePayloads(ctx, 0, 0)
	if len(stale) > 0 {
		t.Fatal("expected no stale payloads when state key doesn't exist")
	}
}

func TestRedisStateManager_StorePayload_NilOrEmpty(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", time.Minute)

	// Nil data
	if err := sm.StorePayload(ctx, "msg-1", nil); err != nil {
		t.Fatalf("StorePayload(nil) failed: %v", err)
	}

	// Empty payload
	if err := sm.StorePayload(ctx, "msg-1", &MessageData{Payload: nil}); err != nil {
		t.Fatalf("StorePayload(empty) failed: %v", err)
	}
}

func TestRedisStateManager_LoadStalePayloads(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire and store payload for msg-1
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})

	// Acquire msg-2 without payload
	sm.Acquire(ctx, "msg-2", time.Hour)

	time.Sleep(60 * time.Millisecond)

	// LoadStalePayloads should only return msg-1 (has payload)
	stale, err := sm.LoadStalePayloads(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("LoadStalePayloads failed: %v", err)
	}
	if len(stale) != 1 {
		t.Fatalf("expected 1 stale with payload, got %d", len(stale))
	}
	if stale[0].MessageID != "msg-1" {
		t.Errorf("expected msg-1, got %s", stale[0].MessageID)
	}
	if string(stale[0].Data.Payload) != `{"order":1}` {
		t.Errorf("unexpected payload: %s", stale[0].Data.Payload)
	}
	if stale[0].Data.EventName != "order.created" {
		t.Errorf("expected event name order.created, got %s", stale[0].Data.EventName)
	}
}

func TestRedisStateManager_ClearPayload(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.StorePayload(ctx, "msg-1", &MessageData{
		Payload:   []byte(`{"order":1}`),
		EventName: "order.created",
	})

	// Clear payload
	if err := sm.ClearPayload(ctx, "msg-1"); err != nil {
		t.Fatalf("ClearPayload failed: %v", err)
	}

	time.Sleep(60 * time.Millisecond)

	// LoadStalePayloads should find no entries with payload
	stale, err := sm.LoadStalePayloads(ctx, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatalf("LoadStalePayloads failed: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected 0 stale with payload after clear, got %d", len(stale))
	}
}

func TestRedisStateManager_Prefix(t *testing.T) {
	sm1, _ := setupRedisStateManager(t, WithPrefix("app1:"))
	sm2, _ := setupRedisStateManager(t, WithPrefix("app2:"))
	ctx := context.Background()

	// Same message ID, different prefixes — both should acquire
	acquired1, _ := sm1.Acquire(ctx, "msg-1", time.Minute)
	acquired2, _ := sm2.Acquire(ctx, "msg-1", time.Minute)

	if !acquired1 {
		t.Fatal("expected app1 acquisition to succeed")
	}
	if !acquired2 {
		t.Fatal("expected app2 acquisition to succeed")
	}
}

func TestRedisStateManager_RecoveryRunner(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	runner := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
	)

	sm.Acquire(ctx, "msg-1", time.Hour)

	// Not stale yet
	reset, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 0 {
		t.Fatalf("expected 0 reset, got %d", reset)
	}

	// Wait for stale timeout
	time.Sleep(60 * time.Millisecond)

	reset, err = runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if reset != 1 {
		t.Fatalf("expected 1 reset, got %d", reset)
	}

	// Should be acquirable again
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	if !acquired {
		t.Fatal("expected acquisition to succeed after recovery")
	}
}

func TestRedisStatusConstants(t *testing.T) {
	// Verify Redis status constants match expected values
	if redisStatusProcessing != "processing" {
		t.Errorf("redisStatusProcessing = %q, want %q", redisStatusProcessing, "processing")
	}
	if redisStatusCompleted != "completed" {
		t.Errorf("redisStatusCompleted = %q, want %q", redisStatusCompleted, "completed")
	}
}

func TestRedisStateManager_StorePayload_AtomicTTL(t *testing.T) {
	sm, mr := setupRedisStateManager(t)
	ctx := context.Background()

	// Acquire with short TTL
	sm.Acquire(ctx, "msg-ttl", 5*time.Second)

	err := sm.StorePayload(ctx, "msg-ttl", &MessageData{
		Payload:   []byte(`{"test":"data"}`),
		EventName: "test.event",
	})
	if err != nil {
		t.Fatalf("StorePayload failed: %v", err)
	}

	// Payload key should exist
	payloadKey := sm.payloadKey("msg-ttl")
	val, err := mr.Get(payloadKey)
	if err != nil {
		t.Fatalf("expected payload key to exist, got error: %v", err)
	}
	if val == "" {
		t.Fatal("expected non-empty payload value")
	}

	// Payload key TTL should match state key TTL (approximately)
	payloadTTL := mr.TTL(payloadKey)
	stateTTL := mr.TTL(sm.prefix + "msg-ttl")
	if payloadTTL <= 0 {
		t.Fatal("expected payload key to have TTL")
	}
	if stateTTL <= 0 {
		t.Fatal("expected state key to have TTL")
	}
	// Both should be close (within 1 second of each other)
	diff := payloadTTL - stateTTL
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second {
		t.Fatalf("TTL mismatch: payload=%v state=%v diff=%v", payloadTTL, stateTTL, diff)
	}
}

func TestRedisStateManager_PayloadRecovery(t *testing.T) {
	sm, _ := setupRedisStateManager(t)
	ctx := context.Background()

	pub := &mockPublisher{}
	runner := NewRecoveryRunner(sm,
		WithStaleTimeout(50*time.Millisecond),
		WithBatchLimit(10),
		WithPublisher(pub),
	)

	// Acquire and store payload (simulates middleware behavior)
	sm.Acquire(ctx, "msg-recovery-1", time.Hour)
	sm.StorePayload(ctx, "msg-recovery-1", &MessageData{
		Payload:   []byte(`{"order":"abc"}`),
		Metadata:  map[string]string{"source": "test"},
		EventName: "order.created",
	})

	// Acquire without payload (should be reset via Phase 2)
	sm.Acquire(ctx, "msg-recovery-2", time.Hour)

	time.Sleep(60 * time.Millisecond)

	// Phase 1: re-publish payload entry, Phase 2: reset no-payload entry
	recovered, err := runner.RecoverOnce(ctx)
	if err != nil {
		t.Fatalf("RecoverOnce failed: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered, got %d", recovered)
	}

	// Publisher should have been called once (for msg-recovery-1)
	if len(pub.calls) != 1 {
		t.Fatalf("expected 1 publish call, got %d", len(pub.calls))
	}
	if pub.calls[0].eventName != "order.created" {
		t.Errorf("expected event name 'order.created', got %q", pub.calls[0].eventName)
	}

	// msg-recovery-2 should be acquirable after reset
	acquired, _ := sm.Acquire(ctx, "msg-recovery-2", time.Hour)
	if !acquired {
		t.Fatal("expected msg-recovery-2 to be acquirable after reset")
	}
}

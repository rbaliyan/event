package distributed_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/distributed"
)

// Example demonstrates basic usage of the distributed package for
// emulating WorkerPool semantics on Broadcast transports.
func Example() {
	// Create an in-memory state manager (use RedisStateManager for production)
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	ctx := context.Background()
	messageID := "order-12345"
	stateTTL := 5 * time.Minute

	// Attempt to acquire the message for processing
	acquired, err := sm.Acquire(ctx, messageID, stateTTL)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if !acquired {
		fmt.Println("Another worker is processing this message")
		return
	}

	// Process the message (simulated)
	fmt.Println("Processing message:", messageID)

	// Mark as processed after successful processing
	if err := sm.MarkProcessed(ctx, messageID); err != nil {
		fmt.Println("Error marking as processed:", err)
	}

	fmt.Println("Message processed successfully")

	// Output:
	// Processing message: order-12345
	// Message processed successfully
}

// ExampleNewMemoryStateManager demonstrates creating a memory state manager
// with custom options.
func ExampleNewMemoryStateManager() {
	// Create state manager with custom TTL settings
	sm := distributed.NewMemoryStateManager(
		distributed.WithStateTTL(10*time.Minute),
		distributed.WithCompletedTTL(24*time.Hour),
		distributed.WithCleanup(true, 30*time.Minute),
	)
	defer sm.Close()

	ctx := context.Background()

	// Acquire and process
	acquired, _ := sm.Acquire(ctx, "msg-001", 5*time.Minute)
	fmt.Println("Acquired:", acquired)

	// Output:
	// Acquired: true
}

// ExampleMemoryStateManager_Acquire demonstrates the atomic acquire operation.
func ExampleMemoryStateManager_Acquire() {
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	ctx := context.Background()
	messageID := "order-abc"
	ttl := 5 * time.Minute

	// First worker acquires successfully
	acquired1, _ := sm.Acquire(ctx, messageID, ttl)
	fmt.Println("Worker 1 acquired:", acquired1)

	// Second worker cannot acquire (already acquired)
	acquired2, _ := sm.Acquire(ctx, messageID, ttl)
	fmt.Println("Worker 2 acquired:", acquired2)

	// Output:
	// Worker 1 acquired: true
	// Worker 2 acquired: false
}

// ExampleMemoryStateManager_Reset demonstrates resetting state for retry.
func ExampleMemoryStateManager_Reset() {
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	ctx := context.Background()
	messageID := "failed-order"
	ttl := 5 * time.Minute

	// Worker 1 acquires
	sm.Acquire(ctx, messageID, ttl)

	// Processing fails, reset for another worker to retry
	sm.Reset(ctx, messageID)
	fmt.Println("State reset for retry")

	// Worker 2 can now acquire
	acquired, _ := sm.Acquire(ctx, messageID, ttl)
	fmt.Println("Worker 2 acquired after reset:", acquired)

	// Output:
	// State reset for retry
	// Worker 2 acquired after reset: true
}

// ExampleMemoryStateManager_ListStale demonstrates finding stale states.
func ExampleMemoryStateManager_ListStale() {
	sm := distributed.NewMemoryStateManager(
		distributed.WithCleanup(false, 0), // Disable cleanup for this example
	)
	defer sm.Close()

	ctx := context.Background()

	// Acquire some messages
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)
	sm.Acquire(ctx, "msg-3", time.Hour)

	// Immediately, nothing is stale (stale = processing longer than staleTimeout)
	stale, _ := sm.ListStale(ctx, time.Minute, 10)
	fmt.Println("Stale immediately:", len(stale))

	// Output:
	// Stale immediately: 0
}

// Example_workerCoordination demonstrates how multiple workers coordinate
// using the state manager.
func Example_workerCoordination() {
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	ctx := context.Background()
	ttl := 5 * time.Minute

	// Simulate message processing by multiple workers
	processMessage := func(workerID string, messageID string) {
		acquired, err := sm.Acquire(ctx, messageID, ttl)
		if err != nil {
			fmt.Printf("Worker %s: error acquiring %s\n", workerID, messageID)
			return
		}

		if !acquired {
			fmt.Printf("Worker %s: skipping %s (acquired by another)\n", workerID, messageID)
			return
		}

		fmt.Printf("Worker %s: processing %s\n", workerID, messageID)
		sm.MarkProcessed(ctx, messageID)
	}

	// Both workers try to process the same message
	processMessage("A", "order-100")
	processMessage("B", "order-100")

	// Each worker processes a different message
	processMessage("A", "order-101")
	processMessage("B", "order-102")

	// Output:
	// Worker A: processing order-100
	// Worker B: skipping order-100 (acquired by another)
	// Worker A: processing order-101
	// Worker B: processing order-102
}

// ExampleWithPrefix demonstrates using prefixes for isolated namespaces.
func ExampleWithPrefix() {
	// Create separate state managers for different worker groups
	smOrders := distributed.NewMemoryStateManager(
		distributed.WithPrefix("orders:"),
	)
	defer smOrders.Close()

	smNotifications := distributed.NewMemoryStateManager(
		distributed.WithPrefix("notifications:"),
	)
	defer smNotifications.Close()

	ctx := context.Background()
	ttl := 5 * time.Minute
	messageID := "msg-001"

	// Same message ID can be processed by both groups
	acquired1, _ := smOrders.Acquire(ctx, messageID, ttl)
	acquired2, _ := smNotifications.Acquire(ctx, messageID, ttl)

	fmt.Println("Orders group acquired:", acquired1)
	fmt.Println("Notifications group acquired:", acquired2)

	// Output:
	// Orders group acquired: true
	// Notifications group acquired: true
}

// ExampleNewRecoveryRunner demonstrates setting up stale state recovery.
func ExampleNewRecoveryRunner() {
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	// Create recovery runner with custom settings
	runner := distributed.NewRecoveryRunner(sm,
		distributed.WithStaleTimeout(2*time.Minute),
		distributed.WithCheckInterval(30*time.Second),
		distributed.WithBatchLimit(100),
	)

	ctx := context.Background()

	// Run a single recovery pass
	reset, err := runner.RecoverOnce(ctx)
	if err != nil {
		fmt.Println("Recovery failed:", err)
		return
	}

	fmt.Println("States reset:", reset)

	// Output:
	// States reset: 0
}

// Example_failureHandling demonstrates handling processing failures.
func Example_failureHandling() {
	sm := distributed.NewMemoryStateManager()
	defer sm.Close()

	ctx := context.Background()
	ttl := 5 * time.Minute

	// Simulate processing with failure handling
	processWithRetry := func(messageID string, shouldFail bool) {
		acquired, _ := sm.Acquire(ctx, messageID, ttl)
		if !acquired {
			fmt.Printf("Message %s: already being processed\n", messageID)
			return
		}

		if shouldFail {
			// Processing failed - reset so another worker can retry
			sm.Reset(ctx, messageID)
			fmt.Printf("Message %s: processing failed, reset for retry\n", messageID)
			return
		}

		// Processing succeeded
		sm.MarkProcessed(ctx, messageID)
		fmt.Printf("Message %s: processed successfully\n", messageID)
	}

	// First attempt fails
	processWithRetry("order-xyz", true)

	// Retry succeeds
	processWithRetry("order-xyz", false)

	// Already completed
	processWithRetry("order-xyz", false)

	// Output:
	// Message order-xyz: processing failed, reset for retry
	// Message order-xyz: processed successfully
	// Message order-xyz: already being processed
}

// ExampleMemoryStateManager_ResetStale demonstrates batch reset of stale states.
func ExampleMemoryStateManager_ResetStale() {
	sm := distributed.NewMemoryStateManager(
		distributed.WithCleanup(false, 0),
	)
	defer sm.Close()

	ctx := context.Background()

	// Acquire messages (they start with "updatedAt" = now)
	sm.Acquire(ctx, "msg-1", time.Hour)
	sm.Acquire(ctx, "msg-2", time.Hour)

	// With staleTimeout of 0, everything is considered stale
	reset, _ := sm.ResetStale(ctx, 0, 10)
	fmt.Println("States reset:", reset)

	// Now messages can be re-acquired
	acquired, _ := sm.Acquire(ctx, "msg-1", time.Hour)
	fmt.Println("Re-acquired after reset:", acquired)

	// Output:
	// States reset: 2
	// Re-acquired after reset: true
}

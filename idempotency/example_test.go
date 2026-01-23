package idempotency_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/idempotency"
)

// Example demonstrates basic usage of the idempotency package.
func Example() {
	// Create an in-memory store with 1-hour TTL
	store := idempotency.NewMemoryStore(time.Hour)
	defer store.Close()

	ctx := context.Background()
	messageID := "order-12345"

	// Check if message was already processed
	isDuplicate, err := store.IsDuplicate(ctx, messageID)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if isDuplicate {
		fmt.Println("Message already processed, skipping")
		return
	}

	// Process the message (simulated)
	fmt.Println("Processing message:", messageID)

	// Mark as processed after successful processing
	if err := store.MarkProcessed(ctx, messageID); err != nil {
		fmt.Println("Error marking as processed:", err)
		return
	}

	// Subsequent checks will return true
	isDuplicate, _ = store.IsDuplicate(ctx, messageID)
	fmt.Println("Is duplicate now:", isDuplicate)

	// Output:
	// Processing message: order-12345
	// Is duplicate now: true
}

// ExampleNewMemoryStore demonstrates creating a memory store with custom TTL.
func ExampleNewMemoryStore() {
	// Create store with 24-hour TTL for tracking processed messages
	store := idempotency.NewMemoryStore(24 * time.Hour)
	defer store.Close()

	ctx := context.Background()

	// Mark some messages as processed
	store.MarkProcessed(ctx, "msg-1")
	store.MarkProcessed(ctx, "msg-2")
	store.MarkProcessed(ctx, "msg-3")

	// Check the count
	fmt.Println("Tracked messages:", store.Len())

	// Output:
	// Tracked messages: 3
}

// ExampleMemoryStore_IsDuplicate demonstrates checking for duplicate messages.
func ExampleMemoryStore_IsDuplicate() {
	store := idempotency.NewMemoryStore(time.Hour)
	defer store.Close()

	ctx := context.Background()

	// New message is not a duplicate
	isDuplicate, _ := store.IsDuplicate(ctx, "new-message")
	fmt.Println("New message is duplicate:", isDuplicate)

	// Mark as processed
	store.MarkProcessed(ctx, "new-message")

	// Now it's a duplicate
	isDuplicate, _ = store.IsDuplicate(ctx, "new-message")
	fmt.Println("After processing is duplicate:", isDuplicate)

	// Output:
	// New message is duplicate: false
	// After processing is duplicate: true
}

// ExampleMemoryStore_MarkProcessedWithTTL demonstrates using custom TTLs.
func ExampleMemoryStore_MarkProcessedWithTTL() {
	store := idempotency.NewMemoryStore(time.Hour)
	defer store.Close()

	ctx := context.Background()

	// High-value transactions: keep for 7 days
	store.MarkProcessedWithTTL(ctx, "payment-xyz", 7*24*time.Hour)

	// Ephemeral notifications: keep for only 5 minutes
	store.MarkProcessedWithTTL(ctx, "notification-abc", 5*time.Minute)

	// Both are tracked
	isDup1, _ := store.IsDuplicate(ctx, "payment-xyz")
	isDup2, _ := store.IsDuplicate(ctx, "notification-abc")

	fmt.Println("Payment is duplicate:", isDup1)
	fmt.Println("Notification is duplicate:", isDup2)

	// Output:
	// Payment is duplicate: true
	// Notification is duplicate: true
}

// ExampleMemoryStore_Remove demonstrates removing an entry to allow reprocessing.
func ExampleMemoryStore_Remove() {
	store := idempotency.NewMemoryStore(time.Hour)
	defer store.Close()

	ctx := context.Background()
	messageID := "order-failed-123"

	// Mark message as processed
	store.MarkProcessed(ctx, messageID)

	isDuplicate, _ := store.IsDuplicate(ctx, messageID)
	fmt.Println("Before remove - is duplicate:", isDuplicate)

	// Remove to allow reprocessing (e.g., after fixing an issue)
	store.Remove(ctx, messageID)

	isDuplicate, _ = store.IsDuplicate(ctx, messageID)
	fmt.Println("After remove - is duplicate:", isDuplicate)

	// Output:
	// Before remove - is duplicate: true
	// After remove - is duplicate: false
}

// ExampleErrAlreadyProcessed demonstrates using the error type.
func ExampleErrAlreadyProcessed() {
	// Simulate an error from processing
	err := idempotency.ErrAlreadyProcessed

	// Check if it's an already processed error
	if errors.Is(err, idempotency.ErrAlreadyProcessed) {
		fmt.Println("Message was already processed")
	}

	// Output:
	// Message was already processed
}

// Example_messageHandler demonstrates a typical message handler pattern.
func Example_messageHandler() {
	store := idempotency.NewMemoryStore(time.Hour)
	defer store.Close()

	// Handler function that processes messages idempotently
	handleMessage := func(ctx context.Context, messageID string, payload string) error {
		// Check for duplicate
		isDuplicate, err := store.IsDuplicate(ctx, messageID)
		if err != nil {
			return fmt.Errorf("idempotency check failed: %w", err)
		}
		if isDuplicate {
			// Already processed - this is not an error, just skip
			return nil
		}

		// Process the message
		fmt.Printf("Processing: %s with payload: %s\n", messageID, payload)

		// Mark as processed after successful processing
		return store.MarkProcessed(ctx, messageID)
	}

	ctx := context.Background()

	// First call - processes the message
	handleMessage(ctx, "msg-001", "data-1")

	// Second call - skips (duplicate)
	handleMessage(ctx, "msg-001", "data-1")

	// Different message - processes
	handleMessage(ctx, "msg-002", "data-2")

	// Output:
	// Processing: msg-001 with payload: data-1
	// Processing: msg-002 with payload: data-2
}

// Example_expiryBehavior demonstrates how TTL expiry works.
func Example_expiryBehavior() {
	// Use very short TTL for demonstration
	store := idempotency.NewMemoryStore(50 * time.Millisecond)
	defer store.Close()

	ctx := context.Background()
	messageID := "expiring-msg"

	// Mark as processed
	store.MarkProcessed(ctx, messageID)

	// Immediately it's a duplicate
	isDuplicate, _ := store.IsDuplicate(ctx, messageID)
	fmt.Println("Immediately after marking:", isDuplicate)

	// Wait for TTL to expire
	time.Sleep(100 * time.Millisecond)

	// After expiry, it's no longer a duplicate
	isDuplicate, _ = store.IsDuplicate(ctx, messageID)
	fmt.Println("After TTL expiry:", isDuplicate)

	// Output:
	// Immediately after marking: true
	// After TTL expiry: false
}

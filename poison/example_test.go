package poison_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/poison"
)

// Example demonstrates basic usage of the poison package for detecting
// and quarantining problematic messages.
func Example() {
	// Create an in-memory store and detector
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store,
		poison.WithThreshold(3),           // Quarantine after 3 failures
		poison.WithQuarantineTime(time.Hour), // Block for 1 hour
	)

	ctx := context.Background()
	messageID := "problematic-order-123"

	// Simulate multiple failures
	for i := 1; i <= 4; i++ {
		// Check if message is quarantined
		if poisoned, _ := detector.Check(ctx, messageID); poisoned {
			fmt.Printf("Attempt %d: Message is quarantined, skipping\n", i)
			continue
		}

		// Simulate processing failure
		fmt.Printf("Attempt %d: Processing failed\n", i)
		quarantined, _ := detector.RecordFailure(ctx, messageID)
		if quarantined {
			fmt.Printf("Attempt %d: Message quarantined after %d failures\n", i, detector.Threshold())
		}
	}

	// Output:
	// Attempt 1: Processing failed
	// Attempt 2: Processing failed
	// Attempt 3: Processing failed
	// Attempt 3: Message quarantined after 3 failures
	// Attempt 4: Message is quarantined, skipping
}

// ExampleNewDetector demonstrates creating a detector with custom options.
func ExampleNewDetector() {
	store := poison.NewMemoryStore()

	// Create detector with custom settings
	detector := poison.NewDetector(store,
		poison.WithThreshold(5),                // 5 failures before quarantine
		poison.WithQuarantineTime(24*time.Hour), // Block for 24 hours
	)

	fmt.Println("Threshold:", detector.Threshold())
	fmt.Println("Quarantine time:", detector.QuarantineTime())

	// Output:
	// Threshold: 5
	// Quarantine time: 24h0m0s
}

// ExampleNewMemoryStore demonstrates creating an in-memory poison store.
func ExampleNewMemoryStore() {
	store := poison.NewMemoryStore()

	ctx := context.Background()

	// Increment failure count
	count, _ := store.IncrementFailure(ctx, "msg-001")
	fmt.Println("Failure count:", count)

	count, _ = store.IncrementFailure(ctx, "msg-001")
	fmt.Println("Failure count:", count)

	// Output:
	// Failure count: 1
	// Failure count: 2
}

// ExampleDetector_Check demonstrates checking if a message is quarantined.
func ExampleDetector_Check() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store, poison.WithThreshold(2))

	ctx := context.Background()
	messageID := "order-xyz"

	// Initially not poisoned
	isPoisoned, _ := detector.Check(ctx, messageID)
	fmt.Println("Initially poisoned:", isPoisoned)

	// Record failures until quarantined
	detector.RecordFailure(ctx, messageID)
	detector.RecordFailure(ctx, messageID)

	// Now it's poisoned
	isPoisoned, _ = detector.Check(ctx, messageID)
	fmt.Println("After failures poisoned:", isPoisoned)

	// Output:
	// Initially poisoned: false
	// After failures poisoned: true
}

// ExampleDetector_RecordSuccess demonstrates clearing failures on success.
func ExampleDetector_RecordSuccess() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store, poison.WithThreshold(3))

	ctx := context.Background()
	messageID := "intermittent-msg"

	// Record some failures
	detector.RecordFailure(ctx, messageID)
	detector.RecordFailure(ctx, messageID)

	count, _ := detector.GetFailureCount(ctx, messageID)
	fmt.Println("Failures before success:", count)

	// Message processes successfully - clear failures
	detector.RecordSuccess(ctx, messageID)

	count, _ = detector.GetFailureCount(ctx, messageID)
	fmt.Println("Failures after success:", count)

	// Output:
	// Failures before success: 2
	// Failures after success: 0
}

// ExampleDetector_Release demonstrates manually releasing a quarantined message.
func ExampleDetector_Release() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store, poison.WithThreshold(2))

	ctx := context.Background()
	messageID := "fixable-msg"

	// Quarantine the message
	detector.RecordFailure(ctx, messageID)
	detector.RecordFailure(ctx, messageID)

	isPoisoned, _ := detector.Check(ctx, messageID)
	fmt.Println("Before release - poisoned:", isPoisoned)

	// After fixing the issue, release the message
	detector.Release(ctx, messageID)

	isPoisoned, _ = detector.Check(ctx, messageID)
	fmt.Println("After release - poisoned:", isPoisoned)

	// Output:
	// Before release - poisoned: true
	// After release - poisoned: false
}

// ExampleNewError demonstrates creating and using poison errors.
func ExampleNewError() {
	// Create a poison error
	err := poison.NewError("msg-123", "exceeded failure threshold")
	fmt.Println("Error:", err)

	// Check error type
	if poison.IsPoisonError(err) {
		fmt.Println("This is a poison message error")
	}

	// Output:
	// Error: poison message msg-123: exceeded failure threshold
	// This is a poison message error
}

// ExampleIsPoisonError demonstrates checking for poison errors.
func ExampleIsPoisonError() {
	regularErr := errors.New("network timeout")
	poisonErr := poison.NewError("msg-456", "message is quarantined")

	fmt.Println("Regular error is poison:", poison.IsPoisonError(regularErr))
	fmt.Println("Poison error is poison:", poison.IsPoisonError(poisonErr))

	// Output:
	// Regular error is poison: false
	// Poison error is poison: true
}

// Example_messageHandler demonstrates a typical handler pattern with poison detection.
func Example_messageHandler() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store, poison.WithThreshold(3))

	// Handler function with poison detection
	handleMessage := func(ctx context.Context, messageID string, shouldFail bool) error {
		// Check if message is quarantined
		if poisoned, _ := detector.Check(ctx, messageID); poisoned {
			fmt.Printf("Message %s: quarantined, skipping\n", messageID)
			return poison.NewError(messageID, "message is quarantined")
		}

		// Simulate processing
		if shouldFail {
			quarantined, _ := detector.RecordFailure(ctx, messageID)
			if quarantined {
				fmt.Printf("Message %s: quarantined after too many failures\n", messageID)
			} else {
				fmt.Printf("Message %s: processing failed, failure recorded\n", messageID)
			}
			return errors.New("processing failed")
		}

		// Success - clear failure history
		detector.RecordSuccess(ctx, messageID)
		fmt.Printf("Message %s: processed successfully\n", messageID)
		return nil
	}

	ctx := context.Background()

	// Message fails repeatedly and gets quarantined
	handleMessage(ctx, "bad-msg", true)
	handleMessage(ctx, "bad-msg", true)
	handleMessage(ctx, "bad-msg", true) // Gets quarantined here
	handleMessage(ctx, "bad-msg", true) // Skipped (quarantined)

	// Good message succeeds
	handleMessage(ctx, "good-msg", false)

	// Output:
	// Message bad-msg: processing failed, failure recorded
	// Message bad-msg: processing failed, failure recorded
	// Message bad-msg: quarantined after too many failures
	// Message bad-msg: quarantined, skipping
	// Message good-msg: processed successfully
}

// ExampleMemoryStore_Cleanup demonstrates cleaning up expired quarantine entries.
func ExampleMemoryStore_Cleanup() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store,
		poison.WithThreshold(1),
		poison.WithQuarantineTime(50*time.Millisecond), // Very short for demo
	)

	ctx := context.Background()

	// Quarantine a message
	detector.RecordFailure(ctx, "temp-msg")

	isPoisoned, _ := detector.Check(ctx, "temp-msg")
	fmt.Println("Immediately - poisoned:", isPoisoned)

	// Wait for quarantine to expire
	time.Sleep(100 * time.Millisecond)

	// Cleanup expired entries
	store.Cleanup()

	isPoisoned, _ = detector.Check(ctx, "temp-msg")
	fmt.Println("After cleanup - poisoned:", isPoisoned)

	// Output:
	// Immediately - poisoned: true
	// After cleanup - poisoned: false
}

// ExampleDetector_GetFailureCount demonstrates monitoring failure counts.
func ExampleDetector_GetFailureCount() {
	store := poison.NewMemoryStore()
	detector := poison.NewDetector(store, poison.WithThreshold(5))

	ctx := context.Background()
	messageID := "monitored-msg"

	// Simulate some failures
	for i := 0; i < 3; i++ {
		detector.RecordFailure(ctx, messageID)
	}

	count, _ := detector.GetFailureCount(ctx, messageID)
	fmt.Printf("Message %s has failed %d times (threshold: %d)\n",
		messageID, count, detector.Threshold())

	// Output:
	// Message monitored-msg has failed 3 times (threshold: 5)
}

// Example_errorHandling demonstrates checking errors with errors.Is.
func Example_errorHandling() {
	var err error = poison.NewError("msg-789", "too many failures")

	// Using errors.Is (requires error interface type)
	if errors.Is(err, &poison.Error{}) {
		fmt.Println("Error is a poison error")
	}

	// Access error details using type assertion
	if pe, ok := err.(*poison.Error); ok {
		fmt.Println("Message ID:", pe.MessageID)
		fmt.Println("Reason:", pe.Reason)
	}

	// Output:
	// Error is a poison error
	// Message ID: msg-789
	// Reason: too many failures
}

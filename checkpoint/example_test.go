package checkpoint_test

import (
	"context"
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/checkpoint"
)

// Example demonstrates the checkpoint package for persisting subscriber positions.
//
// Checkpoint stores enable "start from latest, resume on reconnect" semantics:
//   - First connection: starts from latest messages (no checkpoint exists)
//   - Reconnection: resumes from last saved checkpoint
//
// This is essential for event-driven applications that need to ensure no messages
// are lost during subscriber restarts or reconnections.
func Example() {
	// Create an in-memory checkpoint store for demonstration
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	subscriberID := "order-processor-1"

	// On startup, load the last checkpoint
	position, err := store.Load(ctx, subscriberID)
	if err != nil {
		fmt.Println("Error loading checkpoint:", err)
		return
	}

	if position.IsZero() {
		fmt.Println("No checkpoint found - starting from latest messages")
	} else {
		fmt.Println("Resuming from checkpoint:", position)
	}

	// After processing each message, save the checkpoint
	newPosition := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	if err := store.Save(ctx, subscriberID, newPosition); err != nil {
		fmt.Println("Error saving checkpoint:", err)
		return
	}

	// Verify the checkpoint was saved
	savedPosition, _ := store.Load(ctx, subscriberID)
	fmt.Println("Checkpoint saved:", savedPosition.Format(time.RFC3339))

	// Output:
	// No checkpoint found - starting from latest messages
	// Checkpoint saved: 2024-01-15T10:30:00Z
}

// ExampleNewMemoryCheckpointStore demonstrates the in-memory checkpoint store.
//
// MemoryCheckpointStore is ideal for testing and development. It provides
// the same interface as production stores but doesn't persist data across
// application restarts.
func ExampleNewMemoryCheckpointStore() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Save checkpoints for multiple subscribers
	store.Save(ctx, "subscriber-a", time.Now().Add(-1*time.Hour))
	store.Save(ctx, "subscriber-b", time.Now().Add(-30*time.Minute))
	store.Save(ctx, "subscriber-c", time.Now())

	// Load a specific checkpoint
	position, _ := store.Load(ctx, "subscriber-a")
	fmt.Println("subscriber-a has checkpoint:", !position.IsZero())

	// Load non-existent checkpoint returns zero time (not an error)
	position, err := store.Load(ctx, "non-existent")
	fmt.Println("non-existent returns zero:", position.IsZero())
	fmt.Println("non-existent returns error:", err != nil)

	// Output:
	// subscriber-a has checkpoint: true
	// non-existent returns zero: true
	// non-existent returns error: false
}

// ExampleMemoryCheckpointStore_Save demonstrates saving checkpoints.
func ExampleMemoryCheckpointStore_Save() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Save a checkpoint position
	position := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	err := store.Save(ctx, "my-subscriber", position)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println("Checkpoint saved successfully")

	// Checkpoints can be updated - new position overwrites old
	newPosition := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	store.Save(ctx, "my-subscriber", newPosition)

	loaded, _ := store.Load(ctx, "my-subscriber")
	fmt.Println("Updated position:", loaded.Format(time.RFC3339))

	// Output:
	// Checkpoint saved successfully
	// Updated position: 2024-01-15T11:00:00Z
}

// ExampleMemoryCheckpointStore_Load demonstrates loading checkpoints.
func ExampleMemoryCheckpointStore_Load() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Attempt to load before any checkpoint exists
	position, err := store.Load(ctx, "new-subscriber")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if position.IsZero() {
		fmt.Println("No checkpoint exists - this is a new subscriber")
	}

	// Save and then load
	checkpointTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	store.Save(ctx, "new-subscriber", checkpointTime)

	position, _ = store.Load(ctx, "new-subscriber")
	fmt.Println("Loaded checkpoint:", position.Format(time.RFC3339))

	// Output:
	// No checkpoint exists - this is a new subscriber
	// Loaded checkpoint: 2024-01-15T10:30:00Z
}

// ExampleMemoryCheckpointStore_Delete demonstrates deleting checkpoints.
func ExampleMemoryCheckpointStore_Delete() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Save a checkpoint
	store.Save(ctx, "subscriber-to-delete", time.Now())

	// Verify it exists
	position, _ := store.Load(ctx, "subscriber-to-delete")
	fmt.Println("Before delete, has checkpoint:", !position.IsZero())

	// Delete the checkpoint
	err := store.Delete(ctx, "subscriber-to-delete")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Verify it's gone
	position, _ = store.Load(ctx, "subscriber-to-delete")
	fmt.Println("After delete, has checkpoint:", !position.IsZero())

	// Output:
	// Before delete, has checkpoint: true
	// After delete, has checkpoint: false
}

// Example_redisStore demonstrates the Redis checkpoint store configuration.
//
// RedisStore is production-ready and stores checkpoints in a Redis hash.
// This example shows the configuration pattern - actual Redis connection
// is not established.
func Example_redisStore() {
	// Note: This is configuration example only - no actual Redis connection
	//
	// In production code:
	//
	//   import "github.com/redis/go-redis/v9"
	//
	//   client := redis.NewClient(&redis.Options{
	//       Addr: "localhost:6379",
	//   })
	//
	//   // Basic store
	//   store := checkpoint.NewRedisStore(client, "myapp:checkpoints")
	//
	//   // With TTL - checkpoints expire after 7 days of inactivity
	//   store := checkpoint.NewRedisStore(client, "myapp:checkpoints",
	//       checkpoint.WithTTL(7*24*time.Hour))
	//
	//   // Usage is identical to MemoryCheckpointStore
	//   err := store.Save(ctx, "subscriber-id", time.Now())
	//   position, err := store.Load(ctx, "subscriber-id")
	//
	// Redis stores checkpoints as Unix nanoseconds in a hash:
	//   HSET myapp:checkpoints subscriber-id 1705319400000000000

	fmt.Println("Redis checkpoint store supports:")
	fmt.Println("- Standard client (redis.NewClient)")
	fmt.Println("- Cluster client (redis.NewClusterClient)")
	fmt.Println("- TTL for automatic expiration")

	// Output:
	// Redis checkpoint store supports:
	// - Standard client (redis.NewClient)
	// - Cluster client (redis.NewClusterClient)
	// - TTL for automatic expiration
}

// Example_mongoStore points readers to the MongoDB checkpoint store, which
// now lives in the event-mongodb module after the extraction described in
// the package-level doc.
//
//   import "github.com/rbaliyan/event-mongodb/checkpoint"
//
//   collection := client.Database("myapp").Collection("checkpoints")
//   store := checkpoint.NewMongoStore(collection,
//       checkpoint.WithMongoTTL(7*24*time.Hour))
//   _ = store.EnsureIndexes(ctx) // creates the TTL index once on startup
//   // Save/Load semantics match the in-package memory and Redis stores.
func Example_mongoStore() {
	fmt.Println("see https://github.com/rbaliyan/event-mongodb for the MongoDB checkpoint store")
	// Output:
	// see https://github.com/rbaliyan/event-mongodb for the MongoDB checkpoint store
}

// Example_withEventSubscription demonstrates integrating checkpoints with event subscriptions.
//
// Checkpoints are typically used with event.WithCheckpoint subscribe option
// to enable resumable subscriptions.
func Example_withEventSubscription() {
	// Note: This shows the integration pattern with the event package
	//
	// In production code:
	//
	//   import (
	//       "github.com/rbaliyan/event/v3"
	//       "github.com/rbaliyan/event/v3/checkpoint"
	//   )
	//
	//   // Create checkpoint store (any implementation)
	//   checkpointStore := checkpoint.NewMemoryCheckpointStore()
	//   // Or: checkpoint.NewRedisStore(redisClient, "checkpoints")
	//   // Or: checkpoint.NewMongoStore(mongoCollection)
	//
	//   // Subscribe with checkpoint support
	//   orderEvent.Subscribe(ctx, handler,
	//       event.WithCheckpoint[Order](checkpointStore, "order-processor"),
	//   )
	//
	// The event library will:
	// 1. Load checkpoint on subscription start
	// 2. Start from latest if no checkpoint exists
	// 3. Resume from checkpoint if one exists
	// 4. Save checkpoint after each successful message processing

	fmt.Println("Checkpoint integration with events:")
	fmt.Println("1. Load checkpoint on subscription start")
	fmt.Println("2. Start from latest if no checkpoint exists")
	fmt.Println("3. Resume from checkpoint position if exists")
	fmt.Println("4. Auto-save after successful processing")

	// Output:
	// Checkpoint integration with events:
	// 1. Load checkpoint on subscription start
	// 2. Start from latest if no checkpoint exists
	// 3. Resume from checkpoint position if exists
	// 4. Auto-save after successful processing
}

// Example_partitionedCheckpoints demonstrates using checkpoints with partitioned events.
//
// When using partitioned event processing, each partition typically has its own
// checkpoint to track progress independently.
func Example_partitionedCheckpoints() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Format: {service}-{event}-partition-{n}
	partitions := []string{
		"order-service-orders-partition-0",
		"order-service-orders-partition-1",
		"order-service-orders-partition-2",
		"order-service-orders-partition-3",
	}

	// Simulate different progress on each partition
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	for i, partition := range partitions {
		// Each partition may be at different positions
		position := baseTime.Add(time.Duration(i*10) * time.Minute)
		store.Save(ctx, partition, position)
	}

	// Check progress of all partitions
	fmt.Println("Partition checkpoint positions:")
	for _, partition := range partitions {
		position, _ := store.Load(ctx, partition)
		fmt.Printf("  %s: %s\n", partition, position.Format("15:04:05"))
	}

	// Output:
	// Partition checkpoint positions:
	//   order-service-orders-partition-0: 10:00:00
	//   order-service-orders-partition-1: 10:10:00
	//   order-service-orders-partition-2: 10:20:00
	//   order-service-orders-partition-3: 10:30:00
}

// Example_checkpointRecoveryPattern demonstrates a typical checkpoint recovery pattern.
func Example_checkpointRecoveryPattern() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()
	subscriberID := "payment-processor"

	// Recovery function that would run on application startup
	recover := func() (time.Time, bool) {
		position, err := store.Load(ctx, subscriberID)
		if err != nil {
			// Handle error - might want to start fresh or fail
			return time.Time{}, false
		}

		if position.IsZero() {
			// No checkpoint - this is first run
			return time.Time{}, true
		}

		// Resume from checkpoint
		return position, true
	}

	// First run - no checkpoint exists
	position, ok := recover()
	if ok && position.IsZero() {
		fmt.Println("First run - starting fresh")
	}

	// Simulate processing some messages
	lastProcessed := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	store.Save(ctx, subscriberID, lastProcessed)

	// Simulate restart - checkpoint exists
	position, ok = recover()
	if ok && !position.IsZero() {
		fmt.Println("Recovered - resuming from:", position.Format(time.RFC3339))
	}

	// Output:
	// First run - starting fresh
	// Recovered - resuming from: 2024-01-15T12:30:00Z
}

// Example_multipleSubscribers demonstrates managing checkpoints for multiple subscribers.
func Example_multipleSubscribers() {
	store := checkpoint.NewMemoryCheckpointStore()
	ctx := context.Background()

	// Different subscribers processing the same event stream
	// Using a slice to maintain consistent order for the example output
	type subscriber struct {
		name string
		id   string
	}
	subscribers := []subscriber{
		{name: "analytics", id: "analytics-orders-consumer"},
		{name: "billing", id: "billing-orders-consumer"},
		{name: "notifications", id: "notifications-orders-consumer"},
	}

	// Each subscriber maintains independent progress
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	store.Save(ctx, subscribers[0].id, baseTime)                      // analytics
	store.Save(ctx, subscribers[1].id, baseTime.Add(-10*time.Minute)) // billing (behind)
	store.Save(ctx, subscribers[2].id, baseTime.Add(5*time.Minute))   // notifications (ahead)

	// Check each subscriber's position
	fmt.Println("Subscriber positions:")
	for _, sub := range subscribers {
		position, _ := store.Load(ctx, sub.id)
		fmt.Printf("  %s: %s\n", sub.name, position.Format("15:04:05"))
	}

	// One subscriber might fall behind without affecting others
	// Useful for: different processing speeds, maintenance windows, etc.

	// Output:
	// Subscriber positions:
	//   analytics: 10:00:00
	//   billing: 09:50:00
	//   notifications: 10:05:00
}

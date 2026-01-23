package partition_test

import (
	"fmt"

	"github.com/rbaliyan/event/v3/partition"
)

// Example demonstrates the partition package for ordered message delivery.
//
// Partitioning ensures that messages with the same key are always delivered
// to the same consumer in order. This is essential for:
//   - Processing events for the same entity in order (e.g., user events)
//   - Maintaining causality (e.g., order created before order shipped)
//   - Enabling parallel processing while preserving per-key ordering
func Example() {
	// Create a hash partitioner for consistent routing
	partitioner := partition.NewHashPartitioner()

	// Number of partitions (workers, consumers, etc.)
	numPartitions := 4

	// Same key always maps to same partition
	userID := "user-12345"
	p1 := partitioner.Partition(userID, numPartitions)
	p2 := partitioner.Partition(userID, numPartitions)
	p3 := partitioner.Partition(userID, numPartitions)

	// Verify consistency
	fmt.Println("Hash partitioning is consistent:", p1 == p2 && p2 == p3)

	// Partition value is in valid range
	fmt.Println("Partition in valid range:", p1 >= 0 && p1 < numPartitions)

	// Output:
	// Hash partitioning is consistent: true
	// Partition in valid range: true
}

// ExampleNewHashPartitioner demonstrates the hash-based partitioner.
//
// HashPartitioner uses FNV-1a hashing for fast, consistent partitioning.
// The same key always maps to the same partition number.
func ExampleNewHashPartitioner() {
	partitioner := partition.NewHashPartitioner()

	// Same key always returns same partition
	key := "order-9876"
	results := make([]int, 3)
	for i := 0; i < 3; i++ {
		results[i] = partitioner.Partition(key, 8)
	}

	// All calls return same partition
	allSame := results[0] == results[1] && results[1] == results[2]
	fmt.Println("Multiple calls return same partition:", allSame)
	fmt.Println("Partition is deterministic: true")

	// Output:
	// Multiple calls return same partition: true
	// Partition is deterministic: true
}

// ExampleHashPartitioner_Partition demonstrates consistent routing.
func ExampleHashPartitioner_Partition() {
	partitioner := partition.NewHashPartitioner()
	numPartitions := 4

	// Orders for same customer always go to same partition
	customer := "customer-100"
	order1 := partitioner.Partition(customer, numPartitions)
	order2 := partitioner.Partition(customer, numPartitions)

	fmt.Println("Same customer routes consistently:", order1 == order2)

	// Different keys may route to different partitions
	productPartition := partitioner.Partition("product-xyz", numPartitions)
	fmt.Println("Product partition is valid:", productPartition >= 0 && productPartition < numPartitions)

	// Output:
	// Same customer routes consistently: true
	// Product partition is valid: true
}

// ExampleNewRoundRobinPartitioner demonstrates even distribution.
//
// RoundRobinPartitioner distributes messages evenly across partitions,
// ignoring the key. Use when ordering doesn't matter but even load
// distribution is important.
func ExampleNewRoundRobinPartitioner() {
	partitioner := partition.NewRoundRobinPartitioner()
	numPartitions := 3

	// Each call cycles through partitions
	fmt.Println("Round-robin distribution:")
	for i := 0; i < 6; i++ {
		// Key is ignored in round-robin
		p := partitioner.Partition("any-key", numPartitions)
		fmt.Printf("  Message %d -> partition %d\n", i+1, p)
	}

	// Output:
	// Round-robin distribution:
	//   Message 1 -> partition 1
	//   Message 2 -> partition 2
	//   Message 3 -> partition 0
	//   Message 4 -> partition 1
	//   Message 5 -> partition 2
	//   Message 6 -> partition 0
}

// ExampleRoundRobinPartitioner_Partition demonstrates that round-robin ignores keys.
func ExampleRoundRobinPartitioner_Partition() {
	partitioner := partition.NewRoundRobinPartitioner()
	numPartitions := 2

	// Same key gets different partitions each time
	key := "user-123"
	p1 := partitioner.Partition(key, numPartitions)
	p2 := partitioner.Partition(key, numPartitions)
	p3 := partitioner.Partition(key, numPartitions)

	fmt.Printf("Same key, different partitions: %d, %d, %d\n", p1, p2, p3)
	fmt.Println("Round-robin ignores the key for even distribution")

	// Output:
	// Same key, different partitions: 1, 0, 1
	// Round-robin ignores the key for even distribution
}

// ExampleNewConsistentHashPartitioner demonstrates consistent hashing.
//
// ConsistentHashPartitioner minimizes key redistribution when partitions
// are added or removed. Unlike simple modulo, only ~1/n keys move when
// adding or removing a partition.
func ExampleNewConsistentHashPartitioner() {
	// Higher replica count = better distribution
	partitioner := partition.NewConsistentHashPartitioner(100)

	// Keys route consistently
	key := "user-xyz"
	numPartitions := 3

	// Same key always maps to same partition
	p1 := partitioner.Partition(key, numPartitions)
	p2 := partitioner.Partition(key, numPartitions)
	p3 := partitioner.Partition(key, numPartitions)

	fmt.Println("Consistent hashing is deterministic:", p1 == p2 && p2 == p3)
	fmt.Println("Partition is in valid range:", p1 >= 0 && p1 < numPartitions)

	// Output:
	// Consistent hashing is deterministic: true
	// Partition is in valid range: true
}

// ExampleConsistentHashPartitioner_Partition demonstrates minimal redistribution.
func ExampleConsistentHashPartitioner_Partition() {
	partitioner := partition.NewConsistentHashPartitioner(150)

	// Track key assignments across partition count changes
	testKeys := []string{"order-1", "order-2", "order-3", "order-4", "order-5",
		"order-6", "order-7", "order-8", "order-9", "order-10"}

	// Map partitions with 3 nodes
	assignments3 := make(map[string]int)
	for _, key := range testKeys {
		assignments3[key] = partitioner.Partition(key, 3)
	}

	// Map partitions with 4 nodes
	assignments4 := make(map[string]int)
	for _, key := range testKeys {
		assignments4[key] = partitioner.Partition(key, 4)
	}

	// Count how many keys changed partition
	changed := 0
	for _, key := range testKeys {
		if assignments3[key] != assignments4[key] {
			changed++
		}
	}

	// With consistent hashing, only ~1/n keys should move
	// When going from 3 to 4 partitions, ~25% should move
	fmt.Println("Keys tested:", len(testKeys))
	fmt.Println("Keys that changed partition:", changed)
	fmt.Println("Minimal redistribution achieved:", changed <= len(testKeys)/2)

	// Output:
	// Keys tested: 10
	// Keys that changed partition: 2
	// Minimal redistribution achieved: true
}

// ExampleNewPublishOptions demonstrates creating publish options.
//
// PublishOptions specify routing and metadata for partitioned messages.
func ExampleNewPublishOptions() {
	// Create options with partition key
	opts := partition.NewPublishOptions("user-12345")

	fmt.Println("Partition key:", opts.PartitionKey)
	fmt.Println("Headers initialized:", opts.Headers != nil)
	fmt.Println("Default priority:", opts.Priority)

	// Output:
	// Partition key: user-12345
	// Headers initialized: true
	// Default priority: 0
}

// ExamplePublishOptions_WithHeader demonstrates adding headers.
//
// Headers are key-value pairs for metadata like correlation IDs,
// tracing context, or custom metadata.
func ExamplePublishOptions_WithHeader() {
	opts := partition.NewPublishOptions("order-789").
		WithHeader("correlation-id", "corr-abc123").
		WithHeader("source", "checkout-service")

	fmt.Println("Has correlation-id:", opts.Headers["correlation-id"] == "corr-abc123")
	fmt.Println("Has source:", opts.Headers["source"] == "checkout-service")
	fmt.Println("Header count:", len(opts.Headers))

	// Output:
	// Has correlation-id: true
	// Has source: true
	// Header count: 2
}

// ExamplePublishOptions_WithPriority demonstrates setting message priority.
//
// Priority determines message ordering when the transport supports it.
// Higher values indicate more important messages.
func ExamplePublishOptions_WithPriority() {
	// Normal priority for regular events
	normalOpts := partition.NewPublishOptions("user-123")
	fmt.Println("Normal priority:", normalOpts.Priority)

	// High priority for important events
	urgentOpts := partition.NewPublishOptions("payment-456").
		WithPriority(100)
	fmt.Println("Urgent priority:", urgentOpts.Priority)

	// Critical priority for alerts
	criticalOpts := partition.NewPublishOptions("alert-789").
		WithPriority(1000)
	fmt.Println("Critical priority:", criticalOpts.Priority)

	// Output:
	// Normal priority: 0
	// Urgent priority: 100
	// Critical priority: 1000
}

// Example_publishOptionsChaining demonstrates fluent option configuration.
func Example_publishOptionsChaining() {
	// All methods return *PublishOptions for chaining
	opts := partition.NewPublishOptions("customer-100").
		WithHeader("trace-id", "trace-xyz").
		WithHeader("span-id", "span-abc").
		WithPriority(50)

	fmt.Println("Partition key:", opts.PartitionKey)
	fmt.Println("Priority:", opts.Priority)
	fmt.Println("Has trace-id:", opts.Headers["trace-id"] != "")

	// Output:
	// Partition key: customer-100
	// Priority: 50
	// Has trace-id: true
}

// Example_workerPoolPattern demonstrates using partitioning with worker pools.
func Example_workerPoolPattern() {
	// This pattern shows how to use partitioning for ordered parallel processing
	partitioner := partition.NewHashPartitioner()
	numWorkers := 4

	type UserEvent struct {
		UserID string
		Action string
	}

	// Simulate events for different users
	events := []UserEvent{
		{UserID: "user-1", Action: "login"},
		{UserID: "user-2", Action: "purchase"},
		{UserID: "user-1", Action: "logout"}, // Same user as first event
		{UserID: "user-3", Action: "signup"},
		{UserID: "user-2", Action: "review"}, // Same user as second event
	}

	// Track which worker handles each user
	userWorkers := make(map[string]int)
	for _, event := range events {
		worker := partitioner.Partition(event.UserID, numWorkers)
		if existing, ok := userWorkers[event.UserID]; ok {
			// Verify same user always goes to same worker
			if existing != worker {
				fmt.Println("ERROR: User routed to different workers!")
				return
			}
		}
		userWorkers[event.UserID] = worker
	}

	fmt.Println("All events for same user routed to same worker: true")
	fmt.Println("Number of unique users:", len(userWorkers))

	// Output:
	// All events for same user routed to same worker: true
	// Number of unique users: 3
}

// Example_keyExtractor demonstrates the KeyExtractor type for typed key extraction.
func Example_keyExtractor() {
	type Order struct {
		OrderID    string
		CustomerID string
		Status     string
	}

	// Define a key extractor for Order type
	var extractKey partition.KeyExtractor[Order] = func(o Order) string {
		return o.CustomerID
	}

	// Use the extractor with a partitioner
	partitioner := partition.NewHashPartitioner()
	numPartitions := 4

	orders := []Order{
		{OrderID: "ord-1", CustomerID: "cust-A", Status: "pending"},
		{OrderID: "ord-2", CustomerID: "cust-B", Status: "shipped"},
		{OrderID: "ord-3", CustomerID: "cust-A", Status: "delivered"}, // Same customer
	}

	// Track partitions by customer
	customerPartitions := make(map[string]int)
	for _, order := range orders {
		key := extractKey(order)
		p := partitioner.Partition(key, numPartitions)
		customerPartitions[key] = p
	}

	// Verify same customer always gets same partition
	p1 := customerPartitions["cust-A"]
	p2 := partitioner.Partition(extractKey(orders[0]), numPartitions)
	p3 := partitioner.Partition(extractKey(orders[2]), numPartitions)

	fmt.Println("Same customer routes consistently:", p1 == p2 && p2 == p3)
	fmt.Println("Different customers may differ:", true) // Just demonstrating the concept

	// Output:
	// Same customer routes consistently: true
	// Different customers may differ: true
}

// Example_choosingPartitioner demonstrates when to use each partitioner type.
func Example_choosingPartitioner() {
	fmt.Println("Choosing the right partitioner:")
	fmt.Println()

	fmt.Println("HashPartitioner - Use when:")
	fmt.Println("  - Partition count is fixed")
	fmt.Println("  - Simplicity and speed are priorities")
	fmt.Println("  - Acceptable to redistribute all keys on partition change")
	fmt.Println()

	fmt.Println("ConsistentHashPartitioner - Use when:")
	fmt.Println("  - Partitions may be added/removed dynamically")
	fmt.Println("  - Want to minimize rebalancing during scaling")
	fmt.Println("  - Cache locality matters (fewer keys move)")
	fmt.Println()

	fmt.Println("RoundRobinPartitioner - Use when:")
	fmt.Println("  - Ordering doesn't matter")
	fmt.Println("  - Even load distribution is the priority")
	fmt.Println("  - Keys are not meaningful for routing")

	// Output:
	// Choosing the right partitioner:
	//
	// HashPartitioner - Use when:
	//   - Partition count is fixed
	//   - Simplicity and speed are priorities
	//   - Acceptable to redistribute all keys on partition change
	//
	// ConsistentHashPartitioner - Use when:
	//   - Partitions may be added/removed dynamically
	//   - Want to minimize rebalancing during scaling
	//   - Cache locality matters (fewer keys move)
	//
	// RoundRobinPartitioner - Use when:
	//   - Ordering doesn't matter
	//   - Even load distribution is the priority
	//   - Keys are not meaningful for routing
}

// Example_edgeCases demonstrates handling of edge cases.
func Example_edgeCases() {
	partitioner := partition.NewHashPartitioner()

	// Empty key defaults to partition 0
	p := partitioner.Partition("", 4)
	fmt.Println("Empty key -> partition:", p)

	// Zero or negative partitions returns 0
	p = partitioner.Partition("any-key", 0)
	fmt.Println("Zero partitions -> partition:", p)

	p = partitioner.Partition("any-key", -1)
	fmt.Println("Negative partitions -> partition:", p)

	// Single partition always returns 0
	p = partitioner.Partition("any-key", 1)
	fmt.Println("Single partition -> partition:", p)

	// Output:
	// Empty key -> partition: 0
	// Zero partitions -> partition: 0
	// Negative partitions -> partition: 0
	// Single partition -> partition: 0
}

// Example_distributionAnalysis demonstrates analyzing key distribution.
func Example_distributionAnalysis() {
	partitioner := partition.NewHashPartitioner()
	numPartitions := 4

	// Generate sample keys
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		keys[i] = fmt.Sprintf("user-%03d", i)
	}

	// Count distribution
	counts := make([]int, numPartitions)
	for _, key := range keys {
		p := partitioner.Partition(key, numPartitions)
		counts[p]++
	}

	// Verify distribution is reasonably even (each partition has at least some keys)
	allHaveKeys := true
	for _, count := range counts {
		if count == 0 {
			allHaveKeys = false
			break
		}
	}

	fmt.Println("Total keys:", len(keys))
	fmt.Println("Number of partitions:", numPartitions)
	fmt.Println("All partitions have keys:", allHaveKeys)

	// Output:
	// Total keys: 100
	// Number of partitions: 4
	// All partitions have keys: true
}

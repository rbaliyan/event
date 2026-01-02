// Package partition provides partitioning strategies for ordered message delivery.
//
// Partitioning ensures that messages with the same key are always delivered to
// the same consumer in order. This is essential for:
//   - Processing events for the same entity in order (e.g., user events)
//   - Maintaining causality (e.g., order created before order shipped)
//   - Enabling parallel processing while preserving per-key ordering
//
// # Overview
//
// The package provides:
//   - Partitioner interface for custom partitioning strategies
//   - HashPartitioner for consistent hash-based partitioning
//   - RoundRobinPartitioner for even distribution
//   - ConsistentHashPartitioner for minimal rebalancing (see consistent.go)
//
// # When to Use
//
// Use partitioning when:
//   - Events for the same entity must be processed in order
//   - You need parallel processing with ordering guarantees
//   - Different consumers should handle different key ranges
//
// # Basic Usage
//
//	partitioner := partition.NewHashPartitioner()
//	numPartitions := 4
//
//	// Route user events to consistent partitions
//	userID := "user-123"
//	partition := partitioner.Partition(userID, numPartitions)
//	// Same userID always maps to same partition
//
// # Example with Event Handling
//
//	type UserEvent struct {
//	    UserID string
//	    Action string
//	}
//
//	partitioner := partition.NewHashPartitioner()
//	workers := make([]chan UserEvent, numPartitions)
//
//	func route(event UserEvent) {
//	    p := partitioner.Partition(event.UserID, len(workers))
//	    workers[p] <- event  // Same user always goes to same worker
//	}
package partition

import (
	"hash/fnv"
)

// Partitioner determines which partition a message should be routed to.
//
// Implementations must be deterministic: the same key must always map
// to the same partition (for a given numPartitions).
//
// Implementations:
//   - HashPartitioner: Consistent FNV-1a hash-based partitioning
//   - RoundRobinPartitioner: Even distribution (ignores key)
//   - ConsistentHashPartitioner: Minimal rebalancing on partition changes
type Partitioner interface {
	// Partition returns the partition number for a given key.
	// Returns a value in range [0, numPartitions).
	Partition(key string, numPartitions int) int
}

// HashPartitioner uses FNV-1a hash for consistent partitioning.
//
// FNV-1a is a fast, non-cryptographic hash with good distribution.
// The same key always maps to the same partition, enabling ordered
// processing per key.
//
// Example:
//
//	p := partition.NewHashPartitioner()
//	// Same key always returns same partition
//	p.Partition("user-123", 4)  // e.g., returns 2
//	p.Partition("user-123", 4)  // always returns 2
type HashPartitioner struct{}

// NewHashPartitioner creates a new hash-based partitioner.
func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{}
}

// Partition returns the partition number using FNV-1a hash
func (p *HashPartitioner) Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if key == "" {
		return 0
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(numPartitions))
}

// RoundRobinPartitioner distributes messages evenly across partitions
type RoundRobinPartitioner struct {
	counter uint64
}

// NewRoundRobinPartitioner creates a new round-robin partitioner
func NewRoundRobinPartitioner() *RoundRobinPartitioner {
	return &RoundRobinPartitioner{}
}

// Partition returns the next partition in round-robin order
func (p *RoundRobinPartitioner) Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	p.counter++
	return int(p.counter % uint64(numPartitions))
}

// KeyExtractor extracts a partition key from data
type KeyExtractor[T any] func(data T) string

// Compile-time checks
var _ Partitioner = (*HashPartitioner)(nil)
var _ Partitioner = (*RoundRobinPartitioner)(nil)

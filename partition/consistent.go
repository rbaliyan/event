package partition

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// ConsistentHashPartitioner implements consistent hashing for partition assignment.
//
// Consistent hashing minimizes key redistribution when partitions are added or removed.
// Unlike simple modulo hashing (key % numPartitions), which redistributes ~all keys
// when the partition count changes, consistent hashing only redistributes ~1/n keys
// (where n is the number of partitions).
//
// This is achieved using a hash ring with virtual nodes (replicas). Each partition
// is represented by multiple points on the ring, which improves key distribution
// and reduces the impact of partition changes.
//
// # When to Use
//
// Use ConsistentHashPartitioner when:
//   - Partitions may be added or removed dynamically
//   - You want to minimize rebalancing during scaling
//   - Consistent routing matters more than perfect distribution
//
// Use HashPartitioner when:
//   - Partition count is fixed
//   - Simplicity and speed are priorities
//   - Slight distribution improvements don't justify complexity
//
// # Example
//
//	// Higher replica count = better distribution, more memory
//	partitioner := partition.NewConsistentHashPartitioner(150)
//
//	// Route user events to consistent partitions
//	userID := "user-12345"
//	partition := partitioner.Partition(userID, numPartitions)
//
//	// Adding partitions only moves ~1/numPartitions keys
//	partition = partitioner.Partition(userID, numPartitions+1)
//
// # Thread Safety
//
// ConsistentHashPartitioner is safe for concurrent use. The hash ring is rebuilt
// lazily when the partition count changes.
type ConsistentHashPartitioner struct {
	mu       sync.RWMutex
	ring     []uint32
	nodes    map[uint32]int
	replicas int
}

// NewConsistentHashPartitioner creates a new consistent hash partitioner.
//
// The replicas parameter determines how many virtual nodes each partition gets
// on the hash ring. Higher values provide better key distribution across partitions
// but use more memory.
//
// Parameters:
//   - replicas: Number of virtual nodes per partition (default: 100 if <= 0)
//
// Recommended replica values:
//   - 50-100: Good balance for most use cases
//   - 100-200: Better distribution, slightly higher memory
//   - 200+: Near-perfect distribution, higher memory usage
//
// Example:
//
//	// Default 100 replicas
//	p := partition.NewConsistentHashPartitioner(0)
//
//	// Higher replica count for better distribution
//	p = partition.NewConsistentHashPartitioner(150)
func NewConsistentHashPartitioner(replicas int) *ConsistentHashPartitioner {
	if replicas <= 0 {
		replicas = 100
	}
	return &ConsistentHashPartitioner{
		nodes:    make(map[uint32]int),
		replicas: replicas,
	}
}

// Partition returns the partition number using consistent hashing.
//
// The key is hashed using CRC32 and mapped to a position on the hash ring.
// The partition owning that ring position is returned.
//
// The hash ring is rebuilt lazily when numPartitions changes. This operation
// is thread-safe but may cause a brief lock contention on first call after
// partition count change.
//
// Parameters:
//   - key: The partition key (e.g., user ID, order ID)
//   - numPartitions: Total number of partitions
//
// Returns a value in range [0, numPartitions). Returns 0 for empty key or
// numPartitions <= 0.
func (p *ConsistentHashPartitioner) Partition(key string, numPartitions int) int {
	if numPartitions <= 0 {
		return 0
	}
	if key == "" {
		return 0
	}

	p.mu.Lock()
	// Rebuild ring if partition count changed
	if len(p.ring) != numPartitions*p.replicas {
		p.buildRing(numPartitions)
	}
	p.mu.Unlock()

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.ring) == 0 {
		return 0
	}

	hash := p.hash(key)
	idx := p.search(hash)
	return p.nodes[p.ring[idx]]
}

// buildRing creates the hash ring for the given number of partitions
func (p *ConsistentHashPartitioner) buildRing(numPartitions int) {
	p.ring = make([]uint32, 0, numPartitions*p.replicas)
	p.nodes = make(map[uint32]int)

	for i := 0; i < numPartitions; i++ {
		for j := 0; j < p.replicas; j++ {
			key := strconv.Itoa(i) + "-" + strconv.Itoa(j)
			hash := p.hash(key)
			p.ring = append(p.ring, hash)
			p.nodes[hash] = i
		}
	}

	sort.Slice(p.ring, func(i, j int) bool {
		return p.ring[i] < p.ring[j]
	})
}

// hash computes a hash value for a key
func (p *ConsistentHashPartitioner) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// search finds the index of the first node with hash >= the given hash
func (p *ConsistentHashPartitioner) search(hash uint32) int {
	idx := sort.Search(len(p.ring), func(i int) bool {
		return p.ring[i] >= hash
	})
	if idx >= len(p.ring) {
		idx = 0
	}
	return idx
}

// Compile-time check
var _ Partitioner = (*ConsistentHashPartitioner)(nil)

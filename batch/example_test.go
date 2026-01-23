package batch_test

import (
	"fmt"
	"time"

	"github.com/rbaliyan/event/v3/batch"
)

// Example demonstrates basic usage of the batch package for accumulating
// items and processing them in groups.
func Example() {
	// Create a collector with batch size of 3 and 1 second timeout hint
	collector := batch.NewCollector[string](3, time.Second)

	// Add items - returns true when batch is full
	collector.Add(nil, "item-1")
	collector.Add(nil, "item-2")
	isFull := collector.Add(nil, "item-3")

	fmt.Println("Batch is full:", isFull)

	// Flush the batch
	items, _ := collector.Flush()
	fmt.Println("Batch contents:", items)

	// After flush, collector is empty
	fmt.Println("Size after flush:", collector.Size())

	// Output:
	// Batch is full: true
	// Batch contents: [item-1 item-2 item-3]
	// Size after flush: 0
}

// ExampleNewProcessor demonstrates creating a batch processor with options.
func ExampleNewProcessor() {
	// Create processor with custom batch size and timeout
	processor := batch.NewProcessor[string](
		batch.WithBatchSize(100),
		batch.WithTimeout(5*time.Second),
		batch.WithMaxRetries(3),
	)

	// Processor is ready to use with Process()
	_ = processor

	fmt.Println("Processor created with custom options")

	// Output:
	// Processor created with custom options
}

// ExampleNewCollector demonstrates creating a collector for manual batching.
func ExampleNewCollector() {
	// Create a collector for Order types
	// 50 items per batch, 2 second timeout hint
	collector := batch.NewCollector[int](50, 2*time.Second)

	fmt.Println("Initial size:", collector.Size())

	// Add some items
	collector.Add(nil, 100)
	collector.Add(nil, 200)
	collector.Add(nil, 300)

	fmt.Println("After adding 3 items:", collector.Size())

	// Output:
	// Initial size: 0
	// After adding 3 items: 3
}

// ExampleCollector_Add demonstrates adding items to a collector.
func ExampleCollector_Add() {
	collector := batch.NewCollector[string](3, time.Second)

	// Add returns false until batch is full
	fmt.Println("Add item 1, full:", collector.Add(nil, "a"))
	fmt.Println("Add item 2, full:", collector.Add(nil, "b"))
	fmt.Println("Add item 3, full:", collector.Add(nil, "c")) // Reaches size

	// Output:
	// Add item 1, full: false
	// Add item 2, full: false
	// Add item 3, full: true
}

// ExampleCollector_Flush demonstrates flushing a batch.
func ExampleCollector_Flush() {
	collector := batch.NewCollector[int](10, time.Second)

	// Add some items
	collector.Add(nil, 1)
	collector.Add(nil, 2)
	collector.Add(nil, 3)

	// Flush returns items and messages
	items, messages := collector.Flush()

	fmt.Println("Flushed items:", items)
	fmt.Println("Messages count:", len(messages))

	// Output:
	// Flushed items: [1 2 3]
	// Messages count: 3
}

// ExampleCollector_Size demonstrates checking the current batch size.
func ExampleCollector_Size() {
	collector := batch.NewCollector[string](100, time.Second)

	fmt.Println("Empty:", collector.Size())

	collector.Add(nil, "x")
	collector.Add(nil, "y")
	fmt.Println("After 2 adds:", collector.Size())

	collector.Flush()
	fmt.Println("After flush:", collector.Size())

	// Output:
	// Empty: 0
	// After 2 adds: 2
	// After flush: 0
}

// ExampleWithBatchSize demonstrates configuring batch size.
func ExampleWithBatchSize() {
	opts := batch.DefaultOptions()
	fmt.Println("Default batch size:", opts.BatchSize)

	batch.WithBatchSize(500)(opts)
	fmt.Println("Custom batch size:", opts.BatchSize)

	// Output:
	// Default batch size: 100
	// Custom batch size: 500
}

// ExampleWithTimeout demonstrates configuring batch timeout.
func ExampleWithTimeout() {
	opts := batch.DefaultOptions()
	fmt.Println("Default timeout:", opts.Timeout)

	batch.WithTimeout(5 * time.Second)(opts)
	fmt.Println("Custom timeout:", opts.Timeout)

	// Output:
	// Default timeout: 1s
	// Custom timeout: 5s
}

// ExampleWithMaxRetries demonstrates configuring retry behavior.
func ExampleWithMaxRetries() {
	opts := batch.DefaultOptions()
	fmt.Println("Default max retries:", opts.MaxRetries)

	batch.WithMaxRetries(5)(opts)
	fmt.Println("Custom max retries:", opts.MaxRetries)

	// Disable retries
	batch.WithMaxRetries(0)(opts)
	fmt.Println("Retries disabled:", opts.MaxRetries)

	// Output:
	// Default max retries: 3
	// Custom max retries: 5
	// Retries disabled: 0
}

// ExampleWithOnError demonstrates configuring error handling.
func ExampleWithOnError() {
	opts := batch.DefaultOptions()

	var lastError error
	var lastBatchSize int

	batch.WithOnError(func(b []any, err error) {
		lastBatchSize = len(b)
		lastError = err
	})(opts)

	// Simulate error handler being called
	opts.OnError([]any{"item1", "item2"}, fmt.Errorf("test error"))

	fmt.Println("Batch size in error:", lastBatchSize)
	fmt.Println("Error message:", lastError)

	// Output:
	// Batch size in error: 2
	// Error message: test error
}

// ExampleDefaultOptions demonstrates the default batch options.
func ExampleDefaultOptions() {
	opts := batch.DefaultOptions()

	fmt.Println("BatchSize:", opts.BatchSize)
	fmt.Println("Timeout:", opts.Timeout)
	fmt.Println("MaxRetries:", opts.MaxRetries)

	// Output:
	// BatchSize: 100
	// Timeout: 1s
	// MaxRetries: 3
}

// Example_timeoutBasedFlushing demonstrates flushing based on timeout.
func Example_timeoutBasedFlushing() {
	collector := batch.NewCollector[string](100, 50*time.Millisecond)

	// Add some items (not reaching batch size)
	collector.Add(nil, "a")
	collector.Add(nil, "b")

	fmt.Println("Size before timeout:", collector.Size())

	// In a real application, you would use a ticker to check timeout
	// Here we just demonstrate manual timeout-based flushing
	if collector.Size() > 0 {
		items, _ := collector.Flush()
		fmt.Println("Flushed on timeout:", items)
	}

	// Output:
	// Size before timeout: 2
	// Flushed on timeout: [a b]
}

// Example_batchProcessingPattern demonstrates a typical batch processing pattern.
func Example_batchProcessingPattern() {
	collector := batch.NewCollector[int](5, time.Second)

	// Simulate processing a stream of items
	processedBatches := [][]int{}

	processBatch := func() {
		if collector.Size() > 0 {
			items, _ := collector.Flush()
			processedBatches = append(processedBatches, items)
		}
	}

	// Process 12 items with batch size 5
	for i := 1; i <= 12; i++ {
		if collector.Add(nil, i) {
			// Batch is full, process it
			processBatch()
		}
	}
	// Process remaining items
	processBatch()

	fmt.Printf("Processed %d batches\n", len(processedBatches))
	for i, b := range processedBatches {
		fmt.Printf("Batch %d: %v\n", i+1, b)
	}

	// Output:
	// Processed 3 batches
	// Batch 1: [1 2 3 4 5]
	// Batch 2: [6 7 8 9 10]
	// Batch 3: [11 12]
}

// Example_customTypes demonstrates batching with custom struct types.
func Example_customTypes() {
	type Order struct {
		ID    string
		Total float64
	}

	collector := batch.NewCollector[Order](3, time.Second)

	collector.Add(nil, Order{ID: "ORD-001", Total: 99.99})
	collector.Add(nil, Order{ID: "ORD-002", Total: 149.50})
	collector.Add(nil, Order{ID: "ORD-003", Total: 75.00})

	orders, _ := collector.Flush()

	totalAmount := 0.0
	for _, o := range orders {
		totalAmount += o.Total
	}

	fmt.Printf("Batch of %d orders, total: $%.2f\n", len(orders), totalAmount)

	// Output:
	// Batch of 3 orders, total: $324.49
}

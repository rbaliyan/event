package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()

	if opts.batchSize != 100 {
		t.Errorf("expected batch size 100, got %d", opts.batchSize)
	}
	if opts.timeout != time.Second {
		t.Errorf("expected timeout 1s, got %v", opts.timeout)
	}
	if opts.maxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", opts.maxRetries)
	}
	if opts.onError == nil {
		t.Error("expected onError to be set")
	}
}

func TestWithBatchSize(t *testing.T) {
	opts := defaultOptions()

	WithBatchSize(50)(opts)
	if opts.batchSize != 50 {
		t.Errorf("expected batch size 50, got %d", opts.batchSize)
	}

	// Zero should be ignored
	WithBatchSize(0)(opts)
	if opts.batchSize != 50 {
		t.Errorf("expected batch size 50, got %d", opts.batchSize)
	}

	// Negative should be ignored
	WithBatchSize(-1)(opts)
	if opts.batchSize != 50 {
		t.Errorf("expected batch size 50, got %d", opts.batchSize)
	}
}

func TestWithTimeout(t *testing.T) {
	opts := defaultOptions()

	WithTimeout(5 * time.Second)(opts)
	if opts.timeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", opts.timeout)
	}

	// Zero should be ignored
	WithTimeout(0)(opts)
	if opts.timeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", opts.timeout)
	}
}

func TestWithMaxRetries(t *testing.T) {
	opts := defaultOptions()

	WithMaxRetries(5)(opts)
	if opts.maxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", opts.maxRetries)
	}

	// Zero should be allowed
	WithMaxRetries(0)(opts)
	if opts.maxRetries != 0 {
		t.Errorf("expected max retries 0, got %d", opts.maxRetries)
	}

	// Negative should be ignored
	WithMaxRetries(-1)(opts)
	if opts.maxRetries != 0 {
		t.Errorf("expected max retries 0, got %d", opts.maxRetries)
	}
}

func TestWithOnError(t *testing.T) {
	opts := defaultOptions()
	called := false

	WithOnError(func(batch []any, err error) {
		called = true
	})(opts)

	opts.onError(nil, nil)
	if !called {
		t.Error("expected onError to be called")
	}

	// Nil should be ignored
	prev := opts.onError
	WithOnError(nil)(opts)
	// onError should still be the previous function (nil is ignored)
	opts.onError(nil, nil)
	_ = prev
}

func TestCollector(t *testing.T) {
	t.Run("NewCollector", func(t *testing.T) {
		collector := NewCollector[string](10, time.Second)

		if collector.Size() != 0 {
			t.Errorf("expected size 0, got %d", collector.Size())
		}
	})

	t.Run("Add returns false when not full", func(t *testing.T) {
		collector := NewCollector[int](5, time.Second)

		for i := 0; i < 4; i++ {
			full := collector.Add(nil, i)
			if full {
				t.Errorf("expected not full at index %d", i)
			}
		}

		if collector.Size() != 4 {
			t.Errorf("expected size 4, got %d", collector.Size())
		}
	})

	t.Run("Add returns true when full", func(t *testing.T) {
		collector := NewCollector[int](3, time.Second)

		collector.Add(nil, 1)
		collector.Add(nil, 2)
		full := collector.Add(nil, 3)

		if !full {
			t.Error("expected full when reaching size")
		}
	})

	t.Run("Flush returns batch and clears", func(t *testing.T) {
		collector := NewCollector[string](10, time.Second)

		collector.Add(nil, "a")
		collector.Add(nil, "b")
		collector.Add(nil, "c")

		batch, msgs := collector.Flush()

		if len(batch) != 3 {
			t.Errorf("expected batch length 3, got %d", len(batch))
		}
		if batch[0] != "a" || batch[1] != "b" || batch[2] != "c" {
			t.Error("unexpected batch contents")
		}
		if len(msgs) != 3 {
			t.Errorf("expected msgs length 3, got %d", len(msgs))
		}

		if collector.Size() != 0 {
			t.Errorf("expected size 0 after flush, got %d", collector.Size())
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		collector := NewCollector[int](100, time.Second)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(2)

			go func(val int) {
				defer wg.Done()
				collector.Add(nil, val)
			}(i)

			go func() {
				defer wg.Done()
				collector.Size()
			}()
		}

		wg.Wait()
	})
}

func TestProcessor(t *testing.T) {
	t.Run("NewProcessor with options", func(t *testing.T) {
		processor := NewProcessor[string](
			WithBatchSize(50),
			WithTimeout(2*time.Second),
		)

		if processor.opts.batchSize != 50 {
			t.Errorf("expected batch size 50, got %d", processor.opts.batchSize)
		}
		if processor.opts.timeout != 2*time.Second {
			t.Errorf("expected timeout 2s, got %v", processor.opts.timeout)
		}
	})

	t.Run("Process with empty channel", func(t *testing.T) {
		processor := NewProcessor[string]()

		ch := make(chan transport.Message)
		close(ch)

		var processedBatches [][]string
		err := processor.Process(context.Background(), ch, func(ctx context.Context, batch []string) error {
			processedBatches = append(processedBatches, batch)
			return nil
		})

		if err != nil {
			t.Fatalf("Process failed: %v", err)
		}
	})
}

// mockMessage is a minimal mock for transport.Message
type mockMessage struct {
	payload any
	acked   bool
	err     error
}

func (m *mockMessage) ID() string                  { return "mock-id" }
func (m *mockMessage) Source() string               { return "mock-source" }
func (m *mockMessage) Payload() any                 { return m.payload }
func (m *mockMessage) Metadata() map[string]string  { return nil }
func (m *mockMessage) Context() context.Context     { return context.Background() }
func (m *mockMessage) RetryCount() int              { return 0 }
func (m *mockMessage) Timestamp() time.Time         { return time.Now() }
func (m *mockMessage) SpanContext() any             { return nil }
func (m *mockMessage) Ack(err error) error {
	m.acked = true
	m.err = err
	return nil
}

func TestProcessorIntegration(t *testing.T) {
	t.Skip("Integration test requires transport.Message implementation")
}

func TestOptionsErrorHandler(t *testing.T) {
	var capturedBatch []any
	var capturedErr error

	opts := defaultOptions()
	WithOnError(func(batch []any, err error) {
		capturedBatch = batch
		capturedErr = err
	})(opts)

	testBatch := []any{"a", "b", "c"}
	testErr := errors.New("test error")

	opts.onError(testBatch, testErr)

	if len(capturedBatch) != 3 {
		t.Errorf("expected batch length 3, got %d", len(capturedBatch))
	}
	if capturedErr != testErr {
		t.Errorf("expected test error, got %v", capturedErr)
	}
}

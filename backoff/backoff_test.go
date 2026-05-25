package backoff

import (
	"testing"
	"time"
)

func TestConstant(t *testing.T) {
	t.Parallel()
	strategy := &Constant{Delay: 100 * time.Millisecond}

	for i := 0; i < 10; i++ {
		delay := strategy.NextDelay(i)
		if delay != 100*time.Millisecond {
			t.Errorf("attempt %d: expected 100ms, got %v", i, delay)
		}
	}
}

func TestLinear(t *testing.T) {
	t.Parallel()
	strategy := &Linear{
		Initial: 100 * time.Millisecond,
		Step:    50 * time.Millisecond,
		Max:     300 * time.Millisecond,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 150 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 250 * time.Millisecond},
		{4, 300 * time.Millisecond},  // Max
		{5, 300 * time.Millisecond},  // Max
		{10, 300 * time.Millisecond}, // Max
	}

	for _, tt := range tests {
		delay := strategy.NextDelay(tt.attempt)
		if delay != tt.expected {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expected, delay)
		}
	}
}

func TestLinear_NoMax(t *testing.T) {
	t.Parallel()
	strategy := &Linear{
		Initial: 100 * time.Millisecond,
		Step:    100 * time.Millisecond,
		Max:     0, // No max
	}

	delay := strategy.NextDelay(100)
	expected := 100*time.Millisecond + 100*100*time.Millisecond
	if delay != expected {
		t.Errorf("expected %v, got %v", expected, delay)
	}
}

func TestExponential(t *testing.T) {
	t.Parallel()
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        1 * time.Second,
		Jitter:     0, // No jitter for deterministic test
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1 * time.Second}, // Max
		{5, 1 * time.Second}, // Max
	}

	for _, tt := range tests {
		delay := strategy.NextDelay(tt.attempt)
		if delay != tt.expected {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expected, delay)
		}
	}
}

func TestExponential_DefaultMultiplier(t *testing.T) {
	t.Parallel()
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 0, // Should default to 2.0
		Jitter:     0,
	}

	delay := strategy.NextDelay(1)
	expected := 200 * time.Millisecond
	if delay != expected {
		t.Errorf("expected %v, got %v", expected, delay)
	}
}

func TestExponential_NoMax(t *testing.T) {
	t.Parallel()
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        0, // No max
		Jitter:     0,
	}

	delay := strategy.NextDelay(10)
	expected := 100 * time.Millisecond * time.Duration(1<<10) // 100ms * 1024
	if delay != expected {
		t.Errorf("expected %v, got %v", expected, delay)
	}
}

func TestExponential_WithJitter(t *testing.T) {
	t.Parallel()
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        30 * time.Second,
		Jitter:     0.1, // 10% jitter
	}

	baseDelay := 100 * time.Millisecond

	// With 10% jitter, delay should be in range [90ms, 110ms]
	minExpected := time.Duration(float64(baseDelay) * 0.9)
	maxExpected := time.Duration(float64(baseDelay) * 1.1)

	// Run multiple times to test randomness
	for i := 0; i < 100; i++ {
		delay := strategy.NextDelay(0)
		if delay < minExpected || delay > maxExpected {
			t.Errorf("delay %v out of expected range [%v, %v]", delay, minExpected, maxExpected)
		}
	}
}

func TestDefault(t *testing.T) {
	t.Parallel()
	strategy := Default()

	// Verify it's an Exponential strategy
	exp, ok := strategy.(*Exponential)
	if !ok {
		t.Fatal("Default() should return *Exponential")
	}

	if exp.Initial != 100*time.Millisecond {
		t.Errorf("Initial: expected 100ms, got %v", exp.Initial)
	}
	if exp.Multiplier != 2.0 {
		t.Errorf("Multiplier: expected 2.0, got %v", exp.Multiplier)
	}
	if exp.Max != 30*time.Second {
		t.Errorf("Max: expected 30s, got %v", exp.Max)
	}
	if exp.Jitter != 0.1 {
		t.Errorf("Jitter: expected 0.1, got %v", exp.Jitter)
	}
}

func TestNone(t *testing.T) {
	t.Parallel()
	strategy := None()

	for i := 0; i < 10; i++ {
		delay := strategy.NextDelay(i)
		if delay != 0 {
			t.Errorf("attempt %d: expected 0, got %v", i, delay)
		}
	}
}

func TestFixed(t *testing.T) {
	t.Parallel()
	strategy := Fixed(500 * time.Millisecond)

	for i := 0; i < 10; i++ {
		delay := strategy.NextDelay(i)
		if delay != 500*time.Millisecond {
			t.Errorf("attempt %d: expected 500ms, got %v", i, delay)
		}
	}
}

func TestExponential_CustomMultiplier(t *testing.T) {
	t.Parallel()
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 3.0,
		Max:        0,
		Jitter:     0,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 300 * time.Millisecond},
		{2, 900 * time.Millisecond},
		{3, 2700 * time.Millisecond},
	}

	for _, tt := range tests {
		delay := strategy.NextDelay(tt.attempt)
		if delay != tt.expected {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expected, delay)
		}
	}
}

func BenchmarkConstant(b *testing.B) {
	strategy := &Constant{Delay: time.Second}
	for i := 0; i < b.N; i++ {
		strategy.NextDelay(i % 10)
	}
}

func BenchmarkLinear(b *testing.B) {
	strategy := &Linear{
		Initial: 100 * time.Millisecond,
		Step:    100 * time.Millisecond,
		Max:     time.Second,
	}
	for i := 0; i < b.N; i++ {
		strategy.NextDelay(i % 10)
	}
}

func BenchmarkExponential(b *testing.B) {
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        30 * time.Second,
		Jitter:     0,
	}
	for i := 0; i < b.N; i++ {
		strategy.NextDelay(i % 10)
	}
}

func BenchmarkExponentialWithJitter(b *testing.B) {
	strategy := &Exponential{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        30 * time.Second,
		Jitter:     0.1,
	}
	for i := 0; i < b.N; i++ {
		strategy.NextDelay(i % 10)
	}
}

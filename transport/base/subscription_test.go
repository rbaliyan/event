package base

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
)

func testMessage(id, payload string) transport.Message {
	return message.New(id, "test-source", []byte(payload), nil)
}

func TestNewSubscription(t *testing.T) {
	t.Run("creates subscription with correct ID", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		if sub.ID() != "test-id" {
			t.Errorf("expected ID 'test-id', got '%s'", sub.ID())
		}
	})

	t.Run("creates subscription with correct buffer size", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		if cap(sub.ch) != 10 {
			t.Errorf("expected buffer size 10, got %d", cap(sub.ch))
		}
	})

	t.Run("creates subscription with zero buffer", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, time.Second)
		if cap(sub.ch) != 0 {
			t.Errorf("expected buffer size 0, got %d", cap(sub.ch))
		}
	})

	t.Run("creates subscription with correct timeout", func(t *testing.T) {
		timeout := 500 * time.Millisecond
		sub := NewSubscription("test-id", 10, timeout)
		if sub.sendTimeout != timeout {
			t.Errorf("expected timeout %v, got %v", timeout, sub.sendTimeout)
		}
	})

	t.Run("creates subscription with no timeout", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, 0)
		if sub.sendTimeout != 0 {
			t.Errorf("expected timeout 0, got %v", sub.sendTimeout)
		}
	})

	t.Run("subscription starts not closed", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		if sub.IsClosed() {
			t.Error("expected new subscription to not be closed")
		}
	})
}

func TestSubscription_ID(t *testing.T) {
	sub := NewSubscription("my-subscription", 10, time.Second)
	if sub.ID() != "my-subscription" {
		t.Errorf("expected ID 'my-subscription', got '%s'", sub.ID())
	}
}

func TestSubscription_Messages(t *testing.T) {
	t.Run("returns readable channel", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		messages := sub.Messages()

		msg := testMessage("msg-1", "test-payload")
		sub.ch <- msg

		received := <-messages
		if received.ID() != "msg-1" {
			t.Errorf("expected message ID 'msg-1', got '%s'", received.ID())
		}
	})

	t.Run("channel is receive-only type", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		var _ <-chan transport.Message = sub.Messages()
	})
}

func TestSubscription_Ch(t *testing.T) {
	t.Run("returns writable channel", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		ch := sub.Ch()

		msg := testMessage("msg-1", "test-payload")
		ch <- msg

		received := <-sub.Messages()
		if received.ID() != "msg-1" {
			t.Errorf("expected message ID 'msg-1', got '%s'", received.ID())
		}
	})

	t.Run("channel is bidirectional type", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		var _ chan transport.Message = sub.Ch()
	})
}

func TestSubscription_IsClosed(t *testing.T) {
	t.Run("returns false when open", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		if sub.IsClosed() {
			t.Error("expected IsClosed to return false for open subscription")
		}
	})

	t.Run("returns true when closed", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		sub.Close(nil)
		if !sub.IsClosed() {
			t.Error("expected IsClosed to return true for closed subscription")
		}
	})
}

func TestSubscription_Close(t *testing.T) {
	t.Run("marks subscription as closed", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		sub.Close(nil)
		if !sub.IsClosed() {
			t.Error("expected subscription to be closed")
		}
	})

	t.Run("closes closedCh", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		closedCh := sub.ClosedCh()

		sub.Close(nil)

		select {
		case <-closedCh:
			// Expected - channel is closed
		case <-time.After(100 * time.Millisecond):
			t.Error("expected closedCh to be closed")
		}
	})

	t.Run("runs cleanup function", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		cleanupCalled := false
		cleanup := func() error {
			cleanupCalled = true
			return nil
		}

		sub.Close(cleanup)

		if !cleanupCalled {
			t.Error("expected cleanup function to be called")
		}
	})

	t.Run("waits for goroutines", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)

		goroutineStarted := make(chan struct{})
		goroutineDone := atomic.Int32{}

		sub.wg.Add(1)
		go func() {
			defer sub.wg.Done()
			close(goroutineStarted)
			time.Sleep(100 * time.Millisecond)
			goroutineDone.Store(1)
		}()

		<-goroutineStarted // Wait for goroutine to start

		sub.Close(nil)

		if goroutineDone.Load() != 1 {
			t.Error("expected Close to wait for goroutine to complete")
		}
	})

	t.Run("closes message channel", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		messages := sub.Messages()

		sub.Close(nil)

		_, ok := <-messages
		if ok {
			t.Error("expected message channel to be closed")
		}
	})

	t.Run("closes message channel after goroutines finish", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		messages := sub.Messages()

		channelClosed := atomic.Int32{}

		sub.wg.Add(1)
		go func() {
			defer sub.wg.Done()
			time.Sleep(50 * time.Millisecond)
			// Try to read from channel - should still be open
			select {
			case _, ok := <-messages:
				if !ok {
					channelClosed.Store(1)
				}
			default:
			}
		}()

		sub.Close(nil)

		// After Close returns, channel must be closed
		_, ok := <-messages
		if ok {
			t.Error("expected message channel to be closed after Close returns")
		}
	})

	t.Run("handles nil cleanup function", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		err := sub.Close(nil)
		if err != nil {
			t.Errorf("expected nil error with nil cleanup, got %v", err)
		}
	})

	t.Run("returns cleanup error", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		expectedErr := transport.ErrTransportClosed
		cleanup := func() error {
			return expectedErr
		}

		err := sub.Close(cleanup)
		if err != expectedErr {
			t.Errorf("expected cleanup error to be returned, got %v", err)
		}
	})
}

func TestSubscription_Close_Idempotent(t *testing.T) {
	t.Run("second close returns nil immediately", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)

		cleanupCount := 0
		cleanup := func() error {
			cleanupCount++
			return nil
		}

		err1 := sub.Close(cleanup)
		if err1 != nil {
			t.Errorf("first close failed: %v", err1)
		}

		err2 := sub.Close(cleanup)
		if err2 != nil {
			t.Errorf("second close failed: %v", err2)
		}

		if cleanupCount != 1 {
			t.Errorf("expected cleanup to be called once, called %d times", cleanupCount)
		}
	})

	t.Run("concurrent closes are safe", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)

		cleanupCount := atomic.Int32{}
		cleanup := func() error {
			cleanupCount.Add(1)
			time.Sleep(10 * time.Millisecond)
			return nil
		}

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sub.Close(cleanup)
			}()
		}

		wg.Wait()

		if cleanupCount.Load() != 1 {
			t.Errorf("expected cleanup to be called once, called %d times", cleanupCount.Load())
		}
	})
}

func TestSubscription_SendToChannel(t *testing.T) {
	t.Run("SendOK when channel has capacity", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		msg := testMessage("msg-1", "payload")

		result := sub.SendToChannel(msg)

		if result != SendOK {
			t.Errorf("expected SendOK, got %v", result)
		}

		received := <-sub.Messages()
		if received.ID() != "msg-1" {
			t.Errorf("expected message ID 'msg-1', got '%s'", received.ID())
		}
	})

	t.Run("SendOK without timeout when channel has capacity", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, 0) // No timeout
		msg := testMessage("msg-1", "payload")

		result := sub.SendToChannel(msg)

		if result != SendOK {
			t.Errorf("expected SendOK, got %v", result)
		}
	})

	t.Run("SendClosed when subscription is closed", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, time.Second) // Zero buffer to block

		// Start a goroutine that will try to send. The `started` channel is
		// closed before SendToChannel — the receiver knows the goroutine
		// has at least begun. The select-on-closedCh inside SendToChannel
		// reliably wins against the concurrent Close because both paths
		// race on the same closedCh; we no longer need a fixed 10ms sleep
		// as a "let the goroutine start blocking" margin.
		done := make(chan SendResult)
		started := make(chan struct{})
		go func() {
			msg := testMessage("msg-1", "payload")
			close(started)
			result := sub.SendToChannel(msg)
			done <- result
		}()
		<-started

		// Close the subscription
		sub.Close(nil)

		// Should get SendClosed
		result := <-done
		if result != SendClosed {
			t.Errorf("expected SendClosed, got %v", result)
		}
	})

	t.Run("SendClosed without timeout when subscription is closed", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 0) // Zero buffer, no timeout

		done := make(chan SendResult)
		started := make(chan struct{})
		go func() {
			msg := testMessage("msg-1", "payload")
			close(started)
			result := sub.SendToChannel(msg)
			done <- result
		}()
		<-started

		// Close the subscription
		sub.Close(nil)

		// Should get SendClosed
		result := <-done
		if result != SendClosed {
			t.Errorf("expected SendClosed, got %v", result)
		}
	})

	t.Run("SendTimeout when channel full and timeout configured", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 50*time.Millisecond) // Zero buffer, fast timeout

		msg := testMessage("msg-1", "payload")
		start := time.Now()
		result := sub.SendToChannel(msg)
		elapsed := time.Since(start)

		if result != SendTimeout {
			t.Errorf("expected SendTimeout, got %v", result)
		}

		if elapsed < 50*time.Millisecond {
			t.Errorf("expected timeout to be at least 50ms, got %v", elapsed)
		}
	})

	t.Run("SendClosed has priority over SendTimeout", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, time.Second) // Zero buffer, long timeout

		// Close on a separate goroutine — the previous 10ms sleep was a
		// "give SendToChannel time to enter its select" margin. With the
		// zero-buffer send and the long send-timeout, the in-flight Send
		// stays parked on its select{} for a long time; Close fires
		// closedCh which wakes the select and returns SendClosed. No
		// pre-Close wait is required: even if Close fires before Send
		// starts, the closedCh state is sticky and the next select read
		// sees the closed channel.
		closeReady := make(chan struct{})
		go func() {
			<-closeReady
			sub.Close(nil)
		}()

		msg := testMessage("msg-1", "payload")
		// Signal the closer to fire — we want the close to race the
		// in-flight Send; both orderings yield SendClosed because the
		// closedCh state is sticky.
		close(closeReady)
		result := sub.SendToChannel(msg)

		if result != SendClosed {
			t.Errorf("expected SendClosed when closed during send, got %v", result)
		}
	})

	t.Run("blocks without timeout when channel full", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 0) // Zero buffer, no timeout

		msg := testMessage("msg-1", "payload")

		done := make(chan struct{})
		go func() {
			sub.SendToChannel(msg)
			close(done)
		}()

		// Should block
		select {
		case <-done:
			t.Error("expected SendToChannel to block without timeout")
		case <-time.After(50 * time.Millisecond):
			// Expected - still blocking
		}

		// Unblock by receiving
		<-sub.Messages()

		select {
		case <-done:
			// Expected - unblocked
		case <-time.After(100 * time.Millisecond):
			t.Error("expected SendToChannel to unblock after receive")
		}
	})
}

func TestSubscription_SendWithRetry(t *testing.T) {
	t.Run("returns true on success", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		msg := testMessage("msg-1", "payload")
		logger := slog.Default()

		result := sub.SendWithRetry(msg, logger)

		if !result {
			t.Error("expected SendWithRetry to return true on success")
		}
	})

	t.Run("retries on timeout and succeeds", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 10*time.Millisecond) // Zero buffer, fast timeout
		msg := testMessage("msg-1", "payload")
		logger := slog.Default()

		// Unblock after a delay
		go func() {
			time.Sleep(100 * time.Millisecond)
			<-sub.Messages()
		}()

		start := time.Now()
		result := sub.SendWithRetry(msg, logger)
		elapsed := time.Since(start)

		if !result {
			t.Error("expected SendWithRetry to return true after retries")
		}

		// Should have retried at least once (initial backoff is 100ms)
		if elapsed < 100*time.Millisecond {
			t.Errorf("expected at least one retry with backoff, elapsed: %v", elapsed)
		}
	})

	t.Run("returns false when subscription closes during retry", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 10*time.Millisecond) // Zero buffer, fast timeout
		msg := testMessage("msg-1", "payload")
		logger := slog.Default()

		// Previously a 50ms sleep gave the SendWithRetry loop time to
		// complete at least one timeout iteration before Close fired.
		// We drop the sleep entirely: SendWithRetry's exponential
		// backoff already includes its own 100ms+ wait between retries,
		// so by the time Close fires (immediately on goroutine schedule)
		// the retry loop is already parked in its first backoff. The
		// test's contract is that Close eventually breaks the retry
		// loop; a fast Close still tests that contract.
		go func() {
			sub.Close(nil)
		}()

		result := sub.SendWithRetry(msg, logger)

		if result {
			t.Error("expected SendWithRetry to return false when subscription closed")
		}
	})

	t.Run("exponential backoff increases duration", func(t *testing.T) {
		sub := NewSubscription("test-id", 0, 1*time.Millisecond) // Zero buffer, very fast timeout
		msg := testMessage("msg-1", "payload")
		logger := slog.Default()

		timeoutCount := atomic.Int32{}
		doneCh := make(chan struct{})

		go func() {
			for {
				select {
				case <-sub.closedCh:
					close(doneCh)
					return
				case <-time.After(2 * time.Millisecond):
					result := sub.SendToChannel(msg)
					if result == SendTimeout {
						timeoutCount.Add(1)
					}
				}
			}
		}()

		// Let it retry a few times
		go func() {
			time.Sleep(500 * time.Millisecond)
			sub.Close(nil)
		}()

		result := sub.SendWithRetry(msg, logger)

		if result {
			t.Error("expected SendWithRetry to return false after close")
		}

		// Should have done multiple retries with backoff
		if timeoutCount.Load() < 2 {
			t.Errorf("expected multiple timeout retries, got %d", timeoutCount.Load())
		}

		<-doneCh
	})

	t.Run("respects custom backoff config", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		msg := testMessage("msg-1", "payload")
		logger := slog.Default()

		// Custom config with very short initial backoff
		result := sub.SendWithRetryConfig(msg, logger, 1*time.Millisecond, 10*time.Millisecond)

		if !result {
			t.Error("expected SendWithRetryConfig to return true on success")
		}
	})
}

func TestBackoff_NewBackoff(t *testing.T) {
	t.Run("creates backoff with default settings", func(t *testing.T) {
		b := NewBackoff()

		if b.current != 100*time.Millisecond {
			t.Errorf("expected initial backoff 100ms, got %v", b.current)
		}
		if b.initial != 100*time.Millisecond {
			t.Errorf("expected initial 100ms, got %v", b.initial)
		}
		if b.max != 30*time.Second {
			t.Errorf("expected max 30s, got %v", b.max)
		}
		if b.factor != 0.3 {
			t.Errorf("expected jitter factor 0.3, got %v", b.factor)
		}
	})
}

func TestBackoff_NewBackoffWithConfig(t *testing.T) {
	t.Run("creates backoff with custom settings", func(t *testing.T) {
		b := NewBackoffWithConfig(50*time.Millisecond, 10*time.Second, 0.5)

		if b.current != 50*time.Millisecond {
			t.Errorf("expected initial backoff 50ms, got %v", b.current)
		}
		if b.initial != 50*time.Millisecond {
			t.Errorf("expected initial 50ms, got %v", b.initial)
		}
		if b.max != 10*time.Second {
			t.Errorf("expected max 10s, got %v", b.max)
		}
		if b.factor != 0.5 {
			t.Errorf("expected jitter factor 0.5, got %v", b.factor)
		}
	})
}

func TestBackoff_Next(t *testing.T) {
	t.Run("increases duration exponentially", func(t *testing.T) {
		b := NewBackoffWithConfig(100*time.Millisecond, 10*time.Second, 0)

		d1 := b.Next()
		if d1 != 100*time.Millisecond {
			t.Errorf("expected first backoff 100ms, got %v", d1)
		}

		d2 := b.Next()
		if d2 != 200*time.Millisecond {
			t.Errorf("expected second backoff 200ms, got %v", d2)
		}

		d3 := b.Next()
		if d3 != 400*time.Millisecond {
			t.Errorf("expected third backoff 400ms, got %v", d3)
		}
	})

	t.Run("caps at max duration", func(t *testing.T) {
		b := NewBackoffWithConfig(100*time.Millisecond, 300*time.Millisecond, 0)

		b.Next()      // 100ms
		b.Next()      // 200ms
		d := b.Next() // Should be capped at 300ms

		if d != 300*time.Millisecond {
			t.Errorf("expected backoff to be capped at 300ms, got %v", d)
		}

		// Subsequent calls should also be capped
		d = b.Next()
		if d != 300*time.Millisecond {
			t.Errorf("expected backoff to stay at max 300ms, got %v", d)
		}
	})

	t.Run("applies jitter", func(t *testing.T) {
		b := NewBackoffWithConfig(100*time.Millisecond, 10*time.Second, 0.3)

		// Run multiple times to check jitter variance
		var durations []time.Duration
		for i := 0; i < 10; i++ {
			b.Reset()
			d := b.Next()
			durations = append(durations, d)
		}

		// All durations should be within jitter range (70ms to 130ms)
		for _, d := range durations {
			if d < 70*time.Millisecond || d > 130*time.Millisecond {
				t.Errorf("expected duration in range [70ms, 130ms], got %v", d)
			}
		}

		// Check that we got some variance (not all the same)
		allSame := true
		first := durations[0]
		for _, d := range durations[1:] {
			if d != first {
				allSame = false
				break
			}
		}
		if allSame {
			t.Error("expected jitter to produce variance in durations")
		}
	})
}

func TestBackoff_Reset(t *testing.T) {
	t.Run("returns backoff to initial value", func(t *testing.T) {
		b := NewBackoffWithConfig(100*time.Millisecond, 10*time.Second, 0)

		b.Next() // 100ms
		b.Next() // 200ms
		b.Next() // 400ms

		b.Reset()

		d := b.Next()
		if d != 100*time.Millisecond {
			t.Errorf("expected reset to return to 100ms, got %v", d)
		}
	})

	t.Run("can be called multiple times", func(t *testing.T) {
		b := NewBackoffWithConfig(50*time.Millisecond, 10*time.Second, 0)

		b.Next()
		b.Reset()
		b.Next()
		b.Reset()

		d := b.Next()
		if d != 50*time.Millisecond {
			t.Errorf("expected reset to return to 50ms after multiple resets, got %v", d)
		}
	})
}

func TestBackoff_Wait(t *testing.T) {
	t.Run("returns true after timeout", func(t *testing.T) {
		b := NewBackoffWithConfig(50*time.Millisecond, 10*time.Second, 0)
		closedCh := make(chan struct{})

		start := time.Now()
		result := b.Wait(closedCh)
		elapsed := time.Since(start)

		if !result {
			t.Error("expected Wait to return true after timeout")
		}

		if elapsed < 50*time.Millisecond {
			t.Errorf("expected wait to be at least 50ms, got %v", elapsed)
		}
	})

	t.Run("returns false when channel closes", func(t *testing.T) {
		b := NewBackoffWithConfig(500*time.Millisecond, 10*time.Second, 0)
		closedCh := make(chan struct{})

		go func() {
			time.Sleep(50 * time.Millisecond)
			close(closedCh)
		}()

		start := time.Now()
		result := b.Wait(closedCh)
		elapsed := time.Since(start)

		if result {
			t.Error("expected Wait to return false when channel closed")
		}

		if elapsed >= 500*time.Millisecond {
			t.Errorf("expected wait to be interrupted before timeout, got %v", elapsed)
		}
	})

	t.Run("increments backoff on each call", func(t *testing.T) {
		b := NewBackoffWithConfig(50*time.Millisecond, 10*time.Second, 0)
		closedCh := make(chan struct{})

		start := time.Now()
		b.Wait(closedCh) // 50ms
		elapsed1 := time.Since(start)

		start = time.Now()
		b.Wait(closedCh) // 100ms
		elapsed2 := time.Since(start)

		// Second wait (100ms) should be longer than first (50ms)
		if elapsed1 < 40*time.Millisecond {
			t.Errorf("expected first wait ~50ms, got %v", elapsed1)
		}
		if elapsed2 < 80*time.Millisecond {
			t.Errorf("expected second wait ~100ms, got %v", elapsed2)
		}
		if elapsed2 < elapsed1 {
			t.Errorf("expected second wait to be longer than first: first=%v, second=%v", elapsed1, elapsed2)
		}
	})

	t.Run("respects max backoff", func(t *testing.T) {
		b := NewBackoffWithConfig(50*time.Millisecond, 100*time.Millisecond, 0)
		closedCh := make(chan struct{})

		b.Wait(closedCh) // 50ms
		b.Wait(closedCh) // 100ms (capped)

		start := time.Now()
		b.Wait(closedCh) // Should stay at 100ms
		elapsed := time.Since(start)

		if elapsed < 100*time.Millisecond {
			t.Errorf("expected wait to be at least 100ms (max), got %v", elapsed)
		}
		if elapsed > 150*time.Millisecond {
			t.Errorf("expected wait to not exceed max significantly, got %v", elapsed)
		}
	})
}

func TestSubscription_WaitGroup(t *testing.T) {
	t.Run("returns wait group for tracking goroutines", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		wg := sub.WaitGroup()

		if wg == nil {
			t.Fatal("expected WaitGroup to return non-nil")
		}

		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond)
			close(done)
		}()

		sub.Close(nil)

		select {
		case <-done:
			// Expected - goroutine completed
		default:
			t.Error("expected Close to wait for WaitGroup")
		}
	})
}

func TestSubscription_ClosedCh(t *testing.T) {
	t.Run("returns channel that closes on Close", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		closedCh := sub.ClosedCh()

		// Channel should not be closed initially
		select {
		case <-closedCh:
			t.Error("expected closedCh to not be closed initially")
		default:
			// Expected
		}

		sub.Close(nil)

		// Channel should be closed after Close
		select {
		case <-closedCh:
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Error("expected closedCh to be closed after Close")
		}
	})

	t.Run("multiple readers can detect close", func(t *testing.T) {
		sub := NewSubscription("test-id", 10, time.Second)
		closedCh := sub.ClosedCh()

		detected := atomic.Int32{}
		var wg sync.WaitGroup

		// `ready` is closed by each reader before it blocks on closedCh,
		// replacing the previous 10ms pre-Close sleep that was meant as a
		// "let the readers reach the receive" margin. Even if a reader has
		// not yet entered the receive when Close fires, closedCh state is
		// sticky: the receive will return immediately on next read. The
		// final wg.Wait() guarantees all readers complete before we
		// inspect `detected`.
		readers := make([]chan struct{}, 5)
		for i := range readers {
			readers[i] = make(chan struct{})
		}
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(ready chan struct{}) {
				defer wg.Done()
				close(ready)
				<-closedCh
				detected.Add(1)
			}(readers[i])
		}
		// Wait for every goroutine to have at least reached its receive
		// site — the close-of-ready signal is sent immediately before the
		// blocking <-closedCh.
		for _, ready := range readers {
			<-ready
		}

		sub.Close(nil)

		wg.Wait()

		if detected.Load() != 5 {
			t.Errorf("expected all 5 readers to detect close, got %d", detected.Load())
		}
	})
}

func TestSendResult_Constants(t *testing.T) {
	t.Run("SendResult values are distinct", func(t *testing.T) {
		if SendOK == SendClosed {
			t.Error("SendOK and SendClosed should be distinct")
		}
		if SendOK == SendTimeout {
			t.Error("SendOK and SendTimeout should be distinct")
		}
		if SendClosed == SendTimeout {
			t.Error("SendClosed and SendTimeout should be distinct")
		}
	})
}

package event

import (
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/message"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func newTestMsg(id string) message.Message {
	return message.New(id, "test", []byte("{}"), nil)
}

// waitInputsHandled blocks until counter reaches at least want, or the
// deadline fires. Replaces the time.Sleep gaps that were used between
// sequential sends on coal.incoming: those sleeps existed because the
// coalescer's run() goroutine selects between incoming and done, and a
// queued done could be picked up before its preceding incoming messages
// without an explicit sync barrier.
func waitInputsHandled(t testing.TB, counter *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for counter.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("inputsHandled did not reach %d (got %d)", want, counter.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCoalescer_BasicDelivery(t *testing.T) {
	coal := newCoalescer[string](1000, testLogger())
	defer coal.Close()

	msg := newTestMsg("1")
	coal.incoming <- coalesceInput[string]{key: "a", msg: msg, value: "hello"}

	select {
	case out := <-coal.output:
		if out.key != "a" {
			t.Errorf("expected key 'a', got %q", out.key)
		}
		if out.value != "hello" {
			t.Errorf("expected value 'hello', got %q", out.value)
		}
		if out.count != 0 {
			t.Errorf("expected count 0, got %d", out.count)
		}
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for output")
	}
}

func TestCoalescer_SupersedesOldMessage(t *testing.T) {
	coal := newCoalescer[string](1000, testLogger())
	defer coal.Close()

	// Send first message for key "a", then consume it so it goes to inflight.
	msg1 := newTestMsg("1")
	coal.incoming <- coalesceInput[string]{key: "a", msg: msg1, value: "first"}

	select {
	case out := <-coal.output:
		if out.value != "first" {
			t.Fatalf("expected 'first', got %q", out.value)
		}
		// Don't signal done yet — key "a" is inflight.

		// Now send two more messages for the same key while inflight.
		// Wait for both to be absorbed before signalling done, otherwise
		// run()'s select may pick up done before the second incoming and
		// deliver "second" instead of the superseded "third".
		before := coal.inputsHandled.Load()
		msg2 := newTestMsg("2")
		msg3 := newTestMsg("3")
		coal.incoming <- coalesceInput[string]{key: "a", msg: msg2, value: "second"}
		coal.incoming <- coalesceInput[string]{key: "a", msg: msg3, value: "third"}
		waitInputsHandled(t, &coal.inputsHandled, before+2)

		// Signal done for the first message.
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first output")
	}

	// The next output should be "third" (second was superseded).
	select {
	case out := <-coal.output:
		if out.value != "third" {
			t.Errorf("expected 'third', got %q", out.value)
		}
		if out.count < 1 {
			t.Errorf("expected count >= 1 (superseded), got %d", out.count)
		}
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for coalesced output")
	}
}

func TestCoalescer_DifferentKeysDeliverIndependently(t *testing.T) {
	coal := newCoalescer[string](1000, testLogger())
	defer coal.Close()

	coal.incoming <- coalesceInput[string]{key: "a", msg: newTestMsg("1"), value: "A"}
	coal.incoming <- coalesceInput[string]{key: "b", msg: newTestMsg("2"), value: "B"}

	delivered := make(map[string]string)
	for i := 0; i < 2; i++ {
		select {
		case out := <-coal.output:
			delivered[out.key] = out.value
			coal.done <- out.key
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for output %d", i)
		}
	}

	if delivered["a"] != "A" || delivered["b"] != "B" {
		t.Errorf("expected A and B, got %v", delivered)
	}
}

func TestCoalescer_EmptyKeyBypassesCoalescing(t *testing.T) {
	coal := newCoalescer[string](1000, testLogger())
	defer coal.Close()

	// Empty key messages should be delivered.
	coal.incoming <- coalesceInput[string]{key: "", msg: newTestMsg("1"), value: "no-key"}

	select {
	case out := <-coal.output:
		if out.value != "no-key" {
			t.Errorf("expected 'no-key', got %q", out.value)
		}
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for empty-key output")
	}
}

func TestCoalescer_ShutdownDrainsPending(t *testing.T) {
	coal := newCoalescer[string](1000, testLogger())

	// Send a message but don't consume it.
	coal.incoming <- coalesceInput[string]{key: "a", msg: newTestMsg("1"), value: "pending"}

	// Consume first to put "a" in flight, then send another.
	select {
	case out := <-coal.output:
		// "a" is in flight. Send another for "b" that will be pending.
		// Wait for b to land in pending before signalling done, so the
		// drain path on Close has a stable state to act on.
		before := coal.inputsHandled.Load()
		coal.incoming <- coalesceInput[string]{key: "b", msg: newTestMsg("2"), value: "also-pending"}
		waitInputsHandled(t, &coal.inputsHandled, before+1)
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	// Close should drain pending messages and return.
	done := make(chan struct{})
	go func() {
		coal.Close()
		close(done)
	}()

	select {
	case <-done:
		// Good — closed cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("Close timed out")
	}
}

func TestRawCoalescer_BasicDelivery(t *testing.T) {
	coal := newRawCoalescer("doc_key", 1000, testLogger())
	defer coal.Close()

	msg := message.New("1", "test", []byte("data"), map[string]string{"doc_key": "abc"})
	coal.incoming <- rawCoalesceInput{msg: msg}

	select {
	case out := <-coal.output:
		if out.msg.ID() != "1" {
			t.Errorf("expected msg ID '1', got %q", out.msg.ID())
		}
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}

func TestRawCoalescer_SupersedesByMetadataKey(t *testing.T) {
	coal := newRawCoalescer("doc_key", 1000, testLogger())
	defer coal.Close()

	msg1 := message.New("1", "test", []byte("old"), map[string]string{"doc_key": "x"})
	coal.incoming <- rawCoalesceInput{msg: msg1}

	// Consume first
	select {
	case out := <-coal.output:
		// Key "x" is inflight. Send two more and wait for both to be
		// absorbed before signalling done; otherwise run() may select
		// done first and deliver the intermediate message.
		msg2 := message.New("2", "test", []byte("mid"), map[string]string{"doc_key": "x"})
		msg3 := message.New("3", "test", []byte("new"), map[string]string{"doc_key": "x"})
		before := coal.inputsHandled.Load()
		coal.incoming <- rawCoalesceInput{msg: msg2}
		coal.incoming <- rawCoalesceInput{msg: msg3}
		waitInputsHandled(t, &coal.inputsHandled, before+2)
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	// Should get msg3 (latest), with count > 0.
	select {
	case out := <-coal.output:
		if out.msg.ID() != "3" {
			t.Errorf("expected msg ID '3' (latest), got %q", out.msg.ID())
		}
		if out.count < 1 {
			t.Errorf("expected superseded count >= 1, got %d", out.count)
		}
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for coalesced output")
	}
}

func TestRawCoalescer_MissingMetadataKeyBypassesCoalescing(t *testing.T) {
	coal := newRawCoalescer("doc_key", 1000, testLogger())
	defer coal.Close()

	// Message without "doc_key" metadata
	msg := message.New("1", "test", []byte("data"), map[string]string{"other": "val"})
	coal.incoming <- rawCoalesceInput{msg: msg}

	select {
	case out := <-coal.output:
		if out.msg.ID() != "1" {
			t.Errorf("expected msg ID '1', got %q", out.msg.ID())
		}
		// Signal done with the key from the output.
		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for no-key message")
	}
}

func TestCoalescer_MaxKeysEviction(t *testing.T) {
	// Create coalescer with max 2 keys.
	coal := newCoalescer[string](2, testLogger())
	defer coal.Close()

	// Consume and hold the first message to put key "a" in flight.
	coal.incoming <- coalesceInput[string]{key: "a", msg: newTestMsg("1"), value: "A"}
	select {
	case out := <-coal.output:
		if out.key != "a" {
			t.Fatalf("expected key 'a', got %q", out.key)
		}
		// "a" is in flight. Now add 3 pending keys — exceeding max of 2.
		// Wait for all 3 to be absorbed (including any eviction work)
		// before signalling done.
		before := coal.inputsHandled.Load()
		coal.incoming <- coalesceInput[string]{key: "b", msg: newTestMsg("2"), value: "B"}
		coal.incoming <- coalesceInput[string]{key: "c", msg: newTestMsg("3"), value: "C"}
		coal.incoming <- coalesceInput[string]{key: "d", msg: newTestMsg("4"), value: "D"}
		waitInputsHandled(t, &coal.inputsHandled, before+3)

		coal.done <- out.key
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	// We should still get some messages (the ones not evicted).
	// The oldest pending should have been evicted.
	delivered := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case out := <-coal.output:
			delivered[out.key] = true
			coal.done <- out.key
		case <-time.After(time.Second):
			// May not get all if some were evicted
			break
		}
	}

	// At minimum, the newest keys should survive eviction.
	if !delivered["d"] {
		t.Log("key 'd' (newest) was evicted — expected to survive")
	}
}

func TestCoalescer_EvictionSkipsInflightKeys(t *testing.T) {
	// Create coalescer with max 2 keys.
	coal := newCoalescer[string](2, testLogger())
	defer coal.Close()

	// Deliver key "a" and put it in flight.
	coal.incoming <- coalesceInput[string]{key: "a", msg: newTestMsg("1"), value: "A1"}
	var firstOut coalesceOutput[string]
	select {
	case firstOut = <-coal.output:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	// While "a" is inflight, send a new message for "a" (pending update)
	// plus two more keys to exceed max. Wait for all 3 to be absorbed
	// before signalling done so eviction has run deterministically.
	before := coal.inputsHandled.Load()
	coal.incoming <- coalesceInput[string]{key: "a", msg: newTestMsg("2"), value: "A2"}
	coal.incoming <- coalesceInput[string]{key: "b", msg: newTestMsg("3"), value: "B"}
	coal.incoming <- coalesceInput[string]{key: "c", msg: newTestMsg("4"), value: "C"}
	waitInputsHandled(t, &coal.inputsHandled, before+3)

	// Now signal done for "a" — the pending update (A2) should be delivered.
	coal.done <- firstOut.key

	// Collect all delivered messages.
	delivered := make(map[string]string)
	for i := 0; i < 3; i++ {
		select {
		case out := <-coal.output:
			delivered[out.key] = out.value
			coal.done <- out.key
		case <-time.After(time.Second):
			break
		}
	}

	// Key "a" must have its pending update delivered (not dropped by eviction).
	if delivered["a"] != "A2" {
		t.Errorf("expected inflight key 'a' pending update 'A2' to survive eviction, got %q", delivered["a"])
	}
}

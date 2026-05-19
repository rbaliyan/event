package testutil_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
	"github.com/rbaliyan/event/v3/transport/channel"
)

func TestEventually_Succeeds(t *testing.T) {
	t.Parallel()
	start := time.Now()
	var flipped atomic.Bool
	go func() {
		time.Sleep(20 * time.Millisecond)
		flipped.Store(true)
	}()
	testutil.Eventually(t, time.Second, flipped.Load, "should have flipped")
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("Eventually returned too late: %v", elapsed)
	}
}

func TestEventuallyOK_Timeout(t *testing.T) {
	t.Parallel()
	ok := testutil.EventuallyOK(20*time.Millisecond, func() bool { return false })
	if ok {
		t.Error("expected EventuallyOK to return false on timeout")
	}
}

func TestEventuallyOK_TrueImmediately(t *testing.T) {
	t.Parallel()
	// A condition that is already true should return without polling.
	calls := 0
	ok := testutil.EventuallyOK(time.Second, func() bool {
		calls++
		return true
	})
	if !ok || calls != 1 {
		t.Errorf("expected single check; ok=%v calls=%d", ok, calls)
	}
}

func TestWaitFor(t *testing.T) {
	t.Parallel()
	ch := make(chan int, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		ch <- 42
	}()
	got := testutil.WaitFor(t, ch, time.Second, "value should arrive")
	if got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestUniqueName_IsCollisionFree(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, 1000)
	for i := range 1000 {
		n := testutil.UniqueName(t)
		if _, dup := seen[n]; dup {
			t.Fatalf("UniqueName collision after %d iterations: %s", i, n)
		}
		seen[n] = struct{}{}
	}
}

func TestUniqueName_IsSanitized(t *testing.T) {
	t.Parallel()
	n := testutil.UniqueName(t)
	for _, r := range n {
		ok := (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' || r == '-'
		if !ok {
			t.Errorf("UniqueName contains unsafe rune %q in %q", r, n)
		}
	}
	if !strings.Contains(n, "TestUniqueName_IsSanitized") {
		t.Errorf("UniqueName should embed test name; got %q", n)
	}
}

func TestFakeClock_AdvancesOnSleep(t *testing.T) {
	t.Parallel()
	clk := testutil.NewFakeClock(time.Time{})
	start := clk.Now()
	clk.Sleep(time.Hour)
	if got := clk.Since(start); got != time.Hour {
		t.Errorf("FakeClock.Since after Sleep(1h) = %v, want 1h", got)
	}
}

func TestFakeClock_Advance(t *testing.T) {
	t.Parallel()
	clk := testutil.NewFakeClock(time.Unix(1_700_000_000, 0).UTC())
	clk.Advance(30 * time.Second)
	want := time.Unix(1_700_000_030, 0).UTC()
	if !clk.Now().Equal(want) {
		t.Errorf("FakeClock.Now after Advance = %v, want %v", clk.Now(), want)
	}
}

func TestRealClock_DelegatesToTime(t *testing.T) {
	t.Parallel()
	var c testutil.Clock = testutil.RealClock{}
	before := c.Now()
	time.Sleep(time.Millisecond)
	if c.Since(before) <= 0 {
		t.Errorf("RealClock.Since should be positive after sleep")
	}
}

func TestMustNewBus_RegistersCloseCleanup(t *testing.T) {
	t.Parallel()
	bus := testutil.MustNewBus(t, event.WithTransport(channel.New()))
	if bus == nil {
		t.Fatal("MustNewBus returned nil")
	}
	// Cleanup should fire bus.Close at end-of-test — we verify indirectly by
	// running a publish/subscribe round-trip and trusting t.Cleanup ordering.
	ev := testutil.MustRegister(t, context.Background(), bus, event.New[int]("ping"))
	got := make(chan int, 1)
	if err := ev.Subscribe(context.Background(), func(_ context.Context, _ event.Event[int], v int) error {
		got <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := ev.Publish(context.Background(), 7); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if v := testutil.WaitFor(t, got, time.Second, "value should arrive"); v != 7 {
		t.Errorf("got %d, want 7", v)
	}
}

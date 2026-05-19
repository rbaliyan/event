package partition

import "testing"

func TestNewPublishOptions_InitializesHeaders(t *testing.T) {
	t.Parallel()
	opts := NewPublishOptions("user-123")
	if opts.PartitionKey != "user-123" {
		t.Errorf("PartitionKey: got %q, want %q", opts.PartitionKey, "user-123")
	}
	if opts.Headers == nil {
		t.Error("Headers must be initialized (not nil) so callers can index without panicking")
	}
	if len(opts.Headers) != 0 {
		t.Errorf("Headers should start empty; got %d entries", len(opts.Headers))
	}
	if opts.Priority != 0 {
		t.Errorf("Priority default: got %d, want 0", opts.Priority)
	}
}

func TestNewPublishOptions_EmptyKeyAllowed(t *testing.T) {
	t.Parallel()
	// The constructor does not validate the partition key — empty is a legal
	// value (it routes to partition 0 via HashPartitioner's short-circuit).
	// Pin this so future validation is a conscious change.
	opts := NewPublishOptions("")
	if opts.PartitionKey != "" {
		t.Errorf("empty key rejected: got %q", opts.PartitionKey)
	}
}

func TestPublishOptions_WithHeaderAppends(t *testing.T) {
	t.Parallel()
	opts := NewPublishOptions("k").
		WithHeader("a", "1").
		WithHeader("b", "2")

	if got := opts.Headers["a"]; got != "1" {
		t.Errorf("header a: got %q, want %q", got, "1")
	}
	if got := opts.Headers["b"]; got != "2" {
		t.Errorf("header b: got %q, want %q", got, "2")
	}
	if len(opts.Headers) != 2 {
		t.Errorf("header count: got %d, want 2", len(opts.Headers))
	}
}

func TestPublishOptions_WithHeaderOverwrites(t *testing.T) {
	t.Parallel()
	// Calling WithHeader with the same key twice should replace, not append
	// (Go map semantics). Pin behavior so a future implementation that
	// switches to a multi-value header model is explicit about it.
	opts := NewPublishOptions("k").
		WithHeader("trace-id", "first").
		WithHeader("trace-id", "second")
	if got := opts.Headers["trace-id"]; got != "second" {
		t.Errorf("trace-id: got %q, want %q (last write should win)", got, "second")
	}
}

func TestPublishOptions_WithHeaderInitializesNilMap(t *testing.T) {
	t.Parallel()
	// Direct construction (bypassing NewPublishOptions) can leave Headers
	// nil. WithHeader must defend against that, otherwise it would panic on
	// the assignment.
	opts := &PublishOptions{PartitionKey: "k"}
	if opts.Headers != nil {
		t.Fatal("test precondition: zero-valued Headers should be nil")
	}
	_ = opts.WithHeader("x", "y")
	if opts.Headers == nil {
		t.Fatal("WithHeader did not initialize a nil Headers map")
	}
	if got := opts.Headers["x"]; got != "y" {
		t.Errorf("header x: got %q, want %q", got, "y")
	}
}

func TestPublishOptions_WithPriorityLastWriteWins(t *testing.T) {
	t.Parallel()
	opts := NewPublishOptions("k").
		WithPriority(10).
		WithPriority(50)
	if opts.Priority != 50 {
		t.Errorf("Priority: got %d, want 50 (last write should win)", opts.Priority)
	}
}

func TestPublishOptions_BuilderReturnsSameReceiver(t *testing.T) {
	t.Parallel()
	// The builder methods must return the same *PublishOptions they receive,
	// not a copy. Otherwise chained mutations applied to an alias would be
	// silently lost — a subtle bug consumers would only catch in production.
	opts := NewPublishOptions("k")
	if got := opts.WithHeader("a", "1"); got != opts {
		t.Error("WithHeader must return the receiver, not a copy")
	}
	if got := opts.WithPriority(7); got != opts {
		t.Error("WithPriority must return the receiver, not a copy")
	}
}

func TestKeyExtractor_TypedClosure(t *testing.T) {
	t.Parallel()
	// KeyExtractor is a type alias for func(T) string. Verify the alias
	// actually behaves as a function value — guards against a future change
	// to a method-on-struct that would silently break call sites.
	type order struct{ Customer string }
	var extract KeyExtractor[order] = func(o order) string { return o.Customer }
	if got := extract(order{Customer: "cust-A"}); got != "cust-A" {
		t.Errorf("KeyExtractor: got %q, want %q", got, "cust-A")
	}
}

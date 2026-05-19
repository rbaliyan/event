package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/rbaliyan/event/v3/transport"
)

// These tests exercise constructor validation, the topic-name format, and the
// closed-transport sentinel paths. Pub/sub round-trips against a real broker
// live in smoke_test.go (//go:build smoke, env-gated by KAFKA_BROKERS).

func TestNew_NilClientReturnsErrClientRequired(t *testing.T) {
	t.Parallel()
	if _, err := New(nil); !errors.Is(err, ErrClientRequired) {
		t.Errorf("New(nil): got %v, want ErrClientRequired", err)
	}
}

func TestNew_AutoCommitEnabledRejected(t *testing.T) {
	t.Parallel()
	// At-least-once delivery requires explicit offset commits via
	// session.MarkMessage. If sarama's Consumer.Offsets.AutoCommit.Enable
	// stays true (the sarama default), messages can be lost when handlers
	// fail — the offset advances regardless of ack. New() must guard.
	client := newMockClient(t, true)
	_, err := New(client)
	if !errors.Is(err, ErrAutoCommitEnabled) {
		t.Errorf("New with AutoCommit=true: got %v, want ErrAutoCommitEnabled", err)
	}
}

func TestNew_AutoCommitDisabledSucceeds(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })
	if tr == nil {
		t.Fatal("New returned nil transport without error")
	}
}

func TestNew_DefaultsApplied(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	if tr.groupID != DefaultBusName {
		t.Errorf("default groupID: got %q, want %q", tr.groupID, DefaultBusName)
	}
	if tr.partitions != DefaultPartitions {
		t.Errorf("default partitions: got %d, want %d", tr.partitions, DefaultPartitions)
	}
	if tr.replication != DefaultReplication {
		t.Errorf("default replication: got %d, want %d", tr.replication, DefaultReplication)
	}
	if tr.topicPrefix != topicPrefix {
		t.Errorf("topicPrefix: got %q, want %q", tr.topicPrefix, topicPrefix)
	}
}

func TestTransport_TopicNamePinned(t *testing.T) {
	t.Parallel()
	// "evt." + event name is part of the operator-visible surface (used in
	// kafka-cli topic listing, monitoring dashboards). Pin so a refactor
	// that changes the format is a deliberate, visible decision.
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	cases := map[string]string{
		"order.created": "evt.order.created",
		"foo":           "evt.foo",
		"":              "evt.",
	}
	for in, want := range cases {
		if got := tr.topicName(in); got != want {
			t.Errorf("topicName(%q): got %q, want %q", in, got, want)
		}
	}
}

func TestTransport_NameAndRedeliveryContract(t *testing.T) {
	t.Parallel()
	// Name and SupportsRedelivery are public observability contracts —
	// metric labels, log attributes, reliability-stack decisions key off
	// them. Pin both.
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	if got := tr.Name(); got != "kafka" {
		t.Errorf("Name: got %q, want %q", got, "kafka")
	}
	if !tr.SupportsRedelivery() {
		t.Error("SupportsRedelivery: got false, want true (Kafka commits offsets, so unacked messages redeliver)")
	}
}

func TestTransport_ClosedTransportRejectsOperations(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	ctx := context.Background()
	if err := tr.RegisterEvent(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("RegisterEvent on closed: got %v, want ErrTransportClosed", err)
	}
	if err := tr.UnregisterEvent(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("UnregisterEvent on closed: got %v, want ErrTransportClosed", err)
	}
	msg := transport.NewMessageWithAck("id", "src", []byte("p"), nil, 0, nil)
	if err := tr.Publish(ctx, "evt", msg); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Publish on closed: got %v, want ErrTransportClosed", err)
	}
	if _, err := tr.Subscribe(ctx, "evt"); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("Subscribe on closed: got %v, want ErrTransportClosed", err)
	}
	if _, err := tr.ConsumerLag(ctx); !errors.Is(err, transport.ErrTransportClosed) {
		t.Errorf("ConsumerLag on closed: got %v, want ErrTransportClosed", err)
	}
}

func TestTransport_CloseIdempotent(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tr.Close(context.Background()); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := tr.Close(context.Background()); err != nil {
		t.Errorf("second Close should be a no-op; got %v", err)
	}
}

func TestTransport_UnregisterUnknownEventRejected(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	if err := tr.UnregisterEvent(context.Background(), "never-registered"); !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("UnregisterEvent of unknown: got %v, want ErrEventNotRegistered", err)
	}
}

func TestTransport_PublishUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	msg := transport.NewMessageWithAck("id", "src", []byte("p"), nil, 0, nil)
	err = tr.Publish(context.Background(), "unknown.event", msg)
	if !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Publish of unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestTransport_SubscribeUnregisteredReturnsSentinel(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	if _, err := tr.Subscribe(context.Background(), "unknown.event"); !errors.Is(err, transport.ErrEventNotRegistered) {
		t.Errorf("Subscribe of unknown event: got %v, want ErrEventNotRegistered", err)
	}
}

func TestTransport_Health_UnhealthyWhenTransportClosed(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = tr.Close(context.Background())

	got := tr.Health(context.Background())
	if got.Status != transport.HealthStatusUnhealthy {
		t.Errorf("Health on closed transport: got %v, want Unhealthy", got.Status)
	}
}

func TestTransport_Health_UnhealthyWhenClientClosed(t *testing.T) {
	t.Parallel()
	client := newMockClient(t, false)
	tr, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close(context.Background()) })

	// Close the underlying sarama.Client out from under the transport. The
	// transport itself remains "open" but Health should detect the closed
	// client and report unhealthy rather than crashing.
	_ = client.Close()

	got := tr.Health(context.Background())
	if got.Status != transport.HealthStatusUnhealthy {
		t.Errorf("Health with closed client: got %v, want Unhealthy", got.Status)
	}
	if got.Details["type"] != "kafka" {
		t.Errorf("Health.Details[type]: got %v, want %q", got.Details["type"], "kafka")
	}
}

func TestErrAutoCommitEnabled_MessageDocumentsRemediation(t *testing.T) {
	t.Parallel()
	// The sentinel error's message tells operators exactly which sarama
	// field to flip. The doc-string in kafka.go references this remediation
	// path; consumers searching their logs for the literal field name need
	// to find it here. Pin the message text.
	want := "kafka: auto-commit must be disabled for at-least-once delivery - set Consumer.Offsets.AutoCommit.Enable = false"
	if got := ErrAutoCommitEnabled.Error(); got != want {
		t.Errorf("ErrAutoCommitEnabled.Error() drifted; got %q want %q", got, want)
	}
}

// Compile-time guard: ensure the sarama.Client interface used by New() does
// not change shape. If a sarama bump renames Config() or Closed(), this
// stops compiling.
var _ = func() {
	var _ func(sarama.Client) bool = func(c sarama.Client) bool {
		_ = c.Config()
		return c.Closed()
	}
}

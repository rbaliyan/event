package message

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestMessageNew(t *testing.T) {
	metadata := map[string]string{"key": "value"}
	msg := New("id-1", "source-1", "payload", metadata, trace.SpanContext{})

	if msg.ID() != "id-1" {
		t.Errorf("expected id-1, got %s", msg.ID())
	}
	if msg.Source() != "source-1" {
		t.Errorf("expected source-1, got %s", msg.Source())
	}
	if msg.Payload() != "payload" {
		t.Errorf("expected payload, got %v", msg.Payload())
	}
	if msg.Metadata()["key"] != "value" {
		t.Errorf("expected metadata key=value")
	}
	if msg.RetryCount() != 0 {
		t.Errorf("expected retry count 0, got %d", msg.RetryCount())
	}
	if msg.Timestamp().IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestMessageNewWithRetry(t *testing.T) {
	msg := NewWithRetry("id-1", "source-1", "payload", nil, trace.SpanContext{}, 3)

	if msg.RetryCount() != 3 {
		t.Errorf("expected retry count 3, got %d", msg.RetryCount())
	}
	if msg.Timestamp().IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestMessageNewWithTimestamp(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	msg := NewWithTimestamp("id-1", "source-1", "payload", nil, trace.SpanContext{}, ts)

	if !msg.Timestamp().Equal(ts) {
		t.Errorf("expected %v, got %v", ts, msg.Timestamp())
	}
}

func TestMessageNewWithAck(t *testing.T) {
	ackCalled := false
	ackFn := func(err error) error {
		ackCalled = true
		return nil
	}

	msg := NewWithAck("id-1", "source-1", "payload", nil, 2, ackFn)

	if msg.RetryCount() != 2 {
		t.Errorf("expected retry count 2, got %d", msg.RetryCount())
	}

	msg.Ack(nil)
	if !ackCalled {
		t.Error("expected ack function to be called")
	}
}

func TestMessageNewFull(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	metadata := map[string]string{"env": "test"}
	ackCalled := false
	ackFn := func(err error) error {
		ackCalled = true
		return nil
	}

	msg := NewFull("id-1", "source-1", "payload", metadata, ts, 5, ackFn)

	if msg.ID() != "id-1" {
		t.Errorf("expected id-1, got %s", msg.ID())
	}
	if msg.Source() != "source-1" {
		t.Errorf("expected source-1, got %s", msg.Source())
	}
	if msg.Payload() != "payload" {
		t.Errorf("expected payload, got %v", msg.Payload())
	}
	if msg.Metadata()["env"] != "test" {
		t.Error("expected metadata env=test")
	}
	if !msg.Timestamp().Equal(ts) {
		t.Errorf("expected %v, got %v", ts, msg.Timestamp())
	}
	if msg.RetryCount() != 5 {
		t.Errorf("expected retry count 5, got %d", msg.RetryCount())
	}

	msg.Ack(nil)
	if !ackCalled {
		t.Error("expected ack function to be called")
	}
}

func TestMessageAckWithNilFunction(t *testing.T) {
	msg := New("id-1", "source-1", "payload", nil, trace.SpanContext{})

	// Should not panic
	err := msg.Ack(nil)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestMessageContext(t *testing.T) {
	msg := New("id-1", "source-1", "payload", nil, trace.SpanContext{})

	ctx := msg.Context()
	if ctx == nil {
		t.Error("expected non-nil context")
	}
}

func TestMessageNilMetadata(t *testing.T) {
	msg := New("id-1", "source-1", "payload", nil, trace.SpanContext{})

	if msg.Metadata() != nil {
		t.Error("expected nil metadata")
	}
}

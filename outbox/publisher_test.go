package outbox

import (
	"context"
	"errors"
	"testing"
)

// captureStore implements event.OutboxStore (matches the local Store.Store
// signature) by delegating to fn, letting tests capture what Publisher hands
// to the backend.
type captureStore struct {
	fn func(ctx context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error
}

func (c captureStore) Store(ctx context.Context, eventName, eventID string, payload []byte, metadata map[string]string) error {
	return c.fn(ctx, eventName, eventID, payload, metadata)
}

func TestPublisher_EncodesAndStores(t *testing.T) {
	var gotName, gotID string
	var gotPayload []byte
	var gotMeta map[string]string

	store := captureStore{fn: func(_ context.Context, name, id string, payload []byte, meta map[string]string) error {
		gotName, gotID, gotPayload, gotMeta = name, id, payload, meta
		return nil
	}}
	p := NewPublisher(store)

	meta := map[string]string{"k": "v"}
	if err := p.Publish(context.Background(), "order.created", map[string]string{"k": "v"}, meta); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if gotName != "order.created" {
		t.Fatalf("unexpected event name: %q", gotName)
	}
	if string(gotPayload) != `{"k":"v"}` {
		t.Fatalf("unexpected JSON-encoded payload: %q", gotPayload)
	}
	if gotID == "" {
		t.Fatal("expected a non-empty generated event id")
	}
	if gotMeta["k"] != "v" {
		t.Fatalf("metadata not passed through: %#v", gotMeta)
	}
}

func TestPublisher_DefaultEncoderIsJSON(t *testing.T) {
	var gotPayload []byte
	store := captureStore{fn: func(_ context.Context, _, _ string, payload []byte, _ map[string]string) error {
		gotPayload = payload
		return nil
	}}
	p := NewPublisher(store)

	if err := p.Publish(context.Background(), "e", struct {
		Name string `json:"name"`
	}{Name: "hello"}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if string(gotPayload) != `{"name":"hello"}` {
		t.Fatalf("expected default json.Marshal encoding, got %q", gotPayload)
	}
}

func TestPublisher_WithEncoderOverride(t *testing.T) {
	var gotPayload []byte
	store := captureStore{fn: func(_ context.Context, _, _ string, payload []byte, _ map[string]string) error {
		gotPayload = payload
		return nil
	}}
	p := NewPublisher(store, WithEncoder(func(v any) ([]byte, error) {
		return []byte("custom:" + v.(string)), nil
	}))

	if err := p.Publish(context.Background(), "e", "abc", nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if string(gotPayload) != "custom:abc" {
		t.Fatalf("custom encoder not used, got %q", gotPayload)
	}
}

func TestPublisher_WithEncoderNilIgnored(t *testing.T) {
	var gotPayload []byte
	store := captureStore{fn: func(_ context.Context, _, _ string, payload []byte, _ map[string]string) error {
		gotPayload = payload
		return nil
	}}
	p := NewPublisher(store, WithEncoder(nil))

	if err := p.Publish(context.Background(), "e", map[string]string{"a": "b"}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if string(gotPayload) != `{"a":"b"}` {
		t.Fatalf("nil encoder should leave default json.Marshal in place, got %q", gotPayload)
	}
}

func TestPublisher_EncodeErrorPropagates(t *testing.T) {
	wantErr := errors.New("boom")
	store := captureStore{fn: func(context.Context, string, string, []byte, map[string]string) error {
		t.Fatal("store.Store should not be called when encoding fails")
		return nil
	}}
	p := NewPublisher(store, WithEncoder(func(any) ([]byte, error) {
		return nil, wantErr
	}))

	err := p.Publish(context.Background(), "e", "x", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped %v, got %v", wantErr, err)
	}
}

func TestPublisher_StoreErrorPropagates(t *testing.T) {
	wantErr := errors.New("store failed")
	store := captureStore{fn: func(context.Context, string, string, []byte, map[string]string) error {
		return wantErr
	}}
	p := NewPublisher(store)

	err := p.Publish(context.Background(), "e", "x", nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	event "github.com/rbaliyan/event/v3"
)

// Publisher stores events into the outbox via any backend Store, encoding the
// payload with a pluggable encoder (default JSON). The tx/session is read
// from ctx (event.WithOutboxTx) inside the underlying Store's Store method,
// so Publish itself takes no tx parameter.
type Publisher struct {
	store  event.OutboxStore
	encode func(any) ([]byte, error)
}

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithEncoder overrides the default json.Marshal payload encoder. A nil fn
// is ignored, leaving the current encoder (default json.Marshal) in place.
func WithEncoder(fn func(any) ([]byte, error)) PublisherOption {
	return func(p *Publisher) {
		if fn != nil {
			p.encode = fn
		}
	}
}

// NewPublisher creates a Publisher backed by store. Without WithEncoder, the
// payload is encoded with json.Marshal.
func NewPublisher(store event.OutboxStore, opts ...PublisherOption) *Publisher {
	p := &Publisher{store: store, encode: json.Marshal}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Publish encodes payload and stores it via the backend Store, within
// whatever tx/session is bound to ctx (see event.WithOutboxTx).
func (p *Publisher) Publish(ctx context.Context, eventName string, payload any, metadata map[string]string) error {
	encoded, err := p.encode(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	return p.store.Store(ctx, eventName, uuid.New().String(), encoded, metadata)
}

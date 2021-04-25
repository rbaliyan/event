package event

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

type Nats struct {
	nc  *nats.Conn
	sub *nats.Subscription
	Local
}

func NatsEvent(name string, nc *nats.Conn) *Nats {
	return &Nats{
		nc:    nc,
		Local: Local{name: name},
	}
}

// Publish ...
func (e *Nats) Publish(ctx context.Context, data Data) {
	e.Local.Publish(ctx, data)
	d, err := Marshal(data)
	if err == nil {
		if err := e.nc.Publish(e.name, d); err != nil {
			log.Printf("Publish msg error: %v", err)
		}
	} else {
		log.Printf("encode msg error: %v", err)
	}
}

// Subscribe ...
func (e *Nats) Subscribe(ctx context.Context, handler Handler) {
	e.Local.Subscribe(ctx, handler)
	e.Lock()
	defer e.Unlock()
	if e.sub != nil {
		return
	}
	e.sub, _ = e.nc.Subscribe(e.name, func(msg *nats.Msg) {
		data, err := Unmarshal(msg.Data)
		if err != nil {
			log.Printf("decode msg error: %v", err)
		}
		e.Local.Publish(ctx, data)
	})
}

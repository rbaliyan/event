package event

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
)

type natsImpl struct {
	nc  *nats.Conn
	sub *nats.Subscription
	localImpl
}

func Nats(name string, nc *nats.Conn) Event {
	return &natsImpl{
		nc:        nc,
		localImpl: localImpl{name: name},
	}
}

// Publish ...
func (e *natsImpl) Publish(ctx context.Context, data Data) {
	e.localImpl.Publish(ctx, data)
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
func (e *natsImpl) Subscribe(ctx context.Context, handler Handler) {
	e.localImpl.Subscribe(ctx, handler)
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
		e.localImpl.Publish(ctx, data)
	})
}

package event

import (
	"context"

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
			logger.Printf("Publish msg error: %v", err)
		}
	} else {
		logger.Printf("encode msg error: %v", err)
	}
}

// Subscribe ...
func (e *natsImpl) Subscribe(ctx context.Context, handler Handler) {
	var err error
	e.localImpl.Subscribe(ctx, handler)
	e.Lock()
	defer e.Unlock()
	if e.sub != nil {
		return
	}
	if e.nc == nil {
		logger.Printf("Error!!!, nats connection null")
		return
	}
	e.sub, err = e.nc.Subscribe(e.name, func(msg *nats.Msg) {
		data, err := Unmarshal(msg.Data)
		if err != nil {
			logger.Printf("decode msg error: %v", err)
		}
		e.localImpl.Publish(ctx, data)
	})
	if err != nil {
		logger.Printf("Error on nc subscribe: %v", err)
	}
}

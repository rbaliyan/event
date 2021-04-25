package event

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
)

type redisImpl struct {
	rc     redis.UniversalClient
	pubsub *redis.PubSub
	Local
}

func Redis(name string, rc redis.UniversalClient) *redisImpl {
	return &redisImpl{
		rc:    rc,
		Local: Local{name: name},
	}
}

// Publish ...
func (e *redisImpl) Publish(ctx context.Context, data Data) {
	e.Local.Publish(ctx, data)
	d, err := Marshal(&rpcmsg{Data: data})
	if err == nil {
		if err := e.rc.Publish(ctx, e.name, d); err != nil {
			log.Printf("Publish msg error: %v", err)
		}
	} else {
		log.Printf("encode msg error: %v", err)
	}
}

// Subscribe ...
func (e *redisImpl) Subscribe(ctx context.Context, handler Handler) {
	e.Local.Subscribe(ctx, handler)
	e.Lock()
	defer e.Unlock()
	if e.pubsub != nil {
		return
	}
	e.pubsub = e.rc.Subscribe(ctx, e.name)
	go func() {
		ch := e.pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				data, err := Unmarshal([]byte(msg.Payload))
				if err != nil {
					log.Printf("decode msg error: %v", err)
				}
				e.Local.Publish(ctx, data)

			}
		}
	}()
}

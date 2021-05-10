package event

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
)

type redisImpl struct {
	rc     redis.UniversalClient
	pubsub *redis.PubSub
	localImpl
}

func Redis(name string, rc redis.UniversalClient) Event {
	return &redisImpl{
		rc:        rc,
		localImpl: localImpl{name: name},
	}
}

// Publish ...
func (e *redisImpl) Publish(ctx context.Context, data Data) {
	e.localImpl.Publish(ctx, data)
	d, err := Marshal(data)
	if err == nil {
		if err := e.rc.Publish(ctx, e.name, d).Err(); err != nil {
			log.Printf("Publish msg error: %v", err)
		}
	} else {
		log.Printf("encode msg error: %v", err)
	}
}

// Subscribe ...
func (e *redisImpl) Subscribe(ctx context.Context, handler Handler) {
	e.localImpl.Subscribe(ctx, handler)
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
				e.localImpl.Publish(ctx, data)
			}
		}
	}()
}

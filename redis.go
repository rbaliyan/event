package event

import (
	"context"

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
			logger.Printf("Publish msg error: %v", err)
		}
	} else {
		logger.Printf("encode msg error: %v", err)
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
	if e.rc == nil {
		logger.Printf("Error!!!, redis connection null")
		return
	}
	e.pubsub = e.rc.Subscribe(context.Background(), e.name)
	go func() {
		defer func() {
			e.Lock()
			if e.pubsub != nil {
				e.pubsub.Close()
				e.pubsub = nil
			}
			e.Unlock()
		}()
		ch := e.pubsub.Channel()
		for msg := range ch {
			data, err := Unmarshal([]byte(msg.Payload))
			if err != nil {
				logger.Printf("decode msg error: %v", err)
			}
			e.localImpl.Publish(ctx, data)
		}
	}()
}

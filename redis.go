package event

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type redisImpl struct {
	rc      redis.UniversalClient
	pubsub  *redis.PubSub
	onError func(Event, error)
	localImpl
}

// Redis events
func Redis(name string, rc redis.UniversalClient, onError func(Event, error)) Event {
	return &redisImpl{
		rc:        rc,
		onError:   onError,
		localImpl: localImpl{name: name},
	}
}

// Publish ...
func (e *redisImpl) Publish(ctx context.Context, data Data) {
	msg := &RemoteMsg{Data: data, Source: defaultSource}
	// Set event id if not already set
	if msg.ID = EventIDFromContext(ctx); msg.ID == "" {
		msg.ID = NewID()
		ctx = WithEventID(ctx, msg.ID)
	}
	// Set sender id
	if msg.Source = SourceFromContext(ctx); msg.Source == "" {
		msg.Source = defaultSource
		ctx = WithSource(ctx, msg.Source)
	}
	e.localImpl.Publish(ctx, data)
	d, err := Marshal(msg)
	if err == nil {
		if err := e.rc.Publish(ctx, e.name, d).Err(); err != nil {
			logger.Printf("Publish msg error: %v", err)
			if e.onError != nil {
				e.onError(e, err)
			}
		}
	} else {
		logger.Printf("encode msg error: %v", err)
		if e.onError != nil {
			e.onError(e, err)
		}
	}
}

// Subscribe ...
func (e *redisImpl) Subscribe(ctx context.Context, handler Handler) {
	e.localImpl.Subscribe(ctx, handler)
	e.mutex.Lock()
	defer e.mutex.Unlock()
	if e.pubsub != nil {
		return
	}
	if e.rc == nil {
		logger.Printf("Error!!!, redis connection null")
		return
	}
	e.pubsub = e.rc.Subscribe(ctx, e.name)
	go func() {
		defer func() {
			e.mutex.Lock()
			if e.pubsub != nil {
				e.pubsub.Close()
				e.pubsub = nil
			}
			e.mutex.Unlock()
		}()
		ch := e.pubsub.Channel()
		for msg := range ch {
			data, err := Unmarshal([]byte(msg.Payload))
			if err != nil {
				logger.Printf("decode msg error: %v", err)
				if e.onError != nil {
					e.onError(e, err)
				}
			} else if e.onError != nil {
				e.onError(e, err)
			}
			// Publish with new context
			e.localImpl.Publish(WithSource(WithEventID(ctx, data.ID), data.Source), data)
		}
	}()
}

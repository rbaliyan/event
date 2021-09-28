package event

import (
	"context"
	"encoding/gob"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

var (
	mr *miniredis.Miniredis
)

func init() {
	var err error
	mr, err = miniredis.Run()
	if err != nil {
		panic(err)
	}

	gob.Register(&teststruct{})
}

type teststruct struct {
	Msg string
}

func TestRedisRemoteEvent(t *testing.T) {
	r1 := redis.NewUniversalClient(
		&redis.UniversalOptions{
			Addrs: []string{mr.Addr()},
		},
	)
	ev1 := Redis("test/event", r1, nil)
	ev2 := Redis("test/event", r1, nil)
	ch := make(chan struct{})
	ev2.Subscribe(context.TODO(), func(ctx context.Context, e Event, data Data) {
		ch <- struct{}{}
	})
	ev1.Publish(context.TODO(), &teststruct{"test"})
	<-ch
}

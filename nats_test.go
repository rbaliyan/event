package event

import (
	"context"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const TEST_PORT = 8369

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}

func init() {
	gob.Register(&teststruct1{})
}

type teststruct1 struct {
	Msg string
}

func TestNatsRemoteEvent(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	sUrl := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, err := nats.Connect(sUrl)
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	ev1 := Nats("test/event", nc)
	ev2 := Nats("test/event", nc)
	ch := make(chan struct{})
	ev2.Subscribe(context.TODO(), func(ctx context.Context, e Event, data Data) {
		ch <- struct{}{}
	})
	ev1.Publish(context.TODO(), &teststruct1{"test"})
	<-ch
}

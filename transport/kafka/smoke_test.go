//go:build smoke

// Env-gated bus-level smoke test for the Kafka transport. Requires a real
// broker because the sarama producer + cluster-admin bootstrap dance is
// not faithfully reproducible against an in-process mock.
//
// Run with:
//
//	KAFKA_BROKERS=127.0.0.1:9092 just test-smoke
//	# or directly:
//	KAFKA_BROKERS=127.0.0.1:9092 go test -tags=smoke -race ./transport/kafka/...

package kafka

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/internal/testutil"
)

// kafkaBrokersEnv is the environment variable consulted for broker addresses.
const kafkaBrokersEnv = "KAFKA_BROKERS"

func setupKafkaClient(t testing.TB) sarama.Client {
	t.Helper()

	addrs := os.Getenv(kafkaBrokersEnv)
	if addrs == "" {
		t.Skipf("Kafka smoke skipped: %s not set", kafkaBrokersEnv)
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = false // required by the transport
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewClient(strings.Split(addrs, ","), cfg)
	if err != nil {
		t.Skipf("Kafka unreachable at %s: %v", addrs, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestSmokeKafkaBus_RoundTrip(t *testing.T) {
	t.Parallel()
	client := setupKafkaClient(t)

	// Per-run group ID prevents parallel smoke runs from cross-consuming.
	groupID := "smoke-" + testutil.UniqueName(t)

	tr, err := New(client, WithConsumerGroup(groupID))
	if err != nil {
		t.Fatalf("kafka.New: %v", err)
	}

	ctx := context.Background()
	bus := testutil.MustNewBus(t, event.WithTransport(tr))
	// Per-run topic name to keep replays/retention from other tests out.
	ev := testutil.MustRegister(t, ctx, bus, event.New[string]("smoke_kafka_rt_"+testutil.UniqueName(t)))

	received := make(chan string, 1)
	if err := ev.Subscribe(ctx, func(_ context.Context, _ event.Event[string], v string) error {
		received <- v
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Kafka group joins are async and sarama does not expose join state
	// cheaply, so we cannot poll it reliably. Instead retry publish-and-wait
	// once: the first publish can race with group rebalance; the second
	// always lands.
	for attempt := range 2 {
		if err := ev.Publish(ctx, "hello-kafka"); err != nil {
			t.Fatalf("Publish %d: %v", attempt, err)
		}
		select {
		case got := <-received:
			if got != "hello-kafka" {
				t.Errorf("attempt %d: got %q, want %q", attempt, got, "hello-kafka")
			}
			return
		case <-time.After(10 * time.Second):
			if attempt == 1 {
				t.Fatal("timed out waiting for Kafka message after 2 attempts")
			}
		}
	}
}

package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

// newMockClient returns a sarama.Client backed by an in-process sarama
// MockBroker. The mock handles a minimal set of request types — enough for
// sarama.NewClient to bootstrap metadata, and (when autoCommit=false) for
// the Kafka transport constructor's auto-commit guard to succeed past
// validation.
//
// The MockBroker listens on a free localhost port and is torn down via
// t.Cleanup. No real Kafka broker is required.
//
// Tests that exercise Publish/Subscribe end-to-end need a real broker (see
// smoke_test.go behind //go:build smoke, env-gated by KAFKA_BROKERS).
func newMockClient(t testing.TB, autoCommit bool) sarama.Client {
	t.Helper()

	mb := sarama.NewMockBroker(t, 1)
	t.Cleanup(mb.Close)

	// Metadata handler: tells the client this broker is the controller for a
	// well-known cluster and exposes one topic with one partition led by us.
	// Sarama requires at least one topic visible during bootstrap; the topic
	// itself doesn't need to exist for the unit tests in this file.
	mb.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(mb.BrokerID()).
			SetBroker(mb.Addr(), mb.BrokerID()),
		// CreateTopicsResponse — RegisterEvent calls CreateTopic during
		// happy-path tests. The mock acknowledges every create as success.
		"CreateTopicsRequest": sarama.NewMockCreateTopicsResponse(t),
		// FindCoordinator — sarama producer/admin bootstrap can ping for
		// coordinators; respond with our own broker.
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "", mb),
		// ApiVersions — sarama clients negotiate API versions on connect.
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = autoCommit
	// Producer needs these set explicitly for SyncProducer creation.
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	client, err := sarama.NewClient([]string{mb.Addr()}, cfg)
	if err != nil {
		t.Fatalf("sarama.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

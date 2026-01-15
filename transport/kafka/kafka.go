// Package kafka provides a Kafka-based transport implementation.
//
// This transport uses Kafka for at-least-once delivery guarantees.
// Messages are persisted in Kafka and redelivered if not acknowledged.
//
// Features:
//   - At-least-once delivery via explicit offset marking
//   - Consumer groups for WorkerPool mode (load balancing)
//   - Unique consumer groups for Broadcast mode (fan-out)
//   - Automatic reconnection with exponential backoff
//   - Health checks and consumer lag monitoring
//
// IMPORTANT: Auto-commit must be disabled in the sarama config to ensure
// at-least-once delivery. See New() for recommended configuration.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/codec"
)

// Errors
var (
	ErrClientRequired       = errors.New("kafka client is required")
	ErrProducerFailed       = errors.New("failed to create kafka producer")
	ErrAutoCommitEnabled    = errors.New("kafka: auto-commit must be disabled for at-least-once delivery - set Consumer.Offsets.AutoCommit.Enable = false")
)

// DefaultBusName is used as default consumer group
var DefaultBusName = "event-bus"

// Transport implements transport.Transport using Kafka
type Transport struct {
	status      int32
	client      sarama.Client
	producer    sarama.SyncProducer
	admin       sarama.ClusterAdmin
	groupID     string
	topicPrefix string
	codec       codec.Codec
	events      sync.Map // map[string]*kafkaEvent
	logger      *slog.Logger
	onError     func(error)

	// Topic configuration
	partitions  int32
	replication int16
	retention   time.Duration // Message retention time (0 = use broker default)
	sendTimeout time.Duration // Timeout for sending to subscriber channel (backpressure)

	// Native Kafka features
	deadLetterTopic string // Topic for failed messages (empty = disabled)
	maxRetries      int    // Max retries before sending to DLT
}

// kafkaEvent tracks event-specific state
type kafkaEvent struct {
	name string
}

// subscription implements transport.Subscription for Kafka
type subscription struct {
	*base.Subscription                   // Embedded base subscription
	consumer         sarama.ConsumerGroup // Kafka consumer group
	topic            string               // Topic name
	codec            codec.Codec          // Message codec

	// Native Kafka features
	producer        sarama.SyncProducer // For DLT publishing
	deadLetterTopic string              // Topic for failed messages
	maxRetries      int                 // Max retries before DLT
}

// Default configuration
var (
	DefaultPartitions  = int32(1)
	DefaultReplication = int16(1)
)

// topicPrefix is the fixed prefix for Kafka topics to avoid clashing with user data
const topicPrefix = "evt."

// New creates a new Kafka transport with a pre-initialized client.
//
// IMPORTANT: Auto-commit must be disabled in the sarama config to ensure at-least-once
// delivery. If auto-commit is enabled (the sarama default), messages may be lost when
// handlers fail because offsets are committed automatically regardless of ack status.
//
// Recommended sarama.Config settings for at-least-once delivery:
//
//	config := sarama.NewConfig()
//	config.Consumer.Offsets.AutoCommit.Enable = false  // REQUIRED
//	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
//	    sarama.NewBalanceStrategyRoundRobin(),
//	}
//
// The transport uses explicit offset marking via session.MarkMessage() which only
// commits offsets when the handler acknowledges the message successfully (msg.Ack(nil)).
func New(client sarama.Client, opts ...Option) (*Transport, error) {
	if client == nil {
		return nil, ErrClientRequired
	}

	// CRITICAL: Validate auto-commit is disabled to ensure at-least-once delivery
	// If auto-commit is enabled, messages may be lost when handlers fail because
	// offsets are committed automatically regardless of ack status.
	if client.Config().Consumer.Offsets.AutoCommit.Enable {
		return nil, ErrAutoCommitEnabled
	}

	t := &Transport{
		status:      1,
		client:      client,
		groupID:     DefaultBusName,
		topicPrefix: topicPrefix,
		codec:       codec.Default(),
		partitions:  DefaultPartitions,
		replication: DefaultReplication,
		logger:      transport.Logger("transport>kafka"),
		onError:     func(error) {},
	}

	for _, opt := range opts {
		opt(t)
	}

	// Create sync producer
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, errors.Join(ErrProducerFailed, err)
	}
	t.producer = producer

	// Create cluster admin for topic management
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		producer.Close()
		return nil, err
	}
	t.admin = admin

	return t, nil
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

func (t *Transport) topicName(eventName string) string {
	return t.topicPrefix + eventName
}

// RegisterEvent creates resources for an event
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	// Check if already registered
	if _, loaded := t.events.LoadOrStore(name, &kafkaEvent{
		name: name,
	}); loaded {
		return transport.ErrEventAlreadyExists
	}

	// Create topic if it doesn't exist
	topicName := t.topicName(name)
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     t.partitions,
		ReplicationFactor: t.replication,
	}

	// Set retention.ms if configured
	if t.retention > 0 {
		retentionMs := fmt.Sprintf("%d", t.retention.Milliseconds())
		topicDetail.ConfigEntries = map[string]*string{
			"retention.ms": &retentionMs,
		}
	}

	err := t.admin.CreateTopic(topicName, topicDetail, false)

	// Ignore "topic already exists" error
	if err != nil {
		var topicErr *sarama.TopicError
		if errors.As(err, &topicErr) && topicErr.Err == sarama.ErrTopicAlreadyExists {
			err = nil
		}
	}

	if err != nil {
		t.events.Delete(name)
		return err
	}

	t.logger.Debug("registered event", "event", name)
	return nil
}

// UnregisterEvent cleans up event resources
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.LoadAndDelete(name); !ok {
		return transport.ErrEventNotRegistered
	}

	t.logger.Debug("unregistered event", "event", name)
	return nil
}

// Publish sends a message to an event's subscribers
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}

	if _, ok := t.events.Load(name); !ok {
		return transport.ErrEventNotRegistered
	}

	// Encode message
	data, err := t.codec.Encode(msg)
	if err != nil {
		return err
	}

	// Produce message
	_, _, err = t.producer.SendMessage(&sarama.ProducerMessage{
		Topic: t.topicName(name),
		Key:   sarama.StringEncoder(msg.ID()),
		Value: sarama.ByteEncoder(data),
	})

	if err != nil {
		t.onError(err)
		return err
	}

	t.logger.Debug("published message", "event", name, "msg_id", msg.ID())
	return nil
}

// Subscribe creates a subscription to receive messages for an event
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	subOpts := transport.ApplySubscribeOptions(opts...)

	if _, ok := t.events.Load(name); !ok {
		return nil, transport.ErrEventNotRegistered
	}

	// Determine consumer group based on delivery mode
	var groupID string
	if subOpts.DeliveryMode == transport.WorkerPool {
		if subOpts.WorkerGroup != "" {
			// WorkerPool with named group: workers in same group compete
			// Different groups each receive all messages
			groupID = t.groupID + "-" + name + "-" + subOpts.WorkerGroup
		} else {
			// WorkerPool default: all workers share the base group
			groupID = t.groupID + "-" + name
		}
	} else {
		// Broadcast: unique group per subscriber (fan-out)
		groupID = t.groupID + "-" + name + "-" + transport.NewID()
	}

	// Create consumer group from client
	consumer, err := sarama.NewConsumerGroupFromClient(groupID, t.client)
	if err != nil {
		return nil, err
	}

	bufSize := 100
	if subOpts.BufferSize > 0 {
		bufSize = subOpts.BufferSize
	}

	subID := transport.NewID()
	sub := &subscription{
		Subscription: base.NewSubscription(subID, bufSize, t.sendTimeout),
		consumer:     consumer,
		topic:        t.topicName(name),
		codec:        t.codec,
		// Native Kafka features
		producer:        t.producer,
		deadLetterTopic: t.deadLetterTopic,
		maxRetries:      t.maxRetries,
	}

	// Start consuming in background with WaitGroup tracking
	sub.WaitGroup().Add(1)
	go func() {
		defer sub.WaitGroup().Done()
		sub.consumeLoop(ctx, t.logger)
	}()

	t.logger.Debug("added subscriber", "event", name, "subscriber", sub.ID(), "group", groupID, "mode", subOpts.DeliveryMode)
	return sub, nil
}

// Close shuts down the transport
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}

	var errs []error

	if t.producer != nil {
		if err := t.producer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Note: We don't close the client as it was passed in pre-initialized
	// The caller is responsible for closing it

	t.logger.Debug("transport closed")
	return errors.Join(errs...)
}

// Health performs a health check on the Kafka transport
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	start := time.Now()

	result := &transport.HealthCheckResult{
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	// Check if transport is open
	if !t.isOpen() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "transport is closed"
		result.Latency = time.Since(start)
		return result
	}

	// Check if client is closed
	if t.client.Closed() {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "kafka client is closed"
		result.Latency = time.Since(start)
		result.Details["type"] = "kafka"
		return result
	}

	// Check broker connectivity
	brokerStart := time.Now()
	brokers := t.client.Brokers()
	if len(brokers) == 0 {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "no kafka brokers available"
		result.Latency = time.Since(start)
		result.Details["type"] = "kafka"
		return result
	}

	// Check if we have at least one connected broker
	connectedBrokers := 0
	var brokerAddrs []string
	for _, broker := range brokers {
		connected, _ := broker.Connected()
		if connected {
			connectedBrokers++
		}
		brokerAddrs = append(brokerAddrs, broker.Addr())
	}
	brokerCheckLatency := time.Since(brokerStart)

	if connectedBrokers == 0 {
		result.Status = transport.HealthStatusUnhealthy
		result.Message = "no connected kafka brokers"
		result.Latency = time.Since(start)
		result.Details["type"] = "kafka"
		result.Details["total_brokers"] = len(brokers)
		result.Details["connected_brokers"] = connectedBrokers
		return result
	}

	// Count events
	var eventCount int
	t.events.Range(func(key, value any) bool {
		eventCount++
		return true
	})

	// Determine status based on broker connectivity
	if connectedBrokers < len(brokers) {
		result.Status = transport.HealthStatusDegraded
		result.Message = fmt.Sprintf("kafka transport degraded: %d/%d brokers connected", connectedBrokers, len(brokers))
	} else {
		result.Status = transport.HealthStatusHealthy
		result.Message = "kafka transport is healthy"
	}

	result.Latency = time.Since(start)
	result.Details["type"] = "kafka"
	result.Details["total_brokers"] = len(brokers)
	result.Details["connected_brokers"] = connectedBrokers
	result.Details["broker_check_ms"] = brokerCheckLatency.Milliseconds()
	result.Details["brokers"] = brokerAddrs
	result.Details["events"] = eventCount
	result.Details["consumer_group"] = t.groupID

	return result
}

// ConsumerLag returns the current consumer lag for all events
func (t *Transport) ConsumerLag(ctx context.Context) ([]transport.ConsumerLag, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}

	var lags []transport.ConsumerLag

	t.events.Range(func(key, value any) bool {
		name := key.(string)
		topicName := t.topicName(name)

		// Get topic partitions
		partitions, err := t.client.Partitions(topicName)
		if err != nil {
			t.logger.Error("failed to get partitions", "topic", topicName, "error", err)
			return true
		}

		// Get the latest offsets for each partition
		var totalHighWatermark int64
		for _, partition := range partitions {
			offset, err := t.client.GetOffset(topicName, partition, sarama.OffsetNewest)
			if err != nil {
				t.logger.Error("failed to get offset", "topic", topicName, "partition", partition, "error", err)
				continue
			}
			totalHighWatermark += offset
		}

		// List consumer groups that match our pattern
		groups, err := t.admin.ListConsumerGroups()
		if err != nil {
			t.logger.Error("failed to list consumer groups", "error", err)
			return true
		}

		// Find consumer groups for this event
		groupPrefix := t.groupID + "-" + name
		for groupName := range groups {
			if len(groupName) >= len(groupPrefix) && groupName[:len(groupPrefix)] == groupPrefix {
				// Describe consumer group to get member offsets
				descriptions, err := t.admin.DescribeConsumerGroups([]string{groupName})
				if err != nil {
					t.logger.Error("failed to describe consumer group", "group", groupName, "error", err)
					continue
				}

				// Get committed offsets for this group
				offsets, err := t.admin.ListConsumerGroupOffsets(groupName, map[string][]int32{
					topicName: partitions,
				})
				if err != nil {
					t.logger.Error("failed to get consumer group offsets", "group", groupName, "error", err)
					continue
				}

				var totalCommitted int64
				for _, partition := range partitions {
					if block := offsets.GetBlock(topicName, partition); block != nil && block.Offset >= 0 {
						totalCommitted += block.Offset
					}
				}

				lag := transport.ConsumerLag{
					Event:         name,
					ConsumerGroup: groupName,
					Lag:           totalHighWatermark - totalCommitted,
				}

				// Add member count to pending
				for _, desc := range descriptions {
					if desc.GroupId == groupName && desc.Err == sarama.ErrNoError {
						lag.PendingMessages = int64(len(desc.Members))
					}
				}

				lags = append(lags, lag)
			}
		}

		// If no consumer groups found, report total messages as lag
		foundGroup := false
		for _, l := range lags {
			if l.Event == name {
				foundGroup = true
				break
			}
		}
		if !foundGroup {
			lags = append(lags, transport.ConsumerLag{
				Event: name,
				Lag:   totalHighWatermark,
			})
		}

		return true
	})

	return lags, nil
}

// subscription methods

func (s *subscription) Close(ctx context.Context) error {
	return s.Subscription.Close(func() error {
		if s.consumer != nil {
			s.consumer.Close()
		}
		return nil
	})
}

// sendWithRetry sends a message to the channel with exponential backoff on timeout.
// This is a convenience wrapper around base.Subscription.SendWithRetry with Kafka-specific logging.
func (s *subscription) sendWithRetry(msg transport.Message, logger *slog.Logger) bool {
	return s.Subscription.SendWithRetry(msg, logger, "topic", s.topic)
}

func (s *subscription) consumeLoop(ctx context.Context, logger *slog.Logger) {
	handler := &consumerHandler{
		sub:    s,
		logger: logger,
	}

	// Exponential backoff for consumer errors
	backoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		default:
			if err := s.consumer.Consume(ctx, []string{s.topic}, handler); err != nil {
				jitteredBackoff := transport.Jitter(backoff, 0.3)
				logger.Error("consumer error, retrying with backoff", "error", err, "backoff", jitteredBackoff)

				select {
				case <-s.ClosedCh():
					return
				case <-ctx.Done():
					return
				case <-time.After(jitteredBackoff):
				}

				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			// Reset backoff on successful consume
			backoff = 100 * time.Millisecond
		}
	}
}

// consumerHandler implements sarama.ConsumerGroupHandler
type consumerHandler struct {
	sub    *subscription
	logger *slog.Logger
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-h.sub.ClosedCh():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// Decode message
			decoded, err := h.sub.codec.Decode(msg.Value)
			if err != nil {
				h.logger.Error("failed to decode message", "error", err,
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

				// Send decode error to channel for DLQ routing
				msgID := fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
				errorMsg := transport.NewDecodeErrorMessage(
					msgID, msg.Value, err,
					func(ackErr error) error {
						if ackErr == nil {
							session.MarkMessage(msg, "")
						}
						// If ackErr != nil, don't mark - will be redelivered for DLQ retry
						return nil
					},
				)
				if !h.sub.sendWithRetry(errorMsg, h.logger) {
					return nil // Subscription closed
				}
				continue
			}

			// Wrap with ack function
			wrappedMsg := transport.NewMessageWithAck(
				decoded.ID(),
				decoded.Source(),
				decoded.Payload(),
				decoded.Metadata(),
				decoded.RetryCount(),
				func(err error) error {
					if err == nil {
						session.MarkMessage(msg, "")
					}
					// If err != nil, don't mark - will be redelivered
					return nil
				},
			)

			// Send message to handler channel with exponential backoff on timeout
			if !h.sub.sendWithRetry(wrappedMsg, h.logger) {
				return nil // Subscription closed
			}
		}
	}
}

// Compile-time checks
var _ transport.Transport = (*Transport)(nil)
var _ transport.HealthChecker = (*Transport)(nil)
var _ transport.LagMonitor = (*Transport)(nil)
var _ transport.Subscription = (*subscription)(nil)

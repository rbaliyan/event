package composite

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/base"
	"github.com/rbaliyan/event/v3/transport/codec"
	"github.com/rbaliyan/event/v3/transport/persistent"
)

// subscriptionConfig holds configuration for creating a subscription.
type subscriptionConfig struct {
	ev              *compositeEvent
	eventName       string
	consumerID      string // Stable ID for checkpoint resume; empty = auto-generated
	bufferSize      int
	store           persistent.Store
	checkpointStore persistent.CheckpointStore
	codec           codec.Codec
	pollInterval    time.Duration
	logger          *slog.Logger
}

// subscription implements transport.Subscription with signal-driven
// fetching from the durable store and a poll fallback.
type subscription struct {
	*base.Subscription
	ev              *compositeEvent // Parent event for cleanup on close
	eventName       string
	consumerID      string
	store           persistent.Store
	checkpointStore persistent.CheckpointStore
	codec           codec.Codec
	signalCh        chan struct{} // buffered(1), coalesces multiple signals
	pollInterval    time.Duration
	logger          *slog.Logger
	checkpoint      string
	checkpointMu    sync.RWMutex
	cancel          context.CancelFunc
}

func newSubscription(cfg subscriptionConfig) *subscription {
	subID := transport.NewID()
	consumerID := cfg.consumerID
	if consumerID == "" {
		consumerID = subID
	}
	return &subscription{
		Subscription:    base.NewSubscription(subID, cfg.bufferSize, 0),
		ev:              cfg.ev,
		eventName:       cfg.eventName,
		consumerID:      consumerID,
		store:           cfg.store,
		checkpointStore: cfg.checkpointStore,
		codec:           cfg.codec,
		signalCh:        make(chan struct{}, 1),
		pollInterval:    cfg.pollInterval,
		logger:          cfg.logger,
	}
}

func (s *subscription) getCheckpoint() string {
	s.checkpointMu.RLock()
	defer s.checkpointMu.RUnlock()
	return s.checkpoint
}

func (s *subscription) setCheckpoint(cp string) {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.checkpoint = cp
}

// start launches background goroutines for fetching and signal forwarding.
func (s *subscription) start(ctx context.Context, signalSub transport.Subscription) {
	subCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// Start fetch loop
	s.WaitGroup().Add(1)
	go func() {
		defer s.WaitGroup().Done()
		s.fetchLoop(subCtx)
	}()

	// Start signal forwarder if signal subscription available
	if signalSub != nil {
		s.WaitGroup().Add(1)
		go func() {
			defer s.WaitGroup().Done()
			s.signalForwarder(subCtx, signalSub)
		}()
	}
}

// Close stops the subscription and waits for goroutines to exit.
func (s *subscription) Close(ctx context.Context) error {
	return s.Subscription.Close(func() error {
		if s.ev != nil {
			s.ev.subs.Delete(s.ID())
			atomic.AddInt64(&s.ev.subCount, -1)
		}
		if s.cancel != nil {
			s.cancel()
		}
		return nil
	})
}

// fetchLoop waits for signals or poll timer, then drains messages from the store.
func (s *subscription) fetchLoop(ctx context.Context) {
	pollTimer := time.NewTimer(s.pollInterval)
	defer pollTimer.Stop()

	backoff := base.NewBackoffWithConfig(s.pollInterval, 30*time.Second, 0.1)

	for {
		// Wait for signal or poll timer
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		case <-s.signalCh:
			// Signal received: immediately fetch
		case <-pollTimer.C:
			// Poll fallback: check for missed messages
		}

		// Drain all available messages from the store
		fetched := s.drainStore(ctx, backoff)

		// Reset poll timer
		if !pollTimer.Stop() {
			select {
			case <-pollTimer.C:
			default:
			}
		}

		if fetched {
			backoff.Reset()
		}
		pollTimer.Reset(s.pollInterval)
	}
}

// drainStore fetches and delivers all available messages from the store.
// Returns true if at least one message was fetched.
func (s *subscription) drainStore(ctx context.Context, backoff *base.Backoff) bool {
	fetched := false

	for {
		select {
		case <-s.ClosedCh():
			return fetched
		case <-ctx.Done():
			return fetched
		default:
		}

		stored, err := s.store.Fetch(ctx, s.eventName, s.getCheckpoint())
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return fetched
			}
			s.logger.Error("store fetch error",
				"event", s.eventName, "error", err)
			backoff.Wait(s.ClosedCh())
			return fetched
		}

		if stored == nil {
			// No more messages available
			return fetched
		}

		fetched = true

		// Decode message
		decoded, err := s.codec.Decode(stored.Data)
		if err != nil {
			s.logger.Error("message decode error",
				"event", s.eventName,
				"seq_id", stored.SequenceID,
				"error", err)
			// Ack to skip bad message
			if ackErr := s.store.Ack(ctx, s.eventName, stored.SequenceID); ackErr != nil {
				s.logger.Error("ack error after decode failure",
					"seq_id", stored.SequenceID, "error", ackErr)
			}
			s.setCheckpoint(stored.SequenceID)
			continue
		}

		// Wrap with ack/nack that updates the store and checkpoint
		seqID := stored.SequenceID
		wrappedMsg := transport.NewMessageWithAck(
			decoded.ID(),
			decoded.Source(),
			decoded.Payload(),
			decoded.Metadata(),
			stored.RetryCount,
			func(err error) error {
				if err == nil {
					// Success: ack and advance checkpoint
					if ackErr := s.store.Ack(ctx, s.eventName, seqID); ackErr != nil {
						return ackErr
					}
					s.setCheckpoint(seqID)
					if s.checkpointStore != nil {
						if saveErr := s.checkpointStore.Save(ctx, s.eventName, s.consumerID, seqID); saveErr != nil {
							s.logger.Error("checkpoint save error",
								"seq_id", seqID, "error", saveErr)
						}
					}
					return nil
				}
				// Failure: nack for redelivery
				return s.store.Nack(ctx, s.eventName, seqID)
			},
		)

		// Blocking send to handler
		select {
		case <-s.ClosedCh():
			return fetched
		case <-ctx.Done():
			return fetched
		case s.Ch() <- wrappedMsg:
			// Delivered
		}
	}
}

// signalForwarder reads from the signal subscription and notifies the
// fetch loop via signalCh. Multiple rapid signals are coalesced into one.
func (s *subscription) signalForwarder(ctx context.Context, signalSub transport.Subscription) {
	defer func() { _ = signalSub.Close(ctx) }()

	for {
		select {
		case <-s.ClosedCh():
			return
		case <-ctx.Done():
			return
		case msg, ok := <-signalSub.Messages():
			if !ok {
				return
			}
			// Ack signal immediately (it's just a notification)
			_ = msg.Ack(nil)

			// Non-blocking send to signalCh (coalesces multiple signals)
			select {
			case s.signalCh <- struct{}{}:
			default:
				// Already has a pending signal
			}
		}
	}
}

// Compile-time check
var _ transport.Subscription = (*subscription)(nil)

package migration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/rbaliyan/event/v3/transport"
)

var _ transport.Subscription = (*mergedSubscription)(nil)

// mergedSubscription fans-in messages from two underlying subscriptions.
type mergedSubscription struct {
	id       string
	ch       chan transport.Message
	closedCh chan struct{}
	closed   int32
	ctx      context.Context
	oldSub   transport.Subscription
	newSub   transport.Subscription
	wg       sync.WaitGroup
}

func newMergedSubscription(ctx context.Context, oldSub, newSub transport.Subscription, bufSize int) *mergedSubscription {
	m := &mergedSubscription{
		id:       transport.NewID(),
		ch:       make(chan transport.Message, bufSize),
		closedCh: make(chan struct{}),
		ctx:      ctx,
		oldSub:   oldSub,
		newSub:   newSub,
	}

	m.wg.Add(2)
	go m.forward(oldSub)
	go m.forward(newSub)

	// Close the output channel when both forwarders exit.
	go func() {
		m.wg.Wait()
		close(m.ch)
	}()

	return m
}

// forward reads from a subscription and sends to the merged channel.
func (m *mergedSubscription) forward(sub transport.Subscription) {
	defer m.wg.Done()
	for {
		select {
		case <-m.closedCh:
			return
		case <-m.ctx.Done():
			return
		case msg, ok := <-sub.Messages():
			if !ok {
				return
			}
			select {
			case <-m.closedCh:
				return
			case <-m.ctx.Done():
				return
			case m.ch <- msg:
			}
		}
	}
}

func (m *mergedSubscription) ID() string {
	return m.id
}

func (m *mergedSubscription) Messages() <-chan transport.Message {
	return m.ch
}

func (m *mergedSubscription) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		return nil
	}
	close(m.closedCh)
	oldErr := m.oldSub.Close(ctx)
	newErr := m.newSub.Close(ctx)
	m.wg.Wait()
	return errors.Join(oldErr, newErr)
}

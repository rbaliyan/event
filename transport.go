package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var _ Message = message{}
var _ Transport = &channelMuxTransport{}
var _ Transport = &singleChannelTransport{}

// Transport errors
var (
	ErrTransportTimeout = errors.New("transport send timeout")
)

// Transport used by events for sending data to subscribers
type Transport interface {
	// Send channel for sending data
	Send() chan<- Message
	// Receive channel for receiving data
	Receive(string) <-chan Message
	// Delete receiving channel
	Delete(string)
	// Close shutdown transport (blocks until all pending messages processed)
	Close() error
}

// message event message sent from publisher to subscriber
type message struct {
	id       string
	source   string
	data     any
	metadata Metadata
	span     trace.SpanContext
}

// channelMuxTransport transport implementation using channels with fan-out
type channelMuxTransport struct {
	status      int32
	inFlight    int64 // atomic counter for in-flight async operations
	buffer      uint
	inChannel   chan Message
	outChannels sync.Map // map[string]chan Message - using sync.Map for lock-free access
	timeout     time.Duration
	onError     func(error)
	logger      *slog.Logger
	done        chan struct{} // signals fan-out goroutine has finished
}

// singleChannelTransport transport implementation that uses single channel for all subscribers
type singleChannelTransport struct {
	status   int32
	inFlight int64 // atomic counter for in-flight async operations
	in       chan Message
	out      chan Message
	timeout  time.Duration
	onError  func(error)
	logger   *slog.Logger
	done     chan struct{}
}

// ID message ID
func (p message) ID() string {
	return p.id
}

// Source message source
func (p message) Source() string {
	return p.source
}

// Metadata message metadata
func (p message) Metadata() Metadata {
	return p.metadata
}

// Payload data
func (p message) Payload() any {
	return p.data
}

// Context create context with data
func (p message) Context() context.Context {
	return trace.ContextWithRemoteSpanContext(context.Background(), p.span)
}

// Status get channel status
func (c *channelMuxTransport) Status() bool {
	return atomic.LoadInt32(&c.status) == 1
}

// Send channel
func (c *channelMuxTransport) Send() chan<- Message {
	if !c.Status() {
		return nil
	}
	return c.inChannel
}

// Receive create new channel for subscriber with id
func (c *channelMuxTransport) Receive(id string) <-chan Message {
	if !c.Status() {
		return nil
	}
	// Check if channel already exists
	if ch, ok := c.outChannels.Load(id); ok {
		return ch.(chan Message)
	}
	// Create new channel
	ch := make(chan Message, c.buffer)
	// Store or load existing (handles race)
	actual, loaded := c.outChannels.LoadOrStore(id, ch)
	if loaded {
		// Another goroutine created it first, close our channel
		close(ch)
		return actual.(chan Message)
	}
	return ch
}

// Delete subscriber with id
func (c *channelMuxTransport) Delete(id string) {
	if !c.Status() {
		return // Transport already closed
	}
	if ch, ok := c.outChannels.LoadAndDelete(id); ok {
		close(ch.(chan Message))
	}
}

// Close stop transport and wait for all pending messages to be processed
func (c *channelMuxTransport) Close() error {
	if atomic.CompareAndSwapInt32(&c.status, 1, 0) {
		if c.inChannel != nil {
			close(c.inChannel)
		}
		// Wait for fan-out goroutine to finish processing all messages
		<-c.done
		// Spin-wait for in-flight operations to complete
		for atomic.LoadInt64(&c.inFlight) > 0 {
			time.Sleep(time.Millisecond)
		}
		// Close all subscriber channels
		c.outChannels.Range(func(key, value interface{}) bool {
			close(value.(chan Message))
			return true
		})
	}
	return nil
}

// sendToSubscriber sends message to a subscriber channel with optional timeout
func (c *channelMuxTransport) sendToSubscriber(id string, ch chan Message, msg Message) {
	defer func() {
		// Recover from send on closed channel - can happen during shutdown
		if r := recover(); r != nil {
			c.logger.Warn("recovered from send on closed channel", "error", r)
		}
	}()

	if c.timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		select {
		case <-ctx.Done():
			err := fmt.Errorf("%w: subscriber %s", ErrTransportTimeout, id)
			c.logger.Warn("timeout sending to subscriber", "subscriber_id", id)
			c.onError(err)
		case ch <- msg:
		}
	} else {
		// Non-blocking check if closed before blocking send
		if !c.Status() {
			return
		}
		ch <- msg
	}
}

// NewChannelTransport create new transport with channels
func NewChannelTransport(opts ...TransportOption) Transport {
	o := newTransportOptions(opts...)

	c := &channelMuxTransport{
		status:    1,
		buffer:    o.bufferSize,
		inChannel: make(chan Message, o.bufferSize),
		timeout:   o.timeout,
		onError:   o.onError,
		logger:    o.logger,
		done:      make(chan struct{}),
	}

	// Capture async setting for goroutine
	async := o.async

	// Start goroutine to fan-out data
	go func() {
		defer close(c.done)

		// Process messages from input channel
		for msg := range c.inChannel {
			c.outChannels.Range(func(key, value interface{}) bool {
				id := key.(string)
				ch := value.(chan Message)
				if async {
					atomic.AddInt64(&c.inFlight, 1)
					go func(id string, ch chan Message, msg Message) {
						defer atomic.AddInt64(&c.inFlight, -1)
						c.sendToSubscriber(id, ch, msg)
					}(id, ch, msg)
				} else {
					c.sendToSubscriber(id, ch, msg)
				}
				return true
			})
		}
	}()

	return c
}

// Status get channel status
func (c *singleChannelTransport) Status() bool {
	return atomic.LoadInt32(&c.status) == 1
}

// Send channel
func (c *singleChannelTransport) Send() chan<- Message {
	if !c.Status() {
		return nil
	}
	return c.in
}

// Receive create new channel for subscriber with id
// For single transport, all subscribers share the same output channel
func (c *singleChannelTransport) Receive(id string) <-chan Message {
	if !c.Status() {
		return nil
	}
	return c.out
}

// Delete ignore delete, single channel does not delete subs
// subs can stop receiving data
func (c *singleChannelTransport) Delete(_ string) {}

// Close stop transport and wait for all pending messages to be processed
func (c *singleChannelTransport) Close() error {
	if atomic.CompareAndSwapInt32(&c.status, 1, 0) {
		if c.in != nil {
			close(c.in)
		}
		// Wait for dispatch goroutine to finish
		<-c.done
		// Spin-wait for in-flight operations
		for atomic.LoadInt64(&c.inFlight) > 0 {
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}

// sendToOut sends message to output channel with optional timeout
func (c *singleChannelTransport) sendToOut(msg Message) {
	defer func() {
		// Recover from send on closed channel - can happen during shutdown
		if r := recover(); r != nil {
			c.logger.Warn("recovered from send on closed channel", "error", r)
		}
	}()

	if c.timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		select {
		case <-ctx.Done():
			c.logger.Warn("timeout sending to output channel")
			c.onError(fmt.Errorf("%w: single transport output", ErrTransportTimeout))
		case c.out <- msg:
		}
	} else {
		if !c.Status() {
			return
		}
		c.out <- msg
	}
}

// NewSingleTransport create new single channel transport
// In single transport, messages are delivered to one subscriber at a time (load balancing)
func NewSingleTransport(opts ...TransportOption) Transport {
	o := newTransportOptions(opts...)

	c := &singleChannelTransport{
		status:  1,
		in:      make(chan Message, o.bufferSize),
		out:     make(chan Message, o.bufferSize),
		timeout: o.timeout,
		onError: o.onError,
		logger:  o.logger,
		done:    make(chan struct{}),
	}

	// Capture async setting for goroutine
	async := o.async

	// Start goroutine to forward messages
	go func() {
		defer func() {
			close(c.out)
			close(c.done)
		}()

		for msg := range c.in {
			if async {
				atomic.AddInt64(&c.inFlight, 1)
				go func(msg Message) {
					defer atomic.AddInt64(&c.inFlight, -1)
					c.sendToOut(msg)
				}(msg)
			} else {
				c.sendToOut(msg)
			}
		}
	}()

	return c
}

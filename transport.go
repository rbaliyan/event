package event

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var _ Message = message{}
var _ Transport = &channelMuxTransport{}
var _ Transport = &singleChannelTransport{}

// Message transport message
type Message interface {
	// ID message ID
	ID() string
	// Source message source
	Source() string
	// Metadata message metadata
	Metadata() Metadata
	// Payload data after unmarshall
	Payload() Data
	// Context create context with data, includes tracing information
	Context() context.Context
}

// Transport used by events for sending data to subscribers
type Transport interface {
	// Send channel for sending data
	Send() chan<- Message
	// Receive channel for receiving data
	Receive(string) <-chan Message
	// Delete receiving channel
	Delete(string)
	// Close shutdown transport
	Close() error
}

// singleChannelTransport transport implementation that uses single channel for send and receive
type singleChannelTransport struct {
	status int32
	in     chan Message
	out    chan Message
}

// message event message sent from publisher to subscriber
type message struct {
	id       string
	source   string
	data     Data
	metadata Metadata
	span     trace.SpanContext
}

// channelMuxTransport transport implementation using channels
type channelMuxTransport struct {
	status      int32
	buffer      uint
	inChannel   chan Message
	outChannels map[string]chan Message
	mutex       sync.RWMutex
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
func (p message) Payload() Data {
	return p.data
}

// Context create context with data
func (p message) Context() context.Context {
	return trace.ContextWithRemoteSpanContext(context.Background(), p.span)
}

func sendWithTimeout(id string, msg Message, ch chan Message, timeout time.Duration) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	select {
	case <-ctx.Done():
		logger.Println("pubTimeout while sending data to:", id)
	case ch <- msg:
	}
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ch, ok := c.outChannels[id]; ok {
		return ch
	}
	ch := make(chan Message, c.buffer)
	c.outChannels[id] = ch
	return ch
}

// Delete subscriber with id
func (c *channelMuxTransport) Delete(id string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ch, ok := c.outChannels[id]; ok {
		delete(c.outChannels, id)
		close(ch)
	}
}

// Close stop transport
func (c *channelMuxTransport) Close() error {
	if atomic.CompareAndSwapInt32(&c.status, 1, 0) {
		if c.inChannel != nil {
			close(c.inChannel)
			c.inChannel = nil
		}
	}
	return nil
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
func (c *singleChannelTransport) Receive(id string) <-chan Message {
	if !c.Status() {
		return nil
	}
	return c.out
}

// Delete ignore delete, single channel does not delete subs
// subs can stop receiving data
func (c *singleChannelTransport) Delete(_ string) {}

// Close stop transport
func (c *singleChannelTransport) Close() error {
	if atomic.CompareAndSwapInt32(&c.status, 1, 0) {
		if c.in != nil {
			close(c.in)
			c.in = nil
		}
	}
	return nil
}

// NewChannelTransport create new transport with channels
// pubTimeout is the pubTimeout used for sending data per subscriber
// buffer is the buffer size for channels
func NewChannelTransport(timeout time.Duration, buffer uint) Transport {
	c := &channelMuxTransport{
		status:      1,
		buffer:      buffer,
		inChannel:   make(chan Message, buffer),
		outChannels: make(map[string]chan Message),
	}

	// Start goroutine to fan-outChannels data
	go func() {
		defer func() {
			c.mutex.Lock()
			// traverse on entire map and close all pending channels
			for _, ch := range c.outChannels {
				close(ch)
			}
			c.mutex.Unlock()
		}()
		// Wait for message and fan outChannels to subscribers
		for msg := range c.inChannel {
			c.mutex.RLock()
			for id, ch := range c.outChannels {
				sendWithTimeout(id, msg, ch, timeout)
			}
			c.mutex.RUnlock()
		}
		atomic.StoreInt32(&c.status, 0)
		c.inChannel = nil
	}()
	return c
}

// NewSingleTransport create new single channel transport
func NewSingleTransport(timeout time.Duration, buffer uint) Transport {
	c := &singleChannelTransport{
		status: 1,
		in:     make(chan Message, buffer),
		out:    make(chan Message, buffer),
	}
	// Start goroutine to fan-outChannels data
	go func() {
		defer func() {
			close(c.out)
		}()
		// Wait for message and fan outChannels to subscribers
		for msg := range c.in {
			sendWithTimeout("", msg, c.out, timeout)
		}
		atomic.StoreInt32(&c.status, 0)
		c.in = nil
	}()
	return c
}

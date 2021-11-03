package event

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

var _ Message = message{}
var _ Transport = &Channel{}

// Message transport message
type Message interface {
	// ID message ID
	ID() string
	// Source message source
	Source()string
	// Metadata message metadata
	Metadata()Metadata
	// Payload data after unmarshall
	Payload()Data
	// Context create context with data
	Context()context.Context
}

// Transport used by events for sending data to subscribers
type Transport interface{
	// Send channel for sending data
	Send()chan <- Message
	// Receive channel for receiving data
	Receive(string)<-chan Message
	// Delete receiving channel
	Delete(string)
	// Close shutdown transport
	Close()error
}

// Channel transport implementation using channels
type Channel struct {
	status int32
	buffer int
	in 			 chan Message
	out          map[string]chan Message
	mutex        sync.RWMutex
}

func sendWithTimeout(id string, msg Message, ch chan Message, timeout time.Duration){
	ctx := context.Background()
	if timeout > 0{
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	select{
	case <-ctx.Done():
	case ch<-msg:
	}
}

// NewChannel create new transport with channels
// timeout is the timeout used for sending data per subscriber
// buffer is the buffer size for channels
func NewChannel(timeout time.Duration, buffer uint)Transport{
	c :=  &Channel{
		status: 1,
		buffer: int(buffer),
		in: make(chan Message, buffer),
		out: make(map[string]chan Message),
	}

	// Start goroutine to fan-out data
	go func(){
		defer func(){
			c.mutex.Lock()
			// traverse on entire map and close all pending channels
			for _, ch := range c.out{
				close(ch)
			}
			c.mutex.Unlock()
		}()
		// Wait for message and fan out to subscribers
		for msg := range c.in{
			c.mutex.RLock()
			for id, ch := range c.out{
				sendWithTimeout(id, msg, ch, timeout)
			}
			c.mutex.RUnlock()
		}
	}()
	return c
}

// Send channel
func(c *Channel)Send()chan <- Message {
	return c.in
}


// Receive create new channel for subscriber with id
func(c *Channel)Receive(id string)<-chan Message {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ch, ok := c.out[id]; ok{
		return ch
	}
	ch := make(chan Message, c.buffer)
	c.out[id] = ch
	return ch
}

// Delete subscriber with id
func(c *Channel)Delete(id string){
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ch, ok := c.out[id]; ok{
		close(ch)
		delete(c.out, id)
	}
}

// Close stop transport
func(c *Channel)Close()error{
	if atomic.CompareAndSwapInt32(&c.status, 1, 0){
		close(c.in)
	}
	return nil
}

// message event message sent from publisher to subscriber
type message struct {
	id       string
	source   string
	data     Data
	metadata Metadata
	span     trace.SpanContext
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

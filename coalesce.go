package event

import (
	"container/list"
	"log/slog"

	"github.com/rbaliyan/event/v3/transport/message"
)

// coalesceEntry holds a pending message and its decoded value for post-decode coalescing.
type coalesceEntry[T any] struct {
	msg   message.Message
	value T
	count int // number of superseded messages (for context metadata)
}

// rawCoalesceEntry holds a pending message for pre-decode (metadata-based) coalescing.
type rawCoalesceEntry struct {
	msg   message.Message
	count int
}

// coalescer implements a single-goroutine, channel-based message coalescer.
// It groups messages by key, supersedes pending messages with newer ones,
// and delivers only the latest per key to consumers.
//
// The coalescer runs a single goroutine that owns all mutable state (no mutex needed
// on the hot path). Communication happens via channels.
//
// Type parameter T is used for post-decode coalescing (WithCoalesceByKey).
// For pre-decode coalescing (WithCoalesceByMetadata), use rawCoalescer instead.
type coalescer[T any] struct {
	// incoming receives new decoded messages from the ingestion side.
	incoming chan coalesceInput[T]

	// output delivers the next coalesced message to the consumer.
	output chan coalesceOutput[T]

	// done signals that the handler finished processing a key.
	done chan string

	// stop signals shutdown.
	stop chan struct{}

	// stopped is closed when the run goroutine exits.
	stopped chan struct{}

	maxKeys int
	logger  *slog.Logger
}

type coalesceInput[T any] struct {
	key   string
	msg   message.Message
	value T
}

type coalesceOutput[T any] struct {
	key   string
	msg   message.Message
	value T
	count int // how many messages were superseded
}

func newCoalescer[T any](maxKeys int, logger *slog.Logger) *coalescer[T] {
	c := &coalescer[T]{
		incoming: make(chan coalesceInput[T], 64),
		output:   make(chan coalesceOutput[T]),
		done:     make(chan string, 64),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
		maxKeys:  maxKeys,
		logger:   logger,
	}
	go c.run()
	return c
}

// run is the single goroutine that owns all coalescer state.
func (c *coalescer[T]) run() {
	defer close(c.stopped)
	defer close(c.output)

	pending := make(map[string]*coalesceEntry[T])   // key -> latest entry
	inflight := make(map[string]bool)                // keys currently being handled
	order := list.New()                              // LRU order for eviction
	orderIndex := make(map[string]*list.Element)     // key -> list element

	// ready returns the next deliverable entry (key not in-flight), or nil.
	ready := func() (string, *coalesceEntry[T]) {
		for key, entry := range pending {
			if !inflight[key] {
				return key, entry
			}
		}
		return "", nil
	}

	for {
		// Try to find a ready entry to deliver.
		key, entry := ready()

		if entry != nil {
			// We have something to deliver. Select between delivering and accepting input.
			select {
			case <-c.stop:
				c.drainPending(pending)
				return

			case in, ok := <-c.incoming:
				if !ok {
					c.drainPending(pending)
					return
				}
				c.handleInput(pending, order, orderIndex, inflight, in)

			case c.output <- coalesceOutput[T]{key: key, msg: entry.msg, value: entry.value, count: entry.count}:
				delete(pending, key)
				if elem, ok := orderIndex[key]; ok {
					order.Remove(elem)
					delete(orderIndex, key)
				}
				inflight[key] = true

			case doneKey := <-c.done:
				delete(inflight, doneKey)
			}
		} else {
			// Nothing ready to deliver. Wait for input or a done signal.
			select {
			case <-c.stop:
				c.drainPending(pending)
				return

			case in, ok := <-c.incoming:
				if !ok {
					c.drainPending(pending)
					return
				}
				c.handleInput(pending, order, orderIndex, inflight, in)

			case doneKey := <-c.done:
				delete(inflight, doneKey)
			}
		}
	}
}

// handleInput processes an incoming message, superseding any existing entry for the same key.
func (c *coalescer[T]) handleInput(
	pending map[string]*coalesceEntry[T],
	order *list.List,
	orderIndex map[string]*list.Element,
	inflight map[string]bool,
	in coalesceInput[T],
) {
	key := in.key

	// Handle empty key: bypass coalescing, deliver directly.
	// Use unique key based on message ID to avoid overwriting.
	if key == "" {
		uniqueKey := "__nokey__" + in.msg.ID()
		pending[uniqueKey] = &coalesceEntry[T]{msg: in.msg, value: in.value, count: 0}
		elem := order.PushBack(uniqueKey)
		orderIndex[uniqueKey] = elem
		return
	}

	if old, exists := pending[key]; exists {
		// Supersede: ack the old message, keep the new one.
		_ = old.msg.Ack(nil)
		pending[key] = &coalesceEntry[T]{msg: in.msg, value: in.value, count: old.count + 1}
		// Move to back of LRU.
		if elem, ok := orderIndex[key]; ok {
			order.MoveToBack(elem)
		}
	} else {
		pending[key] = &coalesceEntry[T]{msg: in.msg, value: in.value, count: 0}
		elem := order.PushBack(key)
		orderIndex[key] = elem
	}

	// Evict oldest non-inflight entries if over capacity.
	for len(pending) > c.maxKeys {
		// Find the oldest non-inflight key to evict.
		var evictElem *list.Element
		for elem := order.Front(); elem != nil; elem = elem.Next() {
			if !inflight[elem.Value.(string)] {
				evictElem = elem
				break
			}
		}
		if evictElem == nil {
			// All pending keys are inflight — allow temporary over-capacity.
			break
		}

		evictKey := evictElem.Value.(string)
		order.Remove(evictElem)
		delete(orderIndex, evictKey)

		if entry, ok := pending[evictKey]; ok {
			c.logger.Warn("coalescer evicting entry due to max keys exceeded",
				"key", evictKey, "max_keys", c.maxKeys)
			_ = entry.msg.Ack(nil)
			delete(pending, evictKey)
		}
	}
}

// drainPending acks all remaining pending messages on shutdown.
func (c *coalescer[T]) drainPending(pending map[string]*coalesceEntry[T]) {
	count := 0
	for _, entry := range pending {
		if entry != nil && entry.msg != nil {
			_ = entry.msg.Ack(nil)
			count++
		}
	}
	if count > 0 {
		c.logger.Info("coalescer shutdown, auto-acked pending messages", "count", count)
	}
}

// Close signals the coalescer to shut down and waits for it to finish.
func (c *coalescer[T]) Close() {
	select {
	case <-c.stop:
		// Already stopped.
	default:
		close(c.stop)
	}
	<-c.stopped
}

// rawCoalescer is the pre-decode variant that coalesces by metadata key.
// It operates on raw messages without decoding payloads.
type rawCoalescer struct {
	incoming chan rawCoalesceInput
	output   chan rawCoalesceOutput
	done     chan string
	stop     chan struct{}
	stopped  chan struct{}
	maxKeys  int
	metaKey  string
	logger   *slog.Logger
}

type rawCoalesceInput struct {
	msg message.Message
}

type rawCoalesceOutput struct {
	key   string
	msg   message.Message
	count int
}

func newRawCoalescer(metaKey string, maxKeys int, logger *slog.Logger) *rawCoalescer {
	c := &rawCoalescer{
		incoming: make(chan rawCoalesceInput, 64),
		output:   make(chan rawCoalesceOutput),
		done:     make(chan string, 64),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
		maxKeys:  maxKeys,
		metaKey:  metaKey,
		logger:   logger,
	}
	go c.run()
	return c
}

func (c *rawCoalescer) run() {
	defer close(c.stopped)
	defer close(c.output)

	pending := make(map[string]*rawCoalesceEntry)
	inflight := make(map[string]bool)
	order := list.New()
	orderIndex := make(map[string]*list.Element)
	ready := func() (string, *rawCoalesceEntry) {
		for key, entry := range pending {
			if !inflight[key] {
				return key, entry
			}
		}
		return "", nil
	}

	for {
		key, entry := ready()

		if entry != nil {
			select {
			case <-c.stop:
				c.drainPending(pending)
				return

			case in, ok := <-c.incoming:
				if !ok {
					c.drainPending(pending)
					return
				}
				c.handleInput(pending, order, orderIndex, inflight, in)

			case c.output <- rawCoalesceOutput{key: key, msg: entry.msg, count: entry.count}:
				delete(pending, key)
				if elem, ok := orderIndex[key]; ok {
					order.Remove(elem)
					delete(orderIndex, key)
				}
				inflight[key] = true

			case doneKey := <-c.done:
				delete(inflight, doneKey)
			}
		} else {
			select {
			case <-c.stop:
				c.drainPending(pending)
				return

			case in, ok := <-c.incoming:
				if !ok {
					c.drainPending(pending)
					return
				}
				c.handleInput(pending, order, orderIndex, inflight, in)

			case doneKey := <-c.done:
				delete(inflight, doneKey)
			}
		}
	}
}

func (c *rawCoalescer) handleInput(
	pending map[string]*rawCoalesceEntry,
	order *list.List,
	orderIndex map[string]*list.Element,
	inflight map[string]bool,
	in rawCoalesceInput,
) {
	key := in.msg.Metadata()[c.metaKey]

	if key == "" {
		// No coalesce key in metadata — store with message ID to bypass coalescing.
		uniqueKey := "__nokey__" + in.msg.ID()
		pending[uniqueKey] = &rawCoalesceEntry{msg: in.msg, count: 0}
		elem := order.PushBack(uniqueKey)
		orderIndex[uniqueKey] = elem
		return
	}

	if old, exists := pending[key]; exists {
		_ = old.msg.Ack(nil)
		pending[key] = &rawCoalesceEntry{msg: in.msg, count: old.count + 1}
		if elem, ok := orderIndex[key]; ok {
			order.MoveToBack(elem)
		}
	} else {
		pending[key] = &rawCoalesceEntry{msg: in.msg, count: 0}
		elem := order.PushBack(key)
		orderIndex[key] = elem
	}

	// Evict oldest non-inflight entries if over capacity.
	for len(pending) > c.maxKeys {
		var evictElem *list.Element
		for elem := order.Front(); elem != nil; elem = elem.Next() {
			if !inflight[elem.Value.(string)] {
				evictElem = elem
				break
			}
		}
		if evictElem == nil {
			break
		}

		evictKey := evictElem.Value.(string)
		order.Remove(evictElem)
		delete(orderIndex, evictKey)

		if entry, ok := pending[evictKey]; ok {
			c.logger.Warn("coalescer evicting entry due to max keys exceeded",
				"key", evictKey, "max_keys", c.maxKeys)
			_ = entry.msg.Ack(nil)
			delete(pending, evictKey)
		}
	}
}

func (c *rawCoalescer) drainPending(pending map[string]*rawCoalesceEntry) {
	count := 0
	for _, entry := range pending {
		if entry != nil && entry.msg != nil {
			_ = entry.msg.Ack(nil)
			count++
		}
	}
	if count > 0 {
		c.logger.Info("coalescer shutdown, auto-acked pending messages", "count", count)
	}
}

func (c *rawCoalescer) Close() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.stopped
}

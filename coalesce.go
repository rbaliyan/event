package event

import (
	"container/list"
	"log/slog"
	"sync/atomic"

	"github.com/rbaliyan/event/v3/transport/message"
)

// coalesceEntry holds a pending message for coalescing.
// The msg field is always set. The value field is only used by post-decode coalescing.
type coalesceEntry[T any] struct {
	msg   message.Message
	value T
	count int // number of superseded messages (for context metadata)
}

// baseCoalescer is a generic, single-goroutine message coalescer.
// It groups messages by key, supersedes pending messages with newer ones,
// and delivers only the latest per key to consumers.
//
// All mutable state is owned by a single goroutine (no mutex needed on the hot path).
// Communication happens via channels.
//
// Type parameters:
//   - In: the input type sent to the coalescer
//   - Out: the output type delivered to the consumer
//   - T: the decoded value type stored in entries (use struct{} for raw/pre-decode)
type baseCoalescer[In, Out, T any] struct {
	incoming chan In
	output   chan Out
	done     chan string
	stop     chan struct{}
	stopped  chan struct{}
	maxKeys  int
	logger   *slog.Logger

	// inputsHandled counts handleInput invocations. Used only by tests as a
	// deterministic sync point: after sending to incoming, tests can wait for
	// this counter to advance to know the goroutine has drained the queue.
	inputsHandled atomic.Int64

	// extractKey returns the coalesce key from an input.
	extractKey func(In) string

	// makeEntry creates a new entry from an input.
	makeEntry func(In) *coalesceEntry[T]

	// supersede merges a new input into an existing entry (increments count, acks old).
	supersede func(old *coalesceEntry[T], in In) *coalesceEntry[T]

	// makeOutput builds the output from a key and entry.
	makeOutput func(key string, entry *coalesceEntry[T]) Out
}

func newBaseCoalescer[In, Out, T any](
	maxKeys int,
	logger *slog.Logger,
	extractKey func(In) string,
	makeEntry func(In) *coalesceEntry[T],
	supersede func(old *coalesceEntry[T], in In) *coalesceEntry[T],
	makeOutput func(string, *coalesceEntry[T]) Out,
) *baseCoalescer[In, Out, T] {
	c := &baseCoalescer[In, Out, T]{
		incoming:   make(chan In, 64),
		output:     make(chan Out),
		done:       make(chan string, 64),
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
		maxKeys:    maxKeys,
		logger:     logger,
		extractKey: extractKey,
		makeEntry:  makeEntry,
		supersede:  supersede,
		makeOutput: makeOutput,
	}
	go c.run()
	return c
}

// run is the single goroutine that owns all coalescer state.
func (c *baseCoalescer[In, Out, T]) run() {
	defer close(c.stopped)
	defer close(c.output)

	pending := make(map[string]*coalesceEntry[T])
	inflight := make(map[string]bool)
	order := list.New()
	orderIndex := make(map[string]*list.Element)

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

			case c.output <- c.makeOutput(key, entry):
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
func (c *baseCoalescer[In, Out, T]) handleInput(
	pending map[string]*coalesceEntry[T],
	order *list.List,
	orderIndex map[string]*list.Element,
	inflight map[string]bool,
	in In,
) {
	defer c.inputsHandled.Add(1)
	key := c.extractKey(in)

	// Handle empty key: bypass coalescing, deliver directly.
	if key == "" {
		entry := c.makeEntry(in)
		uniqueKey := "__nokey__" + entry.msg.ID()
		pending[uniqueKey] = entry
		elem := order.PushBack(uniqueKey)
		orderIndex[uniqueKey] = elem
		return
	}

	if old, exists := pending[key]; exists {
		// Supersede: ack the old message, keep the new one.
		_ = old.msg.Ack(nil)
		pending[key] = c.supersede(old, in)
		if elem, ok := orderIndex[key]; ok {
			order.MoveToBack(elem)
		}
	} else {
		pending[key] = c.makeEntry(in)
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

// drainPending acks all remaining pending messages on shutdown.
func (c *baseCoalescer[In, Out, T]) drainPending(pending map[string]*coalesceEntry[T]) {
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
func (c *baseCoalescer[In, Out, T]) Close() {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	<-c.stopped
}

// --- Post-decode coalescer (WithCoalesceByKey) ---

type coalesceInput[T any] struct {
	key   string
	msg   message.Message
	value T
}

type coalesceOutput[T any] struct {
	key   string
	msg   message.Message
	value T
	count int
}

type coalescer[T any] struct {
	*baseCoalescer[coalesceInput[T], coalesceOutput[T], T]
}

func newCoalescer[T any](maxKeys int, logger *slog.Logger) *coalescer[T] {
	return &coalescer[T]{
		baseCoalescer: newBaseCoalescer[coalesceInput[T], coalesceOutput[T], T](
			maxKeys, logger,
			func(in coalesceInput[T]) string { return in.key },
			func(in coalesceInput[T]) *coalesceEntry[T] {
				return &coalesceEntry[T]{msg: in.msg, value: in.value, count: 0}
			},
			func(old *coalesceEntry[T], in coalesceInput[T]) *coalesceEntry[T] {
				return &coalesceEntry[T]{msg: in.msg, value: in.value, count: old.count + 1}
			},
			func(key string, e *coalesceEntry[T]) coalesceOutput[T] {
				return coalesceOutput[T]{key: key, msg: e.msg, value: e.value, count: e.count}
			},
		),
	}
}

// --- Pre-decode coalescer (WithCoalesceByMetadata) ---

type rawCoalesceInput struct {
	msg message.Message
}

type rawCoalesceOutput struct {
	key   string
	msg   message.Message
	count int
}

type rawCoalescer struct {
	*baseCoalescer[rawCoalesceInput, rawCoalesceOutput, struct{}]
}

func newRawCoalescer(metaKey string, maxKeys int, logger *slog.Logger) *rawCoalescer {
	return &rawCoalescer{
		baseCoalescer: newBaseCoalescer[rawCoalesceInput, rawCoalesceOutput, struct{}](
			maxKeys, logger,
			func(in rawCoalesceInput) string { return in.msg.Metadata()[metaKey] },
			func(in rawCoalesceInput) *coalesceEntry[struct{}] {
				return &coalesceEntry[struct{}]{msg: in.msg, count: 0}
			},
			func(old *coalesceEntry[struct{}], in rawCoalesceInput) *coalesceEntry[struct{}] {
				return &coalesceEntry[struct{}]{msg: in.msg, count: old.count + 1}
			},
			func(key string, e *coalesceEntry[struct{}]) rawCoalesceOutput {
				return rawCoalesceOutput{key: key, msg: e.msg, count: e.count}
			},
		),
	}
}

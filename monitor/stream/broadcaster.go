// Package stream provides real-time streaming infrastructure for monitor entries.
package stream

import (
	"context"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
)

// DefaultPollInterval is the default interval for polling the store.
const DefaultPollInterval = 100 * time.Millisecond

// Subscriber represents a subscriber to monitor entry updates.
type Subscriber struct {
	id      string
	filter  monitor.Filter
	entries chan *monitor.Entry
	done    chan struct{}
	closed  bool
	mu      sync.Mutex
}

// Entries returns the channel for receiving entries.
func (s *Subscriber) Entries() <-chan *monitor.Entry {
	return s.entries
}

// Close closes the subscriber.
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
}

// Broadcaster provides real-time streaming of monitor entries.
// It polls the store for new entries and distributes them to subscribers.
type Broadcaster struct {
	store        monitor.Store
	pollInterval time.Duration
	subscribers  map[string]*Subscriber
	mu           sync.RWMutex
	done         chan struct{}
	wg           sync.WaitGroup
	started      bool
	lastCheck    time.Time
}

// NewBroadcaster creates a new Broadcaster with the given store.
func NewBroadcaster(store monitor.Store, pollInterval time.Duration) *Broadcaster {
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}

	return &Broadcaster{
		store:        store,
		pollInterval: pollInterval,
		subscribers:  make(map[string]*Subscriber),
		done:         make(chan struct{}),
		lastCheck:    time.Now(),
	}
}

// Start begins polling for new entries.
func (b *Broadcaster) Start(ctx context.Context) {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return
	}
	b.started = true
	b.mu.Unlock()

	b.wg.Add(1)
	go b.pollLoop(ctx)
}

// Stop stops the broadcaster.
func (b *Broadcaster) Stop() {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()

	close(b.done)
	b.wg.Wait()

	// Close all subscribers
	b.mu.Lock()
	for _, sub := range b.subscribers {
		sub.Close()
	}
	b.mu.Unlock()
}

// Subscribe creates a new subscriber with the given filter.
func (b *Broadcaster) Subscribe(filter monitor.Filter) *Subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub := &Subscriber{
		id:      generateID(),
		filter:  filter,
		entries: make(chan *monitor.Entry, 100),
		done:    make(chan struct{}),
	}

	b.subscribers[sub.id] = sub
	return sub
}

// Unsubscribe removes a subscriber.
func (b *Broadcaster) Unsubscribe(sub *Subscriber) {
	if sub == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[sub.id]; ok {
		sub.Close()
		delete(b.subscribers, sub.id)
	}
}

func (b *Broadcaster) pollLoop(ctx context.Context) {
	defer b.wg.Done()

	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.done:
			return
		case <-ticker.C:
			b.poll(ctx)
		}
	}
}

func (b *Broadcaster) poll(ctx context.Context) {
	now := time.Now()

	b.mu.RLock()
	if len(b.subscribers) == 0 {
		b.mu.RUnlock()
		b.lastCheck = now
		return
	}
	b.mu.RUnlock()

	// Query for entries since last check
	filter := monitor.Filter{
		StartTime: b.lastCheck,
		OrderDesc: false,
		Limit:     1000,
	}

	page, err := b.store.List(ctx, filter)
	if err != nil {
		return
	}

	b.lastCheck = now

	if len(page.Entries) == 0 {
		return
	}

	b.broadcast(page.Entries)
}

func (b *Broadcaster) broadcast(entries []*monitor.Entry) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, entry := range entries {
		for _, sub := range b.subscribers {
			if matchesFilter(entry, sub.filter) {
				select {
				case sub.entries <- entry:
				default:
					// Channel full, skip entry
				}
			}
		}
	}
}

// matchesFilter checks if an entry matches the given filter.
func matchesFilter(entry *monitor.Entry, filter monitor.Filter) bool {
	if filter.EventID != "" && entry.EventID != filter.EventID {
		return false
	}
	if filter.SubscriptionID != "" && entry.SubscriptionID != filter.SubscriptionID {
		return false
	}
	if filter.EventName != "" && entry.EventName != filter.EventName {
		return false
	}
	if filter.BusID != "" && entry.BusID != filter.BusID {
		return false
	}
	if filter.DeliveryMode != nil && entry.DeliveryMode != *filter.DeliveryMode {
		return false
	}
	if len(filter.Status) > 0 {
		found := false
		for _, s := range filter.Status {
			if entry.Status == s {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if filter.HasError != nil {
		hasErr := entry.Error != ""
		if *filter.HasError != hasErr {
			return false
		}
	}
	if !filter.StartTime.IsZero() && entry.StartedAt.Before(filter.StartTime) {
		return false
	}
	if !filter.EndTime.IsZero() && !entry.StartedAt.Before(filter.EndTime) {
		return false
	}
	if filter.MinDuration > 0 && entry.Duration < filter.MinDuration {
		return false
	}
	if filter.MinRetries > 0 && entry.RetryCount < filter.MinRetries {
		return false
	}

	return true
}

var (
	idCounter uint64
	idMu      sync.Mutex
)

func generateID() string {
	idMu.Lock()
	defer idMu.Unlock()
	idCounter++
	return time.Now().Format("20060102150405") + "-" + string(rune('a'+idCounter%26))
}

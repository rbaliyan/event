package event

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport/channel"
)

// mockIdempotencyStore implements IdempotencyStore for testing
type mockIdempotencyStore struct {
	mu        sync.Mutex
	processed map[string]bool
}

func newMockIdempotencyStore() *mockIdempotencyStore {
	return &mockIdempotencyStore{processed: make(map[string]bool)}
}

func (s *mockIdempotencyStore) IsDuplicate(ctx context.Context, messageID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processed[messageID], nil
}

func (s *mockIdempotencyStore) MarkProcessed(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processed[messageID] = true
	return nil
}

// mockPoisonDetector implements PoisonDetector for testing
type mockPoisonDetector struct {
	mu          sync.Mutex
	failures    map[string]int
	quarantined map[string]bool
	threshold   int
}

func newMockPoisonDetector(threshold int) *mockPoisonDetector {
	return &mockPoisonDetector{
		failures:    make(map[string]int),
		quarantined: make(map[string]bool),
		threshold:   threshold,
	}
}

func (d *mockPoisonDetector) Check(ctx context.Context, messageID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.quarantined[messageID], nil
}

func (d *mockPoisonDetector) RecordFailure(ctx context.Context, messageID string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failures[messageID]++
	if d.failures[messageID] >= d.threshold {
		d.quarantined[messageID] = true
		return true, nil
	}
	return false, nil
}

func (d *mockPoisonDetector) RecordSuccess(ctx context.Context, messageID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.failures, messageID)
	return nil
}

// mockMonitorStore implements MonitorStore for testing
type mockMonitorStore struct {
	mu      sync.Mutex
	started map[string]bool
	status  map[string]string
}

func newMockMonitorStore() *mockMonitorStore {
	return &mockMonitorStore{
		started: make(map[string]bool),
		status:  make(map[string]string),
	}
}

func (s *mockMonitorStore) RecordStart(ctx context.Context, params RecordStartParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started[params.EventID] = true
	return nil
}

func (s *mockMonitorStore) RecordComplete(ctx context.Context, params RecordCompleteParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[params.EventID] = params.Status
	return nil
}

func (s *mockMonitorStore) wasRecorded(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started[eventID]
}

// mockSchemaProvider implements SchemaProvider for testing
type mockSchemaProvider struct {
	mu      sync.RWMutex
	schemas map[string]*EventSchema
}

func newMockSchemaProvider() *mockSchemaProvider {
	return &mockSchemaProvider{
		schemas: make(map[string]*EventSchema),
	}
}

func (p *mockSchemaProvider) Get(ctx context.Context, eventName string) (*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if schema, ok := p.schemas[eventName]; ok {
		// Return a copy
		copy := *schema
		return &copy, nil
	}
	return nil, nil
}

func (p *mockSchemaProvider) Set(ctx context.Context, schema *EventSchema) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.schemas[schema.Name] = schema
	return nil
}

func (p *mockSchemaProvider) Delete(ctx context.Context, eventName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.schemas, eventName)
	return nil
}

func (p *mockSchemaProvider) Watch(ctx context.Context) (<-chan SchemaChangeEvent, error) {
	ch := make(chan SchemaChangeEvent, 100)
	return ch, nil
}

func (p *mockSchemaProvider) List(ctx context.Context) ([]*EventSchema, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*EventSchema, 0, len(p.schemas))
	for _, schema := range p.schemas {
		copy := *schema
		result = append(result, &copy)
	}
	return result, nil
}

func (p *mockSchemaProvider) Close() error {
	return nil
}

// TestBusLevelIdempotency verifies that bus-level idempotency automatically skips duplicates
func TestBusLevelIdempotency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idempStore := newMockIdempotencyStore()

	bus := mustNewBus(t, "test-idemp-bus-"+randomString(5),
		WithTransport(channel.New()),
		WithIdempotency(idempStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	// Publish same message twice (same event ID via context)
	msgCtx := ContextWithEventID(ctx, "msg-123")
	ev.Publish(msgCtx, "hello")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")

	ev.Publish(msgCtx, "hello again")
	consistentlyEqInt32(t, 100*time.Millisecond, &callCount, 1, "duplicate publish must not be processed")
}

// TestBusLevelPoisonDetection verifies that bus-level poison detection skips quarantined messages
func TestBusLevelPoisonDetection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	poisonDetector := newMockPoisonDetector(2) // quarantine after 2 failures

	bus := mustNewBus(t, "test-poison-bus-"+randomString(5),
		WithTransport(channel.New()),
		WithPoisonDetection(poisonDetector),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.poison.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return errors.New("always fails")
	})

	// Publish message 3 times with same ID
	msgCtx := ContextWithEventID(ctx, "poison-msg-456")
	ev.Publish(msgCtx, "first")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")

	ev.Publish(msgCtx, "second")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 2, "second publish should trip the quarantine threshold")

	ev.Publish(msgCtx, "third") // should be skipped (quarantined)
	consistentlyEqInt32(t, 100*time.Millisecond, &callCount, 2, "quarantined message must not be processed")

	// Verify message is quarantined
	poisonDetector.mu.Lock()
	isQuarantined := poisonDetector.quarantined["poison-msg-456"]
	poisonDetector.mu.Unlock()
	if !isQuarantined {
		t.Error("expected message to be quarantined")
	}
}

// TestBusLevelMiddlewareCombined verifies both idempotency and poison detection work together
func TestBusLevelMiddlewareCombined(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	idempStore := newMockIdempotencyStore()
	poisonDetector := newMockPoisonDetector(3)

	bus := mustNewBus(t, "test-combined-bus-"+randomString(5),
		WithTransport(channel.New()),
		WithIdempotency(idempStore),
		WithPoisonDetection(poisonDetector),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.combined.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil // success
	})

	// Publish two different messages
	ev.Publish(ContextWithEventID(ctx, "msg-1"), "first")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")

	ev.Publish(ContextWithEventID(ctx, "msg-2"), "second")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 2, "second publish should be processed")

	// Publish duplicate of first message
	ev.Publish(ContextWithEventID(ctx, "msg-1"), "duplicate")
	consistentlyEqInt32(t, 100*time.Millisecond, &callCount, 2, "duplicate publish must not be processed")
}

// TestSchemaLoadingOnRegister verifies that schemas are loaded when events are registered
func TestSchemaLoadingOnRegister(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()

	// Pre-register a schema
	provider.Set(ctx, &EventSchema{
		Name:              "order.created",
		Version:           1,
		SubTimeout:        5 * time.Second,
		MaxRetries:        3,
		EnableMonitor:     true,
		EnableIdempotency: true,
		EnablePoison:      false,
	})

	bus := mustNewBus(t, "test-schema-load-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	ev := New[string]("order.created")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// Verify schema was loaded by checking internal state
	impl := ev.(*eventImpl[string])
	if !impl.schema.loaded {
		t.Error("expected schema to be loaded")
	}
	if !impl.schema.enableMonitor {
		t.Error("expected enableMonitor to be true")
	}
	if !impl.schema.enableIdempotency {
		t.Error("expected enableIdempotency to be true")
	}
	if impl.schema.enablePoison {
		t.Error("expected enablePoison to be false")
	}
	if impl.subTimeout != 5*time.Second {
		t.Errorf("expected subTimeout 5s, got %v", impl.subTimeout)
	}
	if impl.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", impl.maxRetries)
	}
}

// TestSchemaNotFoundFallback verifies that missing schema doesn't prevent registration
func TestSchemaNotFoundFallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()
	// Don't pre-register any schema

	bus := mustNewBus(t, "test-schema-notfound-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	ev := New[string]("order.created")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	// Verify schema was not loaded
	impl := ev.(*eventImpl[string])
	if impl.schema.loaded {
		t.Error("expected schema NOT to be loaded")
	}
}

// TestSchemaControlsMiddleware verifies that schema flags control which middleware is applied
func TestSchemaControlsMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("schema enables only monitor", func(t *testing.T) {
		provider := newMockSchemaProvider()
		provider.Set(ctx, &EventSchema{
			Name:              "test.event",
			Version:           1,
			EnableMonitor:     true,
			EnableIdempotency: false,
			EnablePoison:      false,
		})

		idempStore := newMockIdempotencyStore()
		poisonDetector := newMockPoisonDetector(2)
		monitorStore := newMockMonitorStore()

		bus := mustNewBus(t, "test-schema-monitor-"+randomString(5),
			WithTransport(channel.New()),
			WithSchemaProvider(provider),
			WithIdempotency(idempStore),
			WithPoisonDetection(poisonDetector),
			WithMonitor(monitorStore),
		)
		defer bus.Close(ctx)

		ev := New[string]("test.event")
		if err := Register(ctx, bus, ev); err != nil {
			t.Fatal(err)
		}

		var received atomic.Bool
		ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
			received.Store(true)
			return nil
		})

		msgID := "test-msg-" + randomString(5)
		ev.Publish(ContextWithEventID(ctx, msgID), "hello")
		eventuallyTrue(t, 2*time.Second, received.Load, "handler should have been called")

		// Monitor should have recorded (enabled in schema)
		eventuallyTrue(t, 2*time.Second, func() bool { return monitorStore.wasRecorded(msgID) },
			"monitor should have recorded the event")

		// Idempotency store should NOT have been called (disabled in schema)
		idempStore.mu.Lock()
		_, wasProcessed := idempStore.processed[msgID]
		idempStore.mu.Unlock()
		if wasProcessed {
			t.Error("idempotency should NOT have been applied (disabled in schema)")
		}
	})

	t.Run("schema enables only idempotency", func(t *testing.T) {
		provider := newMockSchemaProvider()
		provider.Set(ctx, &EventSchema{
			Name:              "test.idemp.event",
			Version:           1,
			EnableMonitor:     false,
			EnableIdempotency: true,
			EnablePoison:      false,
		})

		idempStore := newMockIdempotencyStore()
		monitorStore := newMockMonitorStore()

		bus := mustNewBus(t, "test-schema-idemp-"+randomString(5),
			WithTransport(channel.New()),
			WithSchemaProvider(provider),
			WithIdempotency(idempStore),
			WithMonitor(monitorStore),
		)
		defer bus.Close(ctx)

		ev := New[string]("test.idemp.event")
		if err := Register(ctx, bus, ev); err != nil {
			t.Fatal(err)
		}

		var callCount atomic.Int32
		ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
			callCount.Add(1)
			return nil
		})

		msgID := "test-msg-" + randomString(5)
		// Publish same message twice
		ev.Publish(ContextWithEventID(ctx, msgID), "hello")
		eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")
		ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
		consistentlyEqInt32(t, 100*time.Millisecond, &callCount, 1, "duplicate must be skipped by idempotency")

		// Monitor should NOT have recorded (disabled in schema)
		if monitorStore.wasRecorded(msgID) {
			t.Error("monitor should NOT have recorded (disabled in schema)")
		}
	})
}

// TestNoSchemaFallbackToBusMiddleware verifies middleware is applied when no schema exists
func TestNoSchemaFallbackToBusMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()
	// No schema registered

	idempStore := newMockIdempotencyStore()
	monitorStore := newMockMonitorStore()

	bus := mustNewBus(t, "test-no-schema-fallback-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
		WithIdempotency(idempStore),
		WithMonitor(monitorStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("unregistered.event")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	msgID := "test-msg-" + randomString(5)
	// Publish same message twice
	ev.Publish(ContextWithEventID(ctx, msgID), "hello")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")
	ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
	consistentlyEqInt32(t, 100*time.Millisecond, &callCount, 1, "fallback idempotency must skip duplicate")

	// Monitor should have recorded (fallback behavior)
	eventuallyTrue(t, 2*time.Second, func() bool { return monitorStore.wasRecorded(msgID) },
		"monitor should have recorded (fallback behavior)")
}

// TestSchemaDisablesAllMiddleware verifies that schema can disable all middleware
func TestSchemaDisablesAllMiddleware(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:              "test.no.middleware",
		Version:           1,
		EnableMonitor:     false,
		EnableIdempotency: false,
		EnablePoison:      false,
	})

	idempStore := newMockIdempotencyStore()
	poisonDetector := newMockPoisonDetector(2)
	monitorStore := newMockMonitorStore()

	bus := mustNewBus(t, "test-schema-disable-all-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
		WithIdempotency(idempStore),
		WithPoisonDetection(poisonDetector),
		WithMonitor(monitorStore),
	)
	defer bus.Close(ctx)

	ev := New[string]("test.no.middleware")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	var callCount atomic.Int32
	ev.Subscribe(ctx, func(ctx context.Context, e Event[string], data string) error {
		callCount.Add(1)
		return nil
	})

	msgID := "test-msg-" + randomString(5)
	// Publish same message twice
	ev.Publish(ContextWithEventID(ctx, msgID), "hello")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 1, "first publish should be processed")
	ev.Publish(ContextWithEventID(ctx, msgID), "hello again")
	eventuallyEqInt32(t, 2*time.Second, &callCount, 2, "duplicate must be processed when all middleware disabled")

	// Monitor should NOT have recorded
	if monitorStore.wasRecorded(msgID) {
		t.Error("monitor should NOT have recorded (disabled in schema)")
	}

	// Idempotency should NOT have been applied
	idempStore.mu.Lock()
	_, wasProcessed := idempStore.processed[msgID]
	idempStore.mu.Unlock()
	if wasProcessed {
		t.Error("idempotency should NOT have been applied (disabled in schema)")
	}
}

// TestSchemaTimeoutApplied verifies that schema timeout is applied to events
func TestSchemaTimeoutApplied(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:       "test.timeout",
		Version:    1,
		SubTimeout: 100 * time.Millisecond,
	})

	bus := mustNewBus(t, "test-schema-timeout-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	// Event without timeout option - should get timeout from schema
	ev := New[string]("test.timeout")
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	impl := ev.(*eventImpl[string])
	if impl.subTimeout != 100*time.Millisecond {
		t.Errorf("expected subTimeout 100ms from schema, got %v", impl.subTimeout)
	}
}

// TestEventTimeoutOverridesSchema verifies that event-level timeout takes precedence
func TestEventTimeoutOverridesSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := newMockSchemaProvider()
	provider.Set(ctx, &EventSchema{
		Name:       "test.timeout.override",
		Version:    1,
		SubTimeout: 100 * time.Millisecond,
	})

	bus := mustNewBus(t, "test-schema-timeout-override-"+randomString(5),
		WithTransport(channel.New()),
		WithSchemaProvider(provider),
	)
	defer bus.Close(ctx)

	// Event WITH timeout option - should keep event timeout
	ev := New[string]("test.timeout.override", WithSubscriberTimeout(500*time.Millisecond))
	if err := Register(ctx, bus, ev); err != nil {
		t.Fatal(err)
	}

	impl := ev.(*eventImpl[string])
	// Event timeout should be preserved (schema doesn't override existing values)
	if impl.subTimeout != 500*time.Millisecond {
		t.Errorf("expected subTimeout 500ms from event option, got %v", impl.subTimeout)
	}
}

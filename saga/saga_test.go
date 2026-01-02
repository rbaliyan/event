package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// mockStep is a test step implementation
type mockStep struct {
	name           string
	executeCalled  bool
	compensateCalled bool
	executeErr     error
	compensateErr  error
	mu             sync.Mutex
}

func newMockStep(name string) *mockStep {
	return &mockStep{name: name}
}

func (m *mockStep) Name() string {
	return m.name
}

func (m *mockStep) Execute(ctx context.Context, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executeCalled = true
	return m.executeErr
}

func (m *mockStep) Compensate(ctx context.Context, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.compensateCalled = true
	return m.compensateErr
}

func (m *mockStep) wasExecuted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.executeCalled
}

func (m *mockStep) wasCompensated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.compensateCalled
}

func TestStatus(t *testing.T) {
	t.Run("Status constants", func(t *testing.T) {
		if StatusPending != "pending" {
			t.Errorf("expected pending, got %s", StatusPending)
		}
		if StatusRunning != "running" {
			t.Errorf("expected running, got %s", StatusRunning)
		}
		if StatusCompleted != "completed" {
			t.Errorf("expected completed, got %s", StatusCompleted)
		}
		if StatusFailed != "failed" {
			t.Errorf("expected failed, got %s", StatusFailed)
		}
		if StatusCompensating != "compensating" {
			t.Errorf("expected compensating, got %s", StatusCompensating)
		}
		if StatusCompensated != "compensated" {
			t.Errorf("expected compensated, got %s", StatusCompensated)
		}
	})
}

func TestSaga(t *testing.T) {
	ctx := context.Background()

	t.Run("New creates saga", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step2 := newMockStep("step-2")

		s := New("test-saga", step1, step2)

		if s.Name() != "test-saga" {
			t.Errorf("expected name test-saga, got %s", s.Name())
		}
		if len(s.Steps()) != 2 {
			t.Errorf("expected 2 steps, got %d", len(s.Steps()))
		}
	})

	t.Run("Execute runs all steps on success", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step2 := newMockStep("step-2")
		step3 := newMockStep("step-3")

		s := New("test-saga", step1, step2, step3)

		err := s.Execute(ctx, "saga-1", nil)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if !step1.wasExecuted() {
			t.Error("step-1 should have been executed")
		}
		if !step2.wasExecuted() {
			t.Error("step-2 should have been executed")
		}
		if !step3.wasExecuted() {
			t.Error("step-3 should have been executed")
		}

		// No compensations should run on success
		if step1.wasCompensated() {
			t.Error("step-1 should not be compensated")
		}
		if step2.wasCompensated() {
			t.Error("step-2 should not be compensated")
		}
		if step3.wasCompensated() {
			t.Error("step-3 should not be compensated")
		}
	})

	t.Run("Execute compensates on failure", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step2 := newMockStep("step-2")
		step3 := newMockStep("step-3")
		step3.executeErr = errors.New("step-3 failed")

		s := New("test-saga", step1, step2, step3)

		err := s.Execute(ctx, "saga-1", nil)
		if err == nil {
			t.Fatal("expected error")
		}

		// step1 and step2 should be executed
		if !step1.wasExecuted() {
			t.Error("step-1 should have been executed")
		}
		if !step2.wasExecuted() {
			t.Error("step-2 should have been executed")
		}
		if !step3.wasExecuted() {
			t.Error("step-3 should have been executed")
		}

		// step1 and step2 should be compensated (in reverse order)
		if !step1.wasCompensated() {
			t.Error("step-1 should be compensated")
		}
		if !step2.wasCompensated() {
			t.Error("step-2 should be compensated")
		}
		// step3 should NOT be compensated (it failed, wasn't completed)
		if step3.wasCompensated() {
			t.Error("step-3 should not be compensated")
		}
	})

	t.Run("Execute with store persists state", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step2 := newMockStep("step-2")

		store := NewMemoryStore()
		s := New("test-saga", step1, step2).WithStore(store)

		err := s.Execute(ctx, "saga-1", "test-data")
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		state, err := store.Get(ctx, "saga-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if state.Status != StatusCompleted {
			t.Errorf("expected completed, got %s", state.Status)
		}
		if len(state.CompletedSteps) != 2 {
			t.Errorf("expected 2 completed steps, got %d", len(state.CompletedSteps))
		}
	})

	t.Run("Execute with store records failure", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step2 := newMockStep("step-2")
		step2.executeErr = errors.New("step-2 failed")

		store := NewMemoryStore()
		s := New("test-saga", step1, step2).WithStore(store)

		err := s.Execute(ctx, "saga-1", nil)
		if err == nil {
			t.Fatal("expected error")
		}

		state, _ := store.Get(ctx, "saga-1")
		if state.Status != StatusCompensated {
			t.Errorf("expected compensated, got %s", state.Status)
		}
		if state.Error == "" {
			t.Error("expected error to be recorded")
		}
	})

	t.Run("Execute fails when compensation fails", func(t *testing.T) {
		step1 := newMockStep("step-1")
		step1.compensateErr = errors.New("compensation failed")
		step2 := newMockStep("step-2")
		step2.executeErr = errors.New("step-2 failed")

		store := NewMemoryStore()
		s := New("test-saga", step1, step2).WithStore(store)

		err := s.Execute(ctx, "saga-1", nil)
		if err == nil {
			t.Fatal("expected error")
		}

		state, _ := store.Get(ctx, "saga-1")
		if state.Status != StatusFailed {
			t.Errorf("expected failed, got %s", state.Status)
		}
	})

	t.Run("data is passed to steps", func(t *testing.T) {
		var receivedData any
		step := &dataCapturingStep{received: &receivedData}

		s := New("test-saga", step)

		testData := map[string]string{"key": "value"}
		s.Execute(ctx, "saga-1", testData)

		if receivedData == nil {
			t.Fatal("data should be passed to step")
		}
		data := receivedData.(map[string]string)
		if data["key"] != "value" {
			t.Errorf("expected key=value, got %v", data)
		}
	})
}

// dataCapturingStep captures the data passed to it
type dataCapturingStep struct {
	received *any
}

func (s *dataCapturingStep) Name() string {
	return "data-capturing"
}

func (s *dataCapturingStep) Execute(ctx context.Context, data any) error {
	*s.received = data
	return nil
}

func (s *dataCapturingStep) Compensate(ctx context.Context, data any) error {
	return nil
}

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()

	t.Run("Create and Get", func(t *testing.T) {
		store := NewMemoryStore()

		state := &State{
			ID:     "saga-1",
			Name:   "test-saga",
			Status: StatusRunning,
		}

		err := store.Create(ctx, state)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		retrieved, err := store.Get(ctx, "saga-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.ID != "saga-1" {
			t.Errorf("expected ID saga-1, got %s", retrieved.ID)
		}
		if retrieved.Name != "test-saga" {
			t.Errorf("expected name test-saga, got %s", retrieved.Name)
		}
	})

	t.Run("Create duplicate returns error", func(t *testing.T) {
		store := NewMemoryStore()

		state := &State{ID: "saga-1", Name: "test", Status: StatusRunning}
		store.Create(ctx, state)

		err := store.Create(ctx, state)
		if err == nil {
			t.Error("expected error for duplicate")
		}
	})

	t.Run("Get non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()

		_, err := store.Get(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent")
		}
	})

	t.Run("Update modifies state", func(t *testing.T) {
		store := NewMemoryStore()

		state := &State{ID: "saga-1", Name: "test", Status: StatusRunning}
		store.Create(ctx, state)

		state.Status = StatusCompleted
		state.CompletedSteps = []string{"step-1", "step-2"}

		err := store.Update(ctx, state)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		retrieved, _ := store.Get(ctx, "saga-1")
		if retrieved.Status != StatusCompleted {
			t.Errorf("expected completed, got %s", retrieved.Status)
		}
		if len(retrieved.CompletedSteps) != 2 {
			t.Errorf("expected 2 steps, got %d", len(retrieved.CompletedSteps))
		}
	})

	t.Run("Update non-existent returns error", func(t *testing.T) {
		store := NewMemoryStore()

		state := &State{ID: "non-existent", Status: StatusRunning}
		err := store.Update(ctx, state)
		if err == nil {
			t.Error("expected error for non-existent")
		}
	})

	t.Run("List with empty filter returns all", func(t *testing.T) {
		store := NewMemoryStore()

		store.Create(ctx, &State{ID: "saga-1", Name: "order", Status: StatusCompleted})
		store.Create(ctx, &State{ID: "saga-2", Name: "order", Status: StatusFailed})
		store.Create(ctx, &State{ID: "saga-3", Name: "payment", Status: StatusCompleted})

		results, err := store.List(ctx, StoreFilter{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("List with name filter", func(t *testing.T) {
		store := NewMemoryStore()

		store.Create(ctx, &State{ID: "saga-1", Name: "order", Status: StatusCompleted})
		store.Create(ctx, &State{ID: "saga-2", Name: "order", Status: StatusFailed})
		store.Create(ctx, &State{ID: "saga-3", Name: "payment", Status: StatusCompleted})

		results, err := store.List(ctx, StoreFilter{Name: "order"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("expected 2 results, got %d", len(results))
		}
	})

	t.Run("List with status filter", func(t *testing.T) {
		store := NewMemoryStore()

		store.Create(ctx, &State{ID: "saga-1", Name: "order", Status: StatusCompleted})
		store.Create(ctx, &State{ID: "saga-2", Name: "order", Status: StatusFailed})
		store.Create(ctx, &State{ID: "saga-3", Name: "payment", Status: StatusCompleted})

		results, err := store.List(ctx, StoreFilter{
			Status: []Status{StatusFailed},
		})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("expected 1 result, got %d", len(results))
		}
		if results[0].ID != "saga-2" {
			t.Errorf("expected saga-2, got %s", results[0].ID)
		}
	})

	t.Run("List with limit", func(t *testing.T) {
		store := NewMemoryStore()

		for i := 0; i < 10; i++ {
			store.Create(ctx, &State{ID: "saga-" + string(rune('0'+i)), Name: "test", Status: StatusCompleted})
		}

		results, err := store.List(ctx, StoreFilter{Limit: 3})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("expected 3 results, got %d", len(results))
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		store := NewMemoryStore()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(3)

			go func(id int) {
				defer wg.Done()
				store.Create(ctx, &State{
					ID:     "saga-concurrent-" + string(rune('a'+id%26)),
					Name:   "test",
					Status: StatusRunning,
				})
			}(i)

			go func() {
				defer wg.Done()
				store.Get(ctx, "saga-concurrent-a")
			}()

			go func() {
				defer wg.Done()
				store.List(ctx, StoreFilter{})
			}()
		}

		wg.Wait()
	})
}

func TestResume(t *testing.T) {
	ctx := context.Background()

	t.Run("Resume without store returns error", func(t *testing.T) {
		s := New("test-saga", newMockStep("step-1"))

		err := s.Resume(ctx, "saga-1")
		if err == nil {
			t.Error("expected error without store")
		}
	})

	t.Run("Resume non-failed saga returns error", func(t *testing.T) {
		store := NewMemoryStore()
		store.Create(ctx, &State{
			ID:     "saga-1",
			Name:   "test-saga",
			Status: StatusCompleted,
		})

		s := New("test-saga", newMockStep("step-1")).WithStore(store)

		err := s.Resume(ctx, "saga-1")
		if err == nil {
			t.Error("expected error for completed saga")
		}
	})

	t.Run("Resume non-existent saga returns error", func(t *testing.T) {
		store := NewMemoryStore()
		s := New("test-saga", newMockStep("step-1")).WithStore(store)

		err := s.Resume(ctx, "non-existent")
		if err == nil {
			t.Error("expected error for non-existent saga")
		}
	})
}

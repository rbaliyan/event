// Package saga provides saga orchestration for distributed transactions.
//
// Sagas coordinate multiple steps with compensation for rollback on failure.
// This enables reliable distributed transactions without two-phase commit:
//   - Execute steps in order
//   - On failure, compensate completed steps in reverse order
//   - Track saga state for recovery
//
// # Overview
//
// The Saga pattern is used when you need to maintain data consistency across
// multiple services or databases. Unlike traditional distributed transactions
// (2PC), sagas use compensating transactions to undo work when failures occur.
//
// The package provides:
//   - Step interface for defining saga steps and compensations
//   - Saga orchestrator for executing steps in sequence
//   - Store interface for state persistence
//   - RedisStore for distributed deployments
//
// # When to Use Sagas
//
// Use sagas when you need to:
//   - Coordinate operations across multiple services
//   - Maintain consistency without distributed locks
//   - Handle long-running transactions
//   - Recover from partial failures
//
// # Basic Usage
//
// Define steps that implement the Step interface:
//
//	type ReserveInventoryStep struct {
//	    inventoryService *InventoryService
//	}
//
//	func (s *ReserveInventoryStep) Name() string {
//	    return "reserve-inventory"
//	}
//
//	func (s *ReserveInventoryStep) Execute(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.inventoryService.Reserve(ctx, order.ProductID, order.Quantity)
//	}
//
//	func (s *ReserveInventoryStep) Compensate(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.inventoryService.Release(ctx, order.ProductID, order.Quantity)
//	}
//
// Create and execute the saga:
//
//	orderSaga := saga.New("order-creation",
//	    &CreateOrderStep{orderService},
//	    &ReserveInventoryStep{inventoryService},
//	    &ProcessPaymentStep{paymentService},
//	    &SendConfirmationStep{emailService},
//	)
//
//	// With persistence for recovery
//	orderSaga = orderSaga.WithStore(saga.NewRedisStore(redisClient))
//
//	sagaID := uuid.New().String()
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    // Saga failed - compensations were automatically run
//	    log.Error("order creation failed", "saga_id", sagaID, "error", err)
//	}
//
// # Compensation Behavior
//
// When a step fails:
//  1. The saga stops executing forward
//  2. Compensations run in reverse order (LIFO)
//  3. All completed steps are compensated, even if some compensations fail
//  4. The saga status becomes "compensated" or "failed" (if compensation failed)
//
// # State Persistence
//
// Using a store enables:
//   - Recovery of failed sagas after application restart
//   - Visibility into saga state for debugging
//   - Retry of failed sagas with Resume()
//
// Example with Redis store:
//
//	store := saga.NewRedisStore(redisClient).WithTTL(7 * 24 * time.Hour)
//	orderSaga := saga.New("order-creation", steps...).WithStore(store)
//
// # Best Practices
//
//   - Keep steps idempotent - they may be retried
//   - Implement compensations that can be called multiple times safely
//   - Log step execution for debugging
//   - Set appropriate TTLs for saga state
//   - Monitor failed sagas and set up alerts
package saga

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Step represents a single step in a saga.
//
// Each step must provide:
//   - Name: For identification in logs and state tracking
//   - Execute: The forward action to perform
//   - Compensate: The reverse action to undo Execute
//
// Guidelines for implementing steps:
//   - Make Execute idempotent if possible (safe to retry)
//   - Make Compensate idempotent (may be called multiple times)
//   - Handle partial failures in Compensate gracefully
//   - Log actions for debugging
//
// Example:
//
//	type PaymentStep struct {
//	    paymentService *PaymentService
//	}
//
//	func (s *PaymentStep) Name() string {
//	    return "process-payment"
//	}
//
//	func (s *PaymentStep) Execute(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.paymentService.Charge(ctx, order.CustomerID, order.Total)
//	}
//
//	func (s *PaymentStep) Compensate(ctx context.Context, data any) error {
//	    order := data.(*Order)
//	    return s.paymentService.Refund(ctx, order.CustomerID, order.Total)
//	}
type Step interface {
	// Name returns the step name for logging/tracking.
	Name() string

	// Execute performs the step action.
	// Returns nil on success, error on failure (triggers compensation).
	Execute(ctx context.Context, data any) error

	// Compensate undoes the step action on rollback.
	// Called when a later step fails. Must be idempotent.
	Compensate(ctx context.Context, data any) error
}

// State represents the current state of a saga.
//
// The State tracks:
//   - Which saga instance this is (ID)
//   - What type of saga (Name)
//   - Current execution state (Status, CurrentStep)
//   - Which steps completed (CompletedSteps)
//   - The saga data being processed
//   - Any error that occurred
//   - Timing information
//
// State is persisted to the Store after each step for recovery.
type State struct {
	ID             string     // Saga instance ID (unique per execution)
	Name           string     // Saga definition name (e.g., "order-creation")
	Status         Status     // Current status
	CurrentStep    int        // Index of current/failed step
	CompletedSteps []string   // Names of completed steps
	Data           any        // Saga data passed to steps
	Error          string     // Error message if failed
	StartedAt      time.Time  // When saga started
	CompletedAt    *time.Time // When saga completed/failed (nil if running)
	LastUpdatedAt  time.Time  // Last state update
}

// Status represents saga status.
//
// State transitions:
//
//	pending → running → completed
//	                 ↘
//	              compensating → compensated
//	                          ↘
//	                          failed
type Status string

const (
	// StatusPending indicates saga is created but not started.
	StatusPending Status = "pending"

	// StatusRunning indicates saga is executing steps.
	StatusRunning Status = "running"

	// StatusCompleted indicates all steps succeeded.
	StatusCompleted Status = "completed"

	// StatusFailed indicates saga failed and compensation also failed.
	StatusFailed Status = "failed"

	// StatusCompensating indicates saga is running compensations.
	StatusCompensating Status = "compensating"

	// StatusCompensated indicates saga failed but compensations succeeded.
	StatusCompensated Status = "compensated"
)

// Store persists saga state.
//
// Implementations must be safe for concurrent use. The store enables:
//   - Recovery after application restart
//   - Visibility into saga state
//   - Retry of failed sagas
//
// Implementations:
//   - RedisStore: For distributed deployments (see redis.go)
//   - MongoStore: For MongoDB (see mongodb.go)
type Store interface {
	// Create creates a new saga instance.
	// Returns error if saga with this ID already exists.
	Create(ctx context.Context, state *State) error

	// Get retrieves saga state by ID.
	// Returns error if not found.
	Get(ctx context.Context, id string) (*State, error)

	// Update updates saga state.
	// Called after each step to persist progress.
	Update(ctx context.Context, state *State) error

	// List lists sagas matching the filter.
	// Returns empty slice if no matches.
	List(ctx context.Context, filter StoreFilter) ([]*State, error)
}

// StoreFilter specifies criteria for listing sagas.
//
// All fields are optional. Empty filter returns all sagas.
//
// Example:
//
//	// Find failed order sagas
//	filter := saga.StoreFilter{
//	    Name:   "order-creation",
//	    Status: []saga.Status{saga.StatusFailed, saga.StatusCompensated},
//	    Limit:  100,
//	}
//	sagas, err := store.List(ctx, filter)
type StoreFilter struct {
	Name   string   // Filter by saga name (empty = all names)
	Status []Status // Filter by status (empty = all statuses)
	Limit  int      // Maximum results (0 = no limit)
}

// Saga orchestrates a sequence of steps with compensation.
//
// A Saga represents a distributed transaction composed of multiple steps.
// It executes steps in order and, on failure, runs compensations in reverse
// order to undo completed work.
//
// Example:
//
//	orderSaga := saga.New("order-creation",
//	    &CreateOrderStep{},
//	    &ReserveInventoryStep{},
//	    &ProcessPaymentStep{},
//	).WithStore(store)
//
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    // Saga failed, compensations were run
//	}
type Saga struct {
	name   string
	steps  []Step
	store  Store
	logger *slog.Logger
}

// New creates a new saga definition.
//
// The name should be descriptive and consistent across deployments as it's
// used for filtering and logging. Steps are executed in order.
//
// Parameters:
//   - name: Saga name for identification (e.g., "order-creation")
//   - steps: The steps to execute in order
//
// Example:
//
//	saga := saga.New("order-creation",
//	    &CreateOrderStep{orderService},
//	    &ReserveInventoryStep{inventoryService},
//	    &ProcessPaymentStep{paymentService},
//	)
func New(name string, steps ...Step) *Saga {
	return &Saga{
		name:   name,
		steps:  steps,
		logger: slog.Default().With("saga", name),
	}
}

// WithStore sets the saga store for persistence.
//
// Using a store enables:
//   - State persistence after each step
//   - Recovery of failed sagas with Resume()
//   - Visibility into saga state for monitoring
//
// Parameters:
//   - store: The store implementation to use
//
// Returns the saga for method chaining.
//
// Example:
//
//	saga := saga.New("order", steps...).
//	    WithStore(saga.NewRedisStore(redisClient))
func (s *Saga) WithStore(store Store) *Saga {
	s.store = store
	return s
}

// WithLogger sets a custom logger.
//
// The logger is used to log step execution, failures, and compensations.
//
// Parameters:
//   - logger: The slog logger to use
//
// Returns the saga for method chaining.
func (s *Saga) WithLogger(logger *slog.Logger) *Saga {
	s.logger = logger.With("saga", s.name)
	return s
}

// Name returns the saga name.
func (s *Saga) Name() string {
	return s.name
}

// Steps returns the saga steps.
func (s *Saga) Steps() []Step {
	return s.steps
}

// Execute executes the saga with the given ID and data.
//
// Execution proceeds as follows:
//  1. Create initial saga state (if store configured)
//  2. Execute each step in order
//  3. On success: mark saga as completed
//  4. On failure: run compensations in reverse order
//
// The data parameter is passed to each step's Execute and Compensate methods.
// Steps can type-assert to access the actual data type.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: Unique identifier for this saga instance
//   - data: Data to pass to steps
//
// Returns nil if all steps succeed, error if any step fails (after compensation).
//
// Example:
//
//	sagaID := uuid.New().String()
//	order := &Order{CustomerID: custID, Items: items}
//
//	if err := orderSaga.Execute(ctx, sagaID, order); err != nil {
//	    log.Error("order saga failed", "id", sagaID, "error", err)
//	    // Compensations have already run - order is in consistent state
//	}
func (s *Saga) Execute(ctx context.Context, id string, data any) error {
	state := &State{
		ID:            id,
		Name:          s.name,
		Status:        StatusRunning,
		Data:          data,
		StartedAt:     time.Now(),
		LastUpdatedAt: time.Now(),
	}

	// Persist initial state
	if s.store != nil {
		if err := s.store.Create(ctx, state); err != nil {
			return fmt.Errorf("create saga state: %w", err)
		}
	}

	completedSteps := make([]Step, 0, len(s.steps))

	for i, step := range s.steps {
		state.CurrentStep = i

		s.logger.Info("executing step",
			"saga_id", id,
			"step", step.Name(),
			"step_index", i)

		if err := step.Execute(ctx, data); err != nil {
			s.logger.Error("step failed",
				"saga_id", id,
				"step", step.Name(),
				"error", err)

			state.Status = StatusCompensating
			state.Error = err.Error()
			s.updateState(ctx, state)

			// Compensate in reverse order
			compensateErr := s.compensate(ctx, id, completedSteps, data)
			if compensateErr != nil {
				state.Status = StatusFailed
				state.Error = fmt.Sprintf("step failed: %v; compensation failed: %v", err, compensateErr)
			} else {
				state.Status = StatusCompensated
			}

			now := time.Now()
			state.CompletedAt = &now
			s.updateState(ctx, state)

			return fmt.Errorf("saga step %s failed: %w", step.Name(), err)
		}

		completedSteps = append(completedSteps, step)
		state.CompletedSteps = append(state.CompletedSteps, step.Name())
		s.updateState(ctx, state)

		s.logger.Debug("step completed",
			"saga_id", id,
			"step", step.Name())
	}

	state.Status = StatusCompleted
	now := time.Now()
	state.CompletedAt = &now
	s.updateState(ctx, state)

	s.logger.Info("saga completed",
		"saga_id", id,
		"steps", len(s.steps))

	return nil
}

// compensate runs compensations in reverse order
func (s *Saga) compensate(ctx context.Context, id string, completedSteps []Step, data any) error {
	s.logger.Info("starting compensation",
		"saga_id", id,
		"steps_to_compensate", len(completedSteps))

	var compensateErrors []error

	for i := len(completedSteps) - 1; i >= 0; i-- {
		step := completedSteps[i]

		s.logger.Info("compensating step",
			"saga_id", id,
			"step", step.Name())

		if err := step.Compensate(ctx, data); err != nil {
			s.logger.Error("compensation failed",
				"saga_id", id,
				"step", step.Name(),
				"error", err)

			compensateErrors = append(compensateErrors, fmt.Errorf("compensate %s: %w", step.Name(), err))
			// Continue compensating other steps
		}
	}

	if len(compensateErrors) > 0 {
		return fmt.Errorf("compensation errors: %v", compensateErrors)
	}

	s.logger.Info("compensation completed",
		"saga_id", id)

	return nil
}

// updateState updates saga state in store if available
func (s *Saga) updateState(ctx context.Context, state *State) {
	state.LastUpdatedAt = time.Now()

	if s.store != nil {
		if err := s.store.Update(ctx, state); err != nil {
			s.logger.Error("failed to update saga state",
				"saga_id", state.ID,
				"error", err)
		}
	}
}

// Resume attempts to resume a failed saga.
//
// Use Resume to retry a saga after fixing the underlying issue that caused
// the failure. Resume loads the saga state from the store and re-executes
// from the beginning.
//
// Resume only works for sagas in StatusFailed or StatusCompensated state.
// Requires a store to be configured.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - id: The saga instance ID to resume
//
// Returns error if:
//   - No store is configured
//   - Saga not found
//   - Saga is not in a failed state
//   - Re-execution fails
//
// Example:
//
//	// After fixing the underlying issue...
//	failedSagas, _ := store.List(ctx, saga.StoreFilter{
//	    Status: []saga.Status{saga.StatusFailed},
//	})
//
//	for _, state := range failedSagas {
//	    if err := orderSaga.Resume(ctx, state.ID); err != nil {
//	        log.Error("resume failed", "saga_id", state.ID, "error", err)
//	    }
//	}
func (s *Saga) Resume(ctx context.Context, id string) error {
	if s.store == nil {
		return fmt.Errorf("no store configured")
	}

	state, err := s.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get saga state: %w", err)
	}

	if state.Status != StatusFailed && state.Status != StatusCompensated {
		return fmt.Errorf("saga is not in failed state: %s", state.Status)
	}

	// Reset and re-execute
	state.Status = StatusRunning
	state.Error = ""
	state.CompletedAt = nil
	state.CompletedSteps = nil

	return s.Execute(ctx, id, state.Data)
}

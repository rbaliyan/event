package health

import (
	"context"
	"testing"
	"time"
)

// mockChecker implements Checker for testing.
type mockChecker struct {
	result *Result
}

func (m *mockChecker) Health(_ context.Context) *Result {
	return m.result
}

// panicChecker panics when Health is called.
type panicChecker struct{}

func (p *panicChecker) Health(_ context.Context) *Result {
	panic("simulated panic")
}

var _ Checker = (*mockChecker)(nil)
var _ Checker = (*panicChecker)(nil)

func TestResultStatusMethods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		status    Status
		healthy   bool
		degraded  bool
		unhealthy bool
	}{
		{"healthy", StatusHealthy, true, false, false},
		{"degraded", StatusDegraded, false, true, false},
		{"unhealthy", StatusUnhealthy, false, false, true},
		{"unknown status", Status("unknown"), false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Result{Status: tt.status}
			if got := r.IsHealthy(); got != tt.healthy {
				t.Errorf("IsHealthy() = %v, want %v", got, tt.healthy)
			}
			if got := r.IsDegraded(); got != tt.degraded {
				t.Errorf("IsDegraded() = %v, want %v", got, tt.degraded)
			}
			if got := r.IsUnhealthy(); got != tt.unhealthy {
				t.Errorf("IsUnhealthy() = %v, want %v", got, tt.unhealthy)
			}
		})
	}
}

func TestResultFields(t *testing.T) {
	t.Parallel()
	now := time.Now()
	r := &Result{
		Status:    StatusHealthy,
		Message:   "all good",
		Latency:   50 * time.Millisecond,
		CheckedAt: now,
		Details:   map[string]any{"queue_depth": 42},
	}

	if r.Status != StatusHealthy {
		t.Errorf("Status = %v, want %v", r.Status, StatusHealthy)
	}
	if r.Message != "all good" {
		t.Errorf("Message = %q, want %q", r.Message, "all good")
	}
	if r.Latency != 50*time.Millisecond {
		t.Errorf("Latency = %v, want %v", r.Latency, 50*time.Millisecond)
	}
	if !r.CheckedAt.Equal(now) {
		t.Errorf("CheckedAt = %v, want %v", r.CheckedAt, now)
	}
	if r.Details["queue_depth"] != 42 {
		t.Errorf("Details[queue_depth] = %v, want 42", r.Details["queue_depth"])
	}
}

func TestAggregate_AllHealthy(t *testing.T) {
	t.Parallel()
	components := map[string]*Result{
		"db":    {Status: StatusHealthy},
		"cache": {Status: StatusHealthy},
	}

	agg := Aggregate(components)

	if agg.Status != StatusHealthy {
		t.Errorf("aggregate status = %v, want %v", agg.Status, StatusHealthy)
	}
	if len(agg.Components) != 2 {
		t.Errorf("component count = %d, want 2", len(agg.Components))
	}
	if agg.CheckedAt.IsZero() {
		t.Error("CheckedAt should not be zero")
	}
}

func TestAggregate_DegradedWins(t *testing.T) {
	t.Parallel()
	components := map[string]*Result{
		"db":    {Status: StatusHealthy},
		"cache": {Status: StatusDegraded},
	}

	agg := Aggregate(components)

	if agg.Status != StatusDegraded {
		t.Errorf("aggregate status = %v, want %v", agg.Status, StatusDegraded)
	}
}

func TestAggregate_UnhealthyWins(t *testing.T) {
	t.Parallel()
	components := map[string]*Result{
		"db":    {Status: StatusUnhealthy},
		"cache": {Status: StatusDegraded},
		"queue": {Status: StatusHealthy},
	}

	agg := Aggregate(components)

	if agg.Status != StatusUnhealthy {
		t.Errorf("aggregate status = %v, want %v", agg.Status, StatusUnhealthy)
	}
}

func TestAggregate_NilComponent(t *testing.T) {
	t.Parallel()
	components := map[string]*Result{
		"db":    {Status: StatusHealthy},
		"cache": nil,
	}

	agg := Aggregate(components)

	if agg.Status != StatusHealthy {
		t.Errorf("aggregate status = %v, want %v (nil components should be skipped)", agg.Status, StatusHealthy)
	}
}

func TestAggregate_EmptyComponents(t *testing.T) {
	t.Parallel()
	agg := Aggregate(map[string]*Result{})

	if agg.Status != StatusHealthy {
		t.Errorf("aggregate status = %v, want %v for empty components", agg.Status, StatusHealthy)
	}
}

func TestCheckAll_BasicCheckers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	checkers := map[string]Checker{
		"healthy": &mockChecker{result: &Result{
			Status:    StatusHealthy,
			CheckedAt: time.Now(),
		}},
		"degraded": &mockChecker{result: &Result{
			Status:    StatusDegraded,
			Message:   "high latency",
			CheckedAt: time.Now(),
		}},
	}

	agg := CheckAll(ctx, checkers)

	if agg.Status != StatusDegraded {
		t.Errorf("aggregate status = %v, want %v", agg.Status, StatusDegraded)
	}
	if len(agg.Components) != 2 {
		t.Errorf("component count = %d, want 2", len(agg.Components))
	}
	if agg.Components["healthy"].Status != StatusHealthy {
		t.Errorf("healthy component status = %v, want healthy", agg.Components["healthy"].Status)
	}
	if agg.Components["degraded"].Status != StatusDegraded {
		t.Errorf("degraded component status = %v, want degraded", agg.Components["degraded"].Status)
	}
}

func TestCheckAll_PanicRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	checkers := map[string]Checker{
		"healthy": &mockChecker{result: &Result{
			Status:    StatusHealthy,
			CheckedAt: time.Now(),
		}},
		"panicker": &panicChecker{},
	}

	agg := CheckAll(ctx, checkers)

	if agg.Status != StatusUnhealthy {
		t.Errorf("aggregate status = %v, want %v (panicking checker)", agg.Status, StatusUnhealthy)
	}
	panicked := agg.Components["panicker"]
	if panicked == nil {
		t.Fatal("expected panicker component to exist")
	}
	if panicked.Status != StatusUnhealthy {
		t.Errorf("panicker status = %v, want unhealthy", panicked.Status)
	}
	if panicked.Message != "health check panicked" {
		t.Errorf("panicker message = %q, want %q", panicked.Message, "health check panicked")
	}
}

func TestCheckAll_NilChecker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	checkers := map[string]Checker{
		"healthy": &mockChecker{result: &Result{
			Status:    StatusHealthy,
			CheckedAt: time.Now(),
		}},
		"nil_checker": nil,
	}

	agg := CheckAll(ctx, checkers)

	// nil checkers should be skipped
	if _, found := agg.Components["nil_checker"]; found {
		t.Error("nil checker should not appear in components")
	}
	if agg.Status != StatusHealthy {
		t.Errorf("aggregate status = %v, want healthy", agg.Status)
	}
}

func TestCheckAll_EmptyCheckers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	agg := CheckAll(ctx, map[string]Checker{})

	if agg.Status != StatusHealthy {
		t.Errorf("aggregate status = %v, want healthy for empty checkers", agg.Status)
	}
	if len(agg.Components) != 0 {
		t.Errorf("component count = %d, want 0", len(agg.Components))
	}
}

func TestStatusConstants(t *testing.T) {
	t.Parallel()
	if StatusHealthy != "healthy" {
		t.Errorf("StatusHealthy = %q, want %q", StatusHealthy, "healthy")
	}
	if StatusDegraded != "degraded" {
		t.Errorf("StatusDegraded = %q, want %q", StatusDegraded, "degraded")
	}
	if StatusUnhealthy != "unhealthy" {
		t.Errorf("StatusUnhealthy = %q, want %q", StatusUnhealthy, "unhealthy")
	}
}

// Package health provides common health check types for the event ecosystem.
//
// This package defines a standard interface and types for health checking
// components across the event library and its extensions (scheduler, DLQ, saga).
//
// Usage:
//
//	import "github.com/rbaliyan/event/v3/health"
//
//	type MyComponent struct { ... }
//
//	func (c *MyComponent) Health(ctx context.Context) *health.Result {
//	    start := time.Now()
//	    err := c.ping(ctx)
//	    if err != nil {
//	        return &health.Result{
//	            Status:    health.StatusUnhealthy,
//	            Message:   err.Error(),
//	            Latency:   time.Since(start),
//	            CheckedAt: time.Now(),
//	        }
//	    }
//	    return &health.Result{
//	        Status:    health.StatusHealthy,
//	        Latency:   time.Since(start),
//	        CheckedAt: time.Now(),
//	    }
//	}
//
//	// Check if component implements health.Checker
//	var _ health.Checker = (*MyComponent)(nil)
package health

import (
	"context"
	"time"
)

// Status represents the health state of a component.
type Status string

const (
	// StatusHealthy indicates the component is functioning normally.
	StatusHealthy Status = "healthy"
	// StatusDegraded indicates the component is functioning but with issues.
	// Examples: high latency, stuck items, approaching resource limits.
	StatusDegraded Status = "degraded"
	// StatusUnhealthy indicates the component is not functioning.
	// Examples: cannot connect to database, critical errors.
	StatusUnhealthy Status = "unhealthy"
)

// Result contains detailed health information about a component.
//
// This is a standard result structure that can be used by any component
// implementing health checks. The Details field allows for component-specific
// information to be included.
type Result struct {
	// Status indicates the overall health state.
	Status Status `json:"status"`

	// Message provides additional context about the status.
	// Typically used for error messages or degradation reasons.
	Message string `json:"message,omitempty"`

	// Latency is how long the health check took to complete.
	Latency time.Duration `json:"latency,omitempty"`

	// CheckedAt is when this health check was performed.
	CheckedAt time.Time `json:"checked_at"`

	// Details contains component-specific health information.
	// Examples: pending message counts, queue depths, connection pool stats.
	Details map[string]any `json:"details,omitempty"`
}

// IsHealthy returns true if the status is healthy.
func (r *Result) IsHealthy() bool {
	return r.Status == StatusHealthy
}

// IsDegraded returns true if the status is degraded.
func (r *Result) IsDegraded() bool {
	return r.Status == StatusDegraded
}

// IsUnhealthy returns true if the status is unhealthy.
func (r *Result) IsUnhealthy() bool {
	return r.Status == StatusUnhealthy
}

// Checker is an interface for components that support health checks.
//
// Implementations should:
//   - Respect context timeout/cancellation
//   - Return quickly (ideally under 1 second)
//   - Include relevant details for debugging
//   - Return StatusDegraded for non-critical issues
//   - Return StatusUnhealthy only for critical failures
type Checker interface {
	// Health performs a health check and returns the result.
	// The context can be used to set a timeout for the health check.
	Health(ctx context.Context) *Result
}

// AggregateResult represents the health of multiple components.
type AggregateResult struct {
	// Status is the overall status (worst of all components).
	Status Status `json:"status"`

	// Components contains individual component results.
	Components map[string]*Result `json:"components"`

	// CheckedAt is when this aggregate check was performed.
	CheckedAt time.Time `json:"checked_at"`
}

// Aggregate combines multiple health check results into an aggregate result.
// The aggregate status is the worst status among all components:
// unhealthy > degraded > healthy
func Aggregate(components map[string]*Result) *AggregateResult {
	result := &AggregateResult{
		Status:     StatusHealthy,
		Components: components,
		CheckedAt:  time.Now(),
	}

	for _, r := range components {
		if r == nil {
			continue
		}
		switch r.Status {
		case StatusUnhealthy:
			result.Status = StatusUnhealthy
		case StatusDegraded:
			if result.Status != StatusUnhealthy {
				result.Status = StatusDegraded
			}
		}
	}

	return result
}

// CheckAll runs health checks on multiple checkers and aggregates results.
// If a checker panics, it's recorded as unhealthy.
func CheckAll(ctx context.Context, checkers map[string]Checker) *AggregateResult {
	components := make(map[string]*Result, len(checkers))

	for name, checker := range checkers {
		if checker == nil {
			continue
		}
		// Run health check with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					components[name] = &Result{
						Status:    StatusUnhealthy,
						Message:   "health check panicked",
						CheckedAt: time.Now(),
					}
				}
			}()
			components[name] = checker.Health(ctx)
		}()
	}

	return Aggregate(components)
}

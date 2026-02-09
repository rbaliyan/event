package event

import (
	"context"
	"errors"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/transport"
)

// StatusCode represents the health state of the bus
type StatusCode string

const (
	// StatusHealthy indicates the bus is functioning normally
	StatusHealthy StatusCode = "healthy"
	// StatusDegraded indicates the bus is functioning but with issues
	StatusDegraded StatusCode = "degraded"
	// StatusUnhealthy indicates the bus is not functioning
	StatusUnhealthy StatusCode = "unhealthy"
)

// Status contains detailed status information for the bus
type Status struct {
	Code       StatusCode         `json:"status"`
	Message    string             `json:"message,omitempty"`
	Latency    time.Duration      `json:"latency,omitempty"`
	Details    map[string]any     `json:"details,omitempty"`
	Components map[string]*Status `json:"components,omitempty"`
	CheckedAt  time.Time          `json:"checked_at"`
}

// IsHealthy returns true if the status code is healthy
func (s *Status) IsHealthy() bool {
	return s.Code == StatusHealthy
}

// ConsumerLag is an alias for transport.ConsumerLag containing
// information about consumer lag for an event.
type ConsumerLag = transport.ConsumerLag

// Status returns detailed status information about the bus and its transport.
// Use this to inspect the bus state for monitoring dashboards.
// If the transport or any configured stores implement HealthChecker, their status is included.
func (b *Bus) Status(ctx context.Context) *Status {
	result := &Status{
		CheckedAt:  time.Now(),
		Details:    make(map[string]any),
		Components: make(map[string]*Status),
	}

	// Check bus status
	if !b.Running() {
		result.Code = StatusUnhealthy
		result.Message = "bus is closed"
		result.Details["bus_name"] = b.name
		return result
	}

	// Count events
	b.eventMutex.RLock()
	eventCount := len(b.events)
	b.eventMutex.RUnlock()

	result.Details["bus_name"] = b.name
	result.Details["events"] = eventCount

	// Start with healthy status
	result.Code = StatusHealthy
	result.Message = "bus is healthy"

	// Check transport health if it implements HealthChecker
	if hc, ok := b.transport.(transport.HealthChecker); ok {
		transportHealth := hc.Health(ctx)
		result.Components["transport"] = convertTransportStatus(transportHealth)
		aggregateComponentStatus(result, "transport", transportHealth.Status)
	}

	// Check health of configured stores that implement health.Checker
	b.checkComponentHealth(ctx, result, "idempotency_store", b.idempotencyStore)
	b.checkComponentHealth(ctx, result, "poison_detector", b.poisonDetector)
	b.checkComponentHealth(ctx, result, "monitor_store", b.monitorStore)
	b.checkComponentHealth(ctx, result, "schema_provider", b.schemaProvider)
	b.checkComponentHealth(ctx, result, "outbox_store", b.outboxStore)

	return result
}

// checkComponentHealth checks if a component implements health.Checker and aggregates its status.
func (b *Bus) checkComponentHealth(ctx context.Context, result *Status, name string, component any) {
	if component == nil {
		return
	}
	if hc, ok := component.(health.Checker); ok {
		componentHealth := hc.Health(ctx)
		result.Components[name] = convertHealthResult(componentHealth)
		aggregateComponentStatus(result, name, health.Status(componentHealth.Status))
	}
}

// aggregateComponentStatus updates the result status based on component status.
// Uses the worst status: unhealthy > degraded > healthy
func aggregateComponentStatus(result *Status, name string, status any) {
	var statusCode StatusCode
	switch s := status.(type) {
	case transport.HealthStatus:
		switch s {
		case transport.HealthStatusUnhealthy:
			statusCode = StatusUnhealthy
		case transport.HealthStatusDegraded:
			statusCode = StatusDegraded
		default:
			return // healthy, no change needed
		}
	case health.Status:
		switch s {
		case health.StatusUnhealthy:
			statusCode = StatusUnhealthy
		case health.StatusDegraded:
			statusCode = StatusDegraded
		default:
			return // healthy, no change needed
		}
	default:
		return
	}

	// Only update if new status is worse
	if statusCode == StatusUnhealthy {
		result.Code = StatusUnhealthy
		result.Message = name + " is unhealthy"
	} else if statusCode == StatusDegraded && result.Code != StatusUnhealthy {
		result.Code = StatusDegraded
		result.Message = name + " is degraded"
	}
}

// convertHealthResult converts health.Result to bus Status
func convertHealthResult(hr *health.Result) *Status {
	if hr == nil {
		return nil
	}
	return &Status{
		Code:      StatusCode(hr.Status),
		Message:   hr.Message,
		Latency:   hr.Latency,
		Details:   hr.Details,
		CheckedAt: hr.CheckedAt,
	}
}

// Health performs a health check suitable for health probes.
// Returns nil if the bus is healthy, or an error describing the issue.
func (b *Bus) Health(ctx context.Context) error {
	status := b.Status(ctx)
	if status.Code == StatusUnhealthy {
		return errors.New(status.Message)
	}
	return nil
}

// convertTransportStatus converts transport.HealthCheckResult to bus Status
func convertTransportStatus(th *transport.HealthCheckResult) *Status {
	if th == nil {
		return nil
	}

	result := &Status{
		Code:      StatusCode(th.Status),
		Message:   th.Message,
		Latency:   th.Latency,
		Details:   th.Details,
		CheckedAt: th.CheckedAt,
	}

	// Convert nested components
	if len(th.Components) > 0 {
		result.Components = make(map[string]*Status, len(th.Components))
		for k, v := range th.Components {
			result.Components[k] = convertTransportStatus(v)
		}
	}

	return result
}

// ConsumerLag returns consumer lag metrics for all events if the transport supports it.
// Returns nil if the transport doesn't implement LagMonitor.
func (b *Bus) ConsumerLag(ctx context.Context) ([]ConsumerLag, error) {
	if !b.Running() {
		return nil, ErrBusClosed
	}

	if lm, ok := b.transport.(transport.LagMonitor); ok {
		return lm.ConsumerLag(ctx)
	}

	// Transport doesn't support lag monitoring
	return nil, nil
}

package base

import (
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// HealthCheckBuilder helps construct HealthCheckResult with consistent formatting.
type HealthCheckBuilder struct {
	result *transport.HealthCheckResult
	start  time.Time
}

// NewHealthCheck starts building a health check result.
func NewHealthCheck() *HealthCheckBuilder {
	start := time.Now()
	return &HealthCheckBuilder{
		start: start,
		result: &transport.HealthCheckResult{
			CheckedAt: start,
			Details:   make(map[string]any),
		},
	}
}

// Healthy marks the check as healthy with a message.
func (b *HealthCheckBuilder) Healthy(message string) *HealthCheckBuilder {
	b.result.Status = transport.HealthStatusHealthy
	b.result.Message = message
	return b
}

// Unhealthy marks the check as unhealthy with a message.
func (b *HealthCheckBuilder) Unhealthy(message string) *HealthCheckBuilder {
	b.result.Status = transport.HealthStatusUnhealthy
	b.result.Message = message
	return b
}

// Degraded marks the check as degraded with a message.
func (b *HealthCheckBuilder) Degraded(message string) *HealthCheckBuilder {
	b.result.Status = transport.HealthStatusDegraded
	b.result.Message = message
	return b
}

// WithDetail adds a detail to the health check.
func (b *HealthCheckBuilder) WithDetail(key string, value any) *HealthCheckBuilder {
	b.result.Details[key] = value
	return b
}

// WithType sets the transport type detail.
func (b *HealthCheckBuilder) WithType(transportType string) *HealthCheckBuilder {
	b.result.Details["type"] = transportType
	return b
}

// WithEvents sets the event count detail.
func (b *HealthCheckBuilder) WithEvents(count int) *HealthCheckBuilder {
	b.result.Details["events"] = count
	return b
}

// WithSubscribers sets the subscriber count detail.
func (b *HealthCheckBuilder) WithSubscribers(count int64) *HealthCheckBuilder {
	b.result.Details["subscribers"] = count
	return b
}

// Build finalizes and returns the health check result.
func (b *HealthCheckBuilder) Build() *transport.HealthCheckResult {
	b.result.Latency = time.Since(b.start)
	return b.result
}

// QuickHealthCheck creates a simple healthy/unhealthy result based on a condition.
func QuickHealthCheck(isOpen bool, transportType, healthyMsg, unhealthyMsg string) *transport.HealthCheckResult {
	builder := NewHealthCheck().WithType(transportType)
	if isOpen {
		return builder.Healthy(healthyMsg).Build()
	}
	return builder.Unhealthy(unhealthyMsg).Build()
}

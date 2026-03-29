// Package migration provides a transport that bridges an old and new transport
// during a migration. Publishes go to the new transport only, while subscriptions
// receive messages from both, ensuring zero message loss during the transition.
//
// Usage:
//
//	old := existingRedisTransport  // being retired
//	new := newKafkaTransport       // replacing it
//
//	t, _ := migration.New(old, new)
//	bus, _ := event.NewBus("mybus", event.WithTransport(t))
//
// Once the old transport is fully drained, replace the migration transport
// with the new transport directly.
package migration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

var (
	ErrOldTransportRequired = errors.New("old transport is required")
	ErrNewTransportRequired = errors.New("new transport is required")
)

var (
	_ transport.Transport     = (*Transport)(nil)
	_ transport.HealthChecker = (*Transport)(nil)
	_ transport.LagMonitor    = (*Transport)(nil)
	_ transport.Redeliverable = (*Transport)(nil)
	_ transport.Named         = (*Transport)(nil)
)

const defaultMergedBufferSize = 64

// Transport bridges an old and new transport during migration.
// Publishes go to the new transport only. Subscriptions receive from both.
type Transport struct {
	status        int32
	old           transport.Transport
	new           transport.Transport
	logger        *slog.Logger
	mergedBufSize int
}

// New creates a migration transport that bridges old and new transports.
func New(old, new transport.Transport, opts ...Option) (*Transport, error) {
	if old == nil {
		return nil, ErrOldTransportRequired
	}
	if new == nil {
		return nil, ErrNewTransportRequired
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	logger := o.logger
	if logger == nil {
		logger = transport.Logger("transport>migration")
	}

	t := &Transport{
		status:        1,
		old:           old,
		new:           new,
		logger:        logger,
		mergedBufSize: o.mergedBufSize,
	}
	return t, nil
}

// Name returns the transport name.
func (t *Transport) Name() string {
	oldName, newName := "old", "new"
	if n, ok := t.old.(transport.Named); ok {
		oldName = n.Name()
	}
	if n, ok := t.new.(transport.Named); ok {
		newName = n.Name()
	}
	return "migration(" + oldName + "->" + newName + ")"
}

func (t *Transport) isOpen() bool {
	return atomic.LoadInt32(&t.status) == 1
}

// SupportsRedelivery delegates to the new transport, since all new publishes go there.
func (t *Transport) SupportsRedelivery() bool {
	if rd, ok := t.new.(transport.Redeliverable); ok {
		return rd.SupportsRedelivery()
	}
	return false
}

// RegisterEvent registers the event on the new transport only.
// The old transport should already have events registered.
func (t *Transport) RegisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}
	return t.new.RegisterEvent(ctx, name)
}

// UnregisterEvent unregisters the event from both transports.
func (t *Transport) UnregisterEvent(ctx context.Context, name string) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}
	oldErr := t.old.UnregisterEvent(ctx, name)
	newErr := t.new.UnregisterEvent(ctx, name)
	return errors.Join(oldErr, newErr)
}

// Publish sends a message to the new transport only.
func (t *Transport) Publish(ctx context.Context, name string, msg transport.Message) error {
	if !t.isOpen() {
		return transport.ErrTransportClosed
	}
	return t.new.Publish(ctx, name, msg)
}

// Subscribe creates subscriptions on both transports and merges them.
// If the old transport fails to subscribe, the subscription proceeds with new only.
func (t *Transport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	if !t.isOpen() {
		return nil, transport.ErrTransportClosed
	}
	sopts := transport.ApplySubscribeOptions(opts...)
	bufSize := t.mergedBufSize
	if sopts.BufferSize > 0 {
		bufSize = sopts.BufferSize
	}

	newSub, err := t.new.Subscribe(ctx, name, opts...)
	if err != nil {
		return nil, fmt.Errorf("new transport subscribe: %w", err)
	}

	oldSub, err := t.old.Subscribe(ctx, name, opts...)
	if err != nil {
		t.logger.Warn("old transport subscribe failed, using new only",
			"event", name, "error", err)
		return newSub, nil
	}

	return newMergedSubscription(ctx, oldSub, newSub, bufSize), nil
}

// Close shuts down both transports.
func (t *Transport) Close(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&t.status, 1, 0) {
		return nil
	}
	oldErr := t.old.Close(ctx)
	newErr := t.new.Close(ctx)
	return errors.Join(oldErr, newErr)
}

// Health returns combined health status. Intentionally works after Close
// so operators can inspect final state during decommissioning.
// The new transport is the critical one:
// if new is unhealthy, the overall status is unhealthy. If only old is unhealthy,
// the status is degraded (migration can still complete).
func (t *Transport) Health(ctx context.Context) *transport.HealthCheckResult {
	result := &transport.HealthCheckResult{
		Status:     transport.HealthStatusHealthy,
		Message:    "migration transport is healthy",
		Details:    map[string]any{},
		Components: map[string]*transport.HealthCheckResult{},
		CheckedAt:  time.Now(),
	}

	if hc, ok := t.new.(transport.HealthChecker); ok {
		newHealth := hc.Health(ctx)
		result.Components["new"] = newHealth
		switch newHealth.Status {
		case transport.HealthStatusUnhealthy:
			result.Status = transport.HealthStatusUnhealthy
			result.Message = "new transport is unhealthy"
		case transport.HealthStatusDegraded:
			result.Status = transport.HealthStatusDegraded
			result.Message = "new transport is degraded"
		}
	}

	if hc, ok := t.old.(transport.HealthChecker); ok {
		oldHealth := hc.Health(ctx)
		result.Components["old"] = oldHealth
		if oldHealth.Status != transport.HealthStatusHealthy && result.Status == transport.HealthStatusHealthy {
			result.Status = transport.HealthStatusDegraded
			result.Message = "old transport is " + string(oldHealth.Status)
		}
	}

	return result
}

// ConsumerLag returns consumer lag from both transports, prefixed to distinguish them.
// Intentionally works after Close so operators can verify the old transport is fully drained.
func (t *Transport) ConsumerLag(ctx context.Context) ([]transport.ConsumerLag, error) {
	var lags []transport.ConsumerLag
	var errs []error

	if lm, ok := t.old.(transport.LagMonitor); ok {
		oldLags, err := lm.ConsumerLag(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		for i := range oldLags {
			oldLags[i].ConsumerGroup = "old:" + oldLags[i].ConsumerGroup
		}
		lags = append(lags, oldLags...)
	}

	if lm, ok := t.new.(transport.LagMonitor); ok {
		newLags, err := lm.ConsumerLag(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		for i := range newLags {
			newLags[i].ConsumerGroup = "new:" + newLags[i].ConsumerGroup
		}
		lags = append(lags, newLags...)
	}

	return lags, errors.Join(errs...)
}

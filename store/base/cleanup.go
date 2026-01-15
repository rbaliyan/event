package base

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// CleanupManager handles periodic cleanup of expired entries.
// It provides a standardized way to run background cleanup goroutines
// with graceful shutdown support.
type CleanupManager struct {
	interval time.Duration
	stopCh   chan struct{}
	stopped  bool
	mu       sync.Mutex
	logger   *slog.Logger
}

// CleanupFunc is called periodically to perform cleanup operations.
// It receives a context that will be cancelled when the manager is closed.
type CleanupFunc func(ctx context.Context) error

// NewCleanupManager creates a new cleanup manager.
// If interval is 0 or negative, cleanup is disabled.
func NewCleanupManager(interval time.Duration, logger *slog.Logger) *CleanupManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &CleanupManager{
		interval: interval,
		stopCh:   make(chan struct{}),
		logger:   logger,
	}
}

// Start begins the cleanup loop in a background goroutine.
// The cleanup function is called immediately and then at each interval.
// Returns immediately if interval is 0 or negative.
func (m *CleanupManager) Start(cleanupFn CleanupFunc) {
	if m.interval <= 0 {
		return
	}

	go m.run(cleanupFn)
}

// StartDelayed begins the cleanup loop after waiting for the first interval.
// This is useful when you don't want immediate cleanup on startup.
func (m *CleanupManager) StartDelayed(cleanupFn CleanupFunc) {
	if m.interval <= 0 {
		return
	}

	go m.runDelayed(cleanupFn)
}

func (m *CleanupManager) run(cleanupFn CleanupFunc) {
	// Run immediately on start
	ctx, cancel := context.WithTimeout(context.Background(), m.interval)
	if err := cleanupFn(ctx); err != nil {
		m.logger.Warn("cleanup error", "error", err)
	}
	cancel()

	m.runDelayed(cleanupFn)
}

func (m *CleanupManager) runDelayed(cleanupFn CleanupFunc) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), m.interval)
			if err := cleanupFn(ctx); err != nil {
				m.logger.Warn("cleanup error", "error", err)
			}
			cancel()
		case <-m.stopCh:
			return
		}
	}
}

// Stop signals the cleanup goroutine to stop.
// Safe to call multiple times.
func (m *CleanupManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.stopped {
		close(m.stopCh)
		m.stopped = true
	}
}

// StopCh returns the stop channel for use in select statements.
func (m *CleanupManager) StopCh() <-chan struct{} {
	return m.stopCh
}

// SimpleCleanupLoop runs a simple cleanup loop without the full manager.
// This is a convenience function for stores that don't need the full manager.
// The cleanup function should handle its own context timeout.
func SimpleCleanupLoop(interval time.Duration, stopCh <-chan struct{}, cleanupFn func()) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cleanupFn()
		case <-stopCh:
			return
		}
	}
}

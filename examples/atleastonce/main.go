// Package main demonstrates at-least-once delivery using MongoDB transport.
//
// This example shows how to configure all components needed for reliable
// message delivery that survives crashes, restarts, and failures.
//
// Components:
//   - Transactional Outbox: Atomic business writes with event publishing
//   - Resume Tokens: Transport resumes from last position after restart
//   - Ack Store: Tracks which events have been acknowledged
//   - DLQ: Dead letter queue for failed messages
//   - Worker Groups: Competing consumers for load balancing
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/dlq"
	"github.com/rbaliyan/event/v3/outbox"
	"github.com/rbaliyan/event/v3/transport/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Order represents our business entity
type Order struct {
	ID        string    `bson:"_id"`
	Customer  string    `bson:"customer"`
	Amount    float64   `bson:"amount"`
	Status    string    `bson:"status"`
	CreatedAt time.Time `bson:"created_at"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0"))
	if err != nil {
		logger.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)

	db := client.Database("myapp")
	internalDB := client.Database("event_internal")

	// ============================================================
	// STEP 1: Setup Publisher Side (Transactional Outbox)
	// ============================================================
	//
	// The outbox pattern ensures atomic writes: business data and
	// event are written in the same transaction. If the transaction
	// fails, neither is persisted.

	outboxStore := outbox.NewMongoStore(internalDB).
		WithCollection("_outbox")

	// Create indexes for efficient querying
	if err := outboxStore.EnsureIndexes(ctx); err != nil {
		logger.Error("failed to create outbox indexes", "error", err)
		os.Exit(1)
	}

	// ============================================================
	// STEP 2: Setup MongoDB Transport with At-Least-Once Guarantees
	// ============================================================

	// 2a. Resume Token Store - persists position for crash recovery
	resumeTokenStore := mongodb.NewMongoResumeTokenStore(
		internalDB.Collection("_resume_tokens"),
	)

	// 2b. Ack Store - tracks which events have been acknowledged
	ackStore := mongodb.NewMongoAckStore(
		internalDB.Collection("_event_acks"),
		24*time.Hour, // TTL for acknowledged events
	)
	if err := ackStore.CreateIndexes(ctx); err != nil {
		logger.Error("failed to create ack store indexes", "error", err)
		os.Exit(1)
	}

	// 2c. Create transport with all reliability options
	transport, err := mongodb.New(db,
		mongodb.WithCollection("orders"),
		mongodb.WithResumeTokenStore(resumeTokenStore),
		mongodb.WithResumeTokenID(mustHostname()), // Per-instance resume tokens
		mongodb.WithAckStore(ackStore),
		mongodb.WithLogger(logger),
		mongodb.WithFullDocument(mongodb.FullDocumentUpdateLookup),
		mongodb.WithFullDocumentOnly(),
		mongodb.WithBufferSize(1000),
	)
	if err != nil {
		logger.Error("failed to create transport", "error", err)
		os.Exit(1)
	}

	// ============================================================
	// STEP 3: Setup DLQ for Failed Messages
	// ============================================================

	dlqStore := dlq.NewMongoStore(internalDB).WithCollection("_dlq")
	dlqManager := dlq.NewManager(dlqStore, transport).WithLogger(logger)

	// ============================================================
	// STEP 4: Create Event Bus
	// ============================================================

	bus, err := event.NewBus("orders",
		event.WithTransport(transport),
		event.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create bus", "error", err)
		os.Exit(1)
	}
	defer bus.Close(ctx)

	// ============================================================
	// STEP 6: Define Events
	// ============================================================

	orderCreated := event.New[Order]("order.created")
	orderUpdated := event.New[Order]("order.updated")

	// Register events with bus
	if err := event.Register(ctx, bus, orderCreated); err != nil {
		logger.Error("failed to register orderCreated", "error", err)
		os.Exit(1)
	}
	if err := event.Register(ctx, bus, orderUpdated); err != nil {
		logger.Error("failed to register orderUpdated", "error", err)
		os.Exit(1)
	}

	// ============================================================
	// STEP 7: Subscribe with At-Least-Once Handler
	// ============================================================

	// Handler with DLQ integration for permanent failures
	maxRetries := 3
	handler := func(ctx context.Context, e event.Event[Order], order Order) error {
		logger.Info("processing order",
			"order_id", order.ID,
			"customer", order.Customer,
			"amount", order.Amount)

		// Simulate processing that might fail
		if err := processOrder(ctx, order); err != nil {
			// Get retry count from metadata
			metadata := event.ContextMetadata(ctx)
			retryCount := 0
			if rc, ok := metadata["retry_count"]; ok {
				fmt.Sscanf(rc, "%d", &retryCount)
			}

			if retryCount >= maxRetries {
				// Send to DLQ after max retries
				logger.Warn("sending to DLQ after max retries",
					"order_id", order.ID,
					"retries", retryCount,
					"error", err)

				// Get payload for DLQ (would need to serialize order)
				payload := []byte(fmt.Sprintf(`{"id":"%s","customer":"%s"}`, order.ID, order.Customer))

				dlqErr := dlqManager.Store(ctx,
					e.Name(),
					event.ContextEventID(ctx),
					payload,
					metadata,
					err,
					retryCount,
					"order-service",
				)
				if dlqErr != nil {
					logger.Error("failed to store in DLQ", "error", dlqErr)
					return err // Return original error to nack
				}
				return nil // Message safely in DLQ, ack it
			}

			return err // Return error to trigger nack and redelivery
		}

		logger.Info("order processed successfully", "order_id", order.ID)
		return nil // Success - ack the message
	}

	// Subscribe with worker group for competing consumers (load balancing)
	// WithWorkerGroup automatically enables WorkerPool mode
	if err := orderCreated.Subscribe(ctx, handler,
		event.WithWorkerGroup[Order]("order-processors"),
	); err != nil {
		logger.Error("failed to subscribe to orderCreated", "error", err)
		os.Exit(1)
	}

	if err := orderUpdated.Subscribe(ctx, handler,
		event.WithWorkerGroup[Order]("order-processors"),
	); err != nil {
		logger.Error("failed to subscribe to orderUpdated", "error", err)
		os.Exit(1)
	}

	// ============================================================
	// STEP 8: Setup Outbox Relay (Background Publisher)
	// ============================================================

	relay := outbox.NewMongoRelay(outboxStore, transport).
		WithMode(outbox.RelayModeChangeStream). // Real-time with change streams
		WithResumeTokenStore(outbox.NewMongoResumeTokenStore(internalDB, "outbox-relay")).
		WithStuckDuration(5 * time.Minute). // Recover stuck messages
		WithLogger(logger)

	// Start relay in background
	go func() {
		if err := relay.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("relay stopped with error", "error", err)
		}
	}()

	// ============================================================
	// STEP 9: Background DLQ Maintenance
	// ============================================================

	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Cleanup old DLQ messages (older than 30 days)
				deleted, err := dlqManager.Cleanup(ctx, 30*24*time.Hour)
				if err != nil {
					logger.Error("DLQ cleanup failed", "error", err)
				} else if deleted > 0 {
					logger.Info("cleaned up old DLQ messages", "count", deleted)
				}

				// Log DLQ stats
				stats, err := dlqManager.Stats(ctx)
				if err == nil {
					logger.Info("DLQ stats",
						"total", stats.TotalMessages,
						"pending", stats.PendingMessages,
						"retried", stats.RetriedMessages)
				}
			}
		}
	}()

	// ============================================================
	// STEP 10: Example Publisher with Transactional Outbox
	// ============================================================

	// Demonstrate publishing with outbox pattern
	go func() {
		time.Sleep(2 * time.Second) // Wait for setup

		for i := 0; i < 5; i++ {
			order := Order{
				ID:        fmt.Sprintf("order-%d", i+1),
				Customer:  fmt.Sprintf("customer-%d", i+1),
				Amount:    float64((i + 1) * 100),
				Status:    "pending",
				CreatedAt: time.Now(),
			}

			// Use transactional outbox for atomic write
			err := outbox.Transaction(ctx, client, func(sessCtx context.Context) error {
				// 1. Write business data
				_, err := db.Collection("orders").InsertOne(sessCtx, order)
				if err != nil {
					return fmt.Errorf("insert order: %w", err)
				}

				// 2. Publish event (goes to outbox within same transaction)
				if err := orderCreated.Publish(sessCtx, order); err != nil {
					return fmt.Errorf("publish event: %w", err)
				}

				return nil
			})

			if err != nil {
				logger.Error("failed to create order", "error", err)
			} else {
				logger.Info("created order", "order_id", order.ID)
			}

			time.Sleep(time.Second)
		}
	}()

	logger.Info("at-least-once delivery system started")
	logger.Info("press Ctrl+C to stop")

	// Wait for shutdown signal
	<-sigCh
	logger.Info("shutting down...")
	cancel()

	// Give time for graceful shutdown
	time.Sleep(time.Second)
	logger.Info("shutdown complete")
}

// processOrder simulates order processing that might fail
func processOrder(ctx context.Context, order Order) error {
	// Simulate occasional failures for demonstration
	if order.Amount > 400 {
		return fmt.Errorf("payment processing failed for amount %.2f", order.Amount)
	}
	time.Sleep(100 * time.Millisecond)
	return nil
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	pb "github.com/rbaliyan/event/v3/monitor/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestServiceNew(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)
	if svc == nil {
		t.Fatal("expected service, got nil")
	}
}

func TestServiceNewWithPollInterval(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := NewWithPollInterval(store, 50*time.Millisecond)
	if svc == nil {
		t.Fatal("expected service, got nil")
	}
	if svc.pollInterval != 50*time.Millisecond {
		t.Errorf("expected poll interval 50ms, got %v", svc.pollInterval)
	}
}

func TestServiceList(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Add some entries
	now := time.Now()
	for i := 0; i < 5; i++ {
		store.Record(ctx, &monitor.Entry{
			EventID:      "event-" + string(rune('1'+i)),
			EventName:    "orders.created",
			BusID:        "bus-1",
			DeliveryMode: monitor.Broadcast,
			Status:       monitor.StatusCompleted,
			StartedAt:    now.Add(time.Duration(i) * time.Second),
		})
	}

	t.Run("list all entries", func(t *testing.T) {
		resp, err := svc.List(ctx, &pb.ListRequest{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(resp.Entries) != 5 {
			t.Errorf("expected 5 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("list with filter", func(t *testing.T) {
		resp, err := svc.List(ctx, &pb.ListRequest{
			Filter: &pb.Filter{
				EventName: "orders.created",
			},
		})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(resp.Entries) != 5 {
			t.Errorf("expected 5 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("list with limit", func(t *testing.T) {
		resp, err := svc.List(ctx, &pb.ListRequest{
			Filter: &pb.Filter{
				Limit: 3,
			},
		})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(resp.Entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(resp.Entries))
		}
		if !resp.HasMore {
			t.Error("expected has_more to be true")
		}
	})
}

func TestServiceGet(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Add an entry
	store.Record(ctx, &monitor.Entry{
		EventID:        "event-1",
		SubscriptionID: "sub-1",
		EventName:      "orders.created",
		BusID:          "bus-1",
		DeliveryMode:   monitor.Broadcast,
		Status:         monitor.StatusCompleted,
		StartedAt:      time.Now(),
	})

	t.Run("get existing entry", func(t *testing.T) {
		resp, err := svc.Get(ctx, &pb.GetRequest{
			EventId:        "event-1",
			SubscriptionId: "sub-1",
		})
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if resp.Entry == nil {
			t.Fatal("expected entry, got nil")
		}
		if resp.Entry.EventId != "event-1" {
			t.Errorf("expected event-1, got %s", resp.Entry.EventId)
		}
	})

	t.Run("get non-existent entry returns not found", func(t *testing.T) {
		_, err := svc.Get(ctx, &pb.GetRequest{
			EventId:        "non-existent",
			SubscriptionId: "sub-1",
		})
		if err == nil {
			t.Fatal("expected error")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.NotFound {
			t.Errorf("expected NotFound error, got %v", err)
		}
	})

	t.Run("get without event_id returns invalid argument", func(t *testing.T) {
		_, err := svc.Get(ctx, &pb.GetRequest{})
		if err == nil {
			t.Fatal("expected error")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error, got %v", err)
		}
	})
}

func TestServiceGetByEventID(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Add entries for same event ID
	now := time.Now()
	for i := 0; i < 3; i++ {
		store.Record(ctx, &monitor.Entry{
			EventID:        "event-1",
			SubscriptionID: "sub-" + string(rune('1'+i)),
			EventName:      "orders.created",
			BusID:          "bus-1",
			DeliveryMode:   monitor.Broadcast,
			Status:         monitor.StatusCompleted,
			StartedAt:      now.Add(time.Duration(i) * time.Millisecond),
		})
	}

	t.Run("get entries by event ID", func(t *testing.T) {
		resp, err := svc.GetByEventID(ctx, &pb.GetByEventIDRequest{
			EventId: "event-1",
		})
		if err != nil {
			t.Fatalf("GetByEventID failed: %v", err)
		}
		if len(resp.Entries) != 3 {
			t.Errorf("expected 3 entries, got %d", len(resp.Entries))
		}
	})

	t.Run("get without event_id returns invalid argument", func(t *testing.T) {
		_, err := svc.GetByEventID(ctx, &pb.GetByEventIDRequest{})
		if err == nil {
			t.Fatal("expected error")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error, got %v", err)
		}
	})
}

func TestServiceCount(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Add entries
	now := time.Now()
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-1",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusCompleted,
		StartedAt:    now,
	})
	store.Record(ctx, &monitor.Entry{
		EventID:      "event-2",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusFailed,
		Error:        "some error",
		StartedAt:    now,
	})

	t.Run("count all entries", func(t *testing.T) {
		resp, err := svc.Count(ctx, &pb.CountRequest{})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if resp.Count != 2 {
			t.Errorf("expected 2, got %d", resp.Count)
		}
	})

	t.Run("count with status filter", func(t *testing.T) {
		resp, err := svc.Count(ctx, &pb.CountRequest{
			Filter: &pb.Filter{
				Status: []pb.Status{pb.Status_STATUS_FAILED},
			},
		})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if resp.Count != 1 {
			t.Errorf("expected 1, got %d", resp.Count)
		}
	})
}

func TestServiceDeleteOlderThan(t *testing.T) {
	ctx := context.Background()
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Add old and new entries
	now := time.Now()
	store.Record(ctx, &monitor.Entry{
		EventID:      "old-event",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusCompleted,
		StartedAt:    now.Add(-2 * time.Hour),
	})
	store.Record(ctx, &monitor.Entry{
		EventID:      "new-event",
		EventName:    "orders.created",
		DeliveryMode: monitor.WorkerPool,
		Status:       monitor.StatusCompleted,
		StartedAt:    now,
	})

	t.Run("delete entries older than 1 hour", func(t *testing.T) {
		resp, err := svc.DeleteOlderThan(ctx, &pb.DeleteOlderThanRequest{
			Age: durationpb.New(time.Hour),
		})
		if err != nil {
			t.Fatalf("DeleteOlderThan failed: %v", err)
		}
		if resp.Deleted != 1 {
			t.Errorf("expected 1 deleted, got %d", resp.Deleted)
		}

		// Verify old entry is gone
		count, _ := store.Count(ctx, monitor.Filter{})
		if count != 1 {
			t.Errorf("expected 1 remaining entry, got %d", count)
		}
	})

	t.Run("delete with zero age returns invalid argument", func(t *testing.T) {
		_, err := svc.DeleteOlderThan(ctx, &pb.DeleteOlderThanRequest{})
		if err == nil {
			t.Fatal("expected error")
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected InvalidArgument error, got %v", err)
		}
	})
}

func TestServiceStreamRequiresBroadcaster(t *testing.T) {
	store := monitor.NewMemoryStore()
	defer store.Close()

	svc := New(store)

	// Stream without starting broadcaster should fail
	// We can't easily test the streaming behavior without a full gRPC server setup,
	// but we can verify the service is created correctly
	if svc.broadcaster != nil {
		t.Error("broadcaster should be nil before starting")
	}

	ctx := context.Background()
	svc.StartBroadcaster(ctx)
	defer svc.StopBroadcaster()

	if svc.broadcaster == nil {
		t.Error("broadcaster should not be nil after starting")
	}
}

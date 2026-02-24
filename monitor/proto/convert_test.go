package monitorpb

import (
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestEntryToProto_Nil(t *testing.T) {
	if got := EntryToProto(nil); got != nil {
		t.Errorf("EntryToProto(nil) = %v, want nil", got)
	}
}

func TestEntryToProto_RoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	completed := now.Add(5 * time.Second)
	entry := &monitor.Entry{
		EventID:        "evt-1",
		SubscriptionID: "sub-1",
		EventName:      "orders.created",
		BusID:          "bus-1",
		InstanceID:     "inst-1",
		DeliveryMode:   monitor.WorkerPool,
		Metadata:       map[string]string{"key": "val"},
		Status:         monitor.StatusCompleted,
		Error:          "some error",
		RetryCount:     3,
		StartedAt:      now,
		CompletedAt:    &completed,
		Duration:       5 * time.Second,
		TraceID:        "trace-1",
		SpanID:         "span-1",
		WorkerGroup:    "group-a",
	}

	pb := EntryToProto(entry)
	if pb.EventId != "evt-1" {
		t.Errorf("EventId = %q, want %q", pb.EventId, "evt-1")
	}
	if pb.DeliveryMode != DeliveryMode_DELIVERY_MODE_WORKER_POOL {
		t.Errorf("DeliveryMode = %v, want WORKER_POOL", pb.DeliveryMode)
	}
	if pb.Status != Status_STATUS_COMPLETED {
		t.Errorf("Status = %v, want COMPLETED", pb.Status)
	}

	// Round-trip back
	got := ProtoToEntry(pb)
	if got.EventID != entry.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, entry.EventID)
	}
	if got.DeliveryMode != entry.DeliveryMode {
		t.Errorf("DeliveryMode = %v, want %v", got.DeliveryMode, entry.DeliveryMode)
	}
	if got.Status != entry.Status {
		t.Errorf("Status = %v, want %v", got.Status, entry.Status)
	}
	if got.RetryCount != entry.RetryCount {
		t.Errorf("RetryCount = %d, want %d", got.RetryCount, entry.RetryCount)
	}
	if got.WorkerGroup != entry.WorkerGroup {
		t.Errorf("WorkerGroup = %q, want %q", got.WorkerGroup, entry.WorkerGroup)
	}
}

func TestProtoToEntry_Nil(t *testing.T) {
	if got := ProtoToEntry(nil); got != nil {
		t.Errorf("ProtoToEntry(nil) = %v, want nil", got)
	}
}

func TestFilterRoundTrip(t *testing.T) {
	dm := monitor.WorkerPool
	hasErr := true
	f := monitor.Filter{
		EventID:        "evt-1",
		SubscriptionID: "sub-1",
		EventName:      "orders",
		BusID:          "bus",
		InstanceID:     "inst",
		DeliveryMode:   &dm,
		Status:         []monitor.Status{monitor.StatusPending, monitor.StatusFailed},
		HasError:       &hasErr,
		WorkerGroup:    "grp",
		MinRetries:     2,
		Cursor:         "cursor-abc",
		Limit:          50,
		OrderDesc:      true,
		StartTime:      time.Now().Truncate(time.Second),
		EndTime:        time.Now().Add(time.Hour).Truncate(time.Second),
		MinDuration:    time.Second,
	}

	pb := FilterToProto(f)
	got := ProtoToFilter(pb)

	if got.EventID != f.EventID {
		t.Errorf("EventID = %q, want %q", got.EventID, f.EventID)
	}
	if got.DeliveryMode == nil || *got.DeliveryMode != *f.DeliveryMode {
		t.Errorf("DeliveryMode mismatch")
	}
	if len(got.Status) != len(f.Status) {
		t.Errorf("Status len = %d, want %d", len(got.Status), len(f.Status))
	}
	if got.HasError == nil || *got.HasError != *f.HasError {
		t.Errorf("HasError mismatch")
	}
	if got.MinRetries != f.MinRetries {
		t.Errorf("MinRetries = %d, want %d", got.MinRetries, f.MinRetries)
	}
	if got.OrderDesc != f.OrderDesc {
		t.Errorf("OrderDesc = %v, want %v", got.OrderDesc, f.OrderDesc)
	}
}

func TestProtoToFilter_Nil(t *testing.T) {
	f := ProtoToFilter(nil)
	if f.EventID != "" {
		t.Errorf("expected empty filter, got EventID=%q", f.EventID)
	}
}

func TestPageToListResponse_Nil(t *testing.T) {
	resp := PageToListResponse(nil)
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if len(resp.Entries) != 0 {
		t.Errorf("expected empty entries")
	}
}

func TestPageToListResponse(t *testing.T) {
	page := &monitor.Page{
		Entries: []*monitor.Entry{
			{EventID: "e1", EventName: "test"},
			{EventID: "e2", EventName: "test"},
		},
		NextCursor: "next",
		HasMore:    true,
	}

	resp := PageToListResponse(page)
	if len(resp.Entries) != 2 {
		t.Errorf("entries len = %d, want 2", len(resp.Entries))
	}
	if resp.NextCursor != "next" {
		t.Errorf("NextCursor = %q, want %q", resp.NextCursor, "next")
	}
	if !resp.HasMore {
		t.Error("HasMore should be true")
	}
}

func TestEntriesToProto_Nil(t *testing.T) {
	if got := EntriesToProto(nil); got != nil {
		t.Errorf("EntriesToProto(nil) = %v, want nil", got)
	}
}

func TestDurationConversions(t *testing.T) {
	d := 5 * time.Second
	pb := DurationToProto(d)
	got := ProtoToDuration(pb)
	if got != d {
		t.Errorf("round-trip duration = %v, want %v", got, d)
	}

	if ProtoToDuration(nil) != 0 {
		t.Error("ProtoToDuration(nil) should be 0")
	}
}

func TestStatusConversions(t *testing.T) {
	tests := []struct {
		status monitor.Status
		proto  Status
	}{
		{monitor.StatusPending, Status_STATUS_PENDING},
		{monitor.StatusCompleted, Status_STATUS_COMPLETED},
		{monitor.StatusFailed, Status_STATUS_FAILED},
		{monitor.StatusRetrying, Status_STATUS_RETRYING},
	}

	for _, tt := range tests {
		pb := statusToProto(tt.status)
		if pb != tt.proto {
			t.Errorf("statusToProto(%v) = %v, want %v", tt.status, pb, tt.proto)
		}
		got := protoToStatus(tt.proto)
		if got != tt.status {
			t.Errorf("protoToStatus(%v) = %v, want %v", tt.proto, got, tt.status)
		}
	}
}

func TestDeliveryModeConversions(t *testing.T) {
	tests := []struct {
		dm    monitor.DeliveryMode
		proto DeliveryMode
	}{
		{monitor.Broadcast, DeliveryMode_DELIVERY_MODE_BROADCAST},
		{monitor.WorkerPool, DeliveryMode_DELIVERY_MODE_WORKER_POOL},
	}

	for _, tt := range tests {
		pb := deliveryModeToProto(tt.dm)
		if pb != tt.proto {
			t.Errorf("deliveryModeToProto(%v) = %v, want %v", tt.dm, pb, tt.proto)
		}
		got := protoToDeliveryMode(tt.proto)
		if got != tt.dm {
			t.Errorf("protoToDeliveryMode(%v) = %v, want %v", tt.proto, got, tt.dm)
		}
	}
}

func TestDurationToProto_WithDuration(t *testing.T) {
	pb := DurationToProto(0)
	if pb == nil {
		t.Fatal("expected non-nil")
	}
	if pb.AsDuration() != 0 {
		t.Errorf("expected zero duration")
	}

	pb = DurationToProto(time.Minute)
	if pb.Seconds != 60 {
		t.Errorf("expected 60 seconds, got %d", pb.Seconds)
	}
}

func TestProtoToDuration_NilInput(t *testing.T) {
	got := ProtoToDuration(nil)
	if got != 0 {
		t.Errorf("expected 0, got %v", got)
	}
}

func TestProtoToDuration_ValidInput(t *testing.T) {
	pb := &durationpb.Duration{Seconds: 10, Nanos: 500000000}
	got := ProtoToDuration(pb)
	expected := 10*time.Second + 500*time.Millisecond
	if got != expected {
		t.Errorf("got %v, want %v", got, expected)
	}
}

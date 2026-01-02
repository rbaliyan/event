package monitorpb

import (
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EntryToProto converts a monitor.Entry to a protobuf Entry.
func EntryToProto(e *monitor.Entry) *Entry {
	if e == nil {
		return nil
	}

	pb := &Entry{
		EventId:        e.EventID,
		SubscriptionId: e.SubscriptionID,
		EventName:      e.EventName,
		BusId:          e.BusID,
		DeliveryMode:   deliveryModeToProto(e.DeliveryMode),
		Metadata:       e.Metadata,
		Status:         statusToProto(e.Status),
		Error:          e.Error,
		RetryCount:     int32(e.RetryCount),
		StartedAt:      timestamppb.New(e.StartedAt),
		Duration:       durationpb.New(e.Duration),
		TraceId:        e.TraceID,
		SpanId:         e.SpanID,
	}

	if e.CompletedAt != nil {
		pb.CompletedAt = timestamppb.New(*e.CompletedAt)
	}

	return pb
}

// ProtoToEntry converts a protobuf Entry to a monitor.Entry.
func ProtoToEntry(pb *Entry) *monitor.Entry {
	if pb == nil {
		return nil
	}

	e := &monitor.Entry{
		EventID:        pb.EventId,
		SubscriptionID: pb.SubscriptionId,
		EventName:      pb.EventName,
		BusID:          pb.BusId,
		DeliveryMode:   protoToDeliveryMode(pb.DeliveryMode),
		Metadata:       pb.Metadata,
		Status:         protoToStatus(pb.Status),
		Error:          pb.Error,
		RetryCount:     int(pb.RetryCount),
		TraceID:        pb.TraceId,
		SpanID:         pb.SpanId,
	}

	if pb.StartedAt != nil {
		e.StartedAt = pb.StartedAt.AsTime()
	}
	if pb.CompletedAt != nil {
		t := pb.CompletedAt.AsTime()
		e.CompletedAt = &t
	}
	if pb.Duration != nil {
		e.Duration = pb.Duration.AsDuration()
	}

	return e
}

// ProtoToFilter converts a protobuf Filter to a monitor.Filter.
func ProtoToFilter(pb *Filter) monitor.Filter {
	if pb == nil {
		return monitor.Filter{}
	}

	f := monitor.Filter{
		EventID:        pb.EventId,
		SubscriptionID: pb.SubscriptionId,
		EventName:      pb.EventName,
		BusID:          pb.BusId,
		MinRetries:     int(pb.MinRetries),
		Cursor:         pb.Cursor,
		Limit:          int(pb.Limit),
		OrderDesc:      pb.OrderDesc,
	}

	if pb.DeliveryMode != nil {
		dm := protoToDeliveryMode(*pb.DeliveryMode)
		f.DeliveryMode = &dm
	}

	if len(pb.Status) > 0 {
		f.Status = make([]monitor.Status, len(pb.Status))
		for i, s := range pb.Status {
			f.Status[i] = protoToStatus(s)
		}
	}

	if pb.HasError != nil {
		f.HasError = pb.HasError
	}

	if pb.StartTime != nil {
		f.StartTime = pb.StartTime.AsTime()
	}
	if pb.EndTime != nil {
		f.EndTime = pb.EndTime.AsTime()
	}
	if pb.MinDuration != nil {
		f.MinDuration = pb.MinDuration.AsDuration()
	}

	return f
}

// FilterToProto converts a monitor.Filter to a protobuf Filter.
func FilterToProto(f monitor.Filter) *Filter {
	pb := &Filter{
		EventId:        f.EventID,
		SubscriptionId: f.SubscriptionID,
		EventName:      f.EventName,
		BusId:          f.BusID,
		MinRetries:     int32(f.MinRetries),
		Cursor:         f.Cursor,
		Limit:          int32(f.Limit),
		OrderDesc:      f.OrderDesc,
	}

	if f.DeliveryMode != nil {
		dm := deliveryModeToProto(*f.DeliveryMode)
		pb.DeliveryMode = &dm
	}

	if len(f.Status) > 0 {
		pb.Status = make([]Status, len(f.Status))
		for i, s := range f.Status {
			pb.Status[i] = statusToProto(s)
		}
	}

	pb.HasError = f.HasError

	if !f.StartTime.IsZero() {
		pb.StartTime = timestamppb.New(f.StartTime)
	}
	if !f.EndTime.IsZero() {
		pb.EndTime = timestamppb.New(f.EndTime)
	}
	if f.MinDuration > 0 {
		pb.MinDuration = durationpb.New(f.MinDuration)
	}

	return pb
}

// PageToListResponse converts a monitor.Page to a protobuf ListResponse.
func PageToListResponse(p *monitor.Page) *ListResponse {
	if p == nil {
		return &ListResponse{}
	}

	resp := &ListResponse{
		NextCursor: p.NextCursor,
		HasMore:    p.HasMore,
	}

	if len(p.Entries) > 0 {
		resp.Entries = make([]*Entry, len(p.Entries))
		for i, e := range p.Entries {
			resp.Entries[i] = EntryToProto(e)
		}
	}

	return resp
}

// EntriesToProto converts a slice of monitor.Entry to protobuf Entries.
func EntriesToProto(entries []*monitor.Entry) []*Entry {
	if entries == nil {
		return nil
	}

	pb := make([]*Entry, len(entries))
	for i, e := range entries {
		pb[i] = EntryToProto(e)
	}
	return pb
}

// Helper functions for enum conversions

func deliveryModeToProto(dm monitor.DeliveryMode) DeliveryMode {
	switch dm {
	case monitor.Broadcast:
		return DeliveryMode_DELIVERY_MODE_BROADCAST
	case monitor.WorkerPool:
		return DeliveryMode_DELIVERY_MODE_WORKER_POOL
	default:
		return DeliveryMode_DELIVERY_MODE_UNSPECIFIED
	}
}

func protoToDeliveryMode(dm DeliveryMode) monitor.DeliveryMode {
	switch dm {
	case DeliveryMode_DELIVERY_MODE_BROADCAST:
		return monitor.Broadcast
	case DeliveryMode_DELIVERY_MODE_WORKER_POOL:
		return monitor.WorkerPool
	default:
		return monitor.Broadcast
	}
}

func statusToProto(s monitor.Status) Status {
	switch s {
	case monitor.StatusPending:
		return Status_STATUS_PENDING
	case monitor.StatusCompleted:
		return Status_STATUS_COMPLETED
	case monitor.StatusFailed:
		return Status_STATUS_FAILED
	case monitor.StatusRetrying:
		return Status_STATUS_RETRYING
	default:
		return Status_STATUS_UNSPECIFIED
	}
}

func protoToStatus(s Status) monitor.Status {
	switch s {
	case Status_STATUS_PENDING:
		return monitor.StatusPending
	case Status_STATUS_COMPLETED:
		return monitor.StatusCompleted
	case Status_STATUS_FAILED:
		return monitor.StatusFailed
	case Status_STATUS_RETRYING:
		return monitor.StatusRetrying
	default:
		return monitor.StatusPending
	}
}

// DurationToProto converts a time.Duration to a protobuf Duration.
func DurationToProto(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}

// ProtoToDuration converts a protobuf Duration to a time.Duration.
func ProtoToDuration(pb *durationpb.Duration) time.Duration {
	if pb == nil {
		return 0
	}
	return pb.AsDuration()
}

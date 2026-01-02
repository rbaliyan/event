// Package grpc provides a gRPC service for the monitor package.
package grpc

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/monitor"
	pb "github.com/rbaliyan/event/v3/monitor/proto"
	"github.com/rbaliyan/event/v3/monitor/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultPollInterval is the default interval for streaming polls.
const DefaultPollInterval = 100 * time.Millisecond

// Service implements the MonitorService gRPC service.
type Service struct {
	pb.UnimplementedMonitorServiceServer
	store        monitor.Store
	broadcaster  *stream.Broadcaster
	pollInterval time.Duration
}

// New creates a new gRPC service for the monitor.
func New(store monitor.Store) *Service {
	return &Service{
		store:        store,
		pollInterval: DefaultPollInterval,
	}
}

// NewWithPollInterval creates a new gRPC service with a custom poll interval for streaming.
func NewWithPollInterval(store monitor.Store, pollInterval time.Duration) *Service {
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	return &Service{
		store:        store,
		pollInterval: pollInterval,
	}
}

// Register registers the service with a gRPC server.
func (s *Service) Register(server *grpc.Server) {
	pb.RegisterMonitorServiceServer(server, s)
}

// StartBroadcaster starts the broadcaster for streaming.
// Call this before using Stream RPC.
func (s *Service) StartBroadcaster(ctx context.Context) {
	if s.broadcaster == nil {
		s.broadcaster = stream.NewBroadcaster(s.store, s.pollInterval)
	}
	s.broadcaster.Start(ctx)
}

// StopBroadcaster stops the broadcaster.
func (s *Service) StopBroadcaster() {
	if s.broadcaster != nil {
		s.broadcaster.Stop()
	}
}

// List returns entries matching the filter with cursor-based pagination.
func (s *Service) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	filter := pb.ProtoToFilter(req.GetFilter())

	page, err := s.store.List(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list entries: %v", err)
	}

	return pb.PageToListResponse(page), nil
}

// Get retrieves a single entry by event ID and subscription ID.
func (s *Service) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.GetEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "event_id is required")
	}

	entry, err := s.store.Get(ctx, req.GetEventId(), req.GetSubscriptionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get entry: %v", err)
	}
	if entry == nil {
		return nil, status.Error(codes.NotFound, "entry not found")
	}

	return &pb.GetResponse{
		Entry: pb.EntryToProto(entry),
	}, nil
}

// GetByEventID returns all entries for an event ID.
func (s *Service) GetByEventID(ctx context.Context, req *pb.GetByEventIDRequest) (*pb.GetByEventIDResponse, error) {
	if req.GetEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "event_id is required")
	}

	entries, err := s.store.GetByEventID(ctx, req.GetEventId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get entries: %v", err)
	}

	return &pb.GetByEventIDResponse{
		Entries: pb.EntriesToProto(entries),
	}, nil
}

// Count returns the number of entries matching the filter.
func (s *Service) Count(ctx context.Context, req *pb.CountRequest) (*pb.CountResponse, error) {
	filter := pb.ProtoToFilter(req.GetFilter())

	count, err := s.store.Count(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count entries: %v", err)
	}

	return &pb.CountResponse{
		Count: count,
	}, nil
}

// DeleteOlderThan removes entries older than the specified age.
func (s *Service) DeleteOlderThan(ctx context.Context, req *pb.DeleteOlderThanRequest) (*pb.DeleteOlderThanResponse, error) {
	age := pb.ProtoToDuration(req.GetAge())
	if age <= 0 {
		return nil, status.Error(codes.InvalidArgument, "age must be positive")
	}

	deleted, err := s.store.DeleteOlderThan(ctx, age)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete entries: %v", err)
	}

	return &pb.DeleteOlderThanResponse{
		Deleted: deleted,
	}, nil
}

// Stream returns a real-time stream of entries matching the filter.
func (s *Service) Stream(req *pb.StreamRequest, stream pb.MonitorService_StreamServer) error {
	if s.broadcaster == nil {
		return status.Error(codes.FailedPrecondition, "broadcaster not started, call StartBroadcaster first")
	}

	filter := pb.ProtoToFilter(req.GetFilter())
	sub := s.broadcaster.Subscribe(filter)
	defer s.broadcaster.Unsubscribe(sub)

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case entry, ok := <-sub.Entries():
			if !ok {
				return nil
			}
			if err := stream.Send(pb.EntryToProto(entry)); err != nil {
				return err
			}
		}
	}
}

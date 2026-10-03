// Package logging is the gRPC transport for Cloud Logging v2
// (google.logging.v2.LoggingServiceV2). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/logging: it transcodes between
// the generated protobuf messages and the core's typed API and maps core errors
// to gRPC status codes. It owns no business logic and no state.
//
// Implemented RPCs: WriteLogEntries, ListLogEntries, ListLogs, DeleteLog,
// ListMonitoredResourceDescriptors, and TailLogEntries. TailLogEntries is a
// bidirectional stream and stays transport-specific; it consumes the core's
// filter, scope, and entry-listing primitives rather than reimplementing them.
// See the core package for the store-backed behavior and the documented
// exceptions/tail approximation.
package logging

import (
	"context"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"

	labelpb "google.golang.org/genproto/googleapis/api/label"
	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service implements loggingpb.LoggingServiceV2Server over the shared core.
type Service struct {
	loggingpb.UnimplementedLoggingServiceV2Server

	core        *core.Service
	defaultProj string
}

// NewService returns a Logging gRPC service wrapping the core. defaultProj is
// the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// defaultScope resolves the scope parent used when a request omits its resource
// name, from gRPC routing metadata or the configured default project. It returns
// "" when nothing resolves (an empty store key lists nothing).
func (s *Service) defaultScope(ctx context.Context) string {
	scope, err := core.ParseScopeParent(grpcutil.ProjectFromMetadata(ctx, s.defaultProj))
	if err != nil {
		return ""
	}
	return scope
}

// ─── LoggingServiceV2 ─────────────────────────────────────────────────────────

func (s *Service) WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) (*loggingpb.WriteLogEntriesResponse, error) {
	wr := &core.WriteRequest{
		PartialSuccess: req.GetPartialSuccess(),
		DryRun:         req.GetDryRun(),
		LogName:        req.GetLogName(),
		Labels:         req.GetLabels(),
	}
	if res := req.GetResource(); res != nil {
		wr.ResourceType = res.GetType()
		wr.ResourceLabels = res.GetLabels()
	}
	wr.Entries = make([]loggingstore.LogEntry, 0, len(req.GetEntries()))
	for _, p := range req.GetEntries() {
		wr.Entries = append(wr.Entries, entryFromProto(p))
	}
	if err := s.core.WriteEntries(ctx, wr); err != nil {
		return nil, mapError(err)
	}
	return &loggingpb.WriteLogEntriesResponse{}, nil
}

func (s *Service) ListLogEntries(ctx context.Context, req *loggingpb.ListLogEntriesRequest) (*loggingpb.ListLogEntriesResponse, error) {
	res, err := s.core.ListEntries(ctx, &core.ListEntriesRequest{
		ResourceNames: req.GetResourceNames(),
		Filter:        req.GetFilter(),
		OrderBy:       req.GetOrderBy(),
		PageSize:      int(req.GetPageSize()),
		PageToken:     req.GetPageToken(),
		Scope:         s.defaultScope(ctx),
	})
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogEntry, 0, len(res.Entries))
	for _, e := range res.Entries {
		out = append(out, entryToProto(e))
	}
	return &loggingpb.ListLogEntriesResponse{Entries: out, NextPageToken: res.NextPageToken}, nil
}

func (s *Service) ListLogs(ctx context.Context, req *loggingpb.ListLogsRequest) (*loggingpb.ListLogsResponse, error) {
	names, next, err := s.core.ListLogs(ctx, req.GetParent(), s.defaultScope(ctx), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	return &loggingpb.ListLogsResponse{LogNames: names, NextPageToken: next}, nil
}

func (s *Service) DeleteLog(ctx context.Context, req *loggingpb.DeleteLogRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteLog(ctx, req.GetLogName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListMonitoredResourceDescriptors(_ context.Context, req *loggingpb.ListMonitoredResourceDescriptorsRequest) (*loggingpb.ListMonitoredResourceDescriptorsResponse, error) {
	page, next := s.core.ListMonitoredResourceDescriptors(int(req.GetPageSize()), req.GetPageToken())
	out := make([]*monitoredres.MonitoredResourceDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, descriptorToProto(d))
	}
	return &loggingpb.ListMonitoredResourceDescriptorsResponse{
		ResourceDescriptors: out,
		NextPageToken:       next,
	}, nil
}

// descriptorToProto renders a neutral descriptor as the proto type. The Logging
// surface leaves the descriptor resource name unset (real Cloud Logging does).
func descriptorToProto(d core.MonitoredResourceDescriptor) *monitoredres.MonitoredResourceDescriptor {
	labels := make([]*labelpb.LabelDescriptor, 0, len(d.Labels))
	for _, l := range d.Labels {
		labels = append(labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelValueType(l.ValueType),
			Description: l.Description,
		})
	}
	return &monitoredres.MonitoredResourceDescriptor{
		Type:        d.Type,
		DisplayName: d.DisplayName,
		Description: d.Description,
		Labels:      labels,
	}
}

func labelValueType(name string) labelpb.LabelDescriptor_ValueType {
	switch name {
	case "BOOL":
		return labelpb.LabelDescriptor_BOOL
	case "INT64":
		return labelpb.LabelDescriptor_INT64
	default:
		return labelpb.LabelDescriptor_STRING
	}
}

// compile-time assertion that Service implements the generated server.
var _ loggingpb.LoggingServiceV2Server = (*Service)(nil)

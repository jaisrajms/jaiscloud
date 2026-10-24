package logging

import (
	"context"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/gcp/timestamp"

	"google.golang.org/protobuf/types/known/emptypb"
)

// ConfigService implements the Cloud Logging v2 config-plane RPCs
// (google.logging.v2.ConfigServiceV2) over the shared transport-neutral core.
// It covers sinks and exclusions (config.go), and the Admin v2 surface
// (admin.go): log buckets (sync + async), views, BigQuery links, log scopes,
// and the Settings/CMEK records. CopyLogEntries is inherited from the generated
// Unimplemented stub and fails loud — the emulator has no per-bucket entry data
// plane, so a cross-project copy has nothing faithful to copy.
type ConfigService struct {
	loggingpb.UnimplementedConfigServiceV2Server

	core        *core.Service
	defaultProj string
}

// NewConfigService returns the config-plane gRPC service wrapping the core.
func NewConfigService(c *core.Service, defaultProj string) *ConfigService {
	return &ConfigService{core: c, defaultProj: defaultProj}
}

// configParent resolves a create/list parent, falling back to the configured
// default project scope when the request carries none.
func (s *ConfigService) configParent(ctx context.Context, parent string) string {
	if parent != "" {
		return parent
	}
	return resource.ResourceID(grpcutil.ProjectFromMetadata(ctx, s.defaultProj))("project", "")
}

// ─── sinks ────────────────────────────────────────────────────────────────────

func (s *ConfigService) CreateSink(ctx context.Context, req *loggingpb.CreateSinkRequest) (*loggingpb.LogSink, error) {
	sink, err := s.core.CreateSink(ctx, s.configParent(ctx, req.GetParent()),
		sinkFromProto(req.GetSink()), req.GetUniqueWriterIdentity(), "")
	if err != nil {
		return nil, mapError(err)
	}
	return sinkToProto(sink), nil
}

func (s *ConfigService) GetSink(ctx context.Context, req *loggingpb.GetSinkRequest) (*loggingpb.LogSink, error) {
	sink, err := s.core.GetSink(ctx, req.GetSinkName())
	if err != nil {
		return nil, mapError(err)
	}
	return sinkToProto(sink), nil
}

func (s *ConfigService) ListSinks(ctx context.Context, req *loggingpb.ListSinksRequest) (*loggingpb.ListSinksResponse, error) {
	sinks, next, err := s.core.ListSinks(ctx, s.configParent(ctx, req.GetParent()),
		int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogSink, 0, len(sinks))
	for _, sink := range sinks {
		out = append(out, sinkToProto(sink))
	}
	return &loggingpb.ListSinksResponse{Sinks: out, NextPageToken: next}, nil
}

func (s *ConfigService) UpdateSink(ctx context.Context, req *loggingpb.UpdateSinkRequest) (*loggingpb.LogSink, error) {
	sink, err := s.core.UpdateSink(ctx, req.GetSinkName(), sinkFromProto(req.GetSink()),
		req.GetUpdateMask().GetPaths(), req.GetUniqueWriterIdentity(), "")
	if err != nil {
		return nil, mapError(err)
	}
	return sinkToProto(sink), nil
}

func (s *ConfigService) DeleteSink(ctx context.Context, req *loggingpb.DeleteSinkRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteSink(ctx, req.GetSinkName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── exclusions ───────────────────────────────────────────────────────────────

func (s *ConfigService) CreateExclusion(ctx context.Context, req *loggingpb.CreateExclusionRequest) (*loggingpb.LogExclusion, error) {
	e, err := s.core.CreateExclusion(ctx, s.configParent(ctx, req.GetParent()), exclusionFromProto(req.GetExclusion()))
	if err != nil {
		return nil, mapError(err)
	}
	return exclusionToProto(e), nil
}

func (s *ConfigService) GetExclusion(ctx context.Context, req *loggingpb.GetExclusionRequest) (*loggingpb.LogExclusion, error) {
	e, err := s.core.GetExclusion(ctx, req.GetName())
	if err != nil {
		return nil, mapError(err)
	}
	return exclusionToProto(e), nil
}

func (s *ConfigService) ListExclusions(ctx context.Context, req *loggingpb.ListExclusionsRequest) (*loggingpb.ListExclusionsResponse, error) {
	list, next, err := s.core.ListExclusions(ctx, s.configParent(ctx, req.GetParent()),
		int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogExclusion, 0, len(list))
	for _, e := range list {
		out = append(out, exclusionToProto(e))
	}
	return &loggingpb.ListExclusionsResponse{Exclusions: out, NextPageToken: next}, nil
}

func (s *ConfigService) UpdateExclusion(ctx context.Context, req *loggingpb.UpdateExclusionRequest) (*loggingpb.LogExclusion, error) {
	e, err := s.core.UpdateExclusion(ctx, req.GetName(), exclusionFromProto(req.GetExclusion()),
		req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return exclusionToProto(e), nil
}

func (s *ConfigService) DeleteExclusion(ctx context.Context, req *loggingpb.DeleteExclusionRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteExclusion(ctx, req.GetName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── transcoding ──────────────────────────────────────────────────────────────

func sinkFromProto(p *loggingpb.LogSink) loggingstore.LogSink {
	if p == nil {
		return loggingstore.LogSink{}
	}
	s := loggingstore.LogSink{
		Name:                p.GetName(),
		Destination:         p.GetDestination(),
		Filter:              p.GetFilter(),
		Description:         p.GetDescription(),
		Disabled:            p.GetDisabled(),
		IncludeChildren:     p.GetIncludeChildren(),
		OutputVersionFormat: versionFormatName(p.GetOutputVersionFormat()),
	}
	if bo := p.GetBigqueryOptions(); bo != nil {
		// Only `use_partitioned_tables` is writable; the sibling
		// `uses_timestamp_column_partitioning` is output-only.
		s.BigQueryOptions = &loggingstore.LogBigQueryOptions{
			UsePartitionedTables: bo.GetUsePartitionedTables(),
		}
	}
	for _, ex := range p.GetExclusions() {
		s.Exclusions = append(s.Exclusions, exclusionFromProto(ex))
	}
	return s
}

// versionFormatName maps the deprecated OutputVersionFormat enum to its wire
// string, collapsing the unspecified zero value to "" so an unset field is not
// echoed back.
func versionFormatName(v loggingpb.LogSink_VersionFormat) string {
	if v == loggingpb.LogSink_VERSION_FORMAT_UNSPECIFIED {
		return ""
	}
	return v.String()
}

// versionFormatValue maps a stored OutputVersionFormat string back to the enum.
func versionFormatValue(name string) loggingpb.LogSink_VersionFormat {
	if v, ok := loggingpb.LogSink_VersionFormat_value[name]; ok {
		return loggingpb.LogSink_VersionFormat(v)
	}
	return loggingpb.LogSink_VERSION_FORMAT_UNSPECIFIED
}

func sinkToProto(s loggingstore.LogSink) *loggingpb.LogSink {
	out := &loggingpb.LogSink{
		Name:                s.Name,
		Destination:         s.Destination,
		Filter:              s.Filter,
		Description:         s.Description,
		Disabled:            s.Disabled,
		WriterIdentity:      s.WriterIdentity,
		IncludeChildren:     s.IncludeChildren,
		OutputVersionFormat: versionFormatValue(s.OutputVersionFormat),
	}
	if s.BigQueryOptions != nil {
		out.Options = &loggingpb.LogSink_BigqueryOptions{BigqueryOptions: &loggingpb.BigQueryOptions{
			UsePartitionedTables:            s.BigQueryOptions.UsePartitionedTables,
			UsesTimestampColumnPartitioning: s.BigQueryOptions.UsesTimestampColumnPartitioning,
		}}
	}
	for _, ex := range s.Exclusions {
		out.Exclusions = append(out.Exclusions, exclusionToProto(ex))
	}
	if !s.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(s.CreateTime)
	}
	if !s.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(s.UpdateTime)
	}
	return out
}

func exclusionFromProto(p *loggingpb.LogExclusion) loggingstore.LogExclusion {
	if p == nil {
		return loggingstore.LogExclusion{}
	}
	return loggingstore.LogExclusion{
		Name:        p.GetName(),
		Description: p.GetDescription(),
		Filter:      p.GetFilter(),
		Disabled:    p.GetDisabled(),
	}
}

func exclusionToProto(e loggingstore.LogExclusion) *loggingpb.LogExclusion {
	out := &loggingpb.LogExclusion{
		Name:        e.Name,
		Description: e.Description,
		Filter:      e.Filter,
		Disabled:    e.Disabled,
	}
	if !e.CreateTime.IsZero() {
		out.CreateTime = timestamp.Proto(e.CreateTime)
	}
	if !e.UpdateTime.IsZero() {
		out.UpdateTime = timestamp.Proto(e.UpdateTime)
	}
	return out
}

// compile-time assertion that ConfigService implements the generated server.
var _ loggingpb.ConfigServiceV2Server = (*ConfigService)(nil)

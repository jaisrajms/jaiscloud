package logging

import (
	"context"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"

	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MetricsService implements the Cloud Logging v2 logs-based metric plane
// (google.logging.v2.MetricsServiceV2) over the shared transport-neutral core.
// It is a thin proto adapter: it transcodes between the generated messages and
// the core's typed API and owns no business logic.
type MetricsService struct {
	loggingpb.UnimplementedMetricsServiceV2Server

	core        *core.Service
	defaultProj string
}

// NewMetricsService returns the metrics-plane gRPC service wrapping the core.
func NewMetricsService(c *core.Service, defaultProj string) *MetricsService {
	return &MetricsService{core: c, defaultProj: defaultProj}
}

// metricsParent resolves a create/list parent, falling back to the configured
// default project scope when the request carries none.
func (s *MetricsService) metricsParent(ctx context.Context, parent string) string {
	if parent != "" {
		return parent
	}
	return "projects/" + grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

func (s *MetricsService) ListLogMetrics(ctx context.Context, req *loggingpb.ListLogMetricsRequest) (*loggingpb.ListLogMetricsResponse, error) {
	metrics, next, err := s.core.ListMetrics(ctx, s.metricsParent(ctx, req.GetParent()),
		int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*loggingpb.LogMetric, 0, len(metrics))
	for _, m := range metrics {
		out = append(out, metricToProto(m))
	}
	return &loggingpb.ListLogMetricsResponse{Metrics: out, NextPageToken: next}, nil
}

func (s *MetricsService) GetLogMetric(ctx context.Context, req *loggingpb.GetLogMetricRequest) (*loggingpb.LogMetric, error) {
	m, err := s.core.GetMetric(ctx, req.GetMetricName())
	if err != nil {
		return nil, mapError(err)
	}
	return metricToProto(m), nil
}

func (s *MetricsService) CreateLogMetric(ctx context.Context, req *loggingpb.CreateLogMetricRequest) (*loggingpb.LogMetric, error) {
	m, err := s.core.CreateMetric(ctx, s.metricsParent(ctx, req.GetParent()), metricFromProto(req.GetMetric()))
	if err != nil {
		return nil, mapError(err)
	}
	return metricToProto(m), nil
}

func (s *MetricsService) UpdateLogMetric(ctx context.Context, req *loggingpb.UpdateLogMetricRequest) (*loggingpb.LogMetric, error) {
	m, err := s.core.UpdateMetric(ctx, req.GetMetricName(), metricFromProto(req.GetMetric()))
	if err != nil {
		return nil, mapError(err)
	}
	return metricToProto(m), nil
}

func (s *MetricsService) DeleteLogMetric(ctx context.Context, req *loggingpb.DeleteLogMetricRequest) (*emptypb.Empty, error) {
	if err := s.core.DeleteMetric(ctx, req.GetMetricName()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── transcoding ──────────────────────────────────────────────────────────────

func metricFromProto(p *loggingpb.LogMetric) loggingstore.LogMetric {
	if p == nil {
		return loggingstore.LogMetric{}
	}
	m := loggingstore.LogMetric{
		Name:            p.GetName(),
		Description:     p.GetDescription(),
		Filter:          p.GetFilter(),
		Disabled:        p.GetDisabled(),
		BucketName:      p.GetBucketName(),
		ValueExtractor:  p.GetValueExtractor(),
		LabelExtractors: p.GetLabelExtractors(),
	}
	if md := p.GetMetricDescriptor(); md != nil {
		d := loggingstore.LogMetricDescriptor{
			MetricKind:  md.GetMetricKind().String(),
			ValueType:   md.GetValueType().String(),
			Unit:        md.GetUnit(),
			DisplayName: md.GetDisplayName(),
		}
		for _, l := range md.GetLabels() {
			d.Labels = append(d.Labels, loggingstore.LogMetricLabel{
				Key:         l.GetKey(),
				ValueType:   l.GetValueType().String(),
				Description: l.GetDescription(),
			})
		}
		m.Descriptor = d
	}
	if bo := p.GetBucketOptions(); bo != nil {
		if raw, err := protojson.Marshal(bo); err == nil {
			m.BucketOptions = raw
		}
	}
	return m
}

func metricToProto(m loggingstore.LogMetric) *loggingpb.LogMetric {
	out := &loggingpb.LogMetric{
		Name:           m.Name,
		Description:    m.Description,
		Filter:         m.Filter,
		Disabled:       m.Disabled,
		BucketName:     m.BucketName,
		ValueExtractor: m.ValueExtractor,
	}
	if len(m.LabelExtractors) > 0 {
		out.LabelExtractors = m.LabelExtractors
	}
	if len(m.BucketOptions) > 0 {
		bo := &distributionpb.Distribution_BucketOptions{}
		if err := protojson.Unmarshal(m.BucketOptions, bo); err == nil {
			out.BucketOptions = bo
		}
	}
	d := m.Descriptor
	md := &metricpb.MetricDescriptor{
		Name:        d.Name,
		Type:        d.Type,
		Description: d.Description,
		Unit:        d.Unit,
		MetricKind:  metricKindFromName(d.MetricKind),
		ValueType:   metricValueTypeFromName(d.ValueType),
		DisplayName: d.DisplayName,
	}
	for _, l := range d.Labels {
		md.Labels = append(md.Labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelValueType(l.ValueType),
			Description: l.Description,
		})
	}
	out.MetricDescriptor = md
	if !m.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(m.CreateTime)
	}
	if !m.UpdateTime.IsZero() {
		out.UpdateTime = timestamppb.New(m.UpdateTime)
	}
	return out
}

func metricKindFromName(name string) metricpb.MetricDescriptor_MetricKind {
	switch name {
	case "GAUGE":
		return metricpb.MetricDescriptor_GAUGE
	case "CUMULATIVE":
		return metricpb.MetricDescriptor_CUMULATIVE
	default:
		return metricpb.MetricDescriptor_DELTA
	}
}

func metricValueTypeFromName(name string) metricpb.MetricDescriptor_ValueType {
	switch name {
	case "BOOL":
		return metricpb.MetricDescriptor_BOOL
	case "DOUBLE":
		return metricpb.MetricDescriptor_DOUBLE
	case "STRING":
		return metricpb.MetricDescriptor_STRING
	case "DISTRIBUTION":
		return metricpb.MetricDescriptor_DISTRIBUTION
	case "MONEY":
		return metricpb.MetricDescriptor_MONEY
	default:
		return metricpb.MetricDescriptor_INT64
	}
}

// compile-time assertion that MetricsService implements the generated server.
var _ loggingpb.MetricsServiceV2Server = (*MetricsService)(nil)

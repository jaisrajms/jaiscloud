// Package monitoring is the gRPC transport for Cloud Monitoring v3
// (MetricService, AlertPolicyService, NotificationChannelService, and
// ServiceMonitoringService). It is a thin
// proto adapter over the transport-neutral core in
// internal/gcp/service/monitoring: it transcodes between the generated protobuf
// messages and the core's typed API and maps core errors to gRPC status codes.
// It owns no business logic and no state.
package monitoring

import (
	"context"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/model"
)

// Service implements monitoringpb.MetricServiceServer,
// monitoringpb.AlertPolicyServiceServer,
// monitoringpb.NotificationChannelServiceServer, and
// monitoringpb.ServiceMonitoringServiceServer over the shared core.
type Service struct {
	monitoringpb.UnimplementedMetricServiceServer
	monitoringpb.UnimplementedAlertPolicyServiceServer
	monitoringpb.UnimplementedNotificationChannelServiceServer
	monitoringpb.UnimplementedServiceMonitoringServiceServer

	core        *core.Service
	defaultProj string
}

// NewService returns a Monitoring gRPC service wrapping the core. defaultProj
// is the config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// project resolves the owning project: the resource name's project when
// present, else gRPC routing metadata, else the configured default.
func (s *Service) project(ctx context.Context, name string) string {
	if p := core.ProjectFromResourceName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

func timeIntervalFromProto(iv *monitoringpb.TimeInterval) *core.TimeInterval {
	if iv == nil {
		return nil
	}
	out := &core.TimeInterval{}
	if iv.GetStartTime() != nil {
		out.Start = iv.GetStartTime().AsTime()
	}
	if iv.GetEndTime() != nil {
		out.End = iv.GetEndTime().AsTime()
	}
	return out
}

// aggregationFromProto maps the proto Aggregation to the core's transport-neutral
// form. A nil aggregation stays nil (no aggregation requested).
func aggregationFromProto(a *monitoringpb.Aggregation) *core.Aggregation {
	if a == nil {
		return nil
	}
	return &core.Aggregation{
		AlignmentPeriod:    a.GetAlignmentPeriod().AsDuration(),
		PerSeriesAligner:   core.Aligner(a.GetPerSeriesAligner().String()),
		CrossSeriesReducer: core.Reducer(a.GetCrossSeriesReducer().String()),
		GroupByFields:      a.GetGroupByFields(),
	}
}

// ─── MetricService: metric descriptors ────────────────────────────────────────

func (s *Service) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	page, next, err := s.core.ListMetricDescriptors(ctx, project, req.GetFilter(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*metricpb.MetricDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, metricDescriptorToProto(d, project))
	}
	return &monitoringpb.ListMetricDescriptorsResponse{MetricDescriptors: out, NextPageToken: next}, nil
}

func (s *Service) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project, typ, ok := core.SplitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid metric descriptor name: " + req.GetName()))
	}
	d, err := s.core.GetMetricDescriptor(ctx, project, typ)
	if err != nil {
		return nil, mapError(err)
	}
	return metricDescriptorToProto(d, project), nil
}

func (s *Service) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project := s.project(ctx, req.GetName())
	d := metricDescriptorFromProto(req.GetMetricDescriptor())
	stored, err := s.core.CreateMetricDescriptor(ctx, project, d)
	if err != nil {
		return nil, mapError(err)
	}
	return metricDescriptorToProto(stored, project), nil
}

func (s *Service) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
	project, typ, ok := core.SplitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid metric descriptor name: " + req.GetName()))
	}
	if err := s.core.DeleteMetricDescriptor(ctx, project, typ); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── MetricService: time series ───────────────────────────────────────────────

func (s *Service) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	project := s.project(ctx, req.GetName())
	headersOnly := req.GetView() == monitoringpb.ListTimeSeriesRequest_HEADERS
	page, next, err := s.core.ListTimeSeries(ctx, project, req.GetFilter(), timeIntervalFromProto(req.GetInterval()), aggregationFromProto(req.GetAggregation()), aggregationFromProto(req.GetSecondaryAggregation()), req.GetOrderBy(), headersOnly, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.TimeSeries, 0, len(page))
	for _, ts := range page {
		out = append(out, timeSeriesToProto(ts))
	}
	return &monitoringpb.ListTimeSeriesResponse{TimeSeries: out, NextPageToken: next}, nil
}

func (s *Service) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	if err := s.writeTimeSeries(ctx, req.GetName(), req.GetTimeSeries(), false); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) CreateServiceTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	if err := s.writeTimeSeries(ctx, req.GetName(), req.GetTimeSeries(), true); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) writeTimeSeries(ctx context.Context, name string, series []*monitoringpb.TimeSeries, isService bool) error {
	project := s.project(ctx, name)
	out := make([]monitoringstore.TimeSeries, 0, len(series))
	for _, p := range series {
		ts, err := timeSeriesFromProto(p)
		if err != nil {
			return mapError(err)
		}
		out = append(out, ts)
	}
	var err error
	if isService {
		err = s.core.CreateServiceTimeSeries(ctx, project, out)
	} else {
		err = s.core.CreateTimeSeries(ctx, project, out)
	}
	if err != nil {
		return mapError(err)
	}
	return nil
}

// ─── MetricService: monitored resource descriptors ────────────────────────────

func (s *Service) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	page, next, err := s.core.ListMonitoredResourceDescriptors(ctx, project, req.GetFilter(), int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, monitoredResourceDescriptorToProto(d))
	}
	return &monitoringpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: out, NextPageToken: next}, nil
}

func (s *Service) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest) (*monitoredrespb.MonitoredResourceDescriptor, error) {
	project, typ, ok := core.SplitMonitoredResourceDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid monitored resource descriptor name: " + req.GetName()))
	}
	d, err := s.core.GetMonitoredResourceDescriptor(ctx, project, typ)
	if err != nil {
		return nil, mapError(err)
	}
	return monitoredResourceDescriptorToProto(d), nil
}

// ─── AlertPolicyService ───────────────────────────────────────────────────────

func (s *Service) ListAlertPolicies(ctx context.Context, req *monitoringpb.ListAlertPoliciesRequest) (*monitoringpb.ListAlertPoliciesResponse, error) {
	project := s.project(ctx, req.GetName())
	page, total, next, err := s.core.ListAlertPolicies(ctx, project, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.AlertPolicy, 0, len(page))
	for _, p := range page {
		out = append(out, alertPolicyToProto(p, project))
	}
	return &monitoringpb.ListAlertPoliciesResponse{
		AlertPolicies: out,
		NextPageToken: next,
		TotalSize:     int32(total),
	}, nil
}

func (s *Service) GetAlertPolicy(ctx context.Context, req *monitoringpb.GetAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := core.SplitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid alert policy name: " + req.GetName()))
	}
	p, err := s.core.GetAlertPolicy(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) CreateAlertPolicy(ctx context.Context, req *monitoringpb.CreateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project := s.project(ctx, req.GetName())
	incoming, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	p, err := s.core.CreateAlertPolicy(ctx, project, incoming)
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) UpdateAlertPolicy(ctx context.Context, req *monitoringpb.UpdateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := core.SplitAlertPolicyName(req.GetAlertPolicy().GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid alert policy name: " + req.GetAlertPolicy().GetName()))
	}
	incoming, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	p, err := s.core.UpdateAlertPolicy(ctx, project, id, incoming, req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) DeleteAlertPolicy(ctx context.Context, req *monitoringpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
	project, id, ok := core.SplitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid alert policy name: " + req.GetName()))
	}
	if err := s.core.DeleteAlertPolicy(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── NotificationChannelService ──────────────────────────────────────────────

func (s *Service) ListNotificationChannels(ctx context.Context, req *monitoringpb.ListNotificationChannelsRequest) (*monitoringpb.ListNotificationChannelsResponse, error) {
	project := s.project(ctx, req.GetName())
	page, total, next, err := s.core.ListNotificationChannels(ctx, project, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.NotificationChannel, 0, len(page))
	for _, c := range page {
		out = append(out, notificationChannelToProto(c, project))
	}
	return &monitoringpb.ListNotificationChannelsResponse{
		NotificationChannels: out,
		NextPageToken:        next,
		TotalSize:            int32(total),
	}, nil
}

func (s *Service) GetNotificationChannel(ctx context.Context, req *monitoringpb.GetNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetName()))
	}
	c, err := s.core.GetNotificationChannel(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) CreateNotificationChannel(ctx context.Context, req *monitoringpb.CreateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project := s.project(ctx, req.GetName())
	c, err := s.core.CreateNotificationChannel(ctx, project, notificationChannelFromProto(req.GetNotificationChannel()))
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) UpdateNotificationChannel(ctx context.Context, req *monitoringpb.UpdateNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetNotificationChannel().GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetNotificationChannel().GetName()))
	}
	c, err := s.core.UpdateNotificationChannel(ctx, project, id, notificationChannelFromProto(req.GetNotificationChannel()), req.GetUpdateMask().GetPaths())
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

func (s *Service) DeleteNotificationChannel(ctx context.Context, req *monitoringpb.DeleteNotificationChannelRequest) (*emptypb.Empty, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetName()))
	}
	if err := s.core.DeleteNotificationChannel(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListNotificationChannelDescriptors(ctx context.Context, req *monitoringpb.ListNotificationChannelDescriptorsRequest) (*monitoringpb.ListNotificationChannelDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	page, next, err := s.core.ListNotificationChannelDescriptors(ctx, project, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, mapError(err)
	}
	out := make([]*monitoringpb.NotificationChannelDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, notificationChannelDescriptorToProto(d))
	}
	return &monitoringpb.ListNotificationChannelDescriptorsResponse{ChannelDescriptors: out, NextPageToken: next}, nil
}

func (s *Service) GetNotificationChannelDescriptor(ctx context.Context, req *monitoringpb.GetNotificationChannelDescriptorRequest) (*monitoringpb.NotificationChannelDescriptor, error) {
	project, typ, ok := core.SplitNotificationChannelDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel descriptor name: " + req.GetName()))
	}
	d, err := s.core.GetNotificationChannelDescriptor(ctx, project, typ)
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelDescriptorToProto(d), nil
}

func (s *Service) SendNotificationChannelVerificationCode(ctx context.Context, req *monitoringpb.SendNotificationChannelVerificationCodeRequest) (*emptypb.Empty, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetName()))
	}
	if err := s.core.SendNotificationChannelVerificationCode(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) GetNotificationChannelVerificationCode(ctx context.Context, req *monitoringpb.GetNotificationChannelVerificationCodeRequest) (*monitoringpb.GetNotificationChannelVerificationCodeResponse, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetName()))
	}
	var expire *time.Time
	if req.GetExpireTime() != nil {
		t := req.GetExpireTime().AsTime()
		expire = &t
	}
	code, expireTime, err := s.core.GetNotificationChannelVerificationCode(ctx, project, id, expire)
	if err != nil {
		return nil, mapError(err)
	}
	return &monitoringpb.GetNotificationChannelVerificationCodeResponse{
		Code:       code,
		ExpireTime: timestamppb.New(expireTime),
	}, nil
}

func (s *Service) VerifyNotificationChannel(ctx context.Context, req *monitoringpb.VerifyNotificationChannelRequest) (*monitoringpb.NotificationChannel, error) {
	project, id, ok := core.SplitNotificationChannelName(req.GetName())
	if !ok {
		return nil, mapError(invalidArgument("invalid notification channel name: " + req.GetName()))
	}
	c, err := s.core.VerifyNotificationChannel(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return notificationChannelToProto(c, project), nil
}

// compile-time assertions that Service implements the generated servers.
var (
	_ monitoringpb.MetricServiceServer              = (*Service)(nil)
	_ monitoringpb.AlertPolicyServiceServer         = (*Service)(nil)
	_ monitoringpb.NotificationChannelServiceServer = (*Service)(nil)
	_ monitoringpb.ServiceMonitoringServiceServer   = (*Service)(nil)
)

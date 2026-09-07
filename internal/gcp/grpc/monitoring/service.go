// Package monitoring implements the Cloud Monitoring (v3) gRPC service
// (MetricService + AlertPolicyService) over the shared monitoringstore.Store —
// the Amazon CloudWatch metrics+alarms analogue. It manages the metric
// descriptor catalog, the time-series data plane, and the alert-policy (alarm)
// registry. Alert policies are stored verbatim; the emulator does not evaluate
// conditions or trigger notifications.
//
// Documented limitations:
//
//   - GetMonitoredResourceDescriptor and CreateServiceTimeSeries are
//     Unimplemented.
//   - ListMetricDescriptors and ListMonitoredResourceDescriptors ignore the
//     filter field and return synthesized (not canonical)
//     MonitoredResourceDescriptors.
//   - DISTRIBUTION point values are rejected.
//   - Alert policies are stored but never evaluated.
//   - ListTimeSeries supports only the metric.type / resource.type equality
//     filter subset.
package monitoring

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	"jaiscloud/internal/gcp/resource"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/model"

	"github.com/google/uuid"
)

// Service implements monitoringpb.MetricServiceServer and
// monitoringpb.AlertPolicyServiceServer over the shared store.
type Service struct {
	monitoringpb.UnimplementedMetricServiceServer
	monitoringpb.UnimplementedAlertPolicyServiceServer

	store       monitoringstore.Store
	defaultProj string
}

// NewService returns a Monitoring gRPC service backed by the shared store.
// defaultProj is the config-default project used when a request carries none.
func NewService(store monitoringstore.Store, defaultProj string) *Service {
	return &Service{store: store, defaultProj: defaultProj}
}

func mapError(err error) error {
	switch {
	case errors.Is(err, monitoringstore.ErrMetricDescriptorNotFound),
		errors.Is(err, monitoringstore.ErrAlertPolicyNotFound):
		return grpcutil.GRPCStatus(model.NewProviderError("NotFound", "resource not found", 404))
	case errors.Is(err, monitoringstore.ErrAlertPolicyExists):
		return grpcutil.GRPCStatus(model.NewProviderError("AlreadyExists", "alert policy already exists", 409))
	}
	return grpcutil.GRPCStatus(err)
}

// project resolves the project from a "projects/{p}" (or longer) resource name,
// falling back to routing metadata and then the configured default.
func (s *Service) project(ctx context.Context, name string) string {
	if p := projectFromResourceName(name); p != "" {
		return p
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// projectFromResourceName extracts the project id from "projects/{p}" or a
// longer "projects/{p}/..." name.
func projectFromResourceName(name string) string {
	if name == "" {
		return ""
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 2 && parts[0] == "projects" {
		return parts[1]
	}
	return ""
}

// ─── resource-name helpers ────────────────────────────────────────────────────

func metricDescriptorName(project, typ string) string {
	return resource.ResourceID(project)("metric-descriptor", typ)
}

// splitMetricDescriptorName parses "projects/{p}/metricDescriptors/{type}",
// where {type} may itself contain slashes (e.g.
// "custom.googleapis.com/invoice/paid/amount").
func splitMetricDescriptorName(name string) (project, typ string, ok bool) {
	const prefix = "projects/"
	const marker = "metricDescriptors/"
	if !strings.HasPrefix(name, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(name, prefix)
	project, after, found := strings.Cut(rest, "/")
	if !found || !strings.HasPrefix(after, marker) {
		return "", "", false
	}
	typ = strings.TrimPrefix(after, marker)
	if typ == "" {
		return "", "", false
	}
	return project, typ, true
}

func alertPolicyName(project, id string) string {
	return resource.ResourceID(project)("alert-policy", id)
}

// splitAlertPolicyName parses "projects/{p}/alertPolicies/{id}".
func splitAlertPolicyName(name string) (project, id string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "alertPolicies" {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// ─── pagination ───────────────────────────────────────────────────────────────

func pageSlice[T any](items []T, pageSize int32, token string) ([]T, string) {
	size := int(pageSize)
	if size <= 0 {
		size = 10000
	}
	start := decodeOffset(token)
	if start > len(items) {
		start = len(items)
	}
	end := start + size
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeOffset(end)
	}
	return items[start:end], next
}

func encodeOffset(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func decodeOffset(token string) int {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(string(b))
	return n
}

// ─── MetricService ────────────────────────────────────────────────────────────

func (s *Service) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())
	descriptors, err := s.store.ListMetricDescriptors(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := pageSlice(descriptors, req.GetPageSize(), req.GetPageToken())
	out := make([]*metricpb.MetricDescriptor, 0, len(page))
	for _, d := range page {
		out = append(out, descriptorToProto(d, project))
	}
	return &monitoringpb.ListMetricDescriptorsResponse{MetricDescriptors: out, NextPageToken: next}, nil
}

func (s *Service) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project, typ, ok := splitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid metric descriptor name: "+req.GetName(), 400))
	}
	d, err := s.store.GetMetricDescriptor(ctx, project, typ)
	if err != nil {
		return nil, mapError(err)
	}
	return descriptorToProto(d, project), nil
}

func (s *Service) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest) (*metricpb.MetricDescriptor, error) {
	project := s.project(ctx, req.GetName())
	d := descriptorFromProto(req.GetMetricDescriptor())
	if d.Type == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "metric descriptor type is required", 400))
	}
	stored, err := s.store.CreateMetricDescriptor(ctx, project, d)
	if err != nil {
		return nil, mapError(err)
	}
	return descriptorToProto(stored, project), nil
}

func (s *Service) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
	project, typ, ok := splitMetricDescriptorName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid metric descriptor name: "+req.GetName(), 400))
	}
	if err := s.store.DeleteMetricDescriptor(ctx, project, typ); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	project := s.project(ctx, req.GetName())
	filter, err := compileTSFilter(req.GetFilter())
	if err != nil {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid filter: "+err.Error(), 400))
	}

	series, err := s.store.ListTimeSeries(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}

	matching := make([]monitoringstore.TimeSeries, 0, len(series))
	for _, ts := range series {
		if !filter.match(ts) {
			continue
		}
		ts.Points = filterPoints(ts.Points, req.GetInterval())
		if req.GetView() == monitoringpb.ListTimeSeriesRequest_HEADERS {
			ts.Points = nil
		} else if len(ts.Points) == 0 {
			continue
		}
		matching = append(matching, ts)
	}

	page, next := pageSlice(matching, req.GetPageSize(), req.GetPageToken())
	out := make([]*monitoringpb.TimeSeries, 0, len(page))
	for _, ts := range page {
		out = append(out, timeSeriesToProto(ts))
	}
	return &monitoringpb.ListTimeSeriesResponse{TimeSeries: out, NextPageToken: next}, nil
}

func (s *Service) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	project := s.project(ctx, req.GetName())
	for _, p := range req.GetTimeSeries() {
		ts, err := timeSeriesFromProto(p)
		if err != nil {
			return nil, mapError(err)
		}
		if ts.MetricType == "" {
			return nil, mapError(model.NewProviderError("InvalidArgument", "time series metric type is required", 400))
		}
		if err := s.store.CreateTimeSeries(ctx, project, ts); err != nil {
			return nil, mapError(err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	project := s.project(ctx, req.GetName())

	seen := make(map[string]struct{})
	series, err := s.store.ListTimeSeries(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	for _, ts := range series {
		if ts.ResourceType != "" {
			seen[ts.ResourceType] = struct{}{}
		}
	}
	descriptors, err := s.store.ListMetricDescriptors(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	for _, d := range descriptors {
		for _, rt := range d.MonitoredResourceTypes {
			seen[rt] = struct{}{}
		}
	}

	types := make([]string, 0, len(seen))
	for t := range seen {
		types = append(types, t)
	}
	sort.Strings(types)

	all := make([]*monitoredrespb.MonitoredResourceDescriptor, 0, len(types))
	for _, t := range types {
		all = append(all, &monitoredrespb.MonitoredResourceDescriptor{
			Name: resource.ResourceID(project)("monitored-resource-descriptor", t),
			Type: t,
		})
	}
	page, next := pageSlice(all, req.GetPageSize(), req.GetPageToken())
	return &monitoringpb.ListMonitoredResourceDescriptorsResponse{ResourceDescriptors: page, NextPageToken: next}, nil
}

// ─── AlertPolicyService ───────────────────────────────────────────────────────

func (s *Service) ListAlertPolicies(ctx context.Context, req *monitoringpb.ListAlertPoliciesRequest) (*monitoringpb.ListAlertPoliciesResponse, error) {
	project := s.project(ctx, req.GetName())
	policies, err := s.store.ListAlertPolicies(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	page, next := pageSlice(policies, req.GetPageSize(), req.GetPageToken())
	out := make([]*monitoringpb.AlertPolicy, 0, len(page))
	for _, p := range page {
		out = append(out, alertPolicyToProto(p, project))
	}
	return &monitoringpb.ListAlertPoliciesResponse{
		AlertPolicies: out,
		NextPageToken: next,
		TotalSize:     int32(len(policies)),
	}, nil
}

func (s *Service) GetAlertPolicy(ctx context.Context, req *monitoringpb.GetAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := splitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetName(), 400))
	}
	p, err := s.store.GetAlertPolicy(ctx, project, id)
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) CreateAlertPolicy(ctx context.Context, req *monitoringpb.CreateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project := s.project(ctx, req.GetName())
	p, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	if p.DisplayName == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "alert policy display_name is required", 400))
	}
	if p.Enabled == nil {
		enabled := true
		p.Enabled = &enabled
	}
	p.ID = uuid.NewString()
	if err := s.store.CreateAlertPolicy(ctx, project, p); err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) UpdateAlertPolicy(ctx context.Context, req *monitoringpb.UpdateAlertPolicyRequest) (*monitoringpb.AlertPolicy, error) {
	project, id, ok := splitAlertPolicyName(req.GetAlertPolicy().GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetAlertPolicy().GetName(), 400))
	}
	incoming, err := alertPolicyFromProto(req.GetAlertPolicy())
	if err != nil {
		return nil, mapError(err)
	}
	incoming.ID = id

	// An empty/nil update mask is a full replace (the historical behavior).
	// A non-empty mask merges field-by-field: masked paths take the incoming
	// value, unmasked paths retain the stored value. The get-check-merge
	// happens inside UpdateAlertPolicyAtomic's locked section so a concurrent
	// masked update touching different fields can't read the same stale
	// snapshot and silently overwrite this one's change.
	p, err := s.store.UpdateAlertPolicyAtomic(ctx, project, id, func(stored monitoringstore.AlertPolicy) (monitoringstore.AlertPolicy, error) {
		paths := req.GetUpdateMask().GetPaths()
		if len(paths) == 0 {
			return incoming, nil
		}
		return applyAlertPolicyMask(stored, incoming, paths)
	})
	if err != nil {
		return nil, mapError(err)
	}
	return alertPolicyToProto(p, project), nil
}

func (s *Service) DeleteAlertPolicy(ctx context.Context, req *monitoringpb.DeleteAlertPolicyRequest) (*emptypb.Empty, error) {
	project, id, ok := splitAlertPolicyName(req.GetName())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid alert policy name: "+req.GetName(), 400))
	}
	if err := s.store.DeleteAlertPolicy(ctx, project, id); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── proto ↔ internal transcoding ─────────────────────────────────────────────

func descriptorToProto(d monitoringstore.MetricDescriptor, project string) *metricpb.MetricDescriptor {
	out := &metricpb.MetricDescriptor{
		Name:                   metricDescriptorName(project, d.Type),
		Type:                   d.Type,
		MetricKind:             metricpb.MetricDescriptor_MetricKind(d.MetricKind),
		ValueType:              metricpb.MetricDescriptor_ValueType(d.ValueType),
		Unit:                   d.Unit,
		Description:            d.Description,
		DisplayName:            d.DisplayName,
		MonitoredResourceTypes: d.MonitoredResourceTypes,
	}
	for _, l := range d.Labels {
		out.Labels = append(out.Labels, &labelpb.LabelDescriptor{
			Key:         l.Key,
			ValueType:   labelpb.LabelDescriptor_ValueType(l.ValueType),
			Description: l.Description,
		})
	}
	return out
}

func descriptorFromProto(p *metricpb.MetricDescriptor) monitoringstore.MetricDescriptor {
	if p == nil {
		return monitoringstore.MetricDescriptor{}
	}
	d := monitoringstore.MetricDescriptor{
		Type:                   p.GetType(),
		MetricKind:             int32(p.GetMetricKind()),
		ValueType:              int32(p.GetValueType()),
		Unit:                   p.GetUnit(),
		Description:            p.GetDescription(),
		DisplayName:            p.GetDisplayName(),
		MonitoredResourceTypes: p.GetMonitoredResourceTypes(),
	}
	for _, l := range p.GetLabels() {
		d.Labels = append(d.Labels, monitoringstore.LabelDescriptor{
			Key:         l.GetKey(),
			ValueType:   int32(l.GetValueType()),
			Description: l.GetDescription(),
		})
	}
	return d
}

func timeSeriesToProto(ts monitoringstore.TimeSeries) *monitoringpb.TimeSeries {
	out := &monitoringpb.TimeSeries{
		Metric:     &metricpb.Metric{Type: ts.MetricType, Labels: ts.MetricLabels},
		Resource:   &monitoredrespb.MonitoredResource{Type: ts.ResourceType, Labels: ts.ResourceLabels},
		MetricKind: metricpb.MetricDescriptor_MetricKind(ts.MetricKind),
		ValueType:  metricpb.MetricDescriptor_ValueType(ts.ValueType),
		Unit:       ts.Unit,
	}
	// Points are returned reverse-chronologically (most recent first).
	points := make([]monitoringstore.Point, len(ts.Points))
	copy(points, ts.Points)
	sort.Slice(points, func(i, j int) bool { return points[i].EndTime.After(points[j].EndTime) })
	for _, pt := range points {
		out.Points = append(out.Points, pointToProto(pt))
	}
	return out
}

func timeSeriesFromProto(p *monitoringpb.TimeSeries) (monitoringstore.TimeSeries, error) {
	ts := monitoringstore.TimeSeries{}
	if p.GetMetric() != nil {
		ts.MetricType = p.GetMetric().GetType()
		ts.MetricLabels = p.GetMetric().GetLabels()
	}
	if p.GetResource() != nil {
		ts.ResourceType = p.GetResource().GetType()
		ts.ResourceLabels = p.GetResource().GetLabels()
	}
	ts.MetricKind = int32(p.GetMetricKind())
	ts.ValueType = int32(p.GetValueType())
	ts.Unit = p.GetUnit()
	for _, pt := range p.GetPoints() {
		sp, err := pointFromProto(pt)
		if err != nil {
			return ts, err
		}
		ts.Points = append(ts.Points, sp)
	}
	return ts, nil
}

func pointToProto(p monitoringstore.Point) *monitoringpb.Point {
	iv := &monitoringpb.TimeInterval{}
	if !p.EndTime.IsZero() {
		iv.EndTime = timestamppb.New(p.EndTime)
	}
	if !p.StartTime.IsZero() {
		iv.StartTime = timestamppb.New(p.StartTime)
	}
	return &monitoringpb.Point{Interval: iv, Value: typedValueToProto(p.Value)}
}

func pointFromProto(p *monitoringpb.Point) (monitoringstore.Point, error) {
	pt := monitoringstore.Point{}
	if p == nil {
		return pt, nil
	}
	if iv := p.GetInterval(); iv != nil {
		if iv.GetStartTime() != nil {
			pt.StartTime = iv.GetStartTime().AsTime()
		}
		if iv.GetEndTime() != nil {
			pt.EndTime = iv.GetEndTime().AsTime()
		}
	}
	if pt.EndTime.IsZero() {
		pt.EndTime = clock.Now()
	}
	v, err := typedValueFromProto(p.GetValue())
	if err != nil {
		return pt, err
	}
	pt.Value = v
	return pt, nil
}

func typedValueToProto(v monitoringstore.TypedValue) *monitoringpb.TypedValue {
	switch {
	case v.BoolValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: *v.BoolValue}}
	case v.Int64Value != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: *v.Int64Value}}
	case v.DoubleValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: *v.DoubleValue}}
	case v.StringValue != nil:
		return &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_StringValue{StringValue: *v.StringValue}}
	}
	return &monitoringpb.TypedValue{}
}

func typedValueFromProto(p *monitoringpb.TypedValue) (monitoringstore.TypedValue, error) {
	if p == nil {
		return monitoringstore.TypedValue{}, nil
	}
	switch v := p.GetValue().(type) {
	case *monitoringpb.TypedValue_BoolValue:
		return monitoringstore.TypedValue{BoolValue: &v.BoolValue}, nil
	case *monitoringpb.TypedValue_Int64Value:
		return monitoringstore.TypedValue{Int64Value: &v.Int64Value}, nil
	case *monitoringpb.TypedValue_DoubleValue:
		return monitoringstore.TypedValue{DoubleValue: &v.DoubleValue}, nil
	case *monitoringpb.TypedValue_StringValue:
		return monitoringstore.TypedValue{StringValue: &v.StringValue}, nil
	case *monitoringpb.TypedValue_DistributionValue:
		return monitoringstore.TypedValue{}, model.NewProviderError("InvalidArgument", "distribution values are not supported", 400)
	}
	return monitoringstore.TypedValue{}, nil
}

func alertPolicyToProto(p monitoringstore.AlertPolicy, project string) *monitoringpb.AlertPolicy {
	out := &monitoringpb.AlertPolicy{
		Name:                 alertPolicyName(project, p.ID),
		DisplayName:          p.DisplayName,
		Combiner:             monitoringpb.AlertPolicy_ConditionCombinerType(p.Combiner),
		NotificationChannels: p.NotificationChannels,
		UserLabels:           p.UserLabels,
	}
	if p.Enabled != nil {
		out.Enabled = wrapperspb.Bool(*p.Enabled)
	}
	if len(p.Documentation) > 0 && string(p.Documentation) != "null" {
		doc := &monitoringpb.AlertPolicy_Documentation{}
		if protojson.Unmarshal(p.Documentation, doc) == nil {
			out.Documentation = doc
		}
	}
	for _, c := range p.Conditions {
		cond := &monitoringpb.AlertPolicy_Condition{}
		if protojson.Unmarshal(c, cond) == nil {
			out.Conditions = append(out.Conditions, cond)
		}
	}
	return out
}

func alertPolicyFromProto(p *monitoringpb.AlertPolicy) (monitoringstore.AlertPolicy, error) {
	ap := monitoringstore.AlertPolicy{
		DisplayName:          p.GetDisplayName(),
		Combiner:             int32(p.GetCombiner()),
		NotificationChannels: p.GetNotificationChannels(),
		UserLabels:           p.GetUserLabels(),
	}
	if p.GetEnabled() != nil {
		v := p.GetEnabled().GetValue()
		ap.Enabled = &v
	}
	if p.GetDocumentation() != nil {
		b, err := protojson.Marshal(p.GetDocumentation())
		if err != nil {
			return ap, err
		}
		ap.Documentation = json.RawMessage(b)
	}
	for _, c := range p.GetConditions() {
		b, err := protojson.Marshal(c)
		if err != nil {
			return ap, err
		}
		ap.Conditions = append(ap.Conditions, json.RawMessage(b))
	}
	return ap, nil
}

// applyAlertPolicyMask merges an incoming alert policy into the stored policy
// according to the field paths in updateMask. For each masked path the incoming
// value wins; every other field retains the stored value. Supported paths are
// display_name, documentation, conditions, combiner, enabled,
// notification_channels, and user_labels. Any other path is unsupported and
// returns an error mapped to Unimplemented.
func applyAlertPolicyMask(stored, incoming monitoringstore.AlertPolicy, updateMask []string) (monitoringstore.AlertPolicy, error) {
	for _, path := range updateMask {
		switch path {
		case "display_name":
			stored.DisplayName = incoming.DisplayName
		case "documentation":
			stored.Documentation = incoming.Documentation
		case "conditions":
			stored.Conditions = incoming.Conditions
		case "combiner":
			stored.Combiner = incoming.Combiner
		case "enabled":
			stored.Enabled = incoming.Enabled
		case "notification_channels":
			stored.NotificationChannels = incoming.NotificationChannels
		case "user_labels":
			stored.UserLabels = incoming.UserLabels
		default:
			return stored, model.NewProviderError("UnsupportedOperation", "unsupported update_mask path: "+path, 501)
		}
	}
	return stored, nil
}

// filterPoints restricts points to those whose end time falls within the
// half-open interval (start, end]. A nil interval keeps every point.
func filterPoints(points []monitoringstore.Point, iv *monitoringpb.TimeInterval) []monitoringstore.Point {
	if iv == nil || len(points) == 0 {
		return points
	}
	var start, end time.Time
	if iv.GetStartTime() != nil {
		start = iv.GetStartTime().AsTime()
	}
	if iv.GetEndTime() != nil {
		end = iv.GetEndTime().AsTime()
	}
	out := make([]monitoringstore.Point, 0, len(points))
	for _, p := range points {
		if !end.IsZero() && !p.EndTime.IsZero() && p.EndTime.After(end) {
			continue
		}
		if !start.IsZero() && !p.EndTime.IsZero() && !p.EndTime.After(start) {
			continue
		}
		out = append(out, p)
	}
	return out
}

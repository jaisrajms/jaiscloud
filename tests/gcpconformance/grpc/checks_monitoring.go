package grpcconformance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// monitoringChecks covers the Cloud Monitoring v3 gRPC surface — all 34 RPCs of
// google.monitoring.v3.MetricService (9), AlertPolicyService (5),
// NotificationChannelService (10) and ServiceMonitoringService (10) — via the
// official cloud.google.com/go/monitoring/apiv3/v2 clients.
//
// Checks run in registry order and share state through the emulator: the metric
// descriptor created first is read/listed by the following probes and deleted
// last; the alert policy and notification channel are each created, read,
// listed, updated and deleted in one sequence. Names are run-unique via
// cfg.ResourceName, so a long-lived emulator never sees cross-run collisions.
//
// The emulator assigns alert-policy and notification-channel ids server-side,
// so the get/update/delete probes discover the resource by its run-unique
// display name rather than by a precomputed name.
func monitoringChecks() []Check {
	return []Check{
		// ── MetricService ────────────────────────────────────────────────────
		{Service: "monitoring", RPC: "CreateMetricDescriptor", Method: "CreateMetricDescriptor", KeyField: "metricDescriptor.name/type", Run: checkMonitoringCreateMetricDescriptor},
		{Service: "monitoring", RPC: "GetMetricDescriptor", Method: "GetMetricDescriptor", KeyField: "metricDescriptor.type/unit", Run: checkMonitoringGetMetricDescriptor},
		{Service: "monitoring", RPC: "ListMetricDescriptors", Method: "ListMetricDescriptors", KeyField: "metricDescriptors[].type", Run: checkMonitoringListMetricDescriptors},
		{Service: "monitoring", RPC: "CreateTimeSeries", Method: "CreateTimeSeries", KeyField: "series round-trips with value", Run: checkMonitoringCreateTimeSeries},
		{Service: "monitoring", RPC: "CreateServiceTimeSeries", Method: "CreateServiceTimeSeries", KeyField: "series round-trips with value", Run: checkMonitoringCreateServiceTimeSeries},
		{Service: "monitoring", RPC: "ListTimeSeries", Method: "ListTimeSeries", KeyField: "timeSeries[].points[].value", Run: checkMonitoringListTimeSeries},
		{Service: "monitoring", RPC: "ListMonitoredResourceDescriptors", Method: "ListMonitoredResourceDescriptors", KeyField: "resourceDescriptors[].type/name/labels", Run: checkMonitoringListMonitoredResourceDescriptors},
		{Service: "monitoring", RPC: "GetMonitoredResourceDescriptor", Method: "GetMonitoredResourceDescriptor", KeyField: "resourceDescriptor.type/name", Run: checkMonitoringGetMonitoredResourceDescriptor},
		{Service: "monitoring", RPC: "DeleteMetricDescriptor", Method: "DeleteMetricDescriptor", KeyField: "descriptor absent after delete", Run: checkMonitoringDeleteMetricDescriptor},

		// ── AlertPolicyService ───────────────────────────────────────────────
		{Service: "monitoring", RPC: "CreateAlertPolicy", Method: "CreateAlertPolicy", KeyField: "alertPolicy.name/display_name", Run: checkMonitoringCreateAlertPolicy},
		{Service: "monitoring", RPC: "GetAlertPolicy", Method: "GetAlertPolicy", KeyField: "alertPolicy.display_name/conditions", Run: checkMonitoringGetAlertPolicy},
		{Service: "monitoring", RPC: "ListAlertPolicies", Method: "ListAlertPolicies", KeyField: "alertPolicies[] contains policy", Run: checkMonitoringListAlertPolicies},
		{Service: "monitoring", RPC: "UpdateAlertPolicy", Method: "UpdateAlertPolicy", KeyField: "masked user_labels applied", Run: checkMonitoringUpdateAlertPolicy},
		{Service: "monitoring", RPC: "DeleteAlertPolicy", Method: "DeleteAlertPolicy", KeyField: "policy absent after delete", Run: checkMonitoringDeleteAlertPolicy},

		// ── NotificationChannelService ───────────────────────────────────────
		{Service: "monitoring", RPC: "CreateNotificationChannel", Method: "CreateNotificationChannel", KeyField: "channel.name/type/display_name", Run: checkMonitoringCreateNotificationChannel},
		{Service: "monitoring", RPC: "GetNotificationChannel", Method: "GetNotificationChannel", KeyField: "channel.display_name/labels", Run: checkMonitoringGetNotificationChannel},
		{Service: "monitoring", RPC: "ListNotificationChannels", Method: "ListNotificationChannels", KeyField: "channels[] contains channel", Run: checkMonitoringListNotificationChannels},
		{Service: "monitoring", RPC: "UpdateNotificationChannel", Method: "UpdateNotificationChannel", KeyField: "masked description applied", Run: checkMonitoringUpdateNotificationChannel},
		{Service: "monitoring", RPC: "VerifyNotificationChannel", Method: "VerifyNotificationChannel", KeyField: "verification_status=VERIFIED", Run: checkMonitoringVerifyNotificationChannel},
		{Service: "monitoring", RPC: "ListNotificationChannelDescriptors", Method: "ListNotificationChannelDescriptors", KeyField: "channelDescriptors[].type/display_name/labels", Run: checkMonitoringListNotificationChannelDescriptors},
		{Service: "monitoring", RPC: "GetNotificationChannelDescriptor", Method: "GetNotificationChannelDescriptor", KeyField: "descriptor.type=email", Run: checkMonitoringGetNotificationChannelDescriptor},
		{Service: "monitoring", RPC: "SendNotificationChannelVerificationCode", Method: "SendNotificationChannelVerificationCode", KeyField: "success (empty)", Run: checkMonitoringSendVerificationCode},
		{Service: "monitoring", RPC: "GetNotificationChannelVerificationCode", Method: "GetNotificationChannelVerificationCode", KeyField: "code non-empty", Run: checkMonitoringGetVerificationCode},
		{Service: "monitoring", RPC: "DeleteNotificationChannel", Method: "DeleteNotificationChannel", KeyField: "channel absent after delete", Run: checkMonitoringDeleteNotificationChannel},

		// ── ServiceMonitoringService ─────────────────────────────────────────
		{Service: "monitoring", RPC: "CreateService", Method: "CreateService", KeyField: "service.name/display_name/identifier", Run: checkMonitoringCreateService},
		{Service: "monitoring", RPC: "GetService", Method: "GetService", KeyField: "service.display_name/identifier", Run: checkMonitoringGetService},
		{Service: "monitoring", RPC: "ListServices", Method: "ListServices", KeyField: "services[] contains service", Run: checkMonitoringListServices},
		{Service: "monitoring", RPC: "UpdateService", Method: "UpdateService", KeyField: "masked display_name applied", Run: checkMonitoringUpdateService},
		{Service: "monitoring", RPC: "CreateServiceLevelObjective", Method: "CreateServiceLevelObjective", KeyField: "slo.name/goal/rolling_period", Run: checkMonitoringCreateServiceLevelObjective},
		{Service: "monitoring", RPC: "GetServiceLevelObjective", Method: "GetServiceLevelObjective", KeyField: "slo.display_name/indicator", Run: checkMonitoringGetServiceLevelObjective},
		{Service: "monitoring", RPC: "ListServiceLevelObjectives", Method: "ListServiceLevelObjectives", KeyField: "serviceLevelObjectives[] contains slo", Run: checkMonitoringListServiceLevelObjectives},
		{Service: "monitoring", RPC: "UpdateServiceLevelObjective", Method: "UpdateServiceLevelObjective", KeyField: "masked goal applied", Run: checkMonitoringUpdateServiceLevelObjective},
		{Service: "monitoring", RPC: "DeleteServiceLevelObjective", Method: "DeleteServiceLevelObjective", KeyField: "slo absent after delete", Run: checkMonitoringDeleteServiceLevelObjective},
		{Service: "monitoring", RPC: "DeleteService", Method: "DeleteService", KeyField: "service absent after delete", Run: checkMonitoringDeleteService},
	}
}

// ─── client helpers ───────────────────────────────────────────────────────────

func monitoringOptions(cfg Config) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	}
}

func newMonitoringMetricClient(ctx context.Context, cfg Config) (*monitoring.MetricClient, error) {
	return monitoring.NewMetricClient(ctx, monitoringOptions(cfg)...)
}

func newMonitoringAlertPolicyClient(ctx context.Context, cfg Config) (*monitoring.AlertPolicyClient, error) {
	return monitoring.NewAlertPolicyClient(ctx, monitoringOptions(cfg)...)
}

func newMonitoringChannelClient(ctx context.Context, cfg Config) (*monitoring.NotificationChannelClient, error) {
	return monitoring.NewNotificationChannelClient(ctx, monitoringOptions(cfg)...)
}

func newMonitoringServiceClient(ctx context.Context, cfg Config) (*monitoring.ServiceMonitoringClient, error) {
	return monitoring.NewServiceMonitoringClient(ctx, monitoringOptions(cfg)...)
}

// ─── naming helpers ───────────────────────────────────────────────────────────

func monitoringParent(cfg Config) string { return "projects/" + cfg.Project }

func monitoringMetricType(cfg Config) string {
	return "custom.googleapis.com/gcpc/" + cfg.ResourceName("metric")
}

func monitoringSeriesType(cfg Config) string {
	return "custom.googleapis.com/gcpc/" + cfg.ResourceName("series")
}

func monitoringServiceSeriesType(cfg Config) string {
	return "custom.googleapis.com/gcpc/" + cfg.ResourceName("svcseries")
}

func monitoringMetricTypeName(cfg Config) string {
	return monitoringParent(cfg) + "/metricDescriptors/" + monitoringMetricType(cfg)
}

func monitoringPolicyDisplay(cfg Config) string { return cfg.ResourceName("gcpc-grpc-policy") }

func monitoringChannelDisplay(cfg Config) string { return cfg.ResourceName("gcpc-grpc-channel") }

// ─── MetricService checks ─────────────────────────────────────────────────────

// Check 1: CreateMetricDescriptor must return the descriptor with its project-
// scoped name and the requested type.
func checkMonitoringCreateMetricDescriptor(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	created, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: monitoringParent(cfg),
		MetricDescriptor: &metricpb.MetricDescriptor{
			Type:        monitoringMetricType(cfg),
			MetricKind:  metricpb.MetricDescriptor_GAUGE,
			ValueType:   metricpb.MetricDescriptor_DOUBLE,
			Unit:        "1",
			Description: "gRPC conformance metric",
		},
	})
	if err != nil {
		return fmt.Errorf("CreateMetricDescriptor: %w", err)
	}
	if got, want := created.GetName(), monitoringMetricTypeName(cfg); got != want {
		return fmt.Errorf("created name = %q, want %q", got, want)
	}
	if got := created.GetType(); got != monitoringMetricType(cfg) {
		return fmt.Errorf("created type = %q, want %q", got, monitoringMetricType(cfg))
	}
	return nil
}

// Check 2: GetMetricDescriptor must round-trip the descriptor created by check 1.
func checkMonitoringGetMetricDescriptor(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	got, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: monitoringMetricTypeName(cfg)})
	if err != nil {
		return fmt.Errorf("GetMetricDescriptor: %w", err)
	}
	if got.GetType() != monitoringMetricType(cfg) {
		return fmt.Errorf("type = %q, want %q", got.GetType(), monitoringMetricType(cfg))
	}
	if got.GetUnit() != "1" {
		return fmt.Errorf("unit = %q, want 1", got.GetUnit())
	}
	if got.GetValueType() != metricpb.MetricDescriptor_DOUBLE {
		return fmt.Errorf("value_type = %v, want DOUBLE", got.GetValueType())
	}
	return nil
}

// Check 3: ListMetricDescriptors filtered by the run-unique type must return
// exactly the descriptor created by check 1.
func checkMonitoringListMetricDescriptors(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	it := mc.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:   monitoringParent(cfg),
		Filter: fmt.Sprintf("metric.type = %q", monitoringMetricType(cfg)),
	})
	found := false
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListMetricDescriptors: %w", err)
		}
		if d.GetType() == monitoringMetricType(cfg) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("ListMetricDescriptors did not return %q", monitoringMetricType(cfg))
	}
	return nil
}

// Check 4: CreateTimeSeries must accept a point and make it readable back
// through ListTimeSeries with the value intact.
func checkMonitoringCreateTimeSeries(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	if err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: monitoringParent(cfg),
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: monitoringSeriesType(cfg)},
			Resource: &monitoredrespb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": cfg.Project}},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now().UTC())},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 3.5}},
			}},
		}},
	}); err != nil {
		return fmt.Errorf("CreateTimeSeries: %w", err)
	}

	series, err := listMonitoringTimeSeries(ctx, mc, cfg, monitoringSeriesType(cfg))
	if err != nil {
		return err
	}
	if len(series) != 1 {
		return fmt.Errorf("ListTimeSeries returned %d series for %q, want 1", len(series), monitoringSeriesType(cfg))
	}
	if got := series[0].GetPoints()[0].GetValue().GetDoubleValue(); got != 3.5 {
		return fmt.Errorf("round-tripped value = %v, want 3.5", got)
	}
	return nil
}

// Check 5: CreateServiceTimeSeries mirrors CreateTimeSeries for the service-
// scoped write path; its series must also round-trip.
func checkMonitoringCreateServiceTimeSeries(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	if err := mc.CreateServiceTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: monitoringParent(cfg),
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: monitoringServiceSeriesType(cfg)},
			Resource: &monitoredrespb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": cfg.Project}},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now().UTC())},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 7}},
			}},
		}},
	}); err != nil {
		return fmt.Errorf("CreateServiceTimeSeries: %w", err)
	}

	series, err := listMonitoringTimeSeries(ctx, mc, cfg, monitoringServiceSeriesType(cfg))
	if err != nil {
		return err
	}
	if len(series) != 1 {
		return fmt.Errorf("ListTimeSeries returned %d series for %q, want 1", len(series), monitoringServiceSeriesType(cfg))
	}
	if got := series[0].GetPoints()[0].GetValue().GetInt64Value(); got != 7 {
		return fmt.Errorf("round-tripped value = %d, want 7", got)
	}
	return nil
}

// Check 6: ListTimeSeries must return both series written by checks 4 and 5
// under the project parent, with their metric types and points intact.
func checkMonitoringListTimeSeries(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	for _, typ := range []string{monitoringSeriesType(cfg), monitoringServiceSeriesType(cfg)} {
		series, err := listMonitoringTimeSeries(ctx, mc, cfg, typ)
		if err != nil {
			return err
		}
		if len(series) != 1 {
			return fmt.Errorf("ListTimeSeries returned %d series for %q, want 1", len(series), typ)
		}
		if series[0].GetMetric().GetType() != typ {
			return fmt.Errorf("series metric type = %q, want %q", series[0].GetMetric().GetType(), typ)
		}
		if len(series[0].GetPoints()) == 0 {
			return fmt.Errorf("series %q has no points", typ)
		}
	}
	return nil
}

// listMonitoringTimeSeries drains ListTimeSeries for one metric type.
func listMonitoringTimeSeries(ctx context.Context, mc *monitoring.MetricClient, cfg Config, metricType string) ([]*monitoringpb.TimeSeries, error) {
	now := time.Now().UTC()
	it := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   monitoringParent(cfg),
		Filter: fmt.Sprintf("metric.type = %q AND resource.type = %q", metricType, "global"),
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(now.Add(-time.Hour)),
			EndTime:   timestamppb.New(now.Add(time.Hour)),
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	})
	var out []*monitoringpb.TimeSeries
	for {
		ts, err := it.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("ListTimeSeries: %w", err)
		}
		out = append(out, ts)
	}
}

// Check 7: ListMonitoredResourceDescriptors must return a non-empty catalog of
// named descriptors, each with a type, display name and labels.
func checkMonitoringListMonitoredResourceDescriptors(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	it := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{Name: monitoringParent(cfg)})
	count := 0
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListMonitoredResourceDescriptors: %w", err)
		}
		count++
		if d.GetType() == "" {
			return fmt.Errorf("descriptor %d has an empty type", count)
		}
		if d.GetDisplayName() == "" {
			return fmt.Errorf("descriptor %q has an empty display_name", d.GetType())
		}
		if len(d.GetLabels()) == 0 {
			return fmt.Errorf("descriptor %q has no labels", d.GetType())
		}
		if !strings.HasSuffix(d.GetName(), "/monitoredResourceDescriptors/"+d.GetType()) {
			return fmt.Errorf("descriptor name = %q, want a projects/.../monitoredResourceDescriptors/ suffix", d.GetName())
		}
	}
	if count == 0 {
		return errors.New("ListMonitoredResourceDescriptors returned no descriptors")
	}
	return nil
}

// Check 8: GetMonitoredResourceDescriptor must return the canonical
// gce_instance descriptor with its project-scoped name.
func checkMonitoringGetMonitoredResourceDescriptor(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	name := monitoringParent(cfg) + "/monitoredResourceDescriptors/gce_instance"
	got, err := mc.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetMonitoredResourceDescriptor: %w", err)
	}
	if got.GetType() != "gce_instance" {
		return fmt.Errorf("type = %q, want gce_instance", got.GetType())
	}
	if got.GetName() != name {
		return fmt.Errorf("name = %q, want %q", got.GetName(), name)
	}
	if got.GetDisplayName() == "" {
		return errors.New("descriptor has an empty display_name")
	}
	return nil
}

// Check 9: DeleteMetricDescriptor must remove the descriptor created by check 1.
func checkMonitoringDeleteMetricDescriptor(ctx context.Context, cfg Config) error {
	mc, err := newMonitoringMetricClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer mc.Close()

	if err := mc.DeleteMetricDescriptor(ctx, &monitoringpb.DeleteMetricDescriptorRequest{Name: monitoringMetricTypeName(cfg)}); err != nil {
		return fmt.Errorf("DeleteMetricDescriptor: %w", err)
	}
	_, err = mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: monitoringMetricTypeName(cfg)})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetMetricDescriptor after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── AlertPolicyService checks ────────────────────────────────────────────────

// findAlertPolicy lists policies and returns the run-unique one. The emulator
// assigns the id server-side, so probes address the policy by display name.
func findAlertPolicy(ctx context.Context, ac *monitoring.AlertPolicyClient, cfg Config) (*monitoringpb.AlertPolicy, error) {
	it := ac.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{Name: monitoringParent(cfg)})
	for {
		p, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ListAlertPolicies: %w", err)
		}
		if p.GetDisplayName() == monitoringPolicyDisplay(cfg) {
			return p, nil
		}
	}
	return nil, fmt.Errorf("alert policy %q not found", monitoringPolicyDisplay(cfg))
}

// Check 10: CreateAlertPolicy must return a policy with a project-scoped name
// and the requested display name.
func checkMonitoringCreateAlertPolicy(ctx context.Context, cfg Config) error {
	ac, err := newMonitoringAlertPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer ac.Close()

	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: monitoringParent(cfg),
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: monitoringPolicyDisplay(cfg),
			Combiner:    monitoringpb.AlertPolicy_OR,
			Enabled:     wrapperspb.Bool(true),
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "cpu high",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
					ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
						Filter:         fmt.Sprintf("metric.type = %q", monitoringMetricType(cfg)),
						Comparison:     monitoringpb.ComparisonType_COMPARISON_GT,
						ThresholdValue: 0.9,
					},
				},
			}},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateAlertPolicy: %w", err)
	}
	if !strings.HasPrefix(created.GetName(), monitoringParent(cfg)+"/alertPolicies/") {
		return fmt.Errorf("created name = %q, want projects/.../alertPolicies/ prefix", created.GetName())
	}
	if created.GetDisplayName() != monitoringPolicyDisplay(cfg) {
		return fmt.Errorf("created display_name = %q, want %q", created.GetDisplayName(), monitoringPolicyDisplay(cfg))
	}
	return nil
}

// Check 11: GetAlertPolicy must round-trip the created policy with its
// condition intact.
func checkMonitoringGetAlertPolicy(ctx context.Context, cfg Config) error {
	ac, err := newMonitoringAlertPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer ac.Close()

	p, err := findAlertPolicy(ctx, ac, cfg)
	if err != nil {
		return err
	}
	got, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: p.GetName()})
	if err != nil {
		return fmt.Errorf("GetAlertPolicy: %w", err)
	}
	if got.GetDisplayName() != monitoringPolicyDisplay(cfg) {
		return fmt.Errorf("display_name = %q, want %q", got.GetDisplayName(), monitoringPolicyDisplay(cfg))
	}
	if len(got.GetConditions()) != 1 {
		return fmt.Errorf("conditions = %d, want 1", len(got.GetConditions()))
	}
	return nil
}

// Check 12: ListAlertPolicies must include the created policy.
func checkMonitoringListAlertPolicies(ctx context.Context, cfg Config) error {
	ac, err := newMonitoringAlertPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer ac.Close()

	it := ac.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{Name: monitoringParent(cfg)})
	for {
		p, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListAlertPolicies did not include %q", monitoringPolicyDisplay(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListAlertPolicies: %w", err)
		}
		if p.GetDisplayName() == monitoringPolicyDisplay(cfg) {
			return nil
		}
	}
}

// Check 13: UpdateAlertPolicy with a user_labels mask must apply the change and
// return the updated policy.
func checkMonitoringUpdateAlertPolicy(ctx context.Context, cfg Config) error {
	ac, err := newMonitoringAlertPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer ac.Close()

	p, err := findAlertPolicy(ctx, ac, cfg)
	if err != nil {
		return err
	}
	updated, err := ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"user_labels"}},
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:       p.GetName(),
			UserLabels: map[string]string{"conformance": "updated"},
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateAlertPolicy: %w", err)
	}
	if got := updated.GetUserLabels()["conformance"]; got != "updated" {
		return fmt.Errorf("updated user_labels[conformance] = %q, want updated", got)
	}
	return nil
}

// Check 14: DeleteAlertPolicy must remove the policy created by check 10.
func checkMonitoringDeleteAlertPolicy(ctx context.Context, cfg Config) error {
	ac, err := newMonitoringAlertPolicyClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer ac.Close()

	p, err := findAlertPolicy(ctx, ac, cfg)
	if err != nil {
		return err
	}
	if err := ac.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{Name: p.GetName()}); err != nil {
		return fmt.Errorf("DeleteAlertPolicy: %w", err)
	}
	if _, err := findAlertPolicy(ctx, ac, cfg); err == nil {
		return fmt.Errorf("alert policy %q still present after delete", monitoringPolicyDisplay(cfg))
	}
	return nil
}

// ─── NotificationChannelService checks ────────────────────────────────────────

// findNotificationChannel lists channels and returns the run-unique one (the
// emulator assigns the id server-side, so probes address it by display name).
func findNotificationChannel(ctx context.Context, nc *monitoring.NotificationChannelClient, cfg Config) (*monitoringpb.NotificationChannel, error) {
	it := nc.ListNotificationChannels(ctx, &monitoringpb.ListNotificationChannelsRequest{Name: monitoringParent(cfg)})
	for {
		c, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ListNotificationChannels: %w", err)
		}
		if c.GetDisplayName() == monitoringChannelDisplay(cfg) {
			return c, nil
		}
	}
	return nil, fmt.Errorf("notification channel %q not found", monitoringChannelDisplay(cfg))
}

// Check 15: CreateNotificationChannel must return a channel with a project-
// scoped name, the requested type and display name.
func checkMonitoringCreateNotificationChannel(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	created, err := nc.CreateNotificationChannel(ctx, &monitoringpb.CreateNotificationChannelRequest{
		Name: monitoringParent(cfg),
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type:        "email",
			DisplayName: monitoringChannelDisplay(cfg),
			Labels:      map[string]string{"email_address": "conformance@example.com"},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateNotificationChannel: %w", err)
	}
	if !strings.HasPrefix(created.GetName(), monitoringParent(cfg)+"/notificationChannels/") {
		return fmt.Errorf("created name = %q, want projects/.../notificationChannels/ prefix", created.GetName())
	}
	if created.GetType() != "email" {
		return fmt.Errorf("created type = %q, want email", created.GetType())
	}
	if created.GetDisplayName() != monitoringChannelDisplay(cfg) {
		return fmt.Errorf("created display_name = %q, want %q", created.GetDisplayName(), monitoringChannelDisplay(cfg))
	}
	return nil
}

// Check 16: GetNotificationChannel must round-trip the created channel with its
// type-specific labels intact.
func checkMonitoringGetNotificationChannel(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	got, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: c.GetName()})
	if err != nil {
		return fmt.Errorf("GetNotificationChannel: %w", err)
	}
	if got.GetDisplayName() != monitoringChannelDisplay(cfg) {
		return fmt.Errorf("display_name = %q, want %q", got.GetDisplayName(), monitoringChannelDisplay(cfg))
	}
	if got.GetLabels()["email_address"] != "conformance@example.com" {
		return fmt.Errorf("labels[email_address] = %q, want conformance@example.com", got.GetLabels()["email_address"])
	}
	return nil
}

// Check 17: ListNotificationChannels must include the created channel.
func checkMonitoringListNotificationChannels(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	if _, err := findNotificationChannel(ctx, nc, cfg); err != nil {
		return err
	}
	return nil
}

// Check 18: UpdateNotificationChannel with a description mask must apply the
// change while preserving the identity used by later probes.
func checkMonitoringUpdateNotificationChannel(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	updated, err := nc.UpdateNotificationChannel(ctx, &monitoringpb.UpdateNotificationChannelRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		NotificationChannel: &monitoringpb.NotificationChannel{
			Name:        c.GetName(),
			Description: "updated by conformance",
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateNotificationChannel: %w", err)
	}
	if got := updated.GetDescription(); got != "updated by conformance" {
		return fmt.Errorf("description = %q, want updated by conformance", got)
	}
	return nil
}

// Check 19: VerifyNotificationChannel must return the channel marked VERIFIED.
func checkMonitoringVerifyNotificationChannel(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	code, err := nc.GetNotificationChannelVerificationCode(ctx, &monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: c.GetName()})
	if err != nil {
		return fmt.Errorf("GetNotificationChannelVerificationCode: %w", err)
	}
	verified, err := nc.VerifyNotificationChannel(ctx, &monitoringpb.VerifyNotificationChannelRequest{
		Name: c.GetName(),
		Code: code.GetCode(),
	})
	if err != nil {
		return fmt.Errorf("VerifyNotificationChannel: %w", err)
	}
	if verified.GetVerificationStatus() != monitoringpb.NotificationChannel_VERIFIED {
		return fmt.Errorf("verification_status = %v, want VERIFIED", verified.GetVerificationStatus())
	}
	return nil
}

// Check 20: ListNotificationChannelDescriptors must return a non-empty catalog
// whose entries carry a type, display name, project-scoped name and labels.
func checkMonitoringListNotificationChannelDescriptors(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	it := nc.ListNotificationChannelDescriptors(ctx, &monitoringpb.ListNotificationChannelDescriptorsRequest{Name: monitoringParent(cfg)})
	count := 0
	for {
		d, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListNotificationChannelDescriptors: %w", err)
		}
		count++
		if d.GetType() == "" {
			return fmt.Errorf("descriptor %d has an empty type", count)
		}
		if d.GetDisplayName() == "" {
			return fmt.Errorf("descriptor %q has an empty display_name", d.GetType())
		}
		if len(d.GetLabels()) == 0 {
			return fmt.Errorf("descriptor %q has no labels", d.GetType())
		}
		if !strings.HasSuffix(d.GetName(), "/notificationChannelDescriptors/"+d.GetType()) {
			return fmt.Errorf("descriptor name = %q, want a projects/.../notificationChannelDescriptors/ suffix", d.GetName())
		}
	}
	if count == 0 {
		return errors.New("ListNotificationChannelDescriptors returned no descriptors")
	}
	return nil
}

// Check 21: GetNotificationChannelDescriptor must resolve a catalog type and
// return NotFound for an unknown one.
func checkMonitoringGetNotificationChannelDescriptor(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	name := monitoringParent(cfg) + "/notificationChannelDescriptors/email"
	got, err := nc.GetNotificationChannelDescriptor(ctx, &monitoringpb.GetNotificationChannelDescriptorRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetNotificationChannelDescriptor: %w", err)
	}
	if got.GetType() != "email" {
		return fmt.Errorf("type = %q, want email", got.GetType())
	}
	if got.GetName() != name {
		return fmt.Errorf("name = %q, want %q", got.GetName(), name)
	}

	_, err = nc.GetNotificationChannelDescriptor(ctx, &monitoringpb.GetNotificationChannelDescriptorRequest{
		Name: monitoringParent(cfg) + "/notificationChannelDescriptors/not-a-real-type",
	})
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetNotificationChannelDescriptor(unknown) = %v, want NotFound", err)
	}
	return nil
}

// Check 22: SendNotificationChannelVerificationCode is a no-op success for an
// existing channel (the emulator delivers nothing).
func checkMonitoringSendVerificationCode(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	if err := nc.SendNotificationChannelVerificationCode(ctx, &monitoringpb.SendNotificationChannelVerificationCodeRequest{Name: c.GetName()}); err != nil {
		return fmt.Errorf("SendNotificationChannelVerificationCode: %w", err)
	}
	return nil
}

// Check 23: GetNotificationChannelVerificationCode must return a non-empty code
// with an expiration.
func checkMonitoringGetVerificationCode(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	resp, err := nc.GetNotificationChannelVerificationCode(ctx, &monitoringpb.GetNotificationChannelVerificationCodeRequest{Name: c.GetName()})
	if err != nil {
		return fmt.Errorf("GetNotificationChannelVerificationCode: %w", err)
	}
	if resp.GetCode() == "" {
		return errors.New("GetNotificationChannelVerificationCode returned an empty code")
	}
	if resp.GetExpireTime() == nil {
		return errors.New("GetNotificationChannelVerificationCode returned no expire_time")
	}
	return nil
}

// Check 24: DeleteNotificationChannel must remove the channel created by check 15.
func checkMonitoringDeleteNotificationChannel(ctx context.Context, cfg Config) error {
	nc, err := newMonitoringChannelClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer nc.Close()

	c, err := findNotificationChannel(ctx, nc, cfg)
	if err != nil {
		return err
	}
	if err := nc.DeleteNotificationChannel(ctx, &monitoringpb.DeleteNotificationChannelRequest{Name: c.GetName()}); err != nil {
		return fmt.Errorf("DeleteNotificationChannel: %w", err)
	}
	if _, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: c.GetName()}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetNotificationChannel after delete = %v, want NotFound", err)
	}
	return nil
}

// ─── ServiceMonitoringService checks ──────────────────────────────────────────

func monitoringServiceID(cfg Config) string { return cfg.ResourceName("gcpc-svc") }

func monitoringServiceName(cfg Config) string {
	return monitoringParent(cfg) + "/services/" + monitoringServiceID(cfg)
}

func monitoringSLOID(cfg Config) string { return cfg.ResourceName("gcpc-slo") }

func monitoringSLODisplay(cfg Config) string { return cfg.ResourceName("gcpc-slo-display") }

func monitoringSLOName(cfg Config) string {
	return monitoringServiceName(cfg) + "/serviceLevelObjectives/" + monitoringSLOID(cfg)
}

func monitoringAvailabilitySLI() *monitoringpb.ServiceLevelIndicator {
	return &monitoringpb.ServiceLevelIndicator{
		Type: &monitoringpb.ServiceLevelIndicator_BasicSli{
			BasicSli: &monitoringpb.BasicSli{
				SliCriteria: &monitoringpb.BasicSli_Availability{
					Availability: &monitoringpb.BasicSli_AvailabilityCriteria{},
				},
			},
		},
	}
}

// Check 25: CreateService must return a service with a project-scoped name, the
// requested display name, and its Cloud Run identifier.
func checkMonitoringCreateService(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	created, err := sc.CreateService(ctx, &monitoringpb.CreateServiceRequest{
		Parent:    monitoringParent(cfg),
		ServiceId: monitoringServiceID(cfg),
		Service: &monitoringpb.Service{
			DisplayName: cfg.ResourceName("gcpc-svc-display"),
			Identifier: &monitoringpb.Service_CloudRun_{
				CloudRun: &monitoringpb.Service_CloudRun{ServiceName: "checkout"},
			},
			UserLabels: map[string]string{"conformance": "true"},
		},
	})
	if err != nil {
		return fmt.Errorf("CreateService: %w", err)
	}
	if got, want := created.GetName(), monitoringServiceName(cfg); got != want {
		return fmt.Errorf("created name = %q, want %q", got, want)
	}
	if created.GetDisplayName() == "" {
		return errors.New("created service has an empty display_name")
	}
	if created.GetCloudRun().GetServiceName() != "checkout" {
		return fmt.Errorf("created identifier = %+v, want cloudRun", created.GetIdentifier())
	}
	return nil
}

// Check 26: GetService must round-trip the created service and its identifier.
func checkMonitoringGetService(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	got, err := sc.GetService(ctx, &monitoringpb.GetServiceRequest{Name: monitoringServiceName(cfg)})
	if err != nil {
		return fmt.Errorf("GetService: %w", err)
	}
	if got.GetName() != monitoringServiceName(cfg) {
		return fmt.Errorf("name = %q, want %q", got.GetName(), monitoringServiceName(cfg))
	}
	if got.GetCloudRun().GetServiceName() != "checkout" {
		return fmt.Errorf("identifier = %+v, want cloudRun", got.GetIdentifier())
	}
	return nil
}

// Check 27: ListServices must include the created service.
func checkMonitoringListServices(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	it := sc.ListServices(ctx, &monitoringpb.ListServicesRequest{Parent: monitoringParent(cfg)})
	for {
		svc, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListServices did not include %q", monitoringServiceName(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListServices: %w", err)
		}
		if svc.GetName() == monitoringServiceName(cfg) {
			return nil
		}
	}
}

// Check 28: UpdateService with a display_name mask must apply the change while
// preserving the identifier.
func checkMonitoringUpdateService(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	updated, err := sc.UpdateService(ctx, &monitoringpb.UpdateServiceRequest{
		Service: &monitoringpb.Service{
			Name:        monitoringServiceName(cfg),
			DisplayName: "updated by conformance",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateService: %w", err)
	}
	if updated.GetDisplayName() != "updated by conformance" {
		return fmt.Errorf("display_name = %q, want updated by conformance", updated.GetDisplayName())
	}
	if updated.GetCloudRun().GetServiceName() != "checkout" {
		return fmt.Errorf("identifier lost on update: %+v", updated.GetIdentifier())
	}
	return nil
}

// Check 29: CreateServiceLevelObjective must return an SLO with the run-unique
// name, goal, rolling period and basic SLI.
func checkMonitoringCreateServiceLevelObjective(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	created, err := sc.CreateServiceLevelObjective(ctx, &monitoringpb.CreateServiceLevelObjectiveRequest{
		Parent:                  monitoringServiceName(cfg),
		ServiceLevelObjectiveId: monitoringSLOID(cfg),
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{
			DisplayName:           monitoringSLODisplay(cfg),
			Goal:                  0.99,
			Period:                &monitoringpb.ServiceLevelObjective_RollingPeriod{RollingPeriod: durationpb.New(30 * 24 * time.Hour)},
			ServiceLevelIndicator: monitoringAvailabilitySLI(),
		},
	})
	if err != nil {
		return fmt.Errorf("CreateServiceLevelObjective: %w", err)
	}
	if got, want := created.GetName(), monitoringSLOName(cfg); got != want {
		return fmt.Errorf("created name = %q, want %q", got, want)
	}
	if created.GetGoal() != 0.99 {
		return fmt.Errorf("goal = %v, want 0.99", created.GetGoal())
	}
	if created.GetRollingPeriod().AsDuration() != 30*24*time.Hour {
		return fmt.Errorf("rolling_period = %v, want 720h", created.GetRollingPeriod().AsDuration())
	}
	if created.GetServiceLevelIndicator().GetBasicSli().GetAvailability() == nil {
		return fmt.Errorf("indicator = %+v, want basicSli.availability", created.GetServiceLevelIndicator())
	}
	return nil
}

// Check 30: GetServiceLevelObjective must round-trip the created SLO.
func checkMonitoringGetServiceLevelObjective(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	got, err := sc.GetServiceLevelObjective(ctx, &monitoringpb.GetServiceLevelObjectiveRequest{
		Name: monitoringSLOName(cfg),
		View: monitoringpb.ServiceLevelObjective_FULL,
	})
	if err != nil {
		return fmt.Errorf("GetServiceLevelObjective: %w", err)
	}
	if got.GetDisplayName() != monitoringSLODisplay(cfg) {
		return fmt.Errorf("display_name = %q, want %q", got.GetDisplayName(), monitoringSLODisplay(cfg))
	}
	if got.GetServiceLevelIndicator().GetBasicSli().GetAvailability() == nil {
		return fmt.Errorf("indicator = %+v, want basicSli.availability", got.GetServiceLevelIndicator())
	}
	return nil
}

// Check 31: ListServiceLevelObjectives must include the created SLO.
func checkMonitoringListServiceLevelObjectives(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	it := sc.ListServiceLevelObjectives(ctx, &monitoringpb.ListServiceLevelObjectivesRequest{Parent: monitoringServiceName(cfg)})
	for {
		slo, err := it.Next()
		if err == iterator.Done {
			return fmt.Errorf("ListServiceLevelObjectives did not include %q", monitoringSLOName(cfg))
		}
		if err != nil {
			return fmt.Errorf("ListServiceLevelObjectives: %w", err)
		}
		if slo.GetName() == monitoringSLOName(cfg) {
			return nil
		}
	}
}

// Check 32: UpdateServiceLevelObjective with a goal mask must apply the change.
func checkMonitoringUpdateServiceLevelObjective(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	updated, err := sc.UpdateServiceLevelObjective(ctx, &monitoringpb.UpdateServiceLevelObjectiveRequest{
		ServiceLevelObjective: &monitoringpb.ServiceLevelObjective{
			Name: monitoringSLOName(cfg),
			Goal: 0.95,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"goal"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateServiceLevelObjective: %w", err)
	}
	if updated.GetGoal() != 0.95 {
		return fmt.Errorf("goal = %v, want 0.95", updated.GetGoal())
	}
	return nil
}

// Check 33: DeleteServiceLevelObjective must remove the SLO created by check 29.
func checkMonitoringDeleteServiceLevelObjective(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	if err := sc.DeleteServiceLevelObjective(ctx, &monitoringpb.DeleteServiceLevelObjectiveRequest{Name: monitoringSLOName(cfg)}); err != nil {
		return fmt.Errorf("DeleteServiceLevelObjective: %w", err)
	}
	if _, err := sc.GetServiceLevelObjective(ctx, &monitoringpb.GetServiceLevelObjectiveRequest{Name: monitoringSLOName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetServiceLevelObjective after delete = %v, want NotFound", err)
	}
	return nil
}

// Check 34: DeleteService must remove the service created by check 25.
func checkMonitoringDeleteService(ctx context.Context, cfg Config) error {
	sc, err := newMonitoringServiceClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer sc.Close()

	if err := sc.DeleteService(ctx, &monitoringpb.DeleteServiceRequest{Name: monitoringServiceName(cfg)}); err != nil {
		return fmt.Errorf("DeleteService: %w", err)
	}
	if _, err := sc.GetService(ctx, &monitoringpb.GetServiceRequest{Name: monitoringServiceName(cfg)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetService after delete = %v, want NotFound", err)
	}
	return nil
}

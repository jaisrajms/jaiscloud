package monitoring

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	distributionpb "google.golang.org/genproto/googleapis/api/distribution"
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func testServer(t *testing.T) (monitoringpb.MetricServiceClient, monitoringpb.AlertPolicyServiceClient, func()) {
	t.Helper()
	return testServerWithStore(t, monitoringstore.NewMemoryStore())
}

func testServerWithStore(t *testing.T, store monitoringstore.Store) (monitoringpb.MetricServiceClient, monitoringpb.AlertPolicyServiceClient, func()) {
	t.Helper()
	svc := NewService(core.NewService(store, "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	monitoringpb.RegisterMetricServiceServer(srv, svc)
	monitoringpb.RegisterAlertPolicyServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return monitoringpb.NewMetricServiceClient(conn),
		monitoringpb.NewAlertPolicyServiceClient(conn),
		func() { conn.Close(); srv.Stop() }
}

func testChannelServer(t *testing.T) (monitoringpb.NotificationChannelServiceClient, func()) {
	t.Helper()
	store := monitoringstore.NewMemoryStore()
	svc := NewService(core.NewService(store, "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	monitoringpb.RegisterNotificationChannelServiceServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	return monitoringpb.NewNotificationChannelServiceClient(conn),
		func() { conn.Close(); srv.Stop() }
}

func TestNotificationChannelCRUD(t *testing.T) {
	nc, cleanup := testChannelServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := nc.CreateNotificationChannel(ctx, &monitoringpb.CreateNotificationChannelRequest{
		Name: "projects/test",
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type:        "pubsub",
			DisplayName: "Ops topic",
			Labels:      map[string]string{"topic": "projects/test/topics/ops-alerts"},
		},
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if !strings.HasPrefix(created.GetName(), "projects/test/notificationChannels/") {
		t.Fatalf("created name = %q", created.GetName())
	}
	if created.GetType() != "pubsub" || created.GetLabels()["topic"] != "projects/test/topics/ops-alerts" {
		t.Fatalf("created = %+v", created)
	}
	if created.GetEnabled() == nil || !created.GetEnabled().GetValue() {
		t.Fatalf("created enabled = %v, want true", created.GetEnabled())
	}

	got, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get channel: %v", err)
	}
	if got.GetDisplayName() != "Ops topic" {
		t.Fatalf("get display name = %q", got.GetDisplayName())
	}

	list, err := nc.ListNotificationChannels(ctx, &monitoringpb.ListNotificationChannelsRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	if len(list.GetNotificationChannels()) != 1 || list.GetTotalSize() != 1 {
		t.Fatalf("list = %d (total %d), want 1", len(list.GetNotificationChannels()), list.GetTotalSize())
	}

	// Masked update keeps unmasked fields (the labels) intact.
	updated, err := nc.UpdateNotificationChannel(ctx, &monitoringpb.UpdateNotificationChannelRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		NotificationChannel: &monitoringpb.NotificationChannel{
			Name:        created.GetName(),
			DisplayName: "Renamed",
		},
	})
	if err != nil {
		t.Fatalf("update channel: %v", err)
	}
	if updated.GetDisplayName() != "Renamed" {
		t.Fatalf("updated display name = %q", updated.GetDisplayName())
	}
	if updated.GetLabels()["topic"] != "projects/test/topics/ops-alerts" {
		t.Fatalf("labels not preserved across masked update: %+v", updated.GetLabels())
	}

	if _, err := nc.DeleteNotificationChannel(ctx, &monitoringpb.DeleteNotificationChannelRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if _, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete err = %v, want NotFound", err)
	}
}

func TestNotificationChannelValidation(t *testing.T) {
	nc, cleanup := testChannelServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := nc.CreateNotificationChannel(ctx, &monitoringpb.CreateNotificationChannelRequest{
		Name:                "projects/test",
		NotificationChannel: &monitoringpb.NotificationChannel{DisplayName: "no type"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("create without type err = %v, want InvalidArgument", err)
	}
	if _, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: "projects/test/notAChannel/x"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("get invalid name err = %v, want InvalidArgument", err)
	}
	if _, err := nc.GetNotificationChannel(ctx, &monitoringpb.GetNotificationChannelRequest{Name: "projects/test/notificationChannels/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("get missing err = %v, want NotFound", err)
	}
}

func TestMetricDescriptorCRUD(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/test_metric"
	created, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/test",
		MetricDescriptor: &metricpb.MetricDescriptor{
			Type:        metricType,
			MetricKind:  metricpb.MetricDescriptor_GAUGE,
			ValueType:   metricpb.MetricDescriptor_DOUBLE,
			Unit:        "1",
			Description: "a test metric",
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetName() != "projects/test/metricDescriptors/"+metricType {
		t.Fatalf("created name = %q", created.GetName())
	}
	if created.GetValueType() != metricpb.MetricDescriptor_DOUBLE {
		t.Fatalf("created value type = %v", created.GetValueType())
	}

	got, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/test/metricDescriptors/" + metricType,
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetDescription() != "a test metric" {
		t.Fatalf("get description = %q", got.GetDescription())
	}

	list, err := mc.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetMetricDescriptors()) != 1 {
		t.Fatalf("list = %d descriptors, want 1", len(list.GetMetricDescriptors()))
	}

	if _, err := mc.DeleteMetricDescriptor(ctx, &monitoringpb.DeleteMetricDescriptorRequest{
		Name: "projects/test/metricDescriptors/" + metricType,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/test/metricDescriptors/" + metricType,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete err = %v, want NotFound", err)
	}
}

func TestCreateMetricDescriptorUpsert(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/upsert"
	first, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/test",
		MetricDescriptor: &metricpb.MetricDescriptor{
			Type:        metricType,
			MetricKind:  metricpb.MetricDescriptor_GAUGE,
			ValueType:   metricpb.MetricDescriptor_INT64,
			Description: "original",
			Labels: []*labelpb.LabelDescriptor{
				{Key: "old_label", ValueType: labelpb.LabelDescriptor_STRING},
			},
		},
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if len(first.GetLabels()) != 1 {
		t.Fatalf("first labels = %d, want 1", len(first.GetLabels()))
	}

	second, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/test",
		MetricDescriptor: &metricpb.MetricDescriptor{
			Type:        metricType,
			MetricKind:  metricpb.MetricDescriptor_CUMULATIVE,
			ValueType:   metricpb.MetricDescriptor_DOUBLE,
			Description: "updated",
			Labels: []*labelpb.LabelDescriptor{
				{Key: "new_label", ValueType: labelpb.LabelDescriptor_STRING},
			},
		},
	})
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	if second.GetDescription() != "updated" {
		t.Fatalf("updated description = %q", second.GetDescription())
	}
	if second.GetMetricKind() != metricpb.MetricDescriptor_CUMULATIVE {
		t.Fatalf("updated metric kind = %v", second.GetMetricKind())
	}
	if len(second.GetLabels()) != 2 {
		t.Fatalf("merged labels = %d, want 2 (old label retained)", len(second.GetLabels()))
	}

	got, err := mc.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/test/metricDescriptors/" + metricType,
	})
	if err != nil {
		t.Fatalf("get after re-create: %v", err)
	}
	if len(got.GetLabels()) != 2 {
		t.Fatalf("stored labels = %d, want 2", len(got.GetLabels()))
	}
}

func TestTimeSeriesCreateAndList(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/test_metric"
	now := time.Now().UTC().Truncate(time.Second)

	if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: metricType, Labels: map[string]string{"zone": "us-east1-a"}},
			Resource: &monitoredrespb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "test"}},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 42}},
			}},
		}},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}

	filter := `metric.type = "custom.googleapis.com/test_metric" AND resource.type = "global"`
	list, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   filter,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatalf("list time series: %v", err)
	}
	if len(list.GetTimeSeries()) != 1 {
		t.Fatalf("list = %d series, want 1", len(list.GetTimeSeries()))
	}
	pts := list.GetTimeSeries()[0].GetPoints()
	if len(pts) != 1 {
		t.Fatalf("points = %d, want 1", len(pts))
	}
	if pts[0].GetValue().GetDoubleValue() != 42 {
		t.Fatalf("point value = %v, want 42", pts[0].GetValue())
	}

	// A non-matching metric.type filter returns nothing.
	none, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   `metric.type = "custom.googleapis.com/other"`,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatalf("list non-matching: %v", err)
	}
	if len(none.GetTimeSeries()) != 0 {
		t.Fatalf("non-matching list = %d, want 0", len(none.GetTimeSeries()))
	}
}

func TestListTimeSeriesInvalidFilter(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	_, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   `resource.label.foo = "bar"`,
		Interval: &monitoringpb.TimeInterval{},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unsupported filter err = %v, want InvalidArgument", err)
	}
}

func TestListTimeSeriesAggregation(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/agg_test_metric"
	end := time.Now().UTC().Truncate(time.Second)
	write := func(env string, v float64, at time.Time) *monitoringpb.TimeSeries {
		return &monitoringpb.TimeSeries{
			Metric:   &metricpb.Metric{Type: metricType, Labels: map[string]string{"env": env}},
			Resource: &monitoredrespb.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "test"}},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(at)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: v}},
			}},
		}
	}
	if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{
			write("a", 5, end.Add(-30*time.Second)),
			write("b", 7, end.Add(-20*time.Second)),
		},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}

	req := func(period time.Duration) *monitoringpb.ListTimeSeriesRequest {
		return &monitoringpb.ListTimeSeriesRequest{
			Name:     "projects/test",
			Filter:   `metric.type = "` + metricType + `"`,
			Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(end.Add(-10 * time.Minute)), EndTime: timestamppb.New(end)},
			Aggregation: &monitoringpb.Aggregation{
				AlignmentPeriod:    durationpb.New(period),
				PerSeriesAligner:   monitoringpb.Aggregation_ALIGN_SUM,
				CrossSeriesReducer: monitoringpb.Aggregation_REDUCE_SUM,
			},
			View: monitoringpb.ListTimeSeriesRequest_FULL,
		}
	}

	resp, err := mc.ListTimeSeries(ctx, req(time.Hour))
	if err != nil {
		t.Fatalf("aggregated list: %v", err)
	}
	if len(resp.GetTimeSeries()) != 1 {
		t.Fatalf("aggregated series = %d, want 1", len(resp.GetTimeSeries()))
	}
	pts := resp.GetTimeSeries()[0].GetPoints()
	if len(pts) != 1 || pts[0].GetValue().GetDoubleValue() != 12 {
		t.Fatalf("aggregated points = %+v, want one point 12", pts)
	}
	if got := resp.GetTimeSeries()[0].GetValueType(); got != metricpb.MetricDescriptor_DOUBLE {
		t.Fatalf("aggregated value type = %v, want DOUBLE", got)
	}

	// An alignment period below 60s is rejected.
	if _, err := mc.ListTimeSeries(ctx, req(30*time.Second)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("short alignment period err = %v, want InvalidArgument", err)
	}
}

func TestListMonitoredResourceDescriptors(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list resource descriptors: %v", err)
	}
	if len(resp.GetResourceDescriptors()) < 10 {
		t.Fatalf("canonical catalog = %d descriptors, want at least 10", len(resp.GetResourceDescriptors()))
	}
	found := false
	for _, d := range resp.GetResourceDescriptors() {
		if d.GetType() != "gce_instance" {
			continue
		}
		found = true
		if d.GetName() != "projects/test/monitoredResourceDescriptors/gce_instance" {
			t.Fatalf("gce_instance name = %q", d.GetName())
		}
		if d.GetDisplayName() == "" {
			t.Fatalf("gce_instance has no display name")
		}
	}
	if !found {
		t.Fatalf("canonical catalog missing gce_instance: %+v", resp.GetResourceDescriptors())
	}
}

func TestAlertPolicyCRUD(t *testing.T) {
	_, ac, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "High CPU",
			Combiner:    monitoringpb.AlertPolicy_OR,
			Enabled:     wrapperspb.Bool(true),
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "cpu > 90%",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
					Filter:         `metric.type = "custom.googleapis.com/cpu"`,
					Comparison:     monitoringpb.ComparisonType_COMPARISON_GT,
					ThresholdValue: 0.9,
				}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	if !strings.HasPrefix(created.GetName(), "projects/test/alertPolicies/") {
		t.Fatalf("created name = %q", created.GetName())
	}
	if created.GetDisplayName() != "High CPU" {
		t.Fatalf("created display name = %q", created.GetDisplayName())
	}
	if len(created.GetConditions()) != 1 {
		t.Fatalf("created conditions = %d, want 1", len(created.GetConditions()))
	}

	got, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get policy: %v", err)
	}
	if got.GetCombiner() != monitoringpb.AlertPolicy_OR {
		t.Fatalf("combiner = %v, want OR", got.GetCombiner())
	}
	if len(got.GetConditions()) != 1 {
		t.Fatalf("get conditions = %d, want 1", len(got.GetConditions()))
	}

	list, err := ac.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list policies: %v", err)
	}
	if len(list.GetAlertPolicies()) != 1 {
		t.Fatalf("list = %d policies, want 1", len(list.GetAlertPolicies()))
	}
	if list.GetTotalSize() != 1 {
		t.Fatalf("total size = %d, want 1", list.GetTotalSize())
	}

	// Update (full replacement via empty update mask).
	updated, err := ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        created.GetName(),
			DisplayName: "High CPU (updated)",
			Combiner:    monitoringpb.AlertPolicy_AND,
			Enabled:     wrapperspb.Bool(false),
		},
	})
	if err != nil {
		t.Fatalf("update policy: %v", err)
	}
	if updated.GetDisplayName() != "High CPU (updated)" || updated.GetCombiner() != monitoringpb.AlertPolicy_AND {
		t.Fatalf("updated = %+v", updated)
	}

	if _, err := ac.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{Name: created.GetName()}); err != nil {
		t.Fatalf("delete policy: %v", err)
	}
	if _, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("get after delete err = %v, want NotFound", err)
	}
}

func TestUpdateAlertPolicyUpdateMask(t *testing.T) {
	_, ac, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Original",
			Combiner:    monitoringpb.AlertPolicy_AND,
			Enabled:     wrapperspb.Bool(true),
			NotificationChannels: []string{
				"projects/test/notificationChannels/nc1",
			},
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "cond1",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
					Filter: `metric.type = "custom.googleapis.com/cpu"`,
				}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}

	updated, err := ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        created.GetName(),
			DisplayName: "Renamed Only",
		},
	})
	if err != nil {
		t.Fatalf("update with mask: %v", err)
	}
	if updated.GetDisplayName() != "Renamed Only" {
		t.Fatalf("display name = %q", updated.GetDisplayName())
	}
	if len(updated.GetConditions()) != 1 {
		t.Fatalf("conditions after masked update = %d, want 1 (preserved)", len(updated.GetConditions()))
	}
	if len(updated.GetNotificationChannels()) != 1 {
		t.Fatalf("notification channels after masked update = %d, want 1 (preserved)", len(updated.GetNotificationChannels()))
	}
	if updated.GetCombiner() != monitoringpb.AlertPolicy_AND {
		t.Fatalf("combiner after masked update = %v, want AND (preserved)", updated.GetCombiner())
	}

	// An unsupported mask path is rejected with Unimplemented.
	if _, err := ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bogus_field"}},
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name: created.GetName(),
		},
	}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unsupported mask path err = %v, want Unimplemented", err)
	}
}

// delayedGetAlertPolicyStore wraps a monitoringstore.Store, delaying every
// GetAlertPolicy call to widen a TOCTOU race window in tests. It only
// affects the pre-fix code path (UpdateAlertPolicy calling a standalone
// GetAlertPolicy); UpdateAlertPolicyAtomic does its own internal locked read
// and never reaches this override.
type delayedGetAlertPolicyStore struct {
	monitoringstore.Store
	delay time.Duration
}

func (d *delayedGetAlertPolicyStore) GetAlertPolicy(ctx context.Context, project, id string) (monitoringstore.AlertPolicy, error) {
	p, err := d.Store.GetAlertPolicy(ctx, project, id)
	time.Sleep(d.delay)
	return p, err
}

// TestUpdateAlertPolicyConcurrentDisjointMasksNoLostUpdate proves
// UpdateAlertPolicy's get-merge-write cycle is atomic with respect to other
// concurrent masked-update requests. Without atomicity, a masked update
// touching only "display_name" reads a stale full copy of the policy (taken
// before a concurrent "enabled"-only masked update committed), then writes
// that stale copy back — silently reverting the enabled change even though
// the display_name update's mask never named it. 25 goroutines each update
// only "display_name" and 25 update only "enabled"; a delayed-Get store
// wrapper widens the TOCTOU window reliably (the real in-memory round trip
// otherwise completes in nanoseconds).
func TestUpdateAlertPolicyConcurrentDisjointMasksNoLostUpdate(t *testing.T) {
	_, ac, cleanup := testServerWithStore(t, &delayedGetAlertPolicyStore{Store: monitoringstore.NewMemoryStore(), delay: 5 * time.Millisecond})
	defer cleanup()
	ctx := context.Background()

	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/test",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "orig-name",
			Combiner:    monitoringpb.AlertPolicy_AND,
			Enabled:     wrapperspb.Bool(false),
		},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"display_name"}},
				AlertPolicy: &monitoringpb.AlertPolicy{
					Name:        created.GetName(),
					DisplayName: fmt.Sprintf("vName%d", i),
				},
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[perField+i] = ac.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"enabled"}},
				AlertPolicy: &monitoringpb.AlertPolicy{
					Name:    created.GetName(),
					Enabled: wrapperspb.Bool(true),
				},
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !strings.HasPrefix(got.GetDisplayName(), "vName") {
		t.Errorf("display_name reverted to a stale value instead of one of the 25 concurrent writers': got %q", got.GetDisplayName())
	}
	if !got.GetEnabled().GetValue() {
		t.Errorf("enabled reverted to the stale pre-race value (false) instead of the 25 concurrent writers' value (true)")
	}
}

func TestCreateAlertPolicyDefaultEnabled(t *testing.T) {
	_, ac, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	created, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name:        "projects/test",
		AlertPolicy: &monitoringpb.AlertPolicy{DisplayName: "No Enabled"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetEnabled() == nil || !created.GetEnabled().GetValue() {
		t.Fatalf("created enabled = %v, want true", created.GetEnabled())
	}

	got, err := ac.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{Name: created.GetName()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetEnabled() == nil || !got.GetEnabled().GetValue() {
		t.Fatalf("read back enabled = %v, want true", got.GetEnabled())
	}
}

func TestListTimeSeriesPointsDescending(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/desc"
	base := time.Now().UTC().Truncate(time.Second)
	older := base.Add(-time.Minute)
	newer := base

	if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: metricType},
			Resource: &monitoredrespb.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{
				{Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(older)}, Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1}}},
				{Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(newer)}, Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 2}}},
			},
		}},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}

	list, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   `metric.type = "custom.googleapis.com/desc"`,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(base.Add(-time.Hour)), EndTime: timestamppb.New(base.Add(time.Hour))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetTimeSeries()) != 1 {
		t.Fatalf("series = %d, want 1", len(list.GetTimeSeries()))
	}
	pts := list.GetTimeSeries()[0].GetPoints()
	if len(pts) != 2 {
		t.Fatalf("points = %d, want 2", len(pts))
	}
	if pts[0].GetValue().GetDoubleValue() != 2 || pts[1].GetValue().GetDoubleValue() != 1 {
		t.Fatalf("points not reverse-chronological: [%v, %v]", pts[0].GetValue().GetDoubleValue(), pts[1].GetValue().GetDoubleValue())
	}
}

func TestListTimeSeriesOutOfIntervalOmitted(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	metricType := "custom.googleapis.com/outofrange"
	base := time.Now().UTC().Truncate(time.Second)
	old := base.Add(-time.Hour)

	if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: metricType},
			Resource: &monitoredrespb.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(old)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1}},
			}},
		}},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}

	list, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   `metric.type = "custom.googleapis.com/outofrange"`,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(base.Add(-time.Minute)), EndTime: timestamppb.New(base)},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetTimeSeries()) != 0 {
		t.Fatalf("series = %d, want 0 (out-of-interval point omitted)", len(list.GetTimeSeries()))
	}
}

func TestListMetricDescriptorsFilter(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	for _, typ := range []string{
		"custom.googleapis.com/foo",
		"custom.googleapis.com/foobar",
		"compute.googleapis.com/instance/cpu",
	} {
		if _, err := mc.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
			Name:             "projects/test",
			MetricDescriptor: &metricpb.MetricDescriptor{Type: typ},
		}); err != nil {
			t.Fatalf("create %s: %v", typ, err)
		}
	}

	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		{"equality metric.type", `metric.type = "custom.googleapis.com/foo"`, []string{"custom.googleapis.com/foo"}},
		{"equality type alias", `type = "compute.googleapis.com/instance/cpu"`, []string{"compute.googleapis.com/instance/cpu"}},
		{"starts_with", `metric.type = starts_with("custom.googleapis.com/foo")`, []string{"custom.googleapis.com/foo", "custom.googleapis.com/foobar"}},
		{"and", `metric.type = starts_with("custom.googleapis.com") AND type = "custom.googleapis.com/foo"`, []string{"custom.googleapis.com/foo"}},
	}
	for _, tc := range cases {
		resp, err := mc.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
			Name:   "projects/test",
			Filter: tc.filter,
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got := make([]string, 0, len(resp.GetMetricDescriptors()))
		for _, d := range resp.GetMetricDescriptors() {
			got = append(got, d.GetType())
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}

	for _, bad := range []string{
		`metric.label.zone = "us-east1-a"`,  // unsupported key
		`metric.type != "x"`,                // unsupported operator
		`metric.type = "x" AND bogus = "y"`, // unknown key in AND
		`metric.type = "unterminated`,       // malformed literal
	} {
		if _, err := mc.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
			Name:   "projects/test",
			Filter: bad,
		}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("filter %q err = %v, want InvalidArgument", bad, err)
		}
	}
}

func TestListMonitoredResourceDescriptorsFilter(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		{"equality resource.type", `resource.type = "gce_instance"`, []string{"gce_instance"}},
		{"starts_with k8s", `resource.type = starts_with("k8s_")`, []string{"k8s_container", "k8s_node", "k8s_pod"}},
		{"and", `resource.type = starts_with("pubsub_") AND type = "pubsub_topic"`, []string{"pubsub_topic"}},
		{"name", `name = starts_with("projects/test/monitoredResourceDescriptors/cloudsql")`, []string{"cloudsql_database"}},
	}
	for _, tc := range cases {
		resp, err := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{
			Name:   "projects/test",
			Filter: tc.filter,
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got := make([]string, 0, len(resp.GetResourceDescriptors()))
		for _, d := range resp.GetResourceDescriptors() {
			got = append(got, d.GetType())
		}
		if strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}

	// Pagination still applies to the filtered catalog.
	page, err := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name:     "projects/test",
		PageSize: 2,
	})
	if err != nil {
		t.Fatalf("page list: %v", err)
	}
	if len(page.GetResourceDescriptors()) != 2 || page.GetNextPageToken() == "" {
		t.Fatalf("page = %d descriptors, next = %q", len(page.GetResourceDescriptors()), page.GetNextPageToken())
	}

	for _, bad := range []string{
		`metric.type = "gce_instance"`, // wrong surface key
		`resource.type > "gce"`,        // unsupported operator
		`resource.label.zone = "x"`,    // unsupported key
	} {
		if _, err := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{
			Name:   "projects/test",
			Filter: bad,
		}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("filter %q err = %v, want InvalidArgument", bad, err)
		}
	}
}

func TestGetMonitoredResourceDescriptor(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	got, err := mc.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: "projects/test/monitoredResourceDescriptors/gce_instance",
	})
	if err != nil {
		t.Fatalf("get known: %v", err)
	}
	if got.GetType() != "gce_instance" || got.GetDisplayName() == "" {
		t.Fatalf("descriptor = %+v", got)
	}
	var instanceID *labelpb.LabelDescriptor
	for _, l := range got.GetLabels() {
		if l.GetKey() == "instance_id" {
			instanceID = l
		}
	}
	if instanceID == nil || instanceID.GetValueType() != labelpb.LabelDescriptor_INT64 {
		t.Fatalf("instance_id label = %+v, want INT64", instanceID)
	}

	if _, err := mc.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: "projects/test/monitoredResourceDescriptors/not_a_real_type",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("get unknown err = %v, want NotFound", err)
	}
	if _, err := mc.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: "projects/test/notADescriptor/gce_instance",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("get malformed err = %v, want InvalidArgument", err)
	}
}

func TestCreateServiceTimeSeriesRoundTrip(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	if _, err := mc.CreateServiceTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: "custom.googleapis.com/svc"},
			Resource: &monitoredrespb.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 7}},
			}},
		}},
	}); err != nil {
		t.Fatalf("create service time series: %v", err)
	}

	list, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/test",
		Filter:   `metric.type = "custom.googleapis.com/svc"`,
		Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute))},
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetTimeSeries()) != 1 {
		t.Fatalf("series = %d, want 1", len(list.GetTimeSeries()))
	}
	if got := list.GetTimeSeries()[0].GetPoints()[0].GetValue().GetInt64Value(); got != 7 {
		t.Fatalf("value = %d, want 7", got)
	}

	// A series with no metric type is rejected.
	if _, err := mc.CreateServiceTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{Resource: &monitoredrespb.MonitoredResource{Type: "global"}}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty metric type err = %v, want InvalidArgument", err)
	}
}

func linearDistribution() *distributionpb.Distribution {
	return &distributionpb.Distribution{
		Count:                 6,
		Mean:                  3.5,
		SumOfSquaredDeviation: 17.5,
		BucketOptions: &distributionpb.Distribution_BucketOptions{
			Options: &distributionpb.Distribution_BucketOptions_LinearBuckets{
				LinearBuckets: &distributionpb.Distribution_BucketOptions_Linear{
					NumFiniteBuckets: 3,
					Width:            2,
					Offset:           1,
				},
			},
		},
		BucketCounts: []int64{1, 2, 2, 1, 0},
	}
}

func explicitDistribution() *distributionpb.Distribution {
	return &distributionpb.Distribution{
		Count:                 6,
		Mean:                  4.25,
		SumOfSquaredDeviation: 9.25,
		BucketOptions: &distributionpb.Distribution_BucketOptions{
			Options: &distributionpb.Distribution_BucketOptions_ExplicitBuckets{
				ExplicitBuckets: &distributionpb.Distribution_BucketOptions_Explicit{
					Bounds: []float64{1, 2, 5, 10},
				},
			},
		},
		BucketCounts: []int64{0, 1, 2, 2, 1},
	}
}

func TestDistributionTimeSeriesRoundTrip(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	want := map[string]*distributionpb.Distribution{
		"custom.googleapis.com/linear":   linearDistribution(),
		"custom.googleapis.com/explicit": explicitDistribution(),
	}
	for typ, dist := range want {
		if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name: "projects/test",
			TimeSeries: []*monitoringpb.TimeSeries{{
				Metric:   &metricpb.Metric{Type: typ},
				Resource: &monitoredrespb.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DistributionValue{DistributionValue: dist}},
				}},
			}},
		}); err != nil {
			t.Fatalf("create %s: %v", typ, err)
		}
	}

	for typ, wantDist := range want {
		list, err := mc.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
			Name:     "projects/test",
			Filter:   `metric.type = "` + typ + `"`,
			Interval: &monitoringpb.TimeInterval{StartTime: timestamppb.New(now.Add(-time.Minute)), EndTime: timestamppb.New(now.Add(time.Minute))},
			View:     monitoringpb.ListTimeSeriesRequest_FULL,
		})
		if err != nil {
			t.Fatalf("list %s: %v", typ, err)
		}
		if len(list.GetTimeSeries()) != 1 {
			t.Fatalf("%s: series = %d, want 1", typ, len(list.GetTimeSeries()))
		}
		got := list.GetTimeSeries()[0].GetPoints()[0].GetValue().GetDistributionValue()
		if got == nil {
			t.Fatalf("%s: distribution value missing", typ)
		}
		if !proto.Equal(got, wantDist) {
			t.Fatalf("%s: distribution = %+v, want %+v", typ, got, wantDist)
		}
	}
}

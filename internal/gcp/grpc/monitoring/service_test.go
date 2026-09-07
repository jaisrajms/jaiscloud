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
	labelpb "google.golang.org/genproto/googleapis/api/label"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	monitoredrespb "google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func testServer(t *testing.T) (monitoringpb.MetricServiceClient, monitoringpb.AlertPolicyServiceClient, func()) {
	t.Helper()
	return testServerWithStore(t, monitoringstore.NewMemoryStore())
}

func testServerWithStore(t *testing.T, store monitoringstore.Store) (monitoringpb.MetricServiceClient, monitoringpb.AlertPolicyServiceClient, func()) {
	t.Helper()
	svc := NewService(store, "test")

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

func TestListMonitoredResourceDescriptors(t *testing.T) {
	mc, _, cleanup := testServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/test",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: "custom.googleapis.com/m"},
			Resource: &monitoredrespb.MonitoredResource{Type: "gce_instance"},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1}},
			}},
		}},
	}); err != nil {
		t.Fatalf("create time series: %v", err)
	}

	resp, err := mc.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{Name: "projects/test"})
	if err != nil {
		t.Fatalf("list resource descriptors: %v", err)
	}
	if len(resp.GetResourceDescriptors()) != 1 || resp.GetResourceDescriptors()[0].GetType() != "gce_instance" {
		t.Fatalf("resource descriptors = %+v", resp.GetResourceDescriptors())
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

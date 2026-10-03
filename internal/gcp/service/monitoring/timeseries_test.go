package monitoring

import (
	"context"
	"testing"
	"time"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func newTestService() *Service {
	return NewService(monitoringstore.NewMemoryStore(), "default-proj")
}

func TestCreateTimeSeriesAutoCreatesMetricDescriptor(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	typ := "custom.googleapis.com/auto/created"

	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{{
		MetricType: typ, ResourceType: "global", MetricKind: metricKindGauge, ValueType: valueTypeDouble,
		Points: []monitoringstore.Point{pointAt(clockNow(), 1)},
	}}); err != nil {
		t.Fatalf("CreateTimeSeries: %v", err)
	}

	d, err := s.GetMetricDescriptor(ctx, "proj", typ)
	if err != nil {
		t.Fatalf("auto-created descriptor not found: %v", err)
	}
	if d.Type != typ || d.MetricKind != metricKindGauge || d.ValueType != valueTypeDouble {
		t.Fatalf("descriptor = %+v", d)
	}

	// A second write must not clobber the existing descriptor schema.
	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{{
		MetricType: typ, ResourceType: "global", MetricKind: 3, ValueType: valueTypeInt64,
	}}); err != nil {
		t.Fatalf("second CreateTimeSeries: %v", err)
	}
	d, _ = s.GetMetricDescriptor(ctx, "proj", typ)
	if d.MetricKind != metricKindGauge || d.ValueType != valueTypeDouble {
		t.Fatalf("descriptor was overwritten on second write: %+v", d)
	}

	// Deleting an auto-created descriptor succeeds; deleting it twice is 404.
	if err := s.DeleteMetricDescriptor(ctx, "proj", typ); err != nil {
		t.Fatalf("DeleteMetricDescriptor: %v", err)
	}
	if err := s.DeleteMetricDescriptor(ctx, "proj", typ); err == nil {
		t.Fatal("second delete should be NotFound")
	}
}

func TestCreateTimeSeriesInfersDescriptorSchema(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	doubleType := "custom.googleapis.com/auto/inferred-double"
	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{{
		MetricType: doubleType, ResourceType: "global",
		Points: []monitoringstore.Point{{EndTime: clockNow(), Value: floatValue(2.5)}},
	}}); err != nil {
		t.Fatalf("CreateTimeSeries: %v", err)
	}
	d, err := s.GetMetricDescriptor(ctx, "proj", doubleType)
	if err != nil {
		t.Fatalf("inferred descriptor not found: %v", err)
	}
	if d.MetricKind != metricKindGauge || d.ValueType != valueTypeDouble {
		t.Fatalf("inferred descriptor = %+v, want GAUGE/DOUBLE", d)
	}
	if len(d.MonitoredResourceTypes) != 1 || d.MonitoredResourceTypes[0] != "global" {
		t.Fatalf("inferred monitored resource types = %+v, want [global]", d.MonitoredResourceTypes)
	}

	intType := "custom.googleapis.com/auto/inferred-int"
	n := int64(3)
	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{{
		MetricType: intType, ResourceType: "global",
		Points: []monitoringstore.Point{{EndTime: clockNow(), Value: monitoringstore.TypedValue{Int64Value: &n}}},
	}}); err != nil {
		t.Fatalf("CreateTimeSeries int: %v", err)
	}
	d, _ = s.GetMetricDescriptor(ctx, "proj", intType)
	if d.ValueType != valueTypeInt64 {
		t.Fatalf("inferred int descriptor = %+v, want INT64", d)
	}
}

func TestCreateServiceTimeSeriesDoesNotAutoCreate(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	typ := "custom.googleapis.com/service/only"
	if err := s.CreateServiceTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{{
		MetricType: typ, ResourceType: "global",
	}}); err != nil {
		t.Fatalf("CreateServiceTimeSeries: %v", err)
	}
	if _, err := s.GetMetricDescriptor(ctx, "proj", typ); err == nil {
		t.Fatal("service write should not auto-create a descriptor")
	}
}

func TestListMetricDescriptorsPagination(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	types := []string{"custom.googleapis.com/a", "custom.googleapis.com/b"}
	for _, typ := range types {
		if _, err := s.CreateMetricDescriptor(ctx, "proj", monitoringstore.MetricDescriptor{Type: typ}); err != nil {
			t.Fatalf("CreateMetricDescriptor(%s): %v", typ, err)
		}
	}
	filter := `metric.type = starts_with("custom.googleapis.com")`

	page, next, err := s.ListMetricDescriptors(ctx, "proj", filter, 1, "")
	if err != nil {
		t.Fatalf("ListMetricDescriptors: %v", err)
	}
	if len(page) != 1 {
		t.Fatalf("first page = %d, want 1", len(page))
	}
	if next == "" {
		t.Fatal("expected a nextPageToken on a full page")
	}
	_, next2, err := s.ListMetricDescriptors(ctx, "proj", filter, 1, next)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if next2 != "" {
		t.Fatalf("second (last) page should not carry a token, got %q", next2)
	}
}

func TestListTimeSeriesLabelFilterAndAggregation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	typ := "custom.googleapis.com/filtered"
	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{
		testSeries(typ, "env", "production", pointAt(end.Add(-30*time.Second), 5)),
		testSeries(typ, "env", "staging", pointAt(end.Add(-20*time.Second), 7)),
	}); err != nil {
		t.Fatalf("CreateTimeSeries: %v", err)
	}

	iv := &TimeInterval{Start: end.Add(-10 * time.Minute), End: end}
	filter := `metric.type = "` + typ + `" AND metric.labels.env = "production"`
	page, _, err := s.ListTimeSeries(ctx, "proj", filter, iv, nil, nil, "", false, 0, "")
	if err != nil {
		t.Fatalf("ListTimeSeries: %v", err)
	}
	if len(page) != 1 || page[0].MetricLabels["env"] != "production" {
		t.Fatalf("label filter = %+v", page)
	}

	// Aggregating the unfiltered type sums both series into one point.
	all, _, err := s.ListTimeSeries(ctx, "proj", `metric.type = "`+typ+`"`, iv, &Aggregation{
		AlignmentPeriod:    time.Hour,
		PerSeriesAligner:   AlignSum,
		CrossSeriesReducer: ReduceSum,
	}, nil, "", false, 0, "")
	if err != nil {
		t.Fatalf("aggregated ListTimeSeries: %v", err)
	}
	if len(all) != 1 || len(all[0].Points) != 1 {
		t.Fatalf("aggregated = %+v", all)
	}
	if got := *all[0].Points[0].Value.DoubleValue; got != 12 {
		t.Fatalf("aggregated value = %v, want 12", got)
	}
}

// clockNow returns a stable timestamp for tests that only need a non-zero
// point time.
func clockNow() time.Time {
	return time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
}

// TestListTimeSeriesSecondaryAggregation covers J43: a secondary aggregation is
// applied after the primary one, and a non-blank order_by is rejected.
func TestListTimeSeriesSecondaryAggregation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	typ := "custom.googleapis.com/j43"
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Six one-minute points of 1 → primary sums per minute (1 each); the
	// secondary sums ten-minute buckets, collapsing them to a single point of 6.
	pts := make([]monitoringstore.Point, 0, 6)
	for i := 0; i < 6; i++ {
		pts = append(pts, pointAt(base.Add(time.Duration(i+1)*time.Minute), 1))
	}
	if err := s.CreateTimeSeries(ctx, "proj", []monitoringstore.TimeSeries{
		testSeries(typ, "env", "a", pts...),
	}); err != nil {
		t.Fatalf("CreateTimeSeries: %v", err)
	}
	iv := &TimeInterval{Start: base, End: base.Add(time.Hour)}
	filter := `metric.type = "` + typ + `"`
	primary := &Aggregation{AlignmentPeriod: time.Minute, PerSeriesAligner: AlignSum}
	secondary := &Aggregation{AlignmentPeriod: 10 * time.Minute, PerSeriesAligner: AlignSum, CrossSeriesReducer: ReduceSum}

	page, _, err := s.ListTimeSeries(ctx, "proj", filter, iv, primary, secondary, "", false, 0, "")
	if err != nil {
		t.Fatalf("secondary aggregation: %v", err)
	}
	if len(page) != 1 || len(page[0].Points) != 1 {
		t.Fatalf("secondary aggregation = %+v, want one 10-minute point", page)
	}
	if got := *page[0].Points[0].Value.DoubleValue; got != 6 {
		t.Fatalf("secondary sum = %v, want 6", got)
	}
	// The output point interval is the secondary alignment window.
	if d := page[0].Points[0].EndTime.Sub(page[0].Points[0].StartTime); d != 10*time.Minute {
		t.Fatalf("secondary interval width = %s, want 10m", d)
	}

	// order_by is unsupported and must be blank.
	if _, _, err := s.ListTimeSeries(ctx, "proj", filter, iv, primary, nil, "metric.type", false, 0, ""); err == nil {
		t.Fatal("expected order_by to be rejected")
	}
	// secondaryAggregation requires the primary aggregation.
	if _, _, err := s.ListTimeSeries(ctx, "proj", filter, iv, nil, secondary, "", false, 0, ""); err == nil {
		t.Fatal("expected secondary aggregation without primary to be rejected")
	}
}

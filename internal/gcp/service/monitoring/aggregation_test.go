package monitoring

import (
	"testing"
	"time"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func floatValue(v float64) monitoringstore.TypedValue {
	return monitoringstore.TypedValue{DoubleValue: &v}
}

func testSeries(metricType, labelKey, labelValue string, points ...monitoringstore.Point) monitoringstore.TimeSeries {
	return monitoringstore.TimeSeries{
		MetricType:   metricType,
		MetricLabels: map[string]string{labelKey: labelValue},
		ResourceType: "global",
		MetricKind:   1, // GAUGE
		ValueType:    valueTypeDouble,
		Points:       points,
	}
}

func pointAt(t time.Time, v float64) monitoringstore.Point {
	return monitoringstore.Point{EndTime: t, Value: floatValue(v)}
}

func TestApplyAggregationSumReduce(t *testing.T) {
	// The request interval ends at an off-grid time (:30); the aligned bucket
	// must still land on the epoch grid (top of the hour), not at :30.
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a", pointAt(end.Add(-30*time.Second), 5)),
		testSeries("custom.googleapis.com/agg", "env", "b", pointAt(end.Add(-20*time.Second), 7)),
	}
	agg := &Aggregation{
		AlignmentPeriod:    time.Hour,
		PerSeriesAligner:   AlignSum,
		CrossSeriesReducer: ReduceSum,
	}
	out, err := applyAggregation(series, agg)
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("reduced series = %d, want 1", len(out))
	}
	if len(out[0].Points) != 1 {
		t.Fatalf("reduced points = %d, want 1", len(out[0].Points))
	}
	wantEnd := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	if got := out[0].Points[0].EndTime; !got.Equal(wantEnd) {
		t.Fatalf("aligned bucket end = %s, want %s (epoch grid)", got, wantEnd)
	}
	if got := out[0].Points[0].Value; got.DoubleValue == nil || *got.DoubleValue != 12 {
		t.Fatalf("reduced value = %+v, want 12", got)
	}
	// Cross-series reduction drops the collapsed metric label.
	if _, ok := out[0].MetricLabels["env"]; ok {
		t.Fatalf("reduced series kept the collapsed env label: %+v", out[0].MetricLabels)
	}
}

func TestApplyAggregationGroupByKeepsLabel(t *testing.T) {
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a", pointAt(end.Add(-30*time.Second), 5)),
		testSeries("custom.googleapis.com/agg", "env", "b", pointAt(end.Add(-20*time.Second), 7)),
	}
	agg := &Aggregation{
		AlignmentPeriod:    time.Hour,
		PerSeriesAligner:   AlignSum,
		CrossSeriesReducer: ReduceSum,
		GroupByFields:      []string{"metric.labels.env"},
	}
	out, err := applyAggregation(series, agg)
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("grouped series = %d, want 2", len(out))
	}
	want := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	values := map[string]float64{}
	for _, s := range out {
		values[s.MetricLabels["env"]] = *s.Points[0].Value.DoubleValue
		if !s.Points[0].EndTime.Equal(want) {
			t.Fatalf("grouped bucket end = %s, want %s", s.Points[0].EndTime, want)
		}
	}
	if values["a"] != 5 || values["b"] != 7 {
		t.Fatalf("grouped values = %+v, want a=5 b=7", values)
	}
}

func TestApplyAggregationCountType(t *testing.T) {
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a",
			pointAt(end.Add(-30*time.Second), 5), pointAt(end.Add(-20*time.Second), 7)),
	}
	agg := &Aggregation{AlignmentPeriod: time.Hour, PerSeriesAligner: AlignCount}
	out, err := applyAggregation(series, agg)
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if len(out) != 1 || len(out[0].Points) != 1 {
		t.Fatalf("aligned = %+v", out)
	}
	if out[0].ValueType != valueTypeInt64 {
		t.Fatalf("count value type = %d, want INT64", out[0].ValueType)
	}
	if out[0].Points[0].Value.Int64Value == nil || *out[0].Points[0].Value.Int64Value != 2 {
		t.Fatalf("count value = %+v, want 2", out[0].Points[0].Value)
	}
}

// TestApplyAggregationEpochGrid pins the epoch-aligned bucketing: a point lands
// in the half-open `(start, end]` window with the first period boundary at or
// after its end time, independently of the request interval.
func TestApplyAggregationEpochGrid(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/grid", "env", "a",
			pointAt(base, 1), // exact boundary -> 00:00
			pointAt(base.Add(4*time.Minute+59*time.Second), 2),  // -> 00:05
			pointAt(base.Add(5*time.Minute), 3),                 // exact boundary -> 00:05
			pointAt(base.Add(5*time.Minute+time.Second), 4),     // -> 00:10
			pointAt(base.Add(12*time.Minute+30*time.Second), 5), // -> 00:15
		),
	}
	out, err := applyAggregation(series, &Aggregation{AlignmentPeriod: 5 * time.Minute, PerSeriesAligner: AlignSum})
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("aligned series = %d, want 1", len(out))
	}
	want := []struct {
		end time.Time
		sum float64
	}{
		{base, 1},
		{base.Add(5 * time.Minute), 5}, // 2 + 3
		{base.Add(10 * time.Minute), 4},
		{base.Add(15 * time.Minute), 5},
	}
	pts := out[0].Points
	if len(pts) != len(want) {
		t.Fatalf("aligned points = %d, want %d: %+v", len(pts), len(want), pts)
	}
	for i, w := range want {
		if !pts[i].EndTime.Equal(w.end) {
			t.Errorf("point %d end = %s, want %s", i, pts[i].EndTime, w.end)
		}
		if pts[i].Value.DoubleValue == nil || *pts[i].Value.DoubleValue != w.sum {
			t.Errorf("point %d sum = %+v, want %v", i, pts[i].Value, w.sum)
		}
	}
}

func TestApplyAggregationValidation(t *testing.T) {
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a", pointAt(end.Add(-30*time.Second), 5)),
	}

	// alignment_period below the 60s minimum is rejected.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: 30 * time.Second, PerSeriesAligner: AlignSum,
	}); err == nil {
		t.Fatal("short alignment_period should be rejected")
	}
	// A reducer requires an aligner.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: time.Hour, CrossSeriesReducer: ReduceSum,
	}); err == nil {
		t.Fatal("reducer without aligner should be rejected")
	}
	// Unknown aligner/reducer are rejected.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: time.Hour, PerSeriesAligner: "ALIGN_PERCENTILE_99",
	}); err == nil {
		t.Fatal("unsupported aligner should be rejected")
	}
	// Non-numeric values cannot be aggregated.
	str := "x"
	bad := []monitoringstore.TimeSeries{{
		MetricType: "custom.googleapis.com/agg", ResourceType: "global",
		Points: []monitoringstore.Point{{EndTime: end.Add(-time.Second), Value: monitoringstore.TypedValue{StringValue: &str}}},
	}}
	if _, err := applyAggregation(bad, &Aggregation{
		AlignmentPeriod: time.Hour, PerSeriesAligner: AlignSum,
	}); err == nil {
		t.Fatal("string values should not aggregate")
	}
	// ALIGN_NONE / REDUCE_NONE is a no-op.
	out, err := applyAggregation(series, &Aggregation{PerSeriesAligner: AlignNone, CrossSeriesReducer: ReduceNone})
	if err != nil {
		t.Fatalf("no-op aggregation: %v", err)
	}
	if len(out) != 1 || len(out[0].Points) != 1 {
		t.Fatalf("no-op aggregation changed the series: %+v", out)
	}
	// Unknown group_by field is rejected.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: time.Hour, PerSeriesAligner: AlignSum, CrossSeriesReducer: ReduceSum,
		GroupByFields: []string{"metadata.system_labels.foo"},
	}); err == nil {
		t.Fatal("unsupported group_by field should be rejected")
	}
}

func TestCeilToPeriod(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		t    time.Time
		d    time.Duration
		want time.Time
	}{
		{"exact boundary unchanged", base, 5 * time.Minute, base},
		{"rounds up within period", base.Add(time.Minute), 5 * time.Minute, base.Add(5 * time.Minute)},
		{"just before boundary", base.Add(4*time.Minute + 59*time.Second), 5 * time.Minute, base.Add(5 * time.Minute)},
		{"crosses midnight", time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC), time.Hour, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"pre-epoch rounds up to epoch", time.Date(1969, 12, 31, 23, 59, 30, 0, time.UTC), time.Hour, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"pre-epoch within period", time.Date(1969, 12, 31, 22, 30, 0, 0, time.UTC), time.Hour, time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		if got := ceilToPeriod(c.t, c.d); !got.Equal(c.want) {
			t.Errorf("%s: ceilToPeriod(%s,%s) = %s, want %s", c.name, c.t, c.d, got, c.want)
		}
	}
}

func TestFloorDiv(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{10, 3, 3},
		{-1, 3, -1},
		{-3, 3, -1},
		{-4, 3, -2},
		{0, 3, 0},
	}
	for _, c := range cases {
		if got := floorDiv(c.a, c.b); got != c.want {
			t.Errorf("floorDiv(%d,%d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

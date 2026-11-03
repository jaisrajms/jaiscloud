package monitoring

import (
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/model"

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

func TestApplyAggregationIntervalAndMetricKind(t *testing.T) {
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	// A DELTA series aligned to a period becomes a per-period GAUGE.
	delta := testSeries("custom.googleapis.com/delta", "env", "a", pointAt(end, 9))
	delta.MetricKind = 2 // DELTA
	out, err := applyAggregation([]monitoringstore.TimeSeries{delta}, &Aggregation{
		AlignmentPeriod:  time.Minute,
		PerSeriesAligner: AlignMean,
	}, time.Time{})
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if out[0].MetricKind != metricKindGauge {
		t.Fatalf("aligned metric kind = %d, want GAUGE(%d)", out[0].MetricKind, metricKindGauge)
	}
	p := out[0].Points[0]
	wantEnd := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	if !p.EndTime.Equal(wantEnd) {
		t.Fatalf("aligned end = %s, want %s", p.EndTime, wantEnd)
	}
	if !p.StartTime.Equal(wantEnd.Add(-time.Minute)) {
		t.Fatalf("aligned start = %s, want %s (end - alignment period)", p.StartTime, wantEnd.Add(-time.Minute))
	}
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
	out, err := applyAggregation(series, agg, time.Time{})
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
	out, err := applyAggregation(series, agg, time.Time{})
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
	out, err := applyAggregation(series, agg, time.Time{})
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

// TestApplyAggregationEpochGrid pins the legacy epoch-aligned bucketing used
// when an aggregation runs without a request interval end: a point lands in the
// half-open `(start, end]` window with the first epoch period boundary at or
// after its end time. With an interval end the grid is anchored on it instead
// (see TestAlignmentGridAnchorsOnIntervalEnd).
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
	out, err := applyAggregation(series, &Aggregation{AlignmentPeriod: 5 * time.Minute, PerSeriesAligner: AlignSum}, time.Time{})
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
	}, time.Time{}); err == nil {
		t.Fatal("short alignment_period should be rejected")
	}
	// A reducer requires an aligner.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: time.Hour, CrossSeriesReducer: ReduceSum,
	}, time.Time{}); err == nil {
		t.Fatal("reducer without aligner should be rejected")
	}
	// Unknown aligner/reducer are rejected.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: time.Hour, PerSeriesAligner: "ALIGN_PERCENTILE_99",
	}, time.Time{}); err == nil {
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
	}, time.Time{}); err == nil {
		t.Fatal("string values should not aggregate")
	}
	// ALIGN_NONE / REDUCE_NONE is a no-op.
	out, err := applyAggregation(series, &Aggregation{PerSeriesAligner: AlignNone, CrossSeriesReducer: ReduceNone}, time.Time{})
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
	}, time.Time{}); err == nil {
		t.Fatal("unsupported group_by field should be rejected")
	}
}

// TestAlignmentPeriodUpperBound covers NF6: real Cloud Monitoring caps
// alignmentPeriod at 104 weeks, answering 400 INVALID_ARGUMENT with this exact
// message above it and 200 at exactly 104 weeks.
func TestAlignmentPeriodUpperBound(t *testing.T) {
	end := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a", pointAt(end.Add(-time.Second), 5)),
	}
	_, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: 17472*time.Hour + time.Second, PerSeriesAligner: AlignMean,
	}, time.Time{})
	if err == nil {
		t.Fatal("alignment_period above 104 weeks should be rejected")
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("alignment_period overflow err = %v, want a ProviderError", err)
	}
	if perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
		t.Fatalf("alignment_period overflow = %d/%s, want 400/InvalidArgument", perr.HTTPStatus, perr.Code)
	}
	if want := "The alignment window must not exceed 17472h"; perr.Message != want {
		t.Fatalf("alignment_period overflow message = %q, want %q", perr.Message, want)
	}
	// Exactly 104 weeks is still accepted.
	if _, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod: 17472 * time.Hour, PerSeriesAligner: AlignMean,
	}, time.Time{}); err != nil {
		t.Fatalf("alignment_period of exactly 104 weeks should be accepted: %v", err)
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

// TestAlignmentGridAnchorsOnIntervalEnd covers NF7 with the recorded real-GCP
// probe table (§1b of the no-fix audit plan): the newest aligned bucket ends
// exactly at interval.endTime, the buckets are counted back by the period, no
// bucket end exceeds the interval end, and a bucket with no data is not emitted.
func TestAlignmentGridAnchorsOnIntervalEnd(t *testing.T) {
	at := func(h, m, s int) time.Time { return time.Date(2026, 1, 1, h, m, s, 0, time.UTC) }
	cases := []struct {
		name   string
		end    time.Time
		period time.Duration
		want   []time.Time
	}{
		{
			name: "interval end on an epoch boundary",
			end:  at(2, 20, 0), period: 5 * time.Minute,
			want: []time.Time{at(2, 15, 0), at(2, 20, 0)},
		},
		{
			name: "interval end not on the epoch grid",
			end:  at(2, 19, 59), period: 5 * time.Minute,
			want: []time.Time{at(2, 14, 59), at(2, 19, 59)},
		},
		{
			name: "02:22:30Z with 300s (epoch bucketing would give :15/:20)",
			end:  at(2, 22, 30), period: 5 * time.Minute,
			want: []time.Time{at(2, 12, 30), at(2, 17, 30), at(2, 22, 30)},
		},
		{
			name: "02:24:40Z with 180s",
			end:  at(2, 24, 40), period: 3 * time.Minute,
			want: []time.Time{at(2, 12, 40), at(2, 15, 40), at(2, 18, 40), at(2, 21, 40), at(2, 24, 40)},
		},
		{
			name: "02:24:40Z with 420s",
			end:  at(2, 24, 40), period: 7 * time.Minute,
			want: []time.Time{at(2, 17, 40), at(2, 24, 40)},
		},
		{
			name: "02:32:00Z with 300s has no bucket beyond the interval end",
			end:  at(2, 32, 0), period: 5 * time.Minute,
			want: []time.Time{at(2, 12, 0), at(2, 17, 0), at(2, 22, 0), at(2, 27, 0), at(2, 32, 0)},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// One point 30s before each expected bucket end, so every window
			// carries exactly one value and no empty bucket can appear.
			pts := make([]monitoringstore.Point, 0, len(c.want))
			for i := len(c.want) - 1; i >= 0; i-- {
				pts = append(pts, pointAt(c.want[i].Add(-30*time.Second), float64(i)))
			}
			out, err := applyAggregation(
				[]monitoringstore.TimeSeries{testSeries("custom.googleapis.com/grid", "env", "a", pts...)},
				&Aggregation{AlignmentPeriod: c.period, PerSeriesAligner: AlignSum}, c.end)
			if err != nil {
				t.Fatalf("applyAggregation: %v", err)
			}
			if len(out) != 1 {
				t.Fatalf("aligned series = %d, want 1", len(out))
			}
			got := out[0].Points
			if len(got) != len(c.want) {
				t.Fatalf("aligned points = %d, want %d: %+v", len(got), len(c.want), got)
			}
			for i, want := range c.want {
				if !got[i].EndTime.Equal(want) {
					t.Errorf("point %d end = %s, want %s", i, got[i].EndTime, want)
				}
				if got[i].EndTime.After(c.end) {
					t.Errorf("point %d end = %s is after the interval end %s", i, got[i].EndTime, c.end)
				}
				if d := got[i].EndTime.Sub(got[i].StartTime); d != c.period {
					t.Errorf("point %d interval width = %s, want %s", i, d, c.period)
				}
			}
			if last := got[len(got)-1].EndTime; !last.Equal(c.end) {
				t.Errorf("newest bucket end = %s, want the interval end %s", last, c.end)
			}
		})
	}
}

// TestAlignmentGridBoundaryPoint pins the half-open window rule on the
// end-anchored grid: a point exactly on a bucket boundary stays in the window
// that boundary closes.
func TestAlignmentGridBoundaryPoint(t *testing.T) {
	end := time.Date(2026, 1, 1, 2, 22, 30, 0, time.UTC)
	boundary := end.Add(-5 * time.Minute) // 02:17:30
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/grid", "env", "a",
			pointAt(boundary, 1),
			pointAt(boundary.Add(time.Second), 2),
			pointAt(end.Add(-time.Second), 4),
		),
	}
	out, err := applyAggregation(series, &Aggregation{AlignmentPeriod: 5 * time.Minute, PerSeriesAligner: AlignSum}, end)
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	pts := out[0].Points
	if len(pts) != 2 {
		t.Fatalf("aligned points = %d, want 2: %+v", len(pts), pts)
	}
	if !pts[0].EndTime.Equal(boundary) || *pts[0].Value.DoubleValue != 1 {
		t.Fatalf("boundary bucket = %s/%v, want %s/1 (the boundary point closes that window)",
			pts[0].EndTime, *pts[0].Value.DoubleValue, boundary)
	}
	if !pts[1].EndTime.Equal(end) || *pts[1].Value.DoubleValue != 6 {
		t.Fatalf("last bucket = %s/%v, want %s/6", pts[1].EndTime, *pts[1].Value.DoubleValue, end)
	}
}

// TestAlignmentGridCrossSeriesReduction reduces two series over the
// end-anchored grid: buckets sharing an end collapse, and no bucket lands after
// the interval end.
func TestAlignmentGridCrossSeriesReduction(t *testing.T) {
	end := time.Date(2026, 1, 1, 2, 22, 30, 0, time.UTC)
	series := []monitoringstore.TimeSeries{
		testSeries("custom.googleapis.com/agg", "env", "a",
			pointAt(end.Add(-12*time.Minute), 2),               // 02:10:30 -> bucket 02:12:30
			pointAt(end.Add(-6*time.Minute-30*time.Second), 1), // 02:16:00 -> bucket 02:17:30
		),
		testSeries("custom.googleapis.com/agg", "env", "b",
			pointAt(end.Add(-11*time.Minute), 4), // 02:11:30 -> bucket 02:12:30
			pointAt(end.Add(-6*time.Minute), 1),  // 02:16:30 -> bucket 02:17:30
		),
	}
	out, err := applyAggregation(series, &Aggregation{
		AlignmentPeriod:    5 * time.Minute,
		PerSeriesAligner:   AlignSum,
		CrossSeriesReducer: ReduceSum,
	}, end)
	if err != nil {
		t.Fatalf("applyAggregation: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("reduced series = %d, want 1", len(out))
	}
	pts := out[0].Points
	if len(pts) != 2 {
		t.Fatalf("reduced points = %d, want 2: %+v", len(pts), pts)
	}
	want := []struct {
		end time.Time
		v   float64
	}{
		{end.Add(-10 * time.Minute), 6}, // 02:12:30
		{end.Add(-5 * time.Minute), 2},  // 02:17:30
	}
	for i, w := range want {
		if !pts[i].EndTime.Equal(w.end) {
			t.Errorf("reduced point %d end = %s, want %s", i, pts[i].EndTime, w.end)
		}
		if got := *pts[i].Value.DoubleValue; got != w.v {
			t.Errorf("reduced point %d value = %v, want %v", i, got, w.v)
		}
		if pts[i].EndTime.After(end) {
			t.Errorf("reduced point %d end = %s is after the interval end %s", i, pts[i].EndTime, end)
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

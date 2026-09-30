package monitoring

import (
	"math"
	"sort"
	"strings"
	"time"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// Aligner is the transport-neutral per-series aligner (the protojson
// google.monitoring.v3.Aggregation.Aligner enum name). Only the aligners the
// emulator implements are accepted; everything else is InvalidArgument.
type Aligner string

// Supported per-series aligners.
const (
	AlignNone  Aligner = "ALIGN_NONE"
	AlignSum   Aligner = "ALIGN_SUM"
	AlignMean  Aligner = "ALIGN_MEAN"
	AlignMin   Aligner = "ALIGN_MIN"
	AlignMax   Aligner = "ALIGN_MAX"
	AlignCount Aligner = "ALIGN_COUNT"
)

// Reducer is the transport-neutral cross-series reducer (the protojson
// google.monitoring.v3.Aggregation.Reducer enum name).
type Reducer string

// Supported cross-series reducers.
const (
	ReduceNone  Reducer = "REDUCE_NONE"
	ReduceSum   Reducer = "REDUCE_SUM"
	ReduceMean  Reducer = "REDUCE_MEAN"
	ReduceMin   Reducer = "REDUCE_MIN"
	ReduceMax   Reducer = "REDUCE_MAX"
	ReduceCount Reducer = "REDUCE_COUNT"
)

// Aggregation is the transport-neutral google.monitoring.v3.Aggregation applied
// to ListTimeSeries results. An empty/nil Aggregation, or one whose aligner and
// reducer are both the zero value, leaves the series untouched.
type Aggregation struct {
	AlignmentPeriod    time.Duration
	PerSeriesAligner   Aligner
	CrossSeriesReducer Reducer
	GroupByFields      []string
}

// metric value type constants live in types.go (valueTypeInt64, valueTypeDouble,
// …); aggregation output points reuse the canonical metric descriptor encoding.

// minAlignmentPeriod is the smallest alignment period Cloud Monitoring accepts
// when a per-series aligner is set.
const minAlignmentPeriod = 60 * time.Second

// normalize fills defaults for the zero value of each enum.
func (a *Aggregation) normalize() {
	if a.PerSeriesAligner == "" {
		a.PerSeriesAligner = AlignNone
	}
	if a.CrossSeriesReducer == "" {
		a.CrossSeriesReducer = ReduceNone
	}
}

func (a *Aggregation) alignerSet() bool {
	return a.PerSeriesAligner != "" && a.PerSeriesAligner != AlignNone
}

func (a *Aggregation) reducerSet() bool {
	return a.CrossSeriesReducer != "" && a.CrossSeriesReducer != ReduceNone
}

// validate enforces the wire contract: known enum names, a reducer requires an
// aligner, and an aligner requires an alignment period of at least 60s.
func (a *Aggregation) validate() error {
	switch a.PerSeriesAligner {
	case "", AlignNone, AlignSum, AlignMean, AlignMin, AlignMax, AlignCount:
	default:
		return invalidArgument("unsupported per_series_aligner: " + string(a.PerSeriesAligner))
	}
	switch a.CrossSeriesReducer {
	case "", ReduceNone, ReduceSum, ReduceMean, ReduceMin, ReduceMax, ReduceCount:
	default:
		return invalidArgument("unsupported cross_series_reducer: " + string(a.CrossSeriesReducer))
	}
	if a.reducerSet() && !a.alignerSet() {
		return invalidArgument("cross_series_reducer requires per_series_aligner to be set and not ALIGN_NONE")
	}
	if a.alignerSet() && a.AlignmentPeriod < minAlignmentPeriod {
		return invalidArgument("alignment_period must be specified and at least 60 seconds when a per_series_aligner is set")
	}
	return nil
}

// applyAggregation aligns each series to AlignmentPeriod buckets on the
// Unix-epoch period grid and, when a cross-series reducer is set, collapses the
// resulting series by the group-by fields.
//
// Returning the input unchanged when no aggregation is requested keeps the
// unaggregated path allocation-free.
func applyAggregation(series []monitoringstore.TimeSeries, agg *Aggregation) ([]monitoringstore.TimeSeries, error) {
	if agg == nil {
		return series, nil
	}
	aggCopy := *agg
	aggCopy.normalize()
	if err := aggCopy.validate(); err != nil {
		return nil, err
	}
	if !aggCopy.alignerSet() && !aggCopy.reducerSet() {
		return series, nil
	}

	aligned := make([]alignedSeries, 0, len(series))
	for _, ts := range series {
		a, err := alignSeries(ts, aggCopy.PerSeriesAligner, aggCopy.AlignmentPeriod)
		if err != nil {
			return nil, err
		}
		if a != nil {
			aligned = append(aligned, *a)
		}
	}

	if !aggCopy.reducerSet() {
		out := make([]monitoringstore.TimeSeries, 0, len(aligned))
		for _, a := range aligned {
			out = append(out, a.output())
		}
		return out, nil
	}
	return reduceSeries(aligned, aggCopy.CrossSeriesReducer, aggCopy.GroupByFields)
}

// alignedSeries is one series after per-series alignment: a set of buckets keyed
// by bucket end time, plus the output metric kind/value type.
type alignedSeries struct {
	src     monitoringstore.TimeSeries
	kind    int32
	valueTy int32
	buckets map[time.Time]float64
}

// output materialises the aligned buckets as a store TimeSeries.
func (a alignedSeries) output() monitoringstore.TimeSeries {
	out := a.src
	out.MetricKind = a.kind
	out.ValueType = a.valueTy
	out.Points = make([]monitoringstore.Point, 0, len(a.buckets))
	for _, end := range sortedBucketTimes(a.buckets) {
		out.Points = append(out.Points, monitoringstore.Point{
			StartTime: end,
			EndTime:   end,
			Value:     typedValueFor(a.valueTy, a.buckets[end]),
		})
	}
	return out
}

// alignSeries buckets ts.Points into AlignmentPeriod windows on the Unix-epoch
// grid and aligns each bucket. A point belongs to the window ending at the first
// period boundary at or after its end time, matching Cloud Monitoring's
// half-open `(start, end]` alignment windows; a point exactly on a boundary
// stays in that boundary's window. It returns nil for a series with no points.
func alignSeries(ts monitoringstore.TimeSeries, aligner Aligner, period time.Duration) (*alignedSeries, error) {
	if len(ts.Points) == 0 {
		return nil, nil
	}
	if period < time.Millisecond {
		return nil, invalidArgument("alignment_period must be at least one millisecond")
	}

	byBucket := map[int64][]float64{}
	for _, pt := range ts.Points {
		if pt.EndTime.IsZero() {
			continue
		}
		v, ok := pointNumericValue(pt.Value)
		if !ok {
			return nil, invalidArgument("time series values of this type cannot be aggregated")
		}
		end := ceilToPeriod(pt.EndTime, period)
		key := end.UnixMilli()
		byBucket[key] = append(byBucket[key], v)
	}
	if len(byBucket) == 0 {
		return nil, nil
	}

	buckets := make(map[time.Time]float64, len(byBucket))
	for ms, vals := range byBucket {
		buckets[time.UnixMilli(ms).UTC()] = alignValues(vals, aligner)
	}
	return &alignedSeries{
		src:     ts,
		kind:    ts.MetricKind,
		valueTy: alignerOutputType(aligner, ts.ValueType),
		buckets: buckets,
	}, nil
}

// alignValues reduces one alignment bucket's values with the aligner.
func alignValues(vals []float64, aligner Aligner) float64 {
	switch aligner {
	case AlignSum:
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum
	case AlignMean:
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case AlignMin:
		return pickExtreme(vals, false)
	case AlignMax:
		return pickExtreme(vals, true)
	case AlignCount:
		return float64(len(vals))
	default:
		// ALIGN_NONE is not reached (alignerSet gates it); fall back to the
		// first value so a single-value bucket is lossless.
		return vals[0]
	}
}

// alignerOutputType returns the value type of an aligned series: COUNT always
// yields INT64, MEAN always yields DOUBLE, and the other aligners preserve the
// input type (defaulting to DOUBLE when the input is unspecified).
func alignerOutputType(aligner Aligner, in int32) int32 {
	switch aligner {
	case AlignCount:
		return valueTypeInt64
	case AlignMean:
		return valueTypeDouble
	default:
		if in == 0 {
			return valueTypeDouble
		}
		return in
	}
}

// reduceSeries groups aligned series by metric type, resource type, and the
// group-by fields, then reduces the buckets sharing a timestamp.
func reduceSeries(aligned []alignedSeries, reducer Reducer, groupByFields []string) ([]monitoringstore.TimeSeries, error) {
	fields := make([]groupByField, 0, len(groupByFields))
	for _, raw := range groupByFields {
		f, err := parseGroupByField(raw)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	groups := map[string][]alignedSeries{}
	order := make([]string, 0, len(aligned))
	for _, a := range aligned {
		var key strings.Builder
		key.WriteString(a.src.MetricType)
		key.WriteByte('|')
		key.WriteString(a.src.ResourceType)
		for _, f := range fields {
			key.WriteByte('|')
			key.WriteString(f.valueOf(a.src))
		}
		k := key.String()
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], a)
	}

	sort.Strings(order)
	out := make([]monitoringstore.TimeSeries, 0, len(groups))
	for _, k := range order {
		members := groups[k]
		first := members[0]

		byTime := map[time.Time][]float64{}
		for _, m := range members {
			for end, v := range m.buckets {
				byTime[end] = append(byTime[end], v)
			}
		}
		buckets := make(map[time.Time]float64, len(byTime))
		for end, vals := range byTime {
			buckets[end] = reduceValues(vals, reducer)
		}

		outSeries := first.src
		outSeries.MetricKind = first.kind
		outSeries.ValueType = reducerOutputType(reducer, first.valueTy)
		outSeries.MetricLabels = map[string]string{}
		outSeries.ResourceLabels = map[string]string{}
		for _, f := range fields {
			f.copyLabel(first.src, outSeries.MetricLabels, outSeries.ResourceLabels)
		}
		outSeries.Points = make([]monitoringstore.Point, 0, len(buckets))
		for _, end := range sortedBucketTimes(buckets) {
			outSeries.Points = append(outSeries.Points, monitoringstore.Point{
				StartTime: end,
				EndTime:   end,
				Value:     typedValueFor(outSeries.ValueType, buckets[end]),
			})
		}
		out = append(out, outSeries)
	}
	return out, nil
}

// reduceValues reduces the per-series values sharing a bucket timestamp.
func reduceValues(vals []float64, reducer Reducer) float64 {
	switch reducer {
	case ReduceSum:
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum
	case ReduceMean:
		var sum float64
		for _, v := range vals {
			sum += v
		}
		return sum / float64(len(vals))
	case ReduceMin:
		return pickExtreme(vals, false)
	case ReduceMax:
		return pickExtreme(vals, true)
	case ReduceCount:
		return float64(len(vals))
	default:
		return vals[0]
	}
}

// reducerOutputType returns the value type of a reduced series.
func reducerOutputType(reducer Reducer, in int32) int32 {
	switch reducer {
	case ReduceCount:
		return valueTypeInt64
	case ReduceMean:
		return valueTypeDouble
	default:
		if in == 0 {
			return valueTypeDouble
		}
		return in
	}
}

// groupByField is a parsed aggregation.group_by_fields entry.
type groupByField struct {
	resourceType bool
	metricLabel  bool
	key          string
}

// parseGroupByField parses a supported group-by field. Cloud Monitoring
// accepts `resource.type`, `resource.labels.<key>` / `resource.label.<key>`,
// and `metric.labels.<key>` / `metric.label.<key>`.
func parseGroupByField(field string) (groupByField, error) {
	parse := func(metricLabel bool, key string) (groupByField, error) {
		if key == "" {
			return groupByField{}, invalidArgument("invalid group_by field: " + field)
		}
		return groupByField{metricLabel: metricLabel, key: key}, nil
	}
	switch {
	case field == "resource.type":
		return groupByField{resourceType: true}, nil
	case strings.HasPrefix(field, "metric.labels."):
		return parse(true, strings.TrimPrefix(field, "metric.labels."))
	case strings.HasPrefix(field, "metric.label."):
		return parse(true, strings.TrimPrefix(field, "metric.label."))
	case strings.HasPrefix(field, "resource.labels."):
		return parse(false, strings.TrimPrefix(field, "resource.labels."))
	case strings.HasPrefix(field, "resource.label."):
		return parse(false, strings.TrimPrefix(field, "resource.label."))
	default:
		return groupByField{}, invalidArgument("unsupported group_by field: " + field)
	}
}

// valueOf returns the field's value in ts, used to build the group key.
func (f groupByField) valueOf(ts monitoringstore.TimeSeries) string {
	switch {
	case f.resourceType:
		return ts.ResourceType
	case f.metricLabel:
		return ts.MetricLabels[f.key]
	default:
		return ts.ResourceLabels[f.key]
	}
}

// copyLabel copies the field from src into the reduced series' label maps.
func (f groupByField) copyLabel(src monitoringstore.TimeSeries, metricLabels, resourceLabels map[string]string) {
	if f.resourceType {
		return
	}
	if f.metricLabel {
		if v, ok := src.MetricLabels[f.key]; ok {
			metricLabels[f.key] = v
		}
		return
	}
	if v, ok := src.ResourceLabels[f.key]; ok {
		resourceLabels[f.key] = v
	}
}

// pointNumericValue extracts the numeric value of a TypedValue for aggregation.
// Distribution and string values are not aggregatable.
func pointNumericValue(v monitoringstore.TypedValue) (float64, bool) {
	switch {
	case v.Int64Value != nil:
		return float64(*v.Int64Value), true
	case v.DoubleValue != nil:
		return *v.DoubleValue, true
	case v.BoolValue != nil:
		if *v.BoolValue {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

// typedValueFor builds a TypedValue of valueType v.
func typedValueFor(valueType int32, v float64) monitoringstore.TypedValue {
	if valueType == valueTypeInt64 {
		n := int64(math.Round(v))
		return monitoringstore.TypedValue{Int64Value: &n}
	}
	return monitoringstore.TypedValue{DoubleValue: &v}
}

// pickExtreme returns the minimum (max=false) or maximum (max=true) value.
func pickExtreme(vals []float64, max bool) float64 {
	out := vals[0]
	for _, v := range vals[1:] {
		if (max && v > out) || (!max && v < out) {
			out = v
		}
	}
	return out
}

// sortedBucketTimes returns the bucket timestamps in ascending order.
func sortedBucketTimes(buckets map[time.Time]float64) []time.Time {
	out := make([]time.Time, 0, len(buckets))
	for t := range buckets {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// ceilToPeriod returns the smallest instant on the Unix-epoch period grid at or
// after t — the end of the alignment window containing t. Cloud Monitoring
// alignment windows are half-open `(start, end]`, so a t that already falls on a
// boundary is returned unchanged (it stays in that boundary's window).
func ceilToPeriod(t time.Time, period time.Duration) time.Time {
	p := period.Milliseconds()
	if p <= 0 {
		return t.UTC()
	}
	ms := t.UnixMilli()
	end := floorDiv(ms, p) * p
	if end != ms {
		end += p
	}
	return time.UnixMilli(end).UTC()
}

// floorDiv returns floor(a/b) for b > 0, matching the mathematical floor rather
// than Go's truncation toward zero (needed when a point predates the Unix epoch,
// making a-b negative).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

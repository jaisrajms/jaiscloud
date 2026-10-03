package monitoring

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"jaiscloud/internal/clock"
	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// queryToParams copies single-valued query parameters into params as strings.
// Repeated parameters keep their slice so list params are not silently dropped.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) == 0 {
			continue
		}
		if len(vs) == 1 {
			params[k] = vs[0]
			continue
		}
		vals := make([]any, 0, len(vs))
		for _, v := range vs {
			vals = append(vals, v)
		}
		params[k] = vals
	}
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

func int64From(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case int64:
		return x
	case int:
		return int64(x)
	default:
		return 0
	}
}

func floatFrom(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	case int64:
		return float64(x)
	default:
		return 0
	}
}

func boolFrom(v any) bool {
	b, _ := v.(bool)
	return b
}

func mapFrom(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func listFrom(v any) []any {
	l, _ := v.([]any)
	return l
}

func strListFrom(v any) []string {
	switch x := v.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	default:
		return nil
	}
}

func stringMapFrom(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

// rawMessage marshals an arbitrary JSON value back to compact JSON, for the
// opaque protojson sub-messages (alert conditions/documentation) the store keeps
// as bytes.
func rawMessage(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

// rawToAny decodes a stored protojson sub-message back into a JSON value for the
// response map. Unparseable bytes fall back to an empty object.
func rawToAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return map[string]any{}
	}
	return v
}

// ─── enum mappings ────────────────────────────────────────────────────────────

var (
	metricKindValue = map[string]int32{
		"METRIC_KIND_UNSPECIFIED": 0, "GAUGE": 1, "DELTA": 2, "CUMULATIVE": 3,
	}
	valueTypeValue = map[string]int32{
		"VALUE_TYPE_UNSPECIFIED": 0, "BOOL": 1, "INT64": 2, "DOUBLE": 3,
		"STRING": 4, "DISTRIBUTION": 5, "MONEY": 6,
	}
	labelValueTypeValue = map[string]int32{
		"STRING": 0, "BOOL": 1, "INT64": 2,
	}
	combinerValue = map[string]int32{
		"COMBINE_UNSPECIFIED": 0, "AND": 1, "OR": 2, "AND_WITH_MATCHING_RESOURCE": 3,
	}
	verificationStatusValue = map[string]int32{
		"VERIFICATION_STATUS_UNSPECIFIED": 0, "UNVERIFIED": 1, "VERIFIED": 2,
	}
)

// enumValue resolves a JSON enum (a protojson name string or a numeric value) to
// its numeric value. An absent/unknown value is 0.
func enumValue(names map[string]int32, v any) int32 {
	switch x := v.(type) {
	case string:
		if n, ok := names[x]; ok {
			return n
		}
	case float64:
		return int32(x)
	}
	return 0
}

// enumName reverses an enum map, returning ok=false for the zero/unknown value
// so callers can omit default-valued fields (protojson semantics).
func enumName(names map[string]int32, v int32) (string, bool) {
	if v == 0 {
		return "", false
	}
	for name, n := range names {
		if n == v {
			return name, true
		}
	}
	return "", false
}

// ─── metric descriptors ───────────────────────────────────────────────────────

func metricDescriptorFromJSON(m map[string]any) monitoringstore.MetricDescriptor {
	d := monitoringstore.MetricDescriptor{
		Type:                   strFrom(m["type"]),
		MetricKind:             enumValue(metricKindValue, m["metricKind"]),
		ValueType:              enumValue(valueTypeValue, m["valueType"]),
		Unit:                   strFrom(m["unit"]),
		Description:            strFrom(m["description"]),
		DisplayName:            strFrom(m["displayName"]),
		MonitoredResourceTypes: strListFrom(m["monitoredResourceTypes"]),
	}
	for _, l := range listFrom(m["labels"]) {
		lm := mapFrom(l)
		d.Labels = append(d.Labels, monitoringstore.LabelDescriptor{
			Key:         strFrom(lm["key"]),
			ValueType:   enumValue(labelValueTypeValue, lm["valueType"]),
			Description: strFrom(lm["description"]),
		})
	}
	return d
}

func metricDescriptorToJSON(d monitoringstore.MetricDescriptor, project string) map[string]any {
	out := map[string]any{
		"name": core.MetricDescriptorName(project, d.Type),
		"type": d.Type,
	}
	if name, ok := enumName(metricKindValue, d.MetricKind); ok {
		out["metricKind"] = name
	}
	if name, ok := enumName(valueTypeValue, d.ValueType); ok {
		out["valueType"] = name
	}
	if d.Unit != "" {
		out["unit"] = d.Unit
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if d.DisplayName != "" {
		out["displayName"] = d.DisplayName
	}
	if len(d.MonitoredResourceTypes) > 0 {
		out["monitoredResourceTypes"] = d.MonitoredResourceTypes
	}
	if len(d.Labels) > 0 {
		labels := make([]any, 0, len(d.Labels))
		for _, l := range d.Labels {
			lm := map[string]any{"key": l.Key}
			if name, ok := enumName(labelValueTypeValue, l.ValueType); ok {
				lm["valueType"] = name
			}
			if l.Description != "" {
				lm["description"] = l.Description
			}
			labels = append(labels, lm)
		}
		out["labels"] = labels
	}
	return out
}

// ─── monitored resource descriptors ───────────────────────────────────────────

func monitoredResourceDescriptorToJSON(d core.MonitoredResourceDescriptor) map[string]any {
	out := map[string]any{
		"name":        d.Name,
		"type":        d.Type,
		"displayName": d.DisplayName,
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if len(d.Labels) > 0 {
		labels := make([]any, 0, len(d.Labels))
		for _, l := range d.Labels {
			lm := map[string]any{"key": l.Key}
			if vt, ok := labelValueTypeJSON(l.ValueType); ok {
				lm["valueType"] = vt
			}
			if l.Description != "" {
				lm["description"] = l.Description
			}
			labels = append(labels, lm)
		}
		out["labels"] = labels
	}
	return out
}

func notificationChannelDescriptorToJSON(d core.NotificationChannelDescriptor) map[string]any {
	out := map[string]any{
		"name":        d.Name,
		"type":        d.Type,
		"displayName": d.DisplayName,
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if len(d.Labels) > 0 {
		labels := make([]any, 0, len(d.Labels))
		for _, l := range d.Labels {
			lm := map[string]any{"key": l.Key}
			if vt, ok := labelValueTypeJSON(l.ValueType); ok {
				lm["valueType"] = vt
			}
			if l.Description != "" {
				lm["description"] = l.Description
			}
			labels = append(labels, lm)
		}
		out["labels"] = labels
	}
	return out
}

// labelValueTypeJSON renders a neutral label value type as the protojson enum
// name, omitting the zero value (STRING) as protojson does.
func labelValueTypeJSON(name string) (string, bool) {
	if name == "" || name == "STRING" {
		return "", false
	}
	return name, true
}

// ─── time series ──────────────────────────────────────────────────────────────

func timeSeriesFromJSON(v any) monitoringstore.TimeSeries {
	m := mapFrom(v)
	ts := monitoringstore.TimeSeries{}
	if metric := mapFrom(m["metric"]); metric != nil {
		ts.MetricType = strFrom(metric["type"])
		ts.MetricLabels = stringMapFrom(metric["labels"])
	}
	if res := mapFrom(m["resource"]); res != nil {
		ts.ResourceType = strFrom(res["type"])
		ts.ResourceLabels = stringMapFrom(res["labels"])
	}
	ts.MetricKind = enumValue(metricKindValue, m["metricKind"])
	ts.ValueType = enumValue(valueTypeValue, m["valueType"])
	ts.Unit = strFrom(m["unit"])
	for _, p := range listFrom(m["points"]) {
		ts.Points = append(ts.Points, pointFromJSON(p))
	}
	return ts
}

func pointFromJSON(v any) monitoringstore.Point {
	m := mapFrom(v)
	pt := monitoringstore.Point{}
	if iv := mapFrom(m["interval"]); iv != nil {
		pt.StartTime = parseTime(strFrom(iv["startTime"]))
		pt.EndTime = parseTime(strFrom(iv["endTime"]))
	}
	if pt.EndTime.IsZero() {
		pt.EndTime = clock.Now()
	}
	pt.Value = typedValueFromJSON(m["value"])
	return pt
}

func timeSeriesToJSON(ts monitoringstore.TimeSeries) map[string]any {
	out := map[string]any{}
	if ts.MetricType != "" || len(ts.MetricLabels) > 0 {
		metric := map[string]any{}
		if ts.MetricType != "" {
			metric["type"] = ts.MetricType
		}
		if len(ts.MetricLabels) > 0 {
			metric["labels"] = ts.MetricLabels
		}
		out["metric"] = metric
	}
	if ts.ResourceType != "" || len(ts.ResourceLabels) > 0 {
		res := map[string]any{}
		if ts.ResourceType != "" {
			res["type"] = ts.ResourceType
		}
		if len(ts.ResourceLabels) > 0 {
			res["labels"] = ts.ResourceLabels
		}
		out["resource"] = res
	}
	if name, ok := enumName(metricKindValue, ts.MetricKind); ok {
		out["metricKind"] = name
	}
	if name, ok := enumName(valueTypeValue, ts.ValueType); ok {
		out["valueType"] = name
	}
	if ts.Unit != "" {
		out["unit"] = ts.Unit
	}
	points := core.SortedPoints(ts.Points)
	if len(points) > 0 {
		ps := make([]any, 0, len(points))
		for _, p := range points {
			ps = append(ps, pointToJSON(p))
		}
		out["points"] = ps
	}
	return out
}

func pointToJSON(p monitoringstore.Point) map[string]any {
	iv := map[string]any{}
	if !p.StartTime.IsZero() {
		iv["startTime"] = p.StartTime.UTC().Format(time.RFC3339Nano)
	}
	if !p.EndTime.IsZero() {
		iv["endTime"] = p.EndTime.UTC().Format(time.RFC3339Nano)
	}
	return map[string]any{"interval": iv, "value": typedValueToJSON(p.Value)}
}

func typedValueFromJSON(v any) monitoringstore.TypedValue {
	switch x := v.(type) {
	case bool:
		return monitoringstore.TypedValue{BoolValue: &x}
	}
	m := mapFrom(v)
	if m == nil {
		return monitoringstore.TypedValue{}
	}
	switch {
	case hasKey(m, "boolValue"):
		b := boolFrom(m["boolValue"])
		return monitoringstore.TypedValue{BoolValue: &b}
	case hasKey(m, "int64Value"):
		n := int64From(m["int64Value"])
		return monitoringstore.TypedValue{Int64Value: &n}
	case hasKey(m, "doubleValue"):
		f := floatFrom(m["doubleValue"])
		return monitoringstore.TypedValue{DoubleValue: &f}
	case hasKey(m, "stringValue"):
		s := strFrom(m["stringValue"])
		return monitoringstore.TypedValue{StringValue: &s}
	case hasKey(m, "distributionValue"):
		d := distributionFromJSON(m["distributionValue"])
		return monitoringstore.TypedValue{DistributionValue: &d}
	}
	return monitoringstore.TypedValue{}
}

func typedValueToJSON(v monitoringstore.TypedValue) map[string]any {
	switch {
	case v.BoolValue != nil:
		return map[string]any{"boolValue": *v.BoolValue}
	case v.Int64Value != nil:
		return map[string]any{"int64Value": strconv.FormatInt(*v.Int64Value, 10)}
	case v.DoubleValue != nil:
		return map[string]any{"doubleValue": *v.DoubleValue}
	case v.StringValue != nil:
		return map[string]any{"stringValue": *v.StringValue}
	case v.DistributionValue != nil:
		return map[string]any{"distributionValue": distributionToJSON(*v.DistributionValue)}
	}
	return map[string]any{}
}

func distributionFromJSON(v any) monitoringstore.Distribution {
	m := mapFrom(v)
	d := monitoringstore.Distribution{
		Count:                 int64From(m["count"]),
		Mean:                  floatFrom(m["mean"]),
		SumOfSquaredDeviation: floatFrom(m["sumOfSquaredDeviation"]),
	}
	if r := mapFrom(m["range"]); r != nil {
		d.Range = &monitoringstore.DistributionRange{Min: floatFrom(r["min"]), Max: floatFrom(r["max"])}
	}
	if bo := mapFrom(m["bucketOptions"]); bo != nil {
		out := &monitoringstore.BucketOptions{}
		if lin := mapFrom(bo["linearBuckets"]); lin != nil {
			out.Linear = &monitoringstore.LinearBuckets{
				NumFiniteBuckets: int32(intFrom(lin["numFiniteBuckets"])),
				Width:            floatFrom(lin["width"]),
				Offset:           floatFrom(lin["offset"]),
			}
		}
		if exp := mapFrom(bo["exponentialBuckets"]); exp != nil {
			out.Exponential = &monitoringstore.ExponentialBuckets{
				NumFiniteBuckets: int32(intFrom(exp["numFiniteBuckets"])),
				GrowthFactor:     floatFrom(exp["growthFactor"]),
				Scale:            floatFrom(exp["scale"]),
			}
		}
		if exp := mapFrom(bo["explicitBuckets"]); exp != nil {
			bounds := []float64{}
			for _, b := range listFrom(exp["bounds"]) {
				bounds = append(bounds, floatFrom(b))
			}
			out.Explicit = &monitoringstore.ExplicitBuckets{Bounds: bounds}
		}
		d.BucketOptions = out
	}
	for _, b := range listFrom(m["bucketCounts"]) {
		d.BucketCounts = append(d.BucketCounts, int64From(b))
	}
	for _, e := range listFrom(m["exemplars"]) {
		em := mapFrom(e)
		ex := monitoringstore.Exemplar{Value: floatFrom(em["value"])}
		if ts := strFrom(em["timestamp"]); ts != "" {
			ex.Timestamp = parseTime(ts)
		}
		// Attachments are protojson Any objects. The store keeps opaque bytes;
		// for the REST path those bytes are the compact JSON form so a REST
		// round-trip is lossless (a gRPC-created Any cannot be reconstructed
		// and is emitted as an empty object).
		for _, a := range listFrom(em["attachments"]) {
			ex.Attachments = append(ex.Attachments, rawMessage(a))
		}
		d.Exemplars = append(d.Exemplars, ex)
	}
	return d
}

func distributionToJSON(d monitoringstore.Distribution) map[string]any {
	out := map[string]any{}
	if d.Count != 0 {
		out["count"] = strconv.FormatInt(d.Count, 10)
	}
	if d.Mean != 0 {
		out["mean"] = d.Mean
	}
	if d.SumOfSquaredDeviation != 0 {
		out["sumOfSquaredDeviation"] = d.SumOfSquaredDeviation
	}
	if d.Range != nil {
		out["range"] = map[string]any{"min": d.Range.Min, "max": d.Range.Max}
	}
	if d.BucketOptions != nil {
		bo := map[string]any{}
		if d.BucketOptions.Linear != nil {
			bo["linearBuckets"] = map[string]any{
				"numFiniteBuckets": d.BucketOptions.Linear.NumFiniteBuckets,
				"width":            d.BucketOptions.Linear.Width,
				"offset":           d.BucketOptions.Linear.Offset,
			}
		}
		if d.BucketOptions.Exponential != nil {
			bo["exponentialBuckets"] = map[string]any{
				"numFiniteBuckets": d.BucketOptions.Exponential.NumFiniteBuckets,
				"growthFactor":     d.BucketOptions.Exponential.GrowthFactor,
				"scale":            d.BucketOptions.Exponential.Scale,
			}
		}
		if d.BucketOptions.Explicit != nil {
			bo["explicitBuckets"] = map[string]any{"bounds": d.BucketOptions.Explicit.Bounds}
		}
		out["bucketOptions"] = bo
	}
	if len(d.BucketCounts) > 0 {
		counts := make([]any, 0, len(d.BucketCounts))
		for _, c := range d.BucketCounts {
			counts = append(counts, strconv.FormatInt(c, 10))
		}
		out["bucketCounts"] = counts
	}
	if len(d.Exemplars) > 0 {
		exs := make([]any, 0, len(d.Exemplars))
		for _, e := range d.Exemplars {
			ex := map[string]any{"value": e.Value}
			if !e.Timestamp.IsZero() {
				ex["timestamp"] = e.Timestamp.UTC().Format(time.RFC3339Nano)
			}
			if len(e.Attachments) > 0 {
				atts := make([]any, 0, len(e.Attachments))
				for _, a := range e.Attachments {
					atts = append(atts, rawToAny(a))
				}
				ex["attachments"] = atts
			}
			exs = append(exs, ex)
		}
		out["exemplars"] = exs
	}
	return out
}

// ─── alert policies ───────────────────────────────────────────────────────────

func alertPolicyFromJSON(body map[string]any) monitoringstore.AlertPolicy {
	p := monitoringstore.AlertPolicy{
		DisplayName:          strFrom(body["displayName"]),
		Combiner:             enumValue(combinerValue, body["combiner"]),
		NotificationChannels: strListFrom(body["notificationChannels"]),
		UserLabels:           stringMapFrom(body["userLabels"]),
	}
	if enabled, ok := body["enabled"].(bool); ok {
		p.Enabled = &enabled
	}
	if doc, ok := body["documentation"]; ok && doc != nil {
		p.Documentation = rawMessage(doc)
	}
	for _, c := range listFrom(body["conditions"]) {
		p.Conditions = append(p.Conditions, rawMessage(c))
	}
	return p
}

func alertPolicyToJSON(p monitoringstore.AlertPolicy, project string) map[string]any {
	out := map[string]any{"name": core.AlertPolicyName(project, p.ID)}
	if p.DisplayName != "" {
		out["displayName"] = p.DisplayName
	}
	if name, ok := enumName(combinerValue, p.Combiner); ok {
		out["combiner"] = name
	}
	if p.Enabled != nil {
		out["enabled"] = *p.Enabled
	}
	if len(p.NotificationChannels) > 0 {
		out["notificationChannels"] = p.NotificationChannels
	}
	if len(p.UserLabels) > 0 {
		out["userLabels"] = p.UserLabels
	}
	if len(p.Documentation) > 0 && string(p.Documentation) != "null" {
		out["documentation"] = rawToAny(p.Documentation)
	}
	if len(p.Conditions) > 0 {
		conds := make([]any, 0, len(p.Conditions))
		for _, c := range p.Conditions {
			conds = append(conds, rawToAny(c))
		}
		out["conditions"] = conds
	}
	return out
}

// ─── service monitoring: services + service level objectives ──────────────────

// serviceIdentifierJSONKeys are the JSON (lowerCamelCase) names of the Service
// identifier oneof. Exactly one may be present in a request body. basicService
// is a separate Service field (a basic service is identified by service type +
// labels instead of the oneof), handled alongside telemetry.
var serviceIdentifierJSONKeys = []string{
	"custom", "appEngine", "cloudEndpoints", "clusterIstio", "meshIstio",
	"istioCanonicalService", "cloudRun", "gkeNamespace", "gkeWorkload",
	"gkeService",
}

var calendarPeriodValue = map[string]int32{
	"CALENDAR_PERIOD_UNSPECIFIED": 0, "DAY": 1, "WEEK": 2, "FORTNIGHT": 3,
	"MONTH": 4, "QUARTER": 5, "HALF": 6, "YEAR": 7,
}

func serviceFromJSON(body map[string]any) monitoringstore.Service {
	svc := monitoringstore.Service{
		DisplayName: strFrom(body["displayName"]),
		UserLabels:  stringMapFrom(body["userLabels"]),
	}
	for _, key := range serviceIdentifierJSONKeys {
		if v, ok := body[key]; ok && v != nil {
			svc.Identifier = rawMessage(map[string]any{key: v})
			break
		}
	}
	if bs, ok := body["basicService"]; ok && bs != nil {
		svc.BasicService = rawMessage(map[string]any{"basicService": bs})
	}
	if tel, ok := body["telemetry"]; ok && tel != nil {
		svc.Telemetry = rawMessage(map[string]any{"telemetry": tel})
	}
	return svc
}

func serviceToJSON(svc monitoringstore.Service, project string) map[string]any {
	out := map[string]any{"name": core.ServiceName(project, svc.ID)}
	if svc.DisplayName != "" {
		out["displayName"] = svc.DisplayName
	}
	if len(svc.UserLabels) > 0 {
		out["userLabels"] = svc.UserLabels
	}
	if m, ok := rawToAny(svc.Identifier).(map[string]any); ok {
		for k, v := range m {
			out[k] = v
		}
	}
	if m, ok := rawToAny(svc.BasicService).(map[string]any); ok {
		if v, ok := m["basicService"]; ok {
			out["basicService"] = v
		}
	}
	if m, ok := rawToAny(svc.Telemetry).(map[string]any); ok {
		if v, ok := m["telemetry"]; ok {
			out["telemetry"] = v
		}
	}
	return out
}

func serviceLevelObjectiveFromJSON(body map[string]any) monitoringstore.ServiceLevelObjective {
	slo := monitoringstore.ServiceLevelObjective{
		DisplayName:    strFrom(body["displayName"]),
		Goal:           floatFrom(body["goal"]),
		UserLabels:     stringMapFrom(body["userLabels"]),
		CalendarPeriod: enumValue(calendarPeriodValue, body["calendarPeriod"]),
	}
	if sli, ok := body["serviceLevelIndicator"]; ok && sli != nil {
		slo.ServiceLevelIndicator = rawMessage(sli)
	}
	if rp := strFrom(body["rollingPeriod"]); rp != "" {
		if d, err := time.ParseDuration(rp); err == nil {
			slo.RollingPeriod = d
		}
	}
	return slo
}

func serviceLevelObjectiveToJSON(slo monitoringstore.ServiceLevelObjective, project string) map[string]any {
	out := map[string]any{"name": core.ServiceLevelObjectiveName(project, slo.ServiceID, slo.ID)}
	if slo.DisplayName != "" {
		out["displayName"] = slo.DisplayName
	}
	if slo.Goal != 0 {
		out["goal"] = slo.Goal
	}
	if len(slo.ServiceLevelIndicator) > 0 {
		out["serviceLevelIndicator"] = rawToAny(slo.ServiceLevelIndicator)
	}
	if slo.RollingPeriod != 0 {
		out["rollingPeriod"] = durationToJSON(slo.RollingPeriod)
	}
	if name, ok := enumName(calendarPeriodValue, slo.CalendarPeriod); ok {
		out["calendarPeriod"] = name
	}
	if len(slo.UserLabels) > 0 {
		out["userLabels"] = slo.UserLabels
	}
	return out
}

// durationToJSON renders a duration the way protojson/discovery do: decimal
// seconds with an "s" suffix (e.g. "300s" or "1.500s").
func durationToJSON(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

// ─── notification channels ────────────────────────────────────────────────────

func notificationChannelFromJSON(body map[string]any) monitoringstore.NotificationChannel {
	c := monitoringstore.NotificationChannel{
		Type:               strFrom(body["type"]),
		DisplayName:        strFrom(body["displayName"]),
		Description:        strFrom(body["description"]),
		Labels:             stringMapFrom(body["labels"]),
		UserLabels:         stringMapFrom(body["userLabels"]),
		VerificationStatus: enumValue(verificationStatusValue, body["verificationStatus"]),
	}
	if enabled, ok := body["enabled"].(bool); ok {
		c.Enabled = &enabled
	}
	return c
}

func notificationChannelToJSON(c monitoringstore.NotificationChannel, project string) map[string]any {
	out := map[string]any{"name": core.NotificationChannelName(project, c.ID)}
	if c.Type != "" {
		out["type"] = c.Type
	}
	if c.DisplayName != "" {
		out["displayName"] = c.DisplayName
	}
	if c.Description != "" {
		out["description"] = c.Description
	}
	if len(c.Labels) > 0 {
		out["labels"] = c.Labels
	}
	if len(c.UserLabels) > 0 {
		out["userLabels"] = c.UserLabels
	}
	if c.Enabled != nil {
		out["enabled"] = *c.Enabled
	}
	if name, ok := enumName(verificationStatusValue, c.VerificationStatus); ok {
		out["verificationStatus"] = name
	}
	if !c.CreateTime.IsZero() {
		out["creationRecord"] = map[string]any{"mutateTime": c.CreateTime.UTC().Format(time.RFC3339Nano)}
	}
	if !c.UpdateTime.IsZero() {
		out["mutationRecords"] = []any{map[string]any{"mutateTime": c.UpdateTime.UTC().Format(time.RFC3339Nano)}}
	}
	return out
}

// ─── misc ─────────────────────────────────────────────────────────────────────

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// aggregationFromParams parses the REST query parameters that mirror the proto
// google.monitoring.v3.Aggregation: {prefix}.alignmentPeriod,
// {prefix}.perSeriesAligner, {prefix}.crossSeriesReducer, and the repeated
// {prefix}.groupByFields. It returns nil when no such parameter is present.
// prefix is "aggregation" for the primary aggregation or "secondaryAggregation"
// for the second pass (J43).
func aggregationFromParamsPrefix(nr *model.NormalizedRequest, prefix string) (*core.Aggregation, error) {
	period := strParam(nr, prefix+".alignmentPeriod")
	aligner := strParam(nr, prefix+".perSeriesAligner")
	reducer := strParam(nr, prefix+".crossSeriesReducer")
	groupBy := strListFrom(nr.Params[prefix+".groupByFields"])
	if period == "" && aligner == "" && reducer == "" && len(groupBy) == 0 {
		return nil, nil
	}
	agg := &core.Aggregation{
		PerSeriesAligner:   core.Aligner(aligner),
		CrossSeriesReducer: core.Reducer(reducer),
		GroupByFields:      groupBy,
	}
	if period != "" {
		d, err := time.ParseDuration(period)
		if err != nil {
			return nil, invalidArgument("invalid " + prefix + ".alignmentPeriod: " + period)
		}
		agg.AlignmentPeriod = d
	}
	return agg, nil
}

// aggregationFromParams parses the primary aggregation.* query parameters.
func aggregationFromParams(nr *model.NormalizedRequest) (*core.Aggregation, error) {
	return aggregationFromParamsPrefix(nr, "aggregation")
}

// secondaryAggregationFromParams parses the secondaryAggregation.* query
// parameters (the second aggregation applied after the primary one).
func secondaryAggregationFromParams(nr *model.NormalizedRequest) (*core.Aggregation, error) {
	return aggregationFromParamsPrefix(nr, "secondaryAggregation")
}

// updateMaskFromQuery parses the repeated/comma-separated updateMask query
// parameter into field paths.
func updateMaskFromQuery(v any) []string {
	var raw []string
	switch x := v.(type) {
	case string:
		raw = []string{x}
	case []any:
		for _, e := range x {
			if s, ok := e.(string); ok {
				raw = append(raw, s)
			}
		}
	}
	var out []string
	for _, entry := range raw {
		for _, p := range splitComma(entry) {
			if p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

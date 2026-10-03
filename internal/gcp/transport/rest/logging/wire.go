package logging

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
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
// Repeated parameters keep their slice so list params (e.g. resourceNames) are
// not silently dropped.
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

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// strListFrom converts a JSON array of strings into a []string, tolerating a
// single string value (the REST client may send either).
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

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case string:
		var n int
		for i := 0; i < len(x); i++ {
			if x[i] < '0' || x[i] > '9' {
				return 0
			}
		}
		for i := 0; i < len(x); i++ {
			n = n*10 + int(x[i]-'0')
		}
		return n
	default:
		return 0
	}
}

func boolFrom(v any) bool {
	b, _ := v.(bool)
	return b
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

// ─── entry transcoding ────────────────────────────────────────────────────────

// entryFromWire decodes a JSON LogEntry into the neutral stored form. Only the
// text and JSON payload variants are supported (protoPayload is ignored, which
// the emulator's gRPC surface does not implement either).
func entryFromWire(v any) loggingstore.LogEntry {
	var e loggingstore.LogEntry
	m, ok := v.(map[string]any)
	if !ok {
		return e
	}
	e.LogName = strFrom(m["logName"])
	e.InsertID = strFrom(m["insertId"])
	e.Labels = stringMapFrom(m["labels"])
	if res, ok := m["resource"].(map[string]any); ok {
		e.ResourceType = strFrom(res["type"])
		e.ResourceLabels = stringMapFrom(res["labels"])
	}
	switch sv := m["severity"].(type) {
	case string:
		if n, ok := core.SeverityValue(sv); ok {
			e.Severity = n
		}
	case float64:
		e.Severity = int(sv)
	}
	if ts := strFrom(m["timestamp"]); ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			e.Timestamp = t
		}
	}
	switch {
	case hasKey(m, "textPayload"):
		e.PayloadType = "text"
		e.TextPayload = strFrom(m["textPayload"])
	case hasKey(m, "jsonPayload"):
		e.PayloadType = "json"
		if jp, ok := m["jsonPayload"].(map[string]any); ok {
			e.JsonPayload = jp
		}
	}
	return e
}

// entryToWire encodes a neutral stored entry as the Discovery LogEntry JSON.
// Default-valued fields (zero severity, zero timestamp, empty payload) are
// omitted, matching protojson.
func entryToWire(e loggingstore.LogEntry) map[string]any {
	out := map[string]any{}
	if e.LogName != "" {
		out["logName"] = e.LogName
	}
	if e.ResourceType != "" || len(e.ResourceLabels) > 0 {
		res := map[string]any{}
		if e.ResourceType != "" {
			res["type"] = e.ResourceType
		}
		if len(e.ResourceLabels) > 0 {
			res["labels"] = e.ResourceLabels
		}
		out["resource"] = res
	}
	if !e.Timestamp.IsZero() {
		out["timestamp"] = e.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if e.Severity != 0 {
		out["severity"] = core.SeverityName(e.Severity)
	}
	switch e.PayloadType {
	case "json":
		if len(e.JsonPayload) > 0 {
			out["jsonPayload"] = e.JsonPayload
		}
	case "text":
		out["textPayload"] = e.TextPayload
	}
	if e.InsertID != "" {
		out["insertId"] = e.InsertID
	}
	if len(e.Labels) > 0 {
		out["labels"] = e.Labels
	}
	return out
}

// ─── sink / exclusion transcoding ─────────────────────────────────────────────

// intParam reads a signed integer request parameter that may arrive as a JSON
// number (body) or a query string. Unlike intFrom it preserves a leading minus,
// so an out-of-range pageSize (e.g. -1) reaches the core's validation and is
// rejected consistently with the gRPC transport.
func intParam(nr *model.NormalizedRequest, key string) int {
	switch v := nr.Params[key].(type) {
	case float64:
		return int(v)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

// boolParam reads a boolean request parameter that may arrive as a JSON bool
// (body) or a query string ("true"/"1").
func boolParam(nr *model.NormalizedRequest, key string) bool {
	switch v := nr.Params[key].(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	default:
		return false
	}
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

// sinkFromWire decodes a Discovery LogSink body into the neutral stored form.
// The client-assigned name is the short sink id. Output-only fields
// (resourceName, writerIdentity, createTime, updateTime) are ignored.
func sinkFromWire(v map[string]any) loggingstore.LogSink {
	s := loggingstore.LogSink{
		Name:            strFrom(v["name"]),
		Destination:     strFrom(v["destination"]),
		Filter:          strFrom(v["filter"]),
		Description:     strFrom(v["description"]),
		Disabled:        boolFrom(v["disabled"]),
		IncludeChildren: boolFrom(v["includeChildren"]),
	}
	if arr, ok := v["exclusions"].([]any); ok {
		for _, e := range arr {
			if m, ok := e.(map[string]any); ok {
				s.Exclusions = append(s.Exclusions, exclusionFromWire(m))
			}
		}
	}
	return s
}

// sinkToWire encodes a stored sink as the Discovery LogSink JSON. fullName is
// the sink's full resource name, emitted as the output-only resourceName (the
// name field stays the short client-assigned id, per the Discovery document).
func sinkToWire(s loggingstore.LogSink, fullName string) map[string]any {
	out := map[string]any{}
	if s.Name != "" {
		out["name"] = s.Name
	}
	if fullName != "" {
		out["resourceName"] = fullName
	}
	if s.Destination != "" {
		out["destination"] = s.Destination
	}
	if s.Filter != "" {
		out["filter"] = s.Filter
	}
	if s.Description != "" {
		out["description"] = s.Description
	}
	if s.Disabled {
		out["disabled"] = true
	}
	if len(s.Exclusions) > 0 {
		exclusions := make([]any, 0, len(s.Exclusions))
		for _, e := range s.Exclusions {
			exclusions = append(exclusions, exclusionToWire(e))
		}
		out["exclusions"] = exclusions
	}
	if s.WriterIdentity != "" {
		out["writerIdentity"] = s.WriterIdentity
	}
	if s.IncludeChildren {
		out["includeChildren"] = true
	}
	if !s.CreateTime.IsZero() {
		out["createTime"] = s.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	if !s.UpdateTime.IsZero() {
		out["updateTime"] = s.UpdateTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// exclusionFromWire decodes a Discovery LogExclusion body into the neutral
// stored form. Output-only timestamps are ignored.
func exclusionFromWire(v map[string]any) loggingstore.LogExclusion {
	return loggingstore.LogExclusion{
		Name:        strFrom(v["name"]),
		Description: strFrom(v["description"]),
		Filter:      strFrom(v["filter"]),
		Disabled:    boolFrom(v["disabled"]),
	}
}

// exclusionToWire encodes a stored exclusion as the Discovery LogExclusion
// JSON. The name is the short client-assigned id (the Discovery document
// defines name that way for exclusions; there is no resourceName field).
func exclusionToWire(e loggingstore.LogExclusion) map[string]any {
	out := map[string]any{}
	if e.Name != "" {
		out["name"] = e.Name
	}
	if e.Description != "" {
		out["description"] = e.Description
	}
	if e.Filter != "" {
		out["filter"] = e.Filter
	}
	if e.Disabled {
		out["disabled"] = true
	}
	if !e.CreateTime.IsZero() {
		out["createTime"] = e.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	if !e.UpdateTime.IsZero() {
		out["updateTime"] = e.UpdateTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// ─── metric transcoding ───────────────────────────────────────────────────────

// metricFromWire decodes a Discovery LogMetric body into the neutral stored form.
// Output-only fields (resourceName, metricDescriptor.name/type/description,
// createTime, updateTime) are ignored.
func metricFromWire(v map[string]any) loggingstore.LogMetric {
	m := loggingstore.LogMetric{
		Name:            strFrom(v["name"]),
		Description:     strFrom(v["description"]),
		Filter:          strFrom(v["filter"]),
		Disabled:        boolFrom(v["disabled"]),
		BucketName:      strFrom(v["bucketName"]),
		ValueExtractor:  strFrom(v["valueExtractor"]),
		LabelExtractors: stringMapFrom(v["labelExtractors"]),
	}
	if bo, ok := v["bucketOptions"]; ok && bo != nil {
		if raw, err := json.Marshal(bo); err == nil {
			m.BucketOptions = raw
		}
	}
	if md, ok := v["metricDescriptor"].(map[string]any); ok {
		m.Descriptor = metricDescriptorFromWire(md)
	}
	return m
}

func metricDescriptorFromWire(md map[string]any) loggingstore.LogMetricDescriptor {
	d := loggingstore.LogMetricDescriptor{
		MetricKind:  strFrom(md["metricKind"]),
		ValueType:   strFrom(md["valueType"]),
		Unit:        strFrom(md["unit"]),
		DisplayName: strFrom(md["displayName"]),
	}
	if arr, ok := md["labels"].([]any); ok {
		for _, l := range arr {
			if lm, ok := l.(map[string]any); ok {
				d.Labels = append(d.Labels, loggingstore.LogMetricLabel{
					Key:         strFrom(lm["key"]),
					ValueType:   strFrom(lm["valueType"]),
					Description: strFrom(lm["description"]),
				})
			}
		}
	}
	return d
}

// metricToWire encodes a stored metric as the Discovery LogMetric JSON. The
// metric's descriptor must already carry its synthesized output fields (the core
// fills them). The metric id is the short client-assigned name; the full name is
// emitted as the output-only resourceName.
func metricToWire(scopeParent string, m loggingstore.LogMetric) map[string]any {
	out := map[string]any{}
	if m.Name != "" {
		out["name"] = m.Name
	}
	if scopeParent != "" {
		out["resourceName"] = core.MetricResourceName(scopeParent, m.Name)
	}
	if m.Description != "" {
		out["description"] = m.Description
	}
	if m.Filter != "" {
		out["filter"] = m.Filter
	}
	if m.Disabled {
		out["disabled"] = true
	}
	if m.BucketName != "" {
		out["bucketName"] = m.BucketName
	}
	if m.ValueExtractor != "" {
		out["valueExtractor"] = m.ValueExtractor
	}
	if len(m.LabelExtractors) > 0 {
		out["labelExtractors"] = m.LabelExtractors
	}
	if len(m.BucketOptions) > 0 {
		var bo any
		if err := json.Unmarshal(m.BucketOptions, &bo); err == nil {
			out["bucketOptions"] = bo
		}
	}
	out["metricDescriptor"] = metricDescriptorToWire(m.Descriptor)
	if !m.CreateTime.IsZero() {
		out["createTime"] = m.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	if !m.UpdateTime.IsZero() {
		out["updateTime"] = m.UpdateTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func metricDescriptorToWire(d loggingstore.LogMetricDescriptor) map[string]any {
	out := map[string]any{}
	if d.Name != "" {
		out["name"] = d.Name
	}
	if d.Type != "" {
		out["type"] = d.Type
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if d.Unit != "" {
		out["unit"] = d.Unit
	}
	if d.MetricKind != "" {
		out["metricKind"] = d.MetricKind
	}
	if d.ValueType != "" {
		out["valueType"] = d.ValueType
	}
	if d.DisplayName != "" {
		out["displayName"] = d.DisplayName
	}
	if len(d.Labels) > 0 {
		labels := make([]any, 0, len(d.Labels))
		for _, l := range d.Labels {
			labels = append(labels, map[string]any{
				"key":         l.Key,
				"valueType":   l.ValueType,
				"description": l.Description,
			})
		}
		out["labels"] = labels
	}
	return out
}

// descriptorToWire encodes a neutral monitored resource descriptor as the
// Discovery MonitoredResourceDescriptor JSON. The Logging surface leaves `name`
// unset.
func descriptorToWire(d core.MonitoredResourceDescriptor) map[string]any {
	labels := make([]any, 0, len(d.Labels))
	for _, l := range d.Labels {
		labels = append(labels, map[string]any{
			"key":         l.Key,
			"valueType":   l.ValueType,
			"description": l.Description,
		})
	}
	out := map[string]any{
		"type":        d.Type,
		"displayName": d.DisplayName,
	}
	if d.Description != "" {
		out["description"] = d.Description
	}
	if len(labels) > 0 {
		out["labels"] = labels
	}
	return out
}

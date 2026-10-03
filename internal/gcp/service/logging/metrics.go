package logging

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"jaiscloud/internal/clock"
	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// This file adds Cloud Logging's logs-based metrics (the MetricsServiceV2 /
// projects.metrics.* config plane) to the transport-neutral core. A logs-based
// metric counts (or extracts a distribution from) log entries matching a filter.
// The emulator stores the metric definition and round-trips it; it does not
// compute metric time series (there is no Monitoring time-series writer wired to
// the logging write path).
//
// Resource names follow the Logging v2 form {scope}/{scopeID}/metrics/{METRIC_ID}
// where METRIC_ID may itself contain slashes (e.g. "nginx/requests"), percent-
// encoded in the canonical resource name. The store keys a metric by its decoded
// id within the canonical scope parent.

const (
	// metricTypePrefix is the DNS prefix real Cloud Logging assigns to the
	// output-only MetricDescriptor.type of a user logs-based metric.
	metricTypePrefix = "logging.googleapis.com/user/"

	maxMetricIDLen      = 100
	maxMetricFilterLen  = 20000
	maxMetricDescLen    = 8000
	defaultMetricUnit   = "1"
	defaultMetricKind   = "DELTA"
	defaultMetricValue  = "INT64"
	defaultLabelValue   = "STRING"
	metricDescriptorSeg = "/metricDescriptors/"
)

// ─── resource names ───────────────────────────────────────────────────────────

// MetricResourceName builds the full metric resource name from a scope parent
// and a decoded metric id. The id is percent-encoded so ids containing slashes
// render as the canonical single path segment (e.g. "nginx%2Frequests").
func MetricResourceName(scopeParent, id string) string {
	return scopeParent + "/metrics/" + url.PathEscape(id)
}

// MetricDescriptorType returns the output-only DNS type of a metric's descriptor,
// constructed from the metric id per the Cloud Logging contract
// ("logging.googleapis.com/user/{METRIC_ID}").
func MetricDescriptorType(metricID string) string { return metricTypePrefix + metricID }

// MetricDescriptorName returns the output-only full resource name of a metric's
// descriptor. The type contains slashes and, per the MetricDescriptor schema, is
// not URL-encoded.
func MetricDescriptorName(scopeParent, metricID string) string {
	return scopeParent + metricDescriptorSeg + MetricDescriptorType(metricID)
}

// ParseMetricName parses a logs-based metric resource name of the form
// {scope}/{scopeID}/metrics/{METRIC_ID}, where the id may contain slashes (raw
// or percent-encoded). It returns the canonical two-segment scope parent and the
// decoded id. Malformed names (unknown scope, empty segments, a missing "metrics"
// segment, an empty/invalid id, or a bad percent-escape) return InvalidArgument.
func ParseMetricName(name string) (scopeParent, id string, err error) {
	trimmed := strings.TrimPrefix(name, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 4 || parts[2] != "metrics" {
		return "", "", invalidMetricName(name)
	}
	if _, ok := logScopes[parts[0]]; !ok || parts[1] == "" {
		return "", "", invalidMetricName(name)
	}
	decoded, derr := url.PathUnescape(strings.Join(parts[3:], "/"))
	if derr != nil || !validMetricID(decoded) {
		return "", "", invalidMetricName(name)
	}
	return parts[0] + "/" + parts[1], decoded, nil
}

func invalidMetricName(name string) error {
	return invalidArgument("invalid metric name: " + name)
}

// validMetricID reports whether id is a legal logs-based metric identifier:
// 1–100 characters from [A-Za-z0-9_-.,+!*'()%/] with a non-slash first
// character. The slash denotes a name hierarchy and cannot lead.
func validMetricID(id string) bool {
	if id == "" || len(id) > maxMetricIDLen || id[0] == '/' {
		return false
	}
	for i := 0; i < len(id); i++ {
		switch c := id[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '-' || c == '.' || c == ',' || c == '+' || c == '!' ||
			c == '*' || c == '\'' || c == '(' || c == ')' || c == '%' || c == '/':
		default:
			return false
		}
	}
	return true
}

// normalizeMetricID accepts either a bare metric id or a full resource name and
// returns the decoded id. A bare id keeps its exact form (so a leading slash is
// rejected by validation rather than silently trimmed).
func normalizeMetricID(name string) string {
	if strings.Contains(strings.TrimPrefix(name, "/"), "/metrics/") {
		trimmed := strings.TrimPrefix(name, "/")
		if _, id, err := ParseMetricName(trimmed); err == nil {
			return id
		}
	}
	if decoded, err := url.PathUnescape(name); err == nil {
		return decoded
	}
	return name
}

// ─── descriptors ──────────────────────────────────────────────────────────────

// SynthesizeMetricDescriptor applies defaults to a stored descriptor and fills
// its output-only name/type/description. The descriptor's type is derived from
// the metric id and its description from the metric description, exactly as real
// Cloud Logging constructs them.
func SynthesizeMetricDescriptor(scopeParent string, m loggingstore.LogMetric) loggingstore.LogMetricDescriptor {
	d := descriptorWithDefaults(m.Descriptor)
	d.Type = MetricDescriptorType(m.Name)
	d.Name = MetricDescriptorName(scopeParent, m.Name)
	d.Description = m.Description
	return d
}

// normalizeMetricDescriptor applies field defaults and clears the output-only
// name/type/description so a stored descriptor carries configuration only.
func normalizeMetricDescriptor(m loggingstore.LogMetric) loggingstore.LogMetric {
	d := descriptorWithDefaults(m.Descriptor)
	d.Name, d.Type, d.Description = "", "", ""
	m.Descriptor = d
	return m
}

// descriptorWithDefaults fills the configurable descriptor defaults (metric
// kind, value type, unit, label value types). Shared by the write-path
// normalizer and the read-path synthesizer so the two cannot drift.
func descriptorWithDefaults(d loggingstore.LogMetricDescriptor) loggingstore.LogMetricDescriptor {
	d.MetricKind = defaultMetricKindValue(d.MetricKind)
	d.ValueType = defaultMetricValueType(d.ValueType)
	if d.Unit == "" {
		d.Unit = defaultMetricUnit
	}
	for i := range d.Labels {
		if d.Labels[i].ValueType == "" {
			d.Labels[i].ValueType = defaultLabelValue
		}
	}
	return d
}

func defaultMetricKindValue(kind string) string {
	if kind == "" || kind == "METRIC_KIND_UNSPECIFIED" {
		return defaultMetricKind
	}
	return kind
}

func defaultMetricValueType(vt string) string {
	if vt == "" || vt == "VALUE_TYPE_UNSPECIFIED" {
		return defaultMetricValue
	}
	return vt
}

// ─── validation ───────────────────────────────────────────────────────────────

func validateMetric(m loggingstore.LogMetric) error {
	if !validMetricID(m.Name) {
		return invalidArgument("invalid metric name: " + m.Name)
	}
	if strings.TrimSpace(m.Filter) == "" {
		return invalidArgument("metric filter is required")
	}
	if len(m.Filter) > maxMetricFilterLen {
		return invalidArgument("metric filter exceeds the 20000-character limit")
	}
	if _, err := CompileFilter(m.Filter); err != nil {
		return invalidArgument("invalid metric filter: " + err.Error())
	}
	if len(m.Description) > maxMetricDescLen {
		return invalidArgument("metric description exceeds the 8000-character limit")
	}
	d := m.Descriptor
	switch d.MetricKind {
	case "GAUGE", "DELTA", "CUMULATIVE":
	default:
		return invalidArgument("invalid metric_descriptor.metric_kind: " + d.MetricKind)
	}
	switch d.ValueType {
	case "BOOL", "INT64", "DOUBLE", "STRING", "DISTRIBUTION", "MONEY":
	default:
		return invalidArgument("invalid metric_descriptor.value_type: " + d.ValueType)
	}
	if d.ValueType == "DISTRIBUTION" && strings.TrimSpace(m.ValueExtractor) == "" {
		return invalidArgument("value_extractor is required for a DISTRIBUTION metric")
	}
	keys := make(map[string]struct{}, len(d.Labels))
	for _, l := range d.Labels {
		if l.Key == "" {
			return invalidArgument("metric_descriptor label key is required")
		}
		if _, dup := keys[l.Key]; dup {
			return invalidArgument("duplicate metric_descriptor label: " + l.Key)
		}
		switch l.ValueType {
		case "STRING", "BOOL", "INT64":
		default:
			return invalidArgument("invalid value_type for label " + l.Key + ": " + l.ValueType)
		}
		keys[l.Key] = struct{}{}
	}
	for k := range m.LabelExtractors {
		if _, ok := keys[k]; !ok {
			return invalidArgument("label_extractors references an undefined label: " + k)
		}
	}
	for k := range keys {
		if _, ok := m.LabelExtractors[k]; !ok {
			return invalidArgument("metric_descriptor label has no extractor: " + k)
		}
	}
	return nil
}

// mergeMetricDescriptor applies an update's descriptor onto the stored one,
// enforcing the Cloud Logging invariants: metric_kind and value_type are
// immutable, and existing labels may only change their description (new labels
// may be added, but existing ones cannot be removed or retyped).
func mergeMetricDescriptor(stored, incoming loggingstore.LogMetricDescriptor) (loggingstore.LogMetricDescriptor, error) {
	if stored.MetricKind != incoming.MetricKind {
		return stored, invalidArgument("metric_descriptor.metric_kind cannot be changed")
	}
	if stored.ValueType != incoming.ValueType {
		return stored, invalidArgument("metric_descriptor.value_type cannot be changed")
	}
	merged := append([]loggingstore.LogMetricLabel(nil), stored.Labels...)
	index := make(map[string]int, len(merged))
	for i, l := range merged {
		index[l.Key] = i
	}
	for _, in := range incoming.Labels {
		if j, ok := index[in.Key]; ok {
			if merged[j].ValueType != in.ValueType {
				return stored, invalidArgument("value_type of existing label " + in.Key + " cannot be changed")
			}
			merged[j].Description = in.Description
			continue
		}
		merged = append(merged, in)
		index[in.Key] = len(merged) - 1
	}
	incoming.Labels = merged
	return incoming, nil
}

// ─── Service methods ──────────────────────────────────────────────────────────

// CreateMetric adds a logs-based metric under parent (a scope parent such as
// "projects/p"). in.Name is the client-assigned metric id; when empty or
// malformed the request is rejected.
func (s *Service) CreateMetric(ctx context.Context, parent string, in loggingstore.LogMetric) (loggingstore.LogMetric, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return loggingstore.LogMetric{}, err
	}
	return s.createMetric(ctx, scope, normalizeMetricID(in.Name), in)
}

// createMetric stores a new metric in an already-resolved scope, normalizing and
// validating it and assigning the server timestamps. It is shared by CreateMetric
// and the create arm of UpdateMetric (which the API defines as an upsert).
func (s *Service) createMetric(ctx context.Context, scope, id string, in loggingstore.LogMetric) (loggingstore.LogMetric, error) {
	in.Name = id
	in = normalizeMetricDescriptor(in)
	if err := validateMetric(in); err != nil {
		return loggingstore.LogMetric{}, err
	}
	now := clock.Now()
	in.CreateTime = now
	in.UpdateTime = now
	if err := s.store.CreateMetric(ctx, scope, in); err != nil {
		return loggingstore.LogMetric{}, mapConfigStoreError(err)
	}
	return metricView(scope, in), nil
}

// GetMetric fetches a metric by full resource name.
func (s *Service) GetMetric(ctx context.Context, name string) (loggingstore.LogMetric, error) {
	scope, id, err := ParseMetricName(name)
	if err != nil {
		return loggingstore.LogMetric{}, err
	}
	m, err := s.store.GetMetric(ctx, scope, id)
	if err != nil {
		return loggingstore.LogMetric{}, mapConfigStoreError(err)
	}
	return metricView(scope, m), nil
}

// ListMetrics returns a page of the parent's metrics ordered by name.
func (s *Service) ListMetrics(ctx context.Context, parent string, pageSize int, pageToken string) ([]loggingstore.LogMetric, string, error) {
	scope, err := ParseScopeParent(parent)
	if err != nil {
		return nil, "", err
	}
	list, err := s.store.ListMetrics(ctx, scope)
	if err != nil {
		return nil, "", err
	}
	// The metrics list contract treats a non-positive page size as unset.
	if pageSize < 0 {
		pageSize = 0
	}
	page, next, err := paginateConfig(list, pageSize, pageToken)
	if err != nil {
		return nil, "", err
	}
	out := make([]loggingstore.LogMetric, 0, len(page))
	for _, m := range page {
		out = append(out, metricView(scope, m))
	}
	return out, next, nil
}

// UpdateMetric replaces a metric by full resource name. There is no update mask
// in the wire API: the mutable fields are replaced, while metric_kind/value_type
// and the identity of existing labels are enforced immutable.
func (s *Service) UpdateMetric(ctx context.Context, name string, in loggingstore.LogMetric) (loggingstore.LogMetric, error) {
	scope, id, err := ParseMetricName(name)
	if err != nil {
		return loggingstore.LogMetric{}, err
	}
	if in.Name != "" && normalizeMetricID(in.Name) != id {
		return loggingstore.LogMetric{}, invalidArgument("metric name in the request does not match the resource name")
	}
	stored, err := s.store.GetMetric(ctx, scope, id)
	if errors.Is(err, loggingstore.ErrMetricNotFound) {
		// Real Logging defines metrics.update as create-or-update, so an update
		// against an absent metric creates it.
		return s.createMetric(ctx, scope, id, in)
	}
	if err != nil {
		return loggingstore.LogMetric{}, mapConfigStoreError(err)
	}
	in.Name = id
	// Preserve immutable descriptor fields the client omitted: a full-replace
	// update still cannot change metric_kind/value_type, but an omitted value
	// must not be read as a change to the DELTA/INT64 defaults.
	rawKind, rawValueType := in.Descriptor.MetricKind, in.Descriptor.ValueType
	in = normalizeMetricDescriptor(in)
	if rawKind == "" || rawKind == "METRIC_KIND_UNSPECIFIED" {
		in.Descriptor.MetricKind = stored.Descriptor.MetricKind
	}
	if rawValueType == "" || rawValueType == "VALUE_TYPE_UNSPECIFIED" {
		in.Descriptor.ValueType = stored.Descriptor.ValueType
	}
	desc, merr := mergeMetricDescriptor(stored.Descriptor, in.Descriptor)
	if merr != nil {
		return loggingstore.LogMetric{}, merr
	}
	in.Descriptor = desc
	if err := validateMetric(in); err != nil {
		return loggingstore.LogMetric{}, err
	}
	in.CreateTime = stored.CreateTime
	in.UpdateTime = clock.Now()
	if err := s.store.UpdateMetric(ctx, scope, in); err != nil {
		return loggingstore.LogMetric{}, mapConfigStoreError(err)
	}
	return metricView(scope, in), nil
}

// DeleteMetric removes a metric by full resource name.
func (s *Service) DeleteMetric(ctx context.Context, name string) error {
	scope, id, err := ParseMetricName(name)
	if err != nil {
		return err
	}
	return mapConfigStoreError(s.store.DeleteMetric(ctx, scope, id))
}

// metricView returns the stored metric with its output-only descriptor fields
// synthesized for scopeParent.
func metricView(scopeParent string, m loggingstore.LogMetric) loggingstore.LogMetric {
	m.Descriptor = SynthesizeMetricDescriptor(scopeParent, m)
	return m
}

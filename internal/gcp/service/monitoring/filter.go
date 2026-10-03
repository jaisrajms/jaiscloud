package monitoring

import (
	"fmt"
	"regexp"
	"strings"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

// tsFilter is the subset of the Cloud Monitoring filter grammar that
// ListTimeSeries supports: a conjunction (AND) of predicates over the metric
// type, the resource type, and metric/resource labels. Type and label
// predicates accept equality or starts_with; anything else fails closed with an
// error rather than silently matching everything.
type tsFilter struct {
	metricType         string
	metricTypePrefix   bool
	resourceType       string
	resourceTypePrefix bool
	metricLabels       map[string]labelMatcher
	resourceLabels     map[string]labelMatcher
}

// labelMatcher is one parsed metric.labels.<key> / resource.labels.<key>
// predicate: an equality or a starts_with on the label value.
type labelMatcher struct {
	value  string
	prefix bool
}

// tsFilterAnd splits a filter into clauses on a case-insensitive AND operator.
var tsFilterAnd = regexp.MustCompile(`(?i)\s+AND\s+`)

// compileTSFilter parses a minimal ListTimeSeries filter of the form
//
//	metric.type = "custom.googleapis.com/foo" AND metric.labels.env = "production"
//	resource.type = starts_with("gce_")
//
// Type and label clauses are optional individually, but the filter string must
// be non-empty and every clause must be a supported equality or starts_with.
func compileTSFilter(s string) (tsFilter, error) {
	var f tsFilter
	s = strings.TrimSpace(s)
	if s == "" {
		return f, fmt.Errorf("filter must not be empty")
	}
	for _, raw := range tsFilterAnd.Split(s, -1) {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return f, fmt.Errorf("empty filter clause")
		}
		if err := f.addClause(clause); err != nil {
			return tsFilter{}, err
		}
	}
	return f, nil
}

// addClause parses one `key = "value"` or `key = starts_with("prefix")` clause
// into f. Unknown keys and unsupported operators are rejected.
func (f *tsFilter) addClause(clause string) error {
	key, rhs, found := strings.Cut(clause, "=")
	if !found {
		return fmt.Errorf("unsupported filter clause %q", clause)
	}
	key = strings.TrimSpace(key)
	val, prefix, ok := parseClauseValue(strings.TrimSpace(rhs))
	if !ok {
		return fmt.Errorf("unsupported filter clause %q (only equality and starts_with are supported)", clause)
	}
	if val == "" {
		return fmt.Errorf("empty filter value in clause %q", clause)
	}
	switch {
	case key == "metric.type":
		f.metricType, f.metricTypePrefix = val, prefix
	case key == "resource.type":
		f.resourceType, f.resourceTypePrefix = val, prefix
	case strings.HasPrefix(key, "metric.labels."):
		lk := strings.TrimPrefix(key, "metric.labels.")
		if lk == "" {
			return fmt.Errorf("unsupported filter key %q", key)
		}
		if f.metricLabels == nil {
			f.metricLabels = make(map[string]labelMatcher)
		}
		f.metricLabels[lk] = labelMatcher{value: val, prefix: prefix}
	case strings.HasPrefix(key, "resource.labels."):
		lk := strings.TrimPrefix(key, "resource.labels.")
		if lk == "" {
			return fmt.Errorf("unsupported filter key %q", key)
		}
		if f.resourceLabels == nil {
			f.resourceLabels = make(map[string]labelMatcher)
		}
		f.resourceLabels[lk] = labelMatcher{value: val, prefix: prefix}
	default:
		return fmt.Errorf("unsupported filter key %q (only metric.type, resource.type, metric.labels.<key>, and resource.labels.<key> are supported)", key)
	}
	return nil
}

// parseClauseValue parses the right-hand side of a filter clause: a
// double-quoted string literal or a starts_with("prefix") call.
func parseClauseValue(rhs string) (val string, prefix, ok bool) {
	if inner, isPrefix := parseStartsWith(rhs); isPrefix {
		return inner, true, true
	}
	if v, isQuoted := unquote(rhs); isQuoted {
		return v, false, true
	}
	return "", false, false
}

// ParseEquality parses `key = "value"` into key and the unquoted value.
func ParseEquality(clause string) (key, val string, ok bool) {
	key, rhs, found := strings.Cut(clause, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	rhs = strings.TrimSpace(rhs)
	if len(rhs) < 2 || rhs[0] != '"' || rhs[len(rhs)-1] != '"' {
		return "", "", false
	}
	return key, rhs[1 : len(rhs)-1], true
}

// match reports whether a time series satisfies the filter.
func (f tsFilter) match(ts monitoringstore.TimeSeries) bool {
	if !matchString(ts.MetricType, f.metricType, f.metricTypePrefix) {
		return false
	}
	if !matchString(ts.ResourceType, f.resourceType, f.resourceTypePrefix) {
		return false
	}
	return matchLabels(ts.MetricLabels, f.metricLabels) && matchLabels(ts.ResourceLabels, f.resourceLabels)
}

// matchString reports whether actual satisfies an equality (want, no prefix) or
// starts_with (want prefix) predicate. An empty want matches anything.
func matchString(actual, want string, prefix bool) bool {
	if want == "" {
		return true
	}
	if prefix {
		return strings.HasPrefix(actual, want)
	}
	return actual == want
}

// matchLabels reports whether every matcher is satisfied by labels. A matcher
// for a label the series does not carry does not match.
func matchLabels(labels map[string]string, matchers map[string]labelMatcher) bool {
	for key, m := range matchers {
		actual, ok := labels[key]
		if !ok {
			return false
		}
		if m.prefix {
			if !strings.HasPrefix(actual, m.value) {
				return false
			}
			continue
		}
		if actual != m.value {
			return false
		}
	}
	return true
}

// ─── descriptor-list filter (metric + monitored-resource descriptors) ─────────

// descriptorField selects which descriptor attribute a filter clause tests.
type descriptorField int

const (
	descriptorType descriptorField = iota
	descriptorName
)

// metricDescriptorFilterKeys are the filter keys accepted by
// ListMetricDescriptors. Unknown keys fail closed.
var metricDescriptorFilterKeys = map[string]descriptorField{
	"metric.type": descriptorType,
	"type":        descriptorType,
	"name":        descriptorName,
}

// monitoredResourceFilterKeys are the filter keys accepted by
// ListMonitoredResourceDescriptors. Unknown keys fail closed.
var monitoredResourceFilterKeys = map[string]descriptorField{
	"resource.type": descriptorType,
	"type":          descriptorType,
	"name":          descriptorName,
}

// descriptorClause is one parsed predicate: field equals value, or field starts
// with value.
type descriptorClause struct {
	field  descriptorField
	prefix bool
	value  string
}

// descriptorFilter is a conjunction of descriptorClause predicates. An empty
// filter matches everything.
type descriptorFilter struct {
	clauses []descriptorClause
}

// compileDescriptorFilter parses the discovery subset of the Cloud Monitoring
// filter grammar:
//
//	type = "x"
//	metric.type = "custom.googleapis.com/foo"     (ListMetricDescriptors)
//	resource.type = "gce_instance"                (ListMonitoredResourceDescriptors)
//	type = starts_with("gce_")
//	name = starts_with("projects/p/monitoredResourceDescriptors/k8s")
//
// Clauses are joined by " AND " and each is an equality or starts_with over a
// descriptor attribute. Any other key, operator, or malformed clause fails
// closed with an error (mapped to InvalidArgument by the service) instead of
// silently returning everything. keys is the surface-specific allowlist of
// accepted key spellings.
func compileDescriptorFilter(s string, keys map[string]descriptorField) (descriptorFilter, error) {
	var f descriptorFilter
	s = strings.TrimSpace(s)
	if s == "" {
		return f, nil
	}
	for _, raw := range strings.Split(s, " AND ") {
		clause := strings.TrimSpace(raw)
		if clause == "" {
			return f, fmt.Errorf("empty filter clause")
		}
		c, err := parseDescriptorClause(clause, keys)
		if err != nil {
			return f, err
		}
		f.clauses = append(f.clauses, c)
	}
	return f, nil
}

// parseDescriptorClause parses one `key = "value"` or
// `key = starts_with("prefix")` clause.
func parseDescriptorClause(clause string, keys map[string]descriptorField) (descriptorClause, error) {
	key, rhs, found := strings.Cut(clause, "=")
	if !found {
		return descriptorClause{}, fmt.Errorf("unsupported filter clause %q", clause)
	}
	key = strings.TrimSpace(key)
	rhs = strings.TrimSpace(rhs)
	field, ok := keys[key]
	if !ok {
		return descriptorClause{}, fmt.Errorf("unsupported filter key %q", key)
	}
	if inner, ok := parseStartsWith(rhs); ok {
		return descriptorClause{field: field, prefix: true, value: inner}, nil
	}
	if val, ok := unquote(rhs); ok {
		return descriptorClause{field: field, value: val}, nil
	}
	return descriptorClause{}, fmt.Errorf("unsupported filter operator in clause %q (only equality and starts_with are supported)", clause)
}

// parseStartsWith parses rhs of the form starts_with("prefix").
func parseStartsWith(rhs string) (string, bool) {
	const fn = "starts_with("
	if !strings.HasPrefix(rhs, fn) || !strings.HasSuffix(rhs, ")") {
		return "", false
	}
	return unquote(strings.TrimSpace(rhs[len(fn) : len(rhs)-1]))
}

// unquote returns the contents of a double-quoted string literal.
func unquote(s string) (string, bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	return s[1 : len(s)-1], true
}

// match reports whether a descriptor with the given type and resource name
// satisfies every clause.
func (f descriptorFilter) match(typ, name string) bool {
	for _, c := range f.clauses {
		actual := typ
		if c.field == descriptorName {
			actual = name
		}
		if c.prefix {
			if !strings.HasPrefix(actual, c.value) {
				return false
			}
			continue
		}
		if actual != c.value {
			return false
		}
	}
	return true
}

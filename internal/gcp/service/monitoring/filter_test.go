package monitoring

import (
	"testing"

	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
)

func TestCompileTSFilter(t *testing.T) {
	f, err := compileTSFilter(`metric.type = "custom.googleapis.com/foo" AND resource.type = "global"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if f.metricType != "custom.googleapis.com/foo" || f.resourceType != "global" {
		t.Fatalf("filter = %+v", f)
	}

	// Single clause is fine.
	f, err = compileTSFilter(`metric.type = "custom.googleapis.com/foo"`)
	if err != nil || f.metricType != "custom.googleapis.com/foo" || f.resourceType != "" {
		t.Fatalf("single clause = %+v, %v", f, err)
	}

	// Empty filter is rejected.
	if _, err := compileTSFilter(""); err == nil {
		t.Fatal("empty filter should error")
	}

	// Unsupported key is rejected.
	if _, err := compileTSFilter(`resource.label.foo = "bar"`); err == nil {
		t.Fatal("unsupported key should error")
	}

	// Unquoted value is rejected.
	if _, err := compileTSFilter(`metric.type = bare`); err == nil {
		t.Fatal("unquoted value should error")
	}
}

func TestTSFilterMatch(t *testing.T) {
	f := tsFilter{metricType: "custom.googleapis.com/foo", resourceType: "global"}
	ts := monitoringstore.TimeSeries{MetricType: "custom.googleapis.com/foo", ResourceType: "global"}
	if !f.match(ts) {
		t.Fatal("match failed for identical series")
	}
	if f.match(monitoringstore.TimeSeries{MetricType: "other", ResourceType: "global"}) {
		t.Fatal("metric.type mismatch should not match")
	}
	if f.match(monitoringstore.TimeSeries{MetricType: "custom.googleapis.com/foo", ResourceType: "aws_ec2_instance"}) {
		t.Fatal("resource.type mismatch should not match")
	}
}

func TestCompileTSFilterLabelsAndPrefix(t *testing.T) {
	f, err := compileTSFilter(`metric.type = "custom.googleapis.com/foo" AND metric.labels.env = "production" AND resource.labels.zone = starts_with("us-")`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	match := monitoringstore.TimeSeries{
		MetricType:     "custom.googleapis.com/foo",
		MetricLabels:   map[string]string{"env": "production"},
		ResourceType:   "gce_instance",
		ResourceLabels: map[string]string{"zone": "us-east1-b"},
	}
	if !f.match(match) {
		t.Fatal("expected label/prefix filter to match")
	}
	if f.match(monitoringstore.TimeSeries{
		MetricType:     "custom.googleapis.com/foo",
		MetricLabels:   map[string]string{"env": "staging"},
		ResourceLabels: map[string]string{"zone": "us-east1-b"},
	}) {
		t.Fatal("metric label mismatch should not match")
	}
	if f.match(monitoringstore.TimeSeries{
		MetricType:     "custom.googleapis.com/foo",
		MetricLabels:   map[string]string{"env": "production"},
		ResourceLabels: map[string]string{"zone": "europe-west1-b"},
	}) {
		t.Fatal("resource label starts_with mismatch should not match")
	}
	// A missing label never matches.
	if f.match(monitoringstore.TimeSeries{
		MetricType:   "custom.googleapis.com/foo",
		MetricLabels: map[string]string{"env": "production"},
	}) {
		t.Fatal("missing resource label should not match")
	}

	// starts_with on metric.type is accepted, and AND is case-insensitive.
	prefix, err := compileTSFilter(`metric.type = starts_with("custom.") and resource.type = "global"`)
	if err != nil {
		t.Fatalf("compile prefix: %v", err)
	}
	if !prefix.match(monitoringstore.TimeSeries{MetricType: "custom.x", ResourceType: "global"}) {
		t.Fatal("starts_with metric.type should match")
	}

	// Singular label spellings are not part of the timeSeries.list grammar.
	if _, err := compileTSFilter(`metric.label.env = "production"`); err == nil {
		t.Fatal("singular metric.label key should error")
	}
	if _, err := compileTSFilter(`metric.labels. = "production"`); err == nil {
		t.Fatal("empty label key should error")
	}
	// An empty value must not wildcard.
	if _, err := compileTSFilter(`metric.type = ""`); err == nil {
		t.Fatal("empty metric.type value should error")
	}
	if _, err := compileTSFilter(`metric.type = starts_with("")`); err == nil {
		t.Fatal("empty starts_with value should error")
	}
}

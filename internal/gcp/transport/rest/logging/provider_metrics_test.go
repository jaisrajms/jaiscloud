package logging

import (
	"net/http"
	"testing"
)

func TestRESTMetricRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	created, err := call(t, c, p, http.MethodPost, "/v2/projects/test/metrics", map[string]any{
		"name":           "nginx/requests",
		"description":    "request count",
		"filter":         "resource.type=gae_app AND severity>=ERROR",
		"valueExtractor": "EXTRACT(jsonPayload.latency)",
		"labelExtractors": map[string]any{
			"code": "EXTRACT(jsonPayload.code)",
		},
		"metricDescriptor": map[string]any{
			"valueType": "DISTRIBUTION",
			"unit":      "ms",
			"labels": []any{
				map[string]any{"key": "code", "valueType": "INT64"},
			},
		},
		"bucketOptions": map[string]any{
			"linearBuckets": map[string]any{"numFiniteBuckets": 3, "width": 1, "offset": 0},
		},
	})
	if err != nil {
		t.Fatalf("create metric: %v", err)
	}
	data := wireData(t, created)
	if data["name"] != "nginx/requests" {
		t.Fatalf("name = %v", data["name"])
	}
	if data["resourceName"] != "projects/test/metrics/nginx%2Frequests" {
		t.Fatalf("resourceName = %v", data["resourceName"])
	}
	if data["createTime"] == nil || data["updateTime"] == nil {
		t.Fatalf("timestamps missing: %v", data)
	}
	md, _ := data["metricDescriptor"].(map[string]any)
	if md["type"] != "logging.googleapis.com/user/nginx/requests" {
		t.Fatalf("descriptor.type = %v", md["type"])
	}
	if md["name"] != "projects/test/metricDescriptors/logging.googleapis.com/user/nginx/requests" {
		t.Fatalf("descriptor.name = %v", md["name"])
	}
	if md["description"] != "request count" || md["valueType"] != "DISTRIBUTION" || md["unit"] != "ms" {
		t.Fatalf("descriptor = %v", md)
	}
	if bo, _ := data["bucketOptions"].(map[string]any); bo == nil {
		t.Fatalf("bucketOptions missing: %v", data)
	}

	// The encoded metric id round-trips through a GET.
	got, err := call(t, c, p, http.MethodGet, "/v2/projects/test/metrics/nginx%2Frequests", nil)
	if err != nil {
		t.Fatalf("get metric: %v", err)
	}
	if g := wireData(t, got); g["filter"] != "resource.type=gae_app AND severity>=ERROR" {
		t.Fatalf("get metric = %v", g)
	}

	list, err := call(t, c, p, http.MethodGet, "/v2/projects/test/metrics", nil)
	if err != nil {
		t.Fatalf("list metrics: %v", err)
	}
	if metrics, _ := wireData(t, list)["metrics"].([]any); len(metrics) != 1 {
		t.Fatalf("list metrics = %v", wireData(t, list))
	}

	updated, err := call(t, c, p, http.MethodPut, "/v2/projects/test/metrics/nginx%2Frequests", map[string]any{
		"name":           "nginx/requests",
		"filter":         "severity>=WARNING",
		"valueExtractor": "EXTRACT(jsonPayload.latency)",
		"labelExtractors": map[string]any{
			"code": "EXTRACT(jsonPayload.code)",
			"host": "EXTRACT(resource.labels.host)",
		},
		"metricDescriptor": map[string]any{
			"valueType": "DISTRIBUTION",
			"unit":      "ms",
			"labels": []any{
				map[string]any{"key": "code", "valueType": "INT64"},
				map[string]any{"key": "host", "valueType": "STRING"},
			},
		},
	})
	if err != nil {
		t.Fatalf("update metric: %v", err)
	}
	if ud := wireData(t, updated); ud["filter"] != "severity>=WARNING" {
		t.Fatalf("updated metric = %v", ud)
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v2/projects/test/metrics/nginx%2Frequests", nil); err != nil {
		t.Fatalf("delete metric: %v", err)
	}
	if _, err := call(t, c, p, http.MethodGet, "/v2/projects/test/metrics/nginx%2Frequests", nil); err == nil {
		t.Fatal("get after delete = nil error")
	}
}

func TestRESTMetricCodecRouting(t *testing.T) {
	cases := []struct {
		method string
		path   string
		action string
	}{
		{http.MethodGet, "/v2/projects/test/metrics", "MetricList"},
		{http.MethodPost, "/v2/projects/test/metrics", "MetricCreate"},
		{http.MethodGet, "/v2/projects/test/metrics/x", "MetricGet"},
		{http.MethodGet, "/v2/projects/test/metrics/nginx%2Frequests", "MetricGet"},
		{http.MethodGet, "/v2/projects/test/metrics/nginx/requests", "MetricGet"},
		// A raw-slash metric id that itself contains a "logs" segment must still
		// resolve as a metric (not the log list/delete family).
		{http.MethodGet, "/v2/projects/test/metrics/foo/logs/bar", "MetricGet"},
		{http.MethodDelete, "/v2/projects/test/metrics/foo/logs/bar", "MetricDelete"},
		{http.MethodPut, "/v2/projects/test/metrics/x", "MetricUpdate"},
		{http.MethodDelete, "/v2/projects/test/metrics/x", "MetricDelete"},
		{http.MethodGet, "/v2/organizations/12/metrics", "MetricList"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			c, _ := newTestProvider(t)
			nr := decodePath(t, c, tc.method, tc.path)
			if nr.Action != tc.action {
				t.Fatalf("action = %q, want %q", nr.Action, tc.action)
			}
		})
	}

	c, _ := newTestProvider(t)
	if _, err := c.Decode(newRequest(t, http.MethodPatch, "/v2/projects/test/metrics/x"), nil); err == nil {
		t.Fatal("PATCH on a metric = nil error")
	}
}

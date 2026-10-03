package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/model"
)

func newTestProvider(t *testing.T) (*Codec, *Provider) {
	t.Helper()
	return NewCodec(), NewProvider(core.NewService(monitoringstore.NewMemoryStore(), "test"), "test")
}

// call decodes a synthetic REST request through the codec and dispatches it to
// the provider, mirroring the gateway's request flow.
func call(t *testing.T, c *Codec, p *Provider, method, path string, body any) (*model.ProviderResponse, error) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
	}
	req, err := http.NewRequest(method, path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, raw)
	if err != nil {
		return nil, err
	}
	return p.Routes()["Monitoring."+nr.Action](context.Background(), nr)
}

func wireData(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	raw, err := json.Marshal(resp.Data)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return m
}

func TestRESTMetricDescriptorRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)
	const mtype = "custom.googleapis.com/conf/foo"

	created, err := call(t, c, p, http.MethodPost, "/v3/projects/test/metricDescriptors", map[string]any{
		"type":        mtype,
		"metricKind":  "GAUGE",
		"valueType":   "INT64",
		"displayName": "Foo",
		"labels":      []any{map[string]any{"key": "env", "valueType": "STRING", "description": "env"}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	data := wireData(t, created)
	if data["name"] != "projects/test/metricDescriptors/"+mtype || data["metricKind"] != "GAUGE" || data["valueType"] != "INT64" {
		t.Fatalf("created = %+v", data)
	}

	list, err := call(t, c, p, http.MethodGet, "/v3/projects/test/metricDescriptors", nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	descs, _ := wireData(t, list)["metricDescriptors"].([]any)
	if len(descs) != 1 {
		t.Fatalf("list = %+v, want 1", wireData(t, list))
	}

	got, err := call(t, c, p, http.MethodGet, "/v3/projects/test/metricDescriptors/"+mtype, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if wireData(t, got)["type"] != mtype {
		t.Fatalf("get = %+v", wireData(t, got))
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v3/projects/test/metricDescriptors/"+mtype, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	list, _ = call(t, c, p, http.MethodGet, "/v3/projects/test/metricDescriptors", nil)
	if wireData(t, list)["metricDescriptors"] != nil {
		t.Fatalf("after delete = %+v, want empty", wireData(t, list))
	}
}

func TestRESTTimeSeriesRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)
	const tstype = "custom.googleapis.com/conf/ts"
	const distType = "custom.googleapis.com/conf/dist"

	if _, err := call(t, c, p, http.MethodPost, "/v3/projects/test/timeSeries", map[string]any{
		"timeSeries": []any{
			map[string]any{
				"metric":     map[string]any{"type": tstype, "labels": map[string]any{"k": "v"}},
				"resource":   map[string]any{"type": "global"},
				"metricKind": "GAUGE", "valueType": "DOUBLE",
				"points": []any{map[string]any{
					"interval": map[string]any{"endTime": "2026-01-01T00:00:00Z"},
					"value":    map[string]any{"doubleValue": 1.5},
				}},
			},
			map[string]any{
				"metric":     map[string]any{"type": distType},
				"resource":   map[string]any{"type": "global"},
				"metricKind": "GAUGE", "valueType": "DISTRIBUTION",
				"points": []any{map[string]any{
					"interval": map[string]any{"endTime": "2026-01-01T00:00:00Z"},
					"value": map[string]any{"distributionValue": map[string]any{
						"count": "2", "mean": 1.5, "bucketCounts": []any{"1", "1"},
						"bucketOptions": map[string]any{"explicitBuckets": map[string]any{"bounds": []any{1.0}}},
					}},
				}},
			},
		},
	}); err != nil {
		t.Fatalf("create timeSeries: %v", err)
	}

	list, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?filter="+`metric.type%3D%22`+tstype+`%22`, nil)
	if err != nil {
		t.Fatalf("list timeSeries: %v", err)
	}
	series, _ := wireData(t, list)["timeSeries"].([]any)
	if len(series) != 1 {
		t.Fatalf("timeSeries = %+v, want 1", wireData(t, list))
	}
	s := series[0].(map[string]any)
	metric := s["metric"].(map[string]any)
	if metric["type"] != tstype {
		t.Fatalf("metric = %+v", metric)
	}
	points := s["points"].([]any)
	if len(points) != 1 {
		t.Fatalf("points = %+v", points)
	}
	val := points[0].(map[string]any)["value"].(map[string]any)
	if val["doubleValue"] != 1.5 {
		t.Fatalf("value = %+v", val)
	}

	// createService mirrors create.
	if _, err := call(t, c, p, http.MethodPost, "/v3/projects/test/timeSeries:createService", map[string]any{
		"timeSeries": []any{map[string]any{
			"metric":   map[string]any{"type": "custom.googleapis.com/conf/svc"},
			"resource": map[string]any{"type": "global"},
			"points":   []any{map[string]any{"interval": map[string]any{"endTime": "2026-01-01T00:00:00Z"}, "value": map[string]any{"int64Value": "42"}}},
		}},
	}); err != nil {
		t.Fatalf("createService: %v", err)
	}
}

func TestRESTTimeSeriesLabelFilterAndAggregation(t *testing.T) {
	c, p := newTestProvider(t)
	const tstype = "custom.googleapis.com/conf/agg"

	if _, err := call(t, c, p, http.MethodPost, "/v3/projects/test/timeSeries", map[string]any{
		"timeSeries": []any{
			map[string]any{
				"metric":   map[string]any{"type": tstype, "labels": map[string]any{"env": "a"}},
				"resource": map[string]any{"type": "global"},
				"points":   []any{map[string]any{"interval": map[string]any{"endTime": "2026-01-01T00:00:00Z"}, "value": map[string]any{"doubleValue": 5}}},
			},
			map[string]any{
				"metric":   map[string]any{"type": tstype, "labels": map[string]any{"env": "b"}},
				"resource": map[string]any{"type": "global"},
				"points":   []any{map[string]any{"interval": map[string]any{"endTime": "2026-01-01T00:00:00Z"}, "value": map[string]any{"doubleValue": 7}}},
			},
		},
	}); err != nil {
		t.Fatalf("create timeSeries: %v", err)
	}

	// Label equality filters by metric label.
	q := url.Values{}
	q.Set("filter", `metric.type = "`+tstype+`" AND metric.labels.env = "a"`)
	one, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?"+q.Encode(), nil)
	if err != nil {
		t.Fatalf("label-filtered list: %v", err)
	}
	if series, _ := wireData(t, one)["timeSeries"].([]any); len(series) != 1 {
		t.Fatalf("label filter = %+v, want 1 series", wireData(t, one))
	}

	// Aggregation reduces both series into a single summed point.
	agg := url.Values{}
	agg.Set("filter", `metric.type = "`+tstype+`"`)
	agg.Set("aggregation.alignmentPeriod", "3600s")
	agg.Set("aggregation.perSeriesAligner", "ALIGN_SUM")
	agg.Set("aggregation.crossSeriesReducer", "REDUCE_SUM")
	reduced, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?"+agg.Encode(), nil)
	if err != nil {
		t.Fatalf("aggregated list: %v", err)
	}
	data := wireData(t, reduced)
	series, _ := data["timeSeries"].([]any)
	if len(series) != 1 {
		t.Fatalf("aggregated = %+v, want 1 series", data)
	}
	points := series[0].(map[string]any)["points"].([]any)
	if len(points) != 1 {
		t.Fatalf("aggregated points = %+v, want 1", points)
	}
	if got := points[0].(map[string]any)["value"].(map[string]any)["doubleValue"]; got != 12.0 {
		t.Fatalf("aggregated value = %v, want 12", got)
	}

	// A short alignment period is rejected.
	bad := url.Values{}
	bad.Set("filter", `metric.type = "`+tstype+`"`)
	bad.Set("aggregation.alignmentPeriod", "30s")
	bad.Set("aggregation.perSeriesAligner", "ALIGN_SUM")
	bad.Set("aggregation.crossSeriesReducer", "REDUCE_SUM")
	if _, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?"+bad.Encode(), nil); err == nil {
		t.Fatal("short alignment period should be rejected over REST")
	}

	// secondaryAggregation.* is parsed and applied after the primary (J43).
	sec := url.Values{}
	sec.Set("filter", `metric.type = "`+tstype+`"`)
	sec.Set("aggregation.alignmentPeriod", "3600s")
	sec.Set("aggregation.perSeriesAligner", "ALIGN_SUM")
	sec.Set("aggregation.crossSeriesReducer", "REDUCE_SUM")
	sec.Set("secondaryAggregation.alignmentPeriod", "7200s")
	sec.Set("secondaryAggregation.perSeriesAligner", "ALIGN_SUM")
	sec.Set("secondaryAggregation.crossSeriesReducer", "REDUCE_SUM")
	if _, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?"+sec.Encode(), nil); err != nil {
		t.Fatalf("secondary aggregation over REST: %v", err)
	}

	// order_by is unsupported by ListTimeSeries and must be rejected.
	ob := url.Values{}
	ob.Set("filter", `metric.type = "`+tstype+`"`)
	ob.Set("orderBy", "metric.type")
	if _, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?"+ob.Encode(), nil); err == nil {
		t.Fatal("orderBy should be rejected over REST")
	}
}

func TestRESTAlertPolicyRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)
	created, err := call(t, c, p, http.MethodPost, "/v3/projects/test/alertPolicies", map[string]any{
		"displayName": "Conf Policy",
		"combiner":    "OR",
		"conditions": []any{map[string]any{
			"displayName": "cond",
			"conditionThreshold": map[string]any{
				"filter": `metric.type="custom.googleapis.com/conf/ts"`, "comparison": "COMPARISON_GT",
				"thresholdValue": 1, "duration": "60s",
			},
		}},
	})
	if err != nil {
		t.Fatalf("create alert policy: %v", err)
	}
	data := wireData(t, created)
	name, _ := data["name"].(string)
	if name == "" || data["combiner"] != "OR" || data["enabled"] != true {
		t.Fatalf("created = %+v", data)
	}
	conds, _ := data["conditions"].([]any)
	if len(conds) != 1 {
		t.Fatalf("conditions = %+v", data["conditions"])
	}

	got, err := call(t, c, p, http.MethodGet, "/v3/"+name, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if wireData(t, got)["displayName"] != "Conf Policy" {
		t.Fatalf("get = %+v", wireData(t, got))
	}

	patched, err := call(t, c, p, http.MethodPatch, "/v3/"+name+"?updateMask=displayName", map[string]any{
		"displayName": "Updated Policy",
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	// The mask preserves the stored combiner/conditions while replacing the name.
	pd := wireData(t, patched)
	if pd["displayName"] != "Updated Policy" || pd["combiner"] != "OR" {
		t.Fatalf("patched = %+v", pd)
	}

	// A nested camelCase mask path (documentation.content) is accepted and
	// updates the documentation sub-message.
	docPatch, err := call(t, c, p, http.MethodPatch, "/v3/"+name+"?updateMask=documentation.content", map[string]any{
		"documentation": map[string]any{"content": "runbook"},
	})
	if err != nil {
		t.Fatalf("patch documentation.content: %v", err)
	}
	doc, _ := wireData(t, docPatch)["documentation"].(map[string]any)
	if doc["content"] != "runbook" {
		t.Fatalf("documentation = %+v", doc)
	}

	list, err := call(t, c, p, http.MethodGet, "/v3/projects/test/alertPolicies", nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if pols, _ := wireData(t, list)["alertPolicies"].([]any); len(pols) != 1 {
		t.Fatalf("list = %+v", wireData(t, list))
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v3/"+name, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := call(t, c, p, http.MethodGet, "/v3/"+name, nil); err == nil {
		t.Fatal("get after delete should be NotFound")
	}
}

func TestRESTNotificationChannelRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	created, err := call(t, c, p, http.MethodPost, "/v3/projects/test/notificationChannels", map[string]any{
		"type":        "email",
		"displayName": "Conf Channel",
		"labels":      map[string]any{"email_address": "conf@example.com"},
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	data := wireData(t, created)
	name, _ := data["name"].(string)
	if name == "" || data["type"] != "email" || data["enabled"] != true {
		t.Fatalf("created = %+v", data)
	}

	if _, err := call(t, c, p, http.MethodPost, "/v3/"+name+":sendVerificationCode", nil); err != nil {
		t.Fatalf("sendVerificationCode: %v", err)
	}
	codeResp, err := call(t, c, p, http.MethodPost, "/v3/"+name+":getVerificationCode", nil)
	if err != nil {
		t.Fatalf("getVerificationCode: %v", err)
	}
	if wireData(t, codeResp)["code"] == "" {
		t.Fatalf("verification code = %+v", wireData(t, codeResp))
	}
	// A supplied expireTime is echoed back (not the default now+1h).
	exp, err := call(t, c, p, http.MethodPost, "/v3/"+name+":getVerificationCode", map[string]any{
		"expireTime": "2030-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("getVerificationCode(expire): %v", err)
	}
	if wireData(t, exp)["expireTime"] != "2030-01-01T00:00:00Z" {
		t.Fatalf("expireTime = %+v, want 2030-01-01T00:00:00Z", wireData(t, exp)["expireTime"])
	}
	verified, err := call(t, c, p, http.MethodPost, "/v3/"+name+":verify", nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if wireData(t, verified)["verificationStatus"] != "VERIFIED" {
		t.Fatalf("verify = %+v", wireData(t, verified))
	}

	patched, err := call(t, c, p, http.MethodPatch, "/v3/"+name+"?updateMask=description", map[string]any{
		"description": "updated",
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if wireData(t, patched)["description"] != "updated" {
		t.Fatalf("patched = %+v", wireData(t, patched))
	}

	descs, err := call(t, c, p, http.MethodGet, "/v3/projects/test/notificationChannelDescriptors", nil)
	if err != nil {
		t.Fatalf("descriptors: %v", err)
	}
	if ds, _ := wireData(t, descs)["channelDescriptors"].([]any); len(ds) == 0 {
		t.Fatalf("descriptors = %+v", wireData(t, descs))
	}
	email, err := call(t, c, p, http.MethodGet, "/v3/projects/test/notificationChannelDescriptors/email", nil)
	if err != nil {
		t.Fatalf("descriptor get: %v", err)
	}
	if wireData(t, email)["type"] != "email" {
		t.Fatalf("descriptor = %+v", wireData(t, email))
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v3/"+name, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestRESTMonitoredResourceDescriptors(t *testing.T) {
	c, p := newTestProvider(t)
	list, err := call(t, c, p, http.MethodGet, "/v3/projects/test/monitoredResourceDescriptors", nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	descs, _ := wireData(t, list)["resourceDescriptors"].([]any)
	if len(descs) < 10 {
		t.Fatalf("descriptors = %d, want >= 10", len(descs))
	}
	got, err := call(t, c, p, http.MethodGet, "/v3/projects/test/monitoredResourceDescriptors/gce_instance", nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if wireData(t, got)["type"] != "gce_instance" {
		t.Fatalf("get = %+v", wireData(t, got))
	}
	if _, err := call(t, c, p, http.MethodGet, "/v3/projects/test/monitoredResourceDescriptors/nope", nil); err == nil {
		t.Fatal("unknown descriptor should be NotFound")
	}
}

func TestRESTServiceMonitoringRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	createdSvc, err := call(t, c, p, http.MethodPost, "/v3/projects/test/services?serviceId=checkout", map[string]any{
		"displayName": "Checkout",
		"cloudRun":    map[string]any{"serviceName": "checkout"},
		"userLabels":  map[string]any{"team": "payments"},
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	svcData := wireData(t, createdSvc)
	svcName, _ := svcData["name"].(string)
	if svcName != "projects/test/services/checkout" || svcData["displayName"] != "Checkout" {
		t.Fatalf("created service = %+v", svcData)
	}
	if _, ok := svcData["cloudRun"].(map[string]any); !ok {
		t.Fatalf("created service identifier = %+v", svcData["cloudRun"])
	}

	list, err := call(t, c, p, http.MethodGet, "/v3/projects/test/services", nil)
	if err != nil {
		t.Fatalf("list services: %v", err)
	}
	if svcs, _ := wireData(t, list)["services"].([]any); len(svcs) != 1 {
		t.Fatalf("list services = %+v", wireData(t, list))
	}

	// A masked patch changes only displayName, preserving the identifier.
	patched, err := call(t, c, p, http.MethodPatch, "/v3/"+svcName+"?updateMask=displayName", map[string]any{
		"displayName": "Checkout v2",
	})
	if err != nil {
		t.Fatalf("patch service: %v", err)
	}
	pd := wireData(t, patched)
	if pd["displayName"] != "Checkout v2" {
		t.Fatalf("patched = %+v", pd)
	}
	if _, ok := pd["cloudRun"].(map[string]any); !ok {
		t.Fatalf("patched identifier lost = %+v", pd)
	}

	// A documented identifier_case filter narrows the list.
	filtered, err := call(t, c, p, http.MethodGet, `/v3/projects/test/services?filter=identifier_case+%3D+%22CLOUD_RUN%22`, nil)
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if svcs, _ := wireData(t, filtered)["services"].([]any); len(svcs) != 1 {
		t.Fatalf("filtered list = %+v", wireData(t, filtered))
	}
	other, err := call(t, c, p, http.MethodGet, `/v3/projects/test/services?filter=identifier_case+%3D+%22CUSTOM%22`, nil)
	if err != nil {
		t.Fatalf("filtered list (no match): %v", err)
	}
	if svcs, _ := wireData(t, other)["services"].([]any); len(svcs) != 0 {
		t.Fatalf("filtered list (no match) = %+v, want empty", wireData(t, other))
	}

	// A basic service is identified by the separate basicService field.
	basic, err := call(t, c, p, http.MethodPost, "/v3/projects/test/services?serviceId=basic", map[string]any{
		"basicService": map[string]any{"serviceType": "CLOUD_RUN"},
	})
	if err != nil {
		t.Fatalf("create basic service: %v", err)
	}
	bs, ok := wireData(t, basic)["basicService"].(map[string]any)
	if !ok || bs["serviceType"] != "CLOUD_RUN" {
		t.Fatalf("basic service = %+v", wireData(t, basic))
	}
	basicList, err := call(t, c, p, http.MethodGet, `/v3/projects/test/services?filter=identifier_case+%3D+%22BASIC_SERVICE%22`, nil)
	if err != nil {
		t.Fatalf("basic filter: %v", err)
	}
	if svcs, _ := wireData(t, basicList)["services"].([]any); len(svcs) != 1 {
		t.Fatalf("basic filter = %+v, want 1", wireData(t, basicList))
	}

	sloName := svcName + "/serviceLevelObjectives/avail"
	createdSLO, err := call(t, c, p, http.MethodPost, "/v3/"+svcName+"/serviceLevelObjectives?serviceLevelObjectiveId=avail", map[string]any{
		"displayName":           "Availability",
		"goal":                  0.99,
		"rollingPeriod":         "2592000s",
		"serviceLevelIndicator": map[string]any{"basicSli": map[string]any{"availability": map[string]any{}}},
	})
	if err != nil {
		t.Fatalf("create SLO: %v", err)
	}
	sloData := wireData(t, createdSLO)
	if sloData["name"] != sloName || sloData["goal"] != 0.99 || sloData["rollingPeriod"] != "2592000s" {
		t.Fatalf("created SLO = %+v", sloData)
	}

	sloList, err := call(t, c, p, http.MethodGet, "/v3/"+svcName+"/serviceLevelObjectives", nil)
	if err != nil {
		t.Fatalf("list SLOs: %v", err)
	}
	if slos, _ := wireData(t, sloList)["serviceLevelObjectives"].([]any); len(slos) != 1 {
		t.Fatalf("list SLOs = %+v", wireData(t, sloList))
	}

	gotSLO, err := call(t, c, p, http.MethodGet, "/v3/"+sloName, nil)
	if err != nil {
		t.Fatalf("get SLO: %v", err)
	}
	if wireData(t, gotSLO)["displayName"] != "Availability" {
		t.Fatalf("get SLO = %+v", wireData(t, gotSLO))
	}

	updatedSLO, err := call(t, c, p, http.MethodPatch, "/v3/"+sloName+"?updateMask=goal", map[string]any{"goal": 0.95})
	if err != nil {
		t.Fatalf("patch SLO: %v", err)
	}
	if wireData(t, updatedSLO)["goal"] != 0.95 {
		t.Fatalf("patched SLO = %+v", wireData(t, updatedSLO))
	}

	// An invalid goal is rejected at the core, not silently stored.
	if _, err := call(t, c, p, http.MethodPatch, "/v3/"+sloName+"?updateMask=goal", map[string]any{"goal": 1.5}); err == nil {
		t.Fatal("goal > 1 should be rejected")
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v3/"+sloName, nil); err != nil {
		t.Fatalf("delete SLO: %v", err)
	}

	// Deleting the service removes the service itself.
	if _, err := call(t, c, p, http.MethodDelete, "/v3/"+svcName, nil); err != nil {
		t.Fatalf("delete service: %v", err)
	}
	if _, err := call(t, c, p, http.MethodGet, "/v3/"+svcName, nil); err == nil {
		t.Fatal("get after delete should be NotFound")
	}
}

func TestRESTErrorsAndCodecRouting(t *testing.T) {
	c, p := newTestProvider(t)

	if _, err := call(t, c, p, http.MethodGet, "/v3/projects/test/timeSeries?filter="+`bad`, nil); err == nil {
		t.Fatal("invalid time series filter should error")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 400 {
		t.Fatalf("invalid filter err = %v", err)
	}

	for _, tc := range []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/v3/projects/test/dashboards", 404},
		{http.MethodGet, "/v3/uptimeCheckIps", 501},
		{http.MethodGet, "/v3/projects/test/unknownThing", 404},
		{http.MethodPost, "/v2/entries:list", 404},
	} {
		req, _ := http.NewRequest(tc.method, tc.path, nil)
		_, err := c.Decode(req, nil)
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != tc.wantStatus {
			t.Errorf("%s %s err = %v, want status %d", tc.method, tc.path, err, tc.wantStatus)
		}
	}
}

func TestRESTValueTypeEnumIsCanonical(t *testing.T) {
	// google.api.MetricDescriptor.ValueType: BOOL=1, INT64=2, DOUBLE=3.
	for name, want := range map[string]int32{"BOOL": 1, "INT64": 2, "DOUBLE": 3, "STRING": 4, "DISTRIBUTION": 5} {
		if got := enumValue(valueTypeValue, name); got != want {
			t.Errorf("enumValue(valueTypeValue, %q) = %d, want %d", name, got, want)
		}
	}
}

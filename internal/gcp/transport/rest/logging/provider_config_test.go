package logging

import (
	"net/http"
	"testing"

	"jaiscloud/internal/model"
)

func newRequest(t *testing.T, method, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}

func decodePath(t *testing.T, c *Codec, method, path string) *model.NormalizedRequest {
	t.Helper()
	nr, err := c.Decode(newRequest(t, method, path), nil)
	if err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
	return nr
}

func TestRESTSinkRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	created, err := call(t, c, p, http.MethodPost, "/v2/projects/test/sinks", map[string]any{
		"name":        "my-sink",
		"destination": "storage.googleapis.com/bucket",
		"filter":      "severity>=WARNING",
		"description": "keep me",
		"exclusions": []any{
			map[string]any{"name": "quiet", "filter": `textPayload:"noisy"`},
		},
	})
	if err != nil {
		t.Fatalf("create sink: %v", err)
	}
	data := wireData(t, created)
	if data["name"] != "my-sink" {
		t.Fatalf("name = %v, want my-sink", data["name"])
	}
	if data["resourceName"] != "projects/test/sinks/my-sink" {
		t.Fatalf("resourceName = %v", data["resourceName"])
	}
	if data["destination"] != "storage.googleapis.com/bucket" {
		t.Fatalf("destination = %v", data["destination"])
	}
	if data["writerIdentity"] == nil || data["writerIdentity"] == "" {
		t.Fatalf("writerIdentity missing: %v", data["writerIdentity"])
	}
	if data["createTime"] == nil {
		t.Fatalf("createTime missing")
	}
	exclusions, _ := data["exclusions"].([]any)
	if len(exclusions) != 1 {
		t.Fatalf("exclusions = %v", data["exclusions"])
	}

	got, err := call(t, c, p, http.MethodGet, "/v2/projects/test/sinks/my-sink", nil)
	if err != nil {
		t.Fatalf("get sink: %v", err)
	}
	if g := wireData(t, got); g["resourceName"] != "projects/test/sinks/my-sink" {
		t.Fatalf("get resourceName = %v", g["resourceName"])
	}

	list, err := call(t, c, p, http.MethodGet, "/v2/projects/test/sinks", nil)
	if err != nil {
		t.Fatalf("list sinks: %v", err)
	}
	if sinks, _ := wireData(t, list)["sinks"].([]any); len(sinks) != 1 {
		t.Fatalf("list sinks = %v", wireData(t, list)["sinks"])
	}

	patched, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/sinks/my-sink?updateMask=filter",
		map[string]any{"filter": "severity>=ERROR"})
	if err != nil {
		t.Fatalf("patch sink: %v", err)
	}
	pd := wireData(t, patched)
	if pd["filter"] != "severity>=ERROR" || pd["destination"] != "storage.googleapis.com/bucket" {
		t.Fatalf("patch result = %v", pd)
	}

	// An unmasked PUT uses the proto default mask (destination,filter,
	// includeChildren) and must preserve description/exclusions.
	put, err := call(t, c, p, http.MethodPut, "/v2/projects/test/sinks/my-sink",
		map[string]any{"name": "my-sink", "destination": "storage.googleapis.com/other", "filter": "severity>=ERROR"})
	if err != nil {
		t.Fatalf("put sink: %v", err)
	}
	ud := wireData(t, put)
	if ud["destination"] != "storage.googleapis.com/other" {
		t.Fatalf("put destination = %v", ud["destination"])
	}
	if ud["description"] != "keep me" {
		t.Fatalf("unmasked PUT cleared description: %v", ud["description"])
	}
	if ex, _ := ud["exclusions"].([]any); len(ex) != 1 {
		t.Fatalf("unmasked PUT cleared exclusions: %v", ud["exclusions"])
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v2/projects/test/sinks/my-sink", nil); err != nil {
		t.Fatalf("delete sink: %v", err)
	}
	if _, err := call(t, c, p, http.MethodGet, "/v2/projects/test/sinks/my-sink", nil); err == nil {
		t.Fatal("get after delete = nil error")
	}
}

func TestRESTSinkWritableMaskFields(t *testing.T) {
	c, p := newTestProvider(t)

	if _, err := call(t, c, p, http.MethodPost, "/v2/projects/test/sinks", map[string]any{
		"name":            "bq",
		"destination":     "logging.googleapis.com/projects/test",
		"includeChildren": true,
	}); err != nil {
		t.Fatalf("create sink: %v", err)
	}

	// The legacy / destination-dependent writable fields merge.
	patched, err := call(t, c, p, http.MethodPatch,
		"/v2/projects/test/sinks/bq?updateMask=interceptChildren,outputVersionFormat,bigqueryOptions",
		map[string]any{
			"interceptChildren":   true,
			"outputVersionFormat": "V1",
			"bigqueryOptions":     map[string]any{"usePartitionedTables": true},
		})
	if err != nil {
		t.Fatalf("patch sink fields: %v", err)
	}
	pd := wireData(t, patched)
	if pd["interceptChildren"] != true || pd["outputVersionFormat"] != "V1" {
		t.Fatalf("patch fields = %v", pd)
	}
	if bo, _ := pd["bigqueryOptions"].(map[string]any); bo == nil || bo["usePartitionedTables"] != true {
		t.Fatalf("bigqueryOptions = %v", pd["bigqueryOptions"])
	}
	// The output-only sibling is never taken from the request.
	if bo, _ := pd["bigqueryOptions"].(map[string]any); bo["usesTimestampColumnPartitioning"] == true {
		t.Fatalf("output-only subfield was accepted: %v", pd["bigqueryOptions"])
	}

	// interceptChildren without include_children is an invalid request.
	if _, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/sinks/bq?updateMask=includeChildren",
		map[string]any{"includeChildren": false}); err == nil {
		t.Fatal("intercept_children without include_children = nil error")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
		t.Fatalf("intercept_children constraint = %v, want InvalidArgument", err)
	}

	// An update_mask path that names no sink field is InvalidArgument (400),
	// not an unimplemented operation (AIP-134).
	if _, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/sinks/bq?updateMask=labels", map[string]any{}); err == nil {
		t.Fatal("invalid mask = nil error, want InvalidArgument")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("invalid mask = %v, want InvalidArgument/400", err)
	}
}

func TestRESTSinkNestedMaskAndWildcard(t *testing.T) {
	c, p := newTestProvider(t)
	if _, err := call(t, c, p, http.MethodPost, "/v2/projects/test/sinks",
		map[string]any{"name": "bqn", "destination": "bigquery.googleapis.com/projects/test/datasets/d"}); err != nil {
		t.Fatalf("create sink: %v", err)
	}

	// A nested REST path (camelCase) updates only that leaf.
	patched, err := call(t, c, p, http.MethodPatch,
		"/v2/projects/test/sinks/bqn?updateMask=bigqueryOptions.usePartitionedTables",
		map[string]any{"bigqueryOptions": map[string]any{"usePartitionedTables": true}})
	if err != nil {
		t.Fatalf("patch nested sink: %v", err)
	}
	if bo, _ := wireData(t, patched)["bigqueryOptions"].(map[string]any); bo == nil || bo["usePartitionedTables"] != true {
		t.Fatalf("nested bigqueryOptions = %v", wireData(t, patched)["bigqueryOptions"])
	}

	// A nested path naming no writable subfield is InvalidArgument/400.
	if _, err := call(t, c, p, http.MethodPatch,
		"/v2/projects/test/sinks/bqn?updateMask=bigqueryOptions.bogus", map[string]any{}); err == nil {
		t.Fatal("unknown nested mask = nil error, want InvalidArgument")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("unknown nested mask = %v, want InvalidArgument/400", err)
	}

	// The AIP-134 `*` wildcard replaces writable fields.
	wild, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/sinks/bqn?updateMask=*",
		map[string]any{"destination": "storage.googleapis.com/b"})
	if err != nil {
		t.Fatalf("patch wildcard: %v", err)
	}
	if wd := wireData(t, wild); wd["destination"] != "storage.googleapis.com/b" {
		t.Fatalf("wildcard destination = %v", wd["destination"])
	}
}

func TestRESTExclusionRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	created, err := call(t, c, p, http.MethodPost, "/v2/projects/test/exclusions", map[string]any{
		"name":   "no-debug",
		"filter": "severity<DEBUG",
	})
	if err != nil {
		t.Fatalf("create exclusion: %v", err)
	}
	data := wireData(t, created)
	if data["name"] != "no-debug" || data["filter"] != "severity<DEBUG" {
		t.Fatalf("created exclusion = %v", data)
	}

	got, err := call(t, c, p, http.MethodGet, "/v2/projects/test/exclusions/no-debug", nil)
	if err != nil {
		t.Fatalf("get exclusion: %v", err)
	}
	if g := wireData(t, got); g["name"] != "no-debug" {
		t.Fatalf("get exclusion = %v", g)
	}

	list, err := call(t, c, p, http.MethodGet, "/v2/projects/test/exclusions", nil)
	if err != nil {
		t.Fatalf("list exclusions: %v", err)
	}
	if ex, _ := wireData(t, list)["exclusions"].([]any); len(ex) != 1 {
		t.Fatalf("list exclusions = %v", wireData(t, list))
	}

	patched, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/exclusions/no-debug?updateMask=disabled",
		map[string]any{"disabled": true})
	if err != nil {
		t.Fatalf("patch exclusion: %v", err)
	}
	pd := wireData(t, patched)
	if pd["disabled"] != true || pd["filter"] != "severity<DEBUG" {
		t.Fatalf("patched exclusion = %v", pd)
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v2/projects/test/exclusions/no-debug", nil); err != nil {
		t.Fatalf("delete exclusion: %v", err)
	}
}

// TestRESTListNonPositivePageSize pins the wire contract of the config/admin
// lists: sinks/exclusions/buckets/views/logScopes document "Non-positive values
// are ignored" and answer the default page, while links.list — whose Discovery
// document carries no such exception — keeps AIP-158's 400 INVALID_ARGUMENT.
func TestRESTListNonPositivePageSize(t *testing.T) {
	c, p := newTestProvider(t)
	if _, err := call(t, c, p, http.MethodPost, "/v2/projects/test/sinks",
		map[string]any{"name": "s", "destination": "d"}); err != nil {
		t.Fatalf("create sink: %v", err)
	}
	if _, err := call(t, c, p, http.MethodPost, "/v2/projects/test/exclusions",
		map[string]any{"name": "e", "filter": "severity<DEBUG"}); err != nil {
		t.Fatalf("create exclusion: %v", err)
	}

	sinks, err := call(t, c, p, http.MethodGet, "/v2/projects/test/sinks?pageSize=-1", nil)
	if err != nil {
		t.Fatalf("list sinks with pageSize=-1: %v", err)
	}
	if list, _ := wireData(t, sinks)["sinks"].([]any); len(list) != 1 {
		t.Fatalf("sinks = %v, want the single sink", list)
	}
	exclusions, err := call(t, c, p, http.MethodGet, "/v2/projects/test/exclusions?pageSize=-1", nil)
	if err != nil {
		t.Fatalf("list exclusions with pageSize=-1: %v", err)
	}
	if list, _ := wireData(t, exclusions)["exclusions"].([]any); len(list) != 1 {
		t.Fatalf("exclusions = %v, want the single exclusion", list)
	}

	location := "/v2/projects/test/locations/global"
	if _, err := call(t, c, p, http.MethodPost, location+"/buckets?bucketId=b1",
		map[string]any{"description": "d"}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := call(t, c, p, http.MethodPost, location+"/buckets/b1/views?viewId=v1",
		map[string]any{"filter": "severity>=ERROR"}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	if _, err := call(t, c, p, http.MethodPost, location+"/logScopes?logScopeId=s1",
		map[string]any{"resourceNames": []any{"projects/test"}}); err != nil {
		t.Fatalf("create logScope: %v", err)
	}

	for _, tc := range []struct {
		path string
		key  string
	}{
		{location + "/buckets", "buckets"},
		{location + "/buckets/b1/views", "views"},
		{location + "/logScopes", "logScopes"},
	} {
		resp, err := call(t, c, p, http.MethodGet, tc.path+"?pageSize=-1", nil)
		if err != nil {
			t.Fatalf("GET %s with pageSize=-1: %v", tc.path, err)
		}
		if list, _ := wireData(t, resp)[tc.key].([]any); len(list) != 1 {
			t.Fatalf("%s = %v, want the seeded resource", tc.key, list)
		}
	}

	_, err = call(t, c, p, http.MethodGet, location+"/buckets/b1/links?pageSize=-1", nil)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("list links with pageSize=-1 = %v, want InvalidArgument/400", err)
	}

	// entries.list is the documented rejection: negative and over-maximum sizes
	// are 400s on the REST path too.
	for _, pageSize := range []int{-1, 1001} {
		_, err := call(t, c, p, http.MethodPost, "/v2/entries:list",
			map[string]any{"resourceNames": []any{"projects/test"}, "pageSize": pageSize})
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Fatalf("entries:list pageSize %d = %v, want InvalidArgument/400", pageSize, err)
		}
	}
}

func TestRESTConfigCodecRouting(t *testing.T) {
	cases := []struct {
		method string
		path   string
		action string
	}{
		{http.MethodGet, "/v2/projects/test/sinks", "SinkList"},
		{http.MethodPost, "/v2/projects/test/sinks", "SinkCreate"},
		{http.MethodGet, "/v2/projects/test/sinks/x", "SinkGet"},
		{http.MethodPut, "/v2/projects/test/sinks/x", "SinkUpdate"},
		{http.MethodPatch, "/v2/projects/test/sinks/x", "SinkPatch"},
		{http.MethodDelete, "/v2/projects/test/sinks/x", "SinkDelete"},
		{http.MethodGet, "/v2/organizations/12/exclusions", "ExclusionList"},
		{http.MethodPost, "/v2/organizations/12/exclusions", "ExclusionCreate"},
		{http.MethodGet, "/v2/organizations/12/exclusions/e", "ExclusionGet"},
		{http.MethodPatch, "/v2/organizations/12/exclusions/e", "ExclusionPatch"},
		{http.MethodDelete, "/v2/organizations/12/exclusions/e", "ExclusionDelete"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			c, p := newTestProvider(t)
			_ = p
			nr := decodePath(t, c, tc.method, tc.path)
			if nr.Action != tc.action {
				t.Fatalf("action = %q, want %q", nr.Action, tc.action)
			}
		})
	}

	// An unsupported method on a config collection is not silently accepted.
	c, _ := newTestProvider(t)
	if _, err := c.Decode(newRequest(t, http.MethodDelete, "/v2/projects/test/sinks"), nil); err == nil {
		t.Fatal("DELETE on a sink collection = nil error")
	}
}

func TestRESTSinkUnsupportedMask(t *testing.T) {
	c, p := newTestProvider(t)
	if _, err := call(t, c, p, http.MethodPost, "/v2/projects/test/sinks",
		map[string]any{"name": "s", "destination": "d"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := call(t, c, p, http.MethodPatch, "/v2/projects/test/sinks/s?updateMask=writerIdentity",
		map[string]any{"filter": "x"}); err == nil {
		t.Fatal("unsupported mask = nil error")
	}
}

// TestRESTListNonIntegerPageSize pins the int32 binding on the Logging lists: a
// non-integer pageSize (a query string or an entries.list body number) is 400
// INVALID_ARGUMENT, never a silent zero/default.
func TestRESTListNonIntegerPageSize(t *testing.T) {
	c, p := newTestProvider(t)
	for _, path := range []string{
		"/v2/projects/test/sinks",
		"/v2/projects/test/exclusions",
		"/v2/projects/test/metrics",
		"/v2/projects/test/locations/global/buckets",
		"/v2/projects/test/locations/global/buckets/b1/views",
		"/v2/projects/test/locations/global/buckets/b1/links",
		"/v2/projects/test/locations/global/logScopes",
		"/v2/projects/test/logs",
	} {
		for _, v := range []string{"abc", "-0.5", "1.5"} {
			_, err := call(t, c, p, http.MethodGet, path+"?pageSize="+v, nil)
			perr, ok := err.(*model.ProviderError)
			if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
				t.Errorf("GET %s?pageSize=%s err = %v, want InvalidArgument/400", path, v, err)
			}
		}
	}
	for _, pageSize := range []any{"abc", "-0.5"} {
		_, err := call(t, c, p, http.MethodPost, "/v2/entries:list",
			map[string]any{"resourceNames": []any{"projects/test"}, "pageSize": pageSize})
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
			t.Errorf("entries:list pageSize %#v = %v, want InvalidArgument/400", pageSize, err)
		}
	}
}

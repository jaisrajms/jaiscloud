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

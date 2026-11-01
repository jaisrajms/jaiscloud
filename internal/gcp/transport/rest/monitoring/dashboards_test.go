package monitoring

import (
	"bytes"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	core "jaiscloud/internal/gcp/service/monitoring"
	monitoringstore "jaiscloud/internal/gcp/store/monitoring"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// dashboardData returns a single-document dashboard response decoded from the
// codec's raw-JSON carrier.
func dashboardData(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage)
	if !ok {
		t.Fatalf("response is not a raw dashboard document: %+v", resp.Data)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode dashboard document: %v", err)
	}
	return m
}

func newDashboardProvider() (*Codec, *Provider) {
	s := core.NewService(monitoringstore.NewMemoryStore(), "test", core.WithResources(store.NewMemoryResourceStore()))
	return NewCodec(), NewProvider(s, "test")
}

func dashboardWireStatus(t *testing.T, err error, status int, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with status %d, got nil", status)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("error %v is not a *model.ProviderError", err)
	}
	if perr.HTTPStatus != status || (code != "" && perr.Code != code) {
		t.Fatalf("error = %d/%q, want %d/%q (%v)", perr.HTTPStatus, perr.Code, status, code, err)
	}
}

func TestRESTDashboardRoundTrip(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v1/projects/test/dashboards"

	created, err := call(t, c, p, http.MethodPost, base, map[string]any{
		"displayName": "Checkout",
		"gridLayout": map[string]any{
			"columns": "2",
			"widgets": []any{map[string]any{"title": "Requests"}},
		},
		"labels": map[string]any{"team": "checkout"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	data := dashboardData(t, created)
	name, _ := data["name"].(string)
	etag, _ := data["etag"].(string)
	if name == "" || etag == "" || data["displayName"] != "Checkout" {
		t.Fatalf("created = %+v", data)
	}
	if !reflect.DeepEqual(data["gridLayout"], map[string]any{"columns": "2", "widgets": []any{map[string]any{"title": "Requests"}}}) {
		t.Fatalf("gridLayout not returned verbatim: %#v", data["gridLayout"])
	}

	got, err := call(t, c, p, http.MethodGet, "/v1/"+name, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotData := dashboardData(t, got); !reflect.DeepEqual(gotData, data) {
		t.Fatalf("get = %+v, want %+v", gotData, data)
	}

	list, err := call(t, c, p, http.MethodGet, base, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	listData := wireData(t, list)
	dashboards, _ := listData["dashboards"].([]any)
	if len(dashboards) != 1 {
		t.Fatalf("list = %+v", listData)
	}

	updated, err := call(t, c, p, http.MethodPatch, "/v1/"+name, map[string]any{
		"name":        name,
		"etag":        etag,
		"displayName": "Checkout v2",
		"rowLayout":   map[string]any{"rows": []any{map[string]any{"weight": 1}}},
	})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	updatedData := dashboardData(t, updated)
	if updatedData["displayName"] != "Checkout v2" || updatedData["etag"] == etag {
		t.Fatalf("patched = %+v", updatedData)
	}
	if _, ok := updatedData["gridLayout"]; ok {
		t.Fatal("patch did not replace the whole document")
	}

	// A stale etag is 409 ABORTED (the Discovery document's optimistic
	// concurrency contract).
	_, err = call(t, c, p, http.MethodPatch, "/v1/"+name, map[string]any{
		"name": name, "etag": etag, "displayName": "stale",
	})
	dashboardWireStatus(t, err, 409, "Aborted")

	// The Terraform provider's update shape: the dashboard JSON plus the etag
	// from the previous read, with no name in the body.
	if _, err := call(t, c, p, http.MethodPatch, "/v1/"+name, map[string]any{
		"etag": updatedData["etag"], "displayName": "Checkout v3",
	}); err != nil {
		t.Fatalf("name-less patch: %v", err)
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v1/"+name, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = call(t, c, p, http.MethodGet, "/v1/"+name, nil)
	dashboardWireStatus(t, err, 404, "NotFound")
}

func TestRESTDashboardValidationAndPaging(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v1/projects/test/dashboards"

	for name, body := range map[string]map[string]any{
		"wrong project":   {"name": "projects/other/dashboards/x", "displayName": "x"},
		"malformed name":  {"name": "dashboards/x", "displayName": "x"},
		"empty display":   {"displayName": ""},
		"missing display": {"labels": map[string]any{"a": "b"}},
	} {
		_, err := call(t, c, p, http.MethodPost, base, body)
		if err == nil {
			t.Errorf("%s: expected INVALID_ARGUMENT", name)
			continue
		}
		dashboardWireStatus(t, err, 400, "InvalidArgument")
	}

	// An empty POST body is INVALID_ARGUMENT, not a silently created dashboard.
	req, _ := http.NewRequest(http.MethodPost, base, nil)
	nr, derr := c.Decode(req, nil)
	if derr != nil {
		t.Fatalf("decode: %v", derr)
	}
	if _, err := p.Routes()["Monitoring.CreateDashboard"](t.Context(), nr); err == nil {
		t.Fatal("empty dashboard body should be rejected")
	}

	if _, err := call(t, c, p, http.MethodGet, base+"?pageSize=-1", nil); err == nil {
		t.Fatal("negative pageSize should be rejected")
	} else {
		dashboardWireStatus(t, err, 400, "InvalidArgument")
	}

	// Pagination round-trip through the REST surface.
	for _, id := range []string{"a", "b", "c"} {
		body := map[string]any{"name": "projects/test/dashboards/" + id, "displayName": id}
		if _, err := call(t, c, p, http.MethodPost, base, body); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	first, err := call(t, c, p, http.MethodGet, base+"?pageSize=2", nil)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	page1 := wireData(t, first)
	token, _ := page1["nextPageToken"].(string)
	if n := len(page1["dashboards"].([]any)); n != 2 || token == "" {
		t.Fatalf("page 1 = %+v", page1)
	}
	second, err := call(t, c, p, http.MethodGet, base+"?pageSize=2&pageToken="+token, nil)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	page2 := wireData(t, second)
	if n := len(page2["dashboards"].([]any)); n != 1 || page2["nextPageToken"] != nil {
		t.Fatalf("page 2 = %+v", page2)
	}
}

func TestRESTDashboardValidateOnly(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v1/projects/test/dashboards"

	if _, err := call(t, c, p, http.MethodPost, base+"?validateOnly=true", map[string]any{
		"name": "projects/test/dashboards/preview", "displayName": "preview",
	}); err != nil {
		t.Fatalf("validateOnly create: %v", err)
	}
	if _, err := call(t, c, p, http.MethodGet, base+"/preview", nil); err == nil {
		t.Fatal("validateOnly create persisted the dashboard")
	}
}

// TestCodecDashboardRouting pins the version split: /v1 dashboards route to the
// v1 handlers, every other /v1 path stays 404, a /v3 dashboards path stays 404
// (dashboards are not a v3 collection), and the v3 surface is unchanged.
func TestCodecDashboardRouting(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method, path string
		wantAction   string
	}{
		{http.MethodGet, "/v1/projects/test/dashboards", "ListDashboards"},
		{http.MethodPost, "/v1/projects/test/dashboards", "CreateDashboard"},
		{http.MethodGet, "/v1/projects/test/dashboards/d1", "GetDashboard"},
		{http.MethodPatch, "/v1/projects/test/dashboards/d1", "UpdateDashboard"},
		{http.MethodDelete, "/v1/projects/test/dashboards/d1", "DeleteDashboard"},
		{http.MethodGet, "/v3/projects/test/alertPolicies", "ListAlertPolicies"},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, tc.path, nil)
		nr, err := c.Decode(req, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.wantAction {
			t.Errorf("%s %s action = %q, want %q", tc.method, tc.path, nr.Action, tc.wantAction)
		}
		if tc.path[:3] == "/v1" && nr.Params["apiVersion"] != "v1" {
			t.Errorf("%s: apiVersion = %v", tc.path, nr.Params["apiVersion"])
		}
	}

	for _, tc := range []struct {
		method, path string
		wantStatus   int
	}{
		{http.MethodGet, "/v1/projects/test/alertPolicies", 404},
		{http.MethodGet, "/v1/projects/test/dashboards/d1/revisions", 404},
		{http.MethodPut, "/v1/projects/test/dashboards", 404},
		{http.MethodGet, "/v3/projects/test/dashboards", 404},
		{http.MethodGet, "/v3/uptimeCheckIps", 501},
	} {
		req, _ := http.NewRequest(tc.method, tc.path, nil)
		_, err := c.Decode(req, nil)
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.HTTPStatus != tc.wantStatus {
			t.Errorf("%s %s err = %v, want status %d", tc.method, tc.path, err, tc.wantStatus)
		}
	}
}

// TestDashboardCreatePreservesRawNumbers pins the opacity guarantee: the body
// is stored from the codec's captured raw bytes, so an integer beyond 2^53 and
// a high-precision decimal are not re-rendered through the float64 decode that
// builds nr.Params["body"].
func TestDashboardCreatePreservesRawNumbers(t *testing.T) {
	c, p := newDashboardProvider()
	const raw = `{"displayName":"Numbers","mosaicLayout":{"columns":12,"tiles":[{"xPos":0,"yPos":0,"width":6,"height":4,"widget":{"title":"numbers","bigInt":9007199254740993,"preciseDecimal":0.1000000000000000055511151231257827}}]}}`

	req, err := http.NewRequest(http.MethodPost, "/v1/projects/test/dashboards", bytes.NewReader([]byte(raw)))
	if err != nil {
		t.Fatal(err)
	}
	nr, err := c.Decode(req, []byte(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp, err := p.Routes()["Monitoring.CreateDashboard"](t.Context(), nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	doc, ok := resp.Data[wire.RawJSONKey].(json.RawMessage)
	if !ok {
		t.Fatalf("response is not a raw dashboard document: %+v", resp.Data)
	}
	if !strings.Contains(string(doc), "9007199254740993") {
		t.Fatalf("large integer was re-rendered: %s", doc)
	}
	if !strings.Contains(string(doc), "0.1000000000000000055511151231257827") {
		t.Fatalf("precise decimal was re-rendered: %s", doc)
	}

	// Decode with UseNumber to compare the nested numbers exactly, not as float64.
	var decoded struct {
		MosaicLayout struct {
			Tiles []struct {
				Widget struct {
					BigInt         json.Number `json:"bigInt"`
					PreciseDecimal json.Number `json:"preciseDecimal"`
				} `json:"widget"`
			} `json:"tiles"`
		} `json:"mosaicLayout"`
	}
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	widget := decoded.MosaicLayout.Tiles[0].Widget
	if got := widget.BigInt.String(); got != "9007199254740993" {
		t.Fatalf("bigInt = %s", got)
	}
	if got := widget.PreciseDecimal.String(); got != "0.1000000000000000055511151231257827" {
		t.Fatalf("preciseDecimal = %s", got)
	}

	// The stored document (get) is byte-identical too.
	name := dashboardData(t, resp)["name"].(string)
	got, err := call(t, c, p, http.MethodGet, "/v1/"+name, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	gotDoc, _ := got.Data[wire.RawJSONKey].(json.RawMessage)
	if string(gotDoc) != string(doc) {
		t.Fatalf("get document differs from create document:\ncreate: %s\nget:    %s", doc, gotDoc)
	}
}

// TestDashboardEncodeReturnsDocument verifies the get/create/patch responses are
// the stored document itself (the codec's raw-JSON path), not a re-interpreted
// shape.
func TestDashboardEncodeReturnsDocument(t *testing.T) {
	c, p := newDashboardProvider()
	resp, err := call(t, c, p, http.MethodPost, "/v1/projects/test/dashboards", map[string]any{
		"displayName": "Opacity",
		"mosaicLayout": map[string]any{
			"columns": 12,
			"tiles":   []any{map[string]any{"widget": map[string]any{"scorecard": map[string]any{"timeSeriesQuery": map[string]any{"unitOverride": "1"}}}}},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	nr := &model.NormalizedRequest{}
	status, _, body := c.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("encode status = %d", status)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("encoded body is not JSON: %v", err)
	}
	if doc["displayName"] != "Opacity" {
		t.Fatalf("encoded body = %s", body)
	}
	if !reflect.DeepEqual(doc["mosaicLayout"], map[string]any{
		"columns": float64(12),
		"tiles":   []any{map[string]any{"widget": map[string]any{"scorecard": map[string]any{"timeSeriesQuery": map[string]any{"unitOverride": "1"}}}}},
	}) {
		t.Fatalf("mosaicLayout not encoded verbatim: %#v", doc["mosaicLayout"])
	}
}

package monitoring

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newDashboardService() *Service {
	return NewService(nil, "test", WithResources(store.NewMemoryResourceStore()))
}

// dashboardErrStatus asserts the error is a ProviderError with the given HTTP
// status and (when non-empty) code.
func dashboardErrStatus(t *testing.T, err error, status int, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with status %d, got nil", status)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("error %v is not a *model.ProviderError", err)
	}
	if perr.HTTPStatus != status {
		t.Fatalf("error status = %d, want %d (%v)", perr.HTTPStatus, status, err)
	}
	if code != "" && perr.Code != code {
		t.Fatalf("error code = %q, want %q", perr.Code, code)
	}
}

// dashboardDocument returns the stored document decoded as a generic map.
func dashboardDocument(t *testing.T, d Dashboard) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(d.Document, &m); err != nil {
		t.Fatalf("decode dashboard document: %v", err)
	}
	return m
}

const dashboardLayout = `{
	"displayName": "Checkout",
	"gridLayout": {
		"columns": "2",
		"widgets": [
			{"title": "Requests", "xyChart": {"dataSets": [{"timeSeriesQuery": {"prometheusQuery": "rate(requests_total[5m])"}}]}}
		]
	},
	"dashboardFilters": [{"filterType": "VALUE_ONLY", "labelKey": "env"}],
	"labels": {"team": "checkout"}
}`

func TestCreateDashboardGeneratesID(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	d, err := s.CreateDashboard(ctx, "test", json.RawMessage(dashboardLayout), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if d.ID == "" {
		t.Fatal("server-generated dashboard id is empty")
	}
	if want := DashboardName("test", d.ID); d.Name != want {
		t.Fatalf("name = %q, want %q", d.Name, want)
	}
	if d.DisplayName != "Checkout" {
		t.Fatalf("displayName = %q", d.DisplayName)
	}
	if d.Etag == "" {
		t.Fatal("created dashboard has no etag")
	}
	doc := dashboardDocument(t, d)
	if doc["name"] != d.Name {
		t.Fatalf("document name = %v", doc["name"])
	}
	// The opaque layout/widgets are stored verbatim.
	var want map[string]any
	if err := json.Unmarshal([]byte(dashboardLayout), &want); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"gridLayout", "dashboardFilters", "labels"} {
		if !reflect.DeepEqual(doc[key], want[key]) {
			t.Fatalf("%s not stored verbatim: got %#v want %#v", key, doc[key], want[key])
		}
	}

	got, err := s.GetDashboard(ctx, "test", d.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Etag != d.Etag || got.Name != d.Name {
		t.Fatalf("get = %+v, want etag %q name %q", got, d.Etag, d.Name)
	}
}

func TestCreateDashboardHonoursExplicitName(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	body := json.RawMessage(`{"name":"projects/test/dashboards/my-dash","displayName":"Mine"}`)
	d, err := s.CreateDashboard(ctx, "test", body, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if d.ID != "my-dash" || d.Name != "projects/test/dashboards/my-dash" {
		t.Fatalf("explicit name not honoured: %+v", d)
	}

	// A duplicate explicit id is ALREADY_EXISTS.
	_, err = s.CreateDashboard(ctx, "test", body, false)
	dashboardErrStatus(t, err, 409, "AlreadyExists")
}

func TestCreateDashboardValidation(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	cases := map[string]string{
		"wrong project":   `{"name":"projects/other/dashboards/d1","displayName":"x"}`,
		"malformed name":  `{"name":"dashboards/d1","displayName":"x"}`,
		"empty name id":   `{"name":"projects/test/dashboards/","displayName":"x"}`,
		"extra segments":  `{"name":"projects/test/dashboards/a/b","displayName":"x"}`,
		"non-string name": `{"name":5,"displayName":"x"}`,
		"missing display": `{"name":"projects/test/dashboards/d1"}`,
		"empty display":   `{"name":"projects/test/dashboards/d1","displayName":""}`,
		"non-string disp": `{"displayName":true}`,
		"empty body":      ``,
		"array body":      `[]`,
		"null body":       `null`,
	}
	for name, body := range cases {
		_, err := s.CreateDashboard(ctx, "test", json.RawMessage(body), false)
		if err == nil {
			t.Errorf("%s: expected INVALID_ARGUMENT, got nil", name)
			continue
		}
		dashboardErrStatus(t, err, 400, "InvalidArgument")
	}
}

func TestDashboardListGetDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	for _, id := range []string{"a", "b", "c"} {
		body := json.RawMessage(`{"name":"projects/test/dashboards/` + id + `","displayName":"` + id + `"}`)
		if _, err := s.CreateDashboard(ctx, "test", body, false); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	// A dashboard in another project is not listed.
	other := json.RawMessage(`{"displayName":"other"}`)
	if _, err := s.CreateDashboard(ctx, "other", other, false); err != nil {
		t.Fatalf("create other: %v", err)
	}

	page, next, err := s.ListDashboards(ctx, "test", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 3 || next != "" {
		t.Fatalf("list = %d dashboards next=%q, want 3 and no token", len(page), next)
	}
	for i, want := range []string{"a", "b", "c"} {
		if page[i].ID != want {
			t.Fatalf("page[%d].ID = %q, want %q (ordered by id)", i, page[i].ID, want)
		}
	}

	// Pagination: pageSize 2 -> [a b] + token, then [c].
	first, token, err := s.ListDashboards(ctx, "test", 2, "")
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(first) != 2 || token == "" {
		t.Fatalf("page 1 = %d dashboards token=%q", len(first), token)
	}
	second, token2, err := s.ListDashboards(ctx, "test", 2, token)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(second) != 1 || token2 != "" || second[0].ID != "c" {
		t.Fatalf("page 2 = %+v token=%q", second, token2)
	}
	if _, _, err := s.ListDashboards(ctx, "test", -1, ""); err == nil {
		t.Fatal("negative pageSize should be rejected")
	} else {
		dashboardErrStatus(t, err, 400, "InvalidArgument")
	}

	if _, err := s.GetDashboard(ctx, "test", "missing"); err == nil {
		t.Fatal("get missing should be NOT_FOUND")
	} else {
		dashboardErrStatus(t, err, 404, "NotFound")
	}

	if err := s.DeleteDashboard(ctx, "test", "b"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetDashboard(ctx, "test", "b"); err == nil {
		t.Fatal("get after delete should be NOT_FOUND")
	}
	if err := s.DeleteDashboard(ctx, "test", "b"); err == nil {
		t.Fatal("delete missing should be NOT_FOUND")
	} else {
		dashboardErrStatus(t, err, 404, "NotFound")
	}
}

func TestUpdateDashboardFullReplaceAndEtag(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	created, err := s.CreateDashboard(ctx, "test", json.RawMessage(dashboardLayout), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	name := created.Name

	replacement := `{
		"name": "` + name + `",
		"etag": "` + created.Etag + `",
		"displayName": "Checkout v2",
		"mosaicLayout": {"columns": 12, "tiles": [{"xPos": 0, "yPos": 0, "width": 6, "height": 4, "widget": {"title": "Latency"}}]}
	}`
	updated, err := s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(replacement), false)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.DisplayName != "Checkout v2" {
		t.Fatalf("displayName = %q", updated.DisplayName)
	}
	if updated.Etag == created.Etag || updated.Etag == "" {
		t.Fatalf("update did not mint a fresh etag (old %q, new %q)", created.Etag, updated.Etag)
	}
	doc := dashboardDocument(t, updated)
	if _, ok := doc["gridLayout"]; ok {
		t.Fatal("patch must replace the whole document: stale gridLayout survived")
	}
	var wantMosaic map[string]any
	if err := json.Unmarshal([]byte(`{"columns": 12, "tiles": [{"xPos": 0, "yPos": 0, "width": 6, "height": 4, "widget": {"title": "Latency"}}]}`), &wantMosaic); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(doc["mosaicLayout"], wantMosaic) {
		t.Fatalf("mosaicLayout not stored verbatim: %#v", doc["mosaicLayout"])
	}

	// The stored etag is what get returns, and the old etag is now stale.
	got, err := s.GetDashboard(ctx, "test", created.ID)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.Etag != updated.Etag {
		t.Fatalf("get etag = %q, want %q", got.Etag, updated.Etag)
	}

	stale := `{"name":"` + name + `","etag":"` + created.Etag + `","displayName":"stale write"}`
	_, err = s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(stale), false)
	dashboardErrStatus(t, err, 409, "Aborted")
	if got, _ := s.GetDashboard(ctx, "test", created.ID); got.DisplayName != "Checkout v2" {
		t.Fatalf("stale update mutated the dashboard: %q", got.DisplayName)
	}

	// A matching etag succeeds and mints another fresh one.
	next := `{"name":"` + name + `","etag":"` + updated.Etag + `","displayName":"Checkout v3"}`
	third, err := s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(next), false)
	if err != nil {
		t.Fatalf("etag-matched update: %v", err)
	}
	if third.Etag == updated.Etag {
		t.Fatal("second update reused the etag")
	}

	// Omitting the etag is allowed (no precondition).
	noEtag := `{"name":"` + name + `","displayName":"Checkout v4"}`
	if _, err := s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(noEtag), false); err != nil {
		t.Fatalf("etag-free update: %v", err)
	}
}

func TestUpdateDashboardValidation(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()
	created, err := s.CreateDashboard(ctx, "test", json.RawMessage(dashboardLayout), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Body name must equal the path name when supplied.
	_, err = s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(`{"name":"projects/test/dashboards/other","displayName":"x"}`), false)
	dashboardErrStatus(t, err, 400, "InvalidArgument")
	// Omitting the name is the documented update shape (the Terraform provider
	// patches the dashboard JSON plus its etag, with no name): the path-bound
	// name is filled in.
	updated, err := s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(`{"displayName":"Renamed"}`), false)
	if err != nil {
		t.Fatalf("name-less update: %v", err)
	}
	if updated.Name != created.Name || updated.DisplayName != "Renamed" {
		t.Fatalf("name-less update = %+v", updated)
	}
	// Empty displayName is rejected on update too.
	_, err = s.UpdateDashboard(ctx, "test", created.ID, json.RawMessage(`{"name":"`+created.Name+`","displayName":""}`), false)
	dashboardErrStatus(t, err, 400, "InvalidArgument")
	// A missing dashboard is NOT_FOUND.
	_, err = s.UpdateDashboard(ctx, "test", "missing", json.RawMessage(`{"name":"projects/test/dashboards/missing","displayName":"x"}`), false)
	dashboardErrStatus(t, err, 404, "NotFound")
}

func TestDashboardValidateOnlyDoesNotPersist(t *testing.T) {
	ctx := context.Background()
	s := newDashboardService()

	preview, err := s.CreateDashboard(ctx, "test", json.RawMessage(`{"name":"projects/test/dashboards/preview","displayName":"x"}`), true)
	if err != nil {
		t.Fatalf("validateOnly create: %v", err)
	}
	if preview.ID != "preview" {
		t.Fatalf("preview id = %q", preview.ID)
	}
	if _, err := s.GetDashboard(ctx, "test", "preview"); err == nil {
		t.Fatal("validateOnly create persisted the dashboard")
	}

	created, err := s.CreateDashboard(ctx, "test", json.RawMessage(dashboardLayout), false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	preview, err = s.UpdateDashboard(ctx, "test", created.ID,
		json.RawMessage(`{"name":"`+created.Name+`","displayName":"Preview"} `), true)
	if err != nil {
		t.Fatalf("validateOnly update: %v", err)
	}
	if preview.DisplayName != "Preview" {
		t.Fatalf("preview displayName = %q", preview.DisplayName)
	}
	stored, err := s.GetDashboard(ctx, "test", created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.DisplayName != "Checkout" || stored.Etag != created.Etag {
		t.Fatalf("validateOnly update mutated stored dashboard: %+v", stored)
	}
}

func TestDashboardRequiresResourceStore(t *testing.T) {
	ctx := context.Background()
	s := NewService(nil, "test")
	_, _, err := s.ListDashboards(ctx, "test", 0, "")
	if err == nil {
		t.Fatal("a core without a ResourceStore must fail loud")
	}
	dashboardErrStatus(t, err, 501, "Unimplemented")
}

package datastoreui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func newTestRouter() http.Handler {
	svc := core.NewService(dsstore.NewMemoryStore(), "test-project")
	return BuildRouter(svc, testCfg())
}

// do runs one request against a fresh router.
func do(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	newTestRouter().ServeHTTP(w, r)
	return w
}

// keyQuery renders a KeyRef as the ?key= parameter value.
func keyQuery(t *testing.T, ref KeyRef) string {
	t.Helper()
	b, err := json.Marshal(ref)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return url.QueryEscape(string(b))
}

// putEntity upserts an entity through the API and returns the decoded result.
func putEntity(t *testing.T, ref KeyRef, props map[string]any) Entity {
	t.Helper()
	body, err := json.Marshal(UpsertEntityRequest{Key: ref, Properties: props})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	w := do(t, http.MethodPut, "/entity", string(body))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT /entity status = %d: %s", w.Code, w.Body.String())
	}
	var got Entity
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode entity: %v", err)
	}
	return got
}

func TestKindsList(t *testing.T) {
	router := newTestRouter()

	// Two entities of one kind and one of another.
	for _, tc := range []struct {
		ref   KeyRef
		props map[string]any
	}{
		{KeyRef{Path: []KeyElement{{Kind: "Task", Name: "a"}}}, map[string]any{"n": map[string]any{"integerValue": "1"}}},
		{KeyRef{Path: []KeyElement{{Kind: "Task", Name: "b"}}}, map[string]any{"n": map[string]any{"integerValue": "2"}}},
		{KeyRef{Path: []KeyElement{{Kind: "User", Name: "u"}}}, map[string]any{"email": map[string]any{"stringValue": "u@example.com"}}},
	} {
		body, _ := json.Marshal(UpsertEntityRequest{Key: tc.ref, Properties: tc.props})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
		if w.Code != http.StatusOK {
			t.Fatalf("seed %v: %d %s", tc.ref, w.Code, w.Body.String())
		}
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/kinds", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET /kinds status = %d: %s", w.Code, w.Body.String())
	}
	var resp ListKindsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode kinds: %v", err)
	}
	if resp.Total != 2 || len(resp.Kinds) != 2 {
		t.Fatalf("kinds = %+v, want Task, User", resp)
	}
	if resp.Kinds[0].Name != "Task" || resp.Kinds[1].Name != "User" {
		t.Fatalf("kinds = %+v", resp.Kinds)
	}
}

func TestEntityCRUDRoundTrip(t *testing.T) {
	router := newTestRouter()
	ref := KeyRef{Path: []KeyElement{{Kind: "Task", Name: "a"}}}
	props := map[string]any{
		"n":    map[string]any{"integerValue": "7"},
		"done": map[string]any{"booleanValue": true},
		"tags": map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"stringValue": "x"},
			map[string]any{"stringValue": "y"},
		}}},
	}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: props})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", w.Code, w.Body.String())
	}
	var created Entity
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.Kind != "Task" || len(created.Key.Path) != 1 || created.Key.Path[0].Name != "a" {
		t.Fatalf("created key = %+v", created.Key)
	}
	if created.Version == "" {
		t.Fatalf("expected a version, got %+v", created)
	}

	// GET returns the same entity with values round-tripped.
	gw := httptest.NewRecorder()
	router.ServeHTTP(gw, httptest.NewRequest(http.MethodGet, "/entity?key="+keyQuery(t, ref), nil))
	if gw.Code != http.StatusOK {
		t.Fatalf("GET status = %d: %s", gw.Code, gw.Body.String())
	}
	var got Entity
	if err := json.Unmarshal(gw.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if v, ok := got.Properties["n"].(map[string]any); !ok || v["integerValue"] != "7" {
		t.Fatalf("n = %#v, want integerValue 7 string", got.Properties["n"])
	}

	// DELETE then GET -> 404.
	dw := httptest.NewRecorder()
	router.ServeHTTP(dw, httptest.NewRequest(http.MethodDelete, "/entity?key="+keyQuery(t, ref), nil))
	if dw.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d: %s", dw.Code, dw.Body.String())
	}
	gw2 := httptest.NewRecorder()
	router.ServeHTTP(gw2, httptest.NewRequest(http.MethodGet, "/entity?key="+keyQuery(t, ref), nil))
	if gw2.Code != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", gw2.Code)
	}
}

// TestUpsertCanonicalizesTimestamp pins TD42: a timestampValue written through
// the console is canonicalized the same way the transports canonicalize it
// (UTC, microsecond precision, protojson fraction spelling), so the shared store
// holds one form no matter which path wrote it. A malformed value is rejected.
func TestUpsertCanonicalizesTimestamp(t *testing.T) {
	router := newTestRouter()
	ref := KeyRef{Path: []KeyElement{{Kind: "Task", Name: "ts"}}}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{
		"t": map[string]any{"timestampValue": "2019-12-31T19:00:00.123456789-05:00"},
	}})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", w.Code, w.Body.String())
	}
	var created Entity
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	v, _ := created.Properties["t"].(map[string]any)
	if v["timestampValue"] != "2020-01-01T00:00:00.123456Z" {
		t.Fatalf("timestampValue = %v, want 2020-01-01T00:00:00.123456Z", v["timestampValue"])
	}

	// A malformed timestamp is rejected rather than stored verbatim.
	bad, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{
		"t": map[string]any{"timestampValue": "not-a-time"},
	}})
	bw := httptest.NewRecorder()
	router.ServeHTTP(bw, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(bad))))
	if bw.Code != http.StatusBadRequest {
		t.Fatalf("PUT bad timestamp status = %d, want 400: %s", bw.Code, bw.Body.String())
	}
}

func TestUpsertAutoAllocatesID(t *testing.T) {
	router := newTestRouter()
	// An incomplete final element (no id/name) makes the core allocate one.
	ref := KeyRef{Path: []KeyElement{{Kind: "Task"}}}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{"n": map[string]any{"integerValue": "1"}}})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", w.Code, w.Body.String())
	}
	var created Entity
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(created.Key.Path) != 1 || created.Key.Path[0].ID == "" {
		t.Fatalf("expected an allocated id, got %+v", created.Key)
	}
}

func TestListEntitiesPagination(t *testing.T) {
	router := newTestRouter()
	for _, name := range []string{"a", "b", "c"} {
		ref := KeyRef{Path: []KeyElement{{Kind: "Task", Name: name}}}
		body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{"n": map[string]any{"integerValue": "1"}}})
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
		if w.Code != http.StatusOK {
			t.Fatalf("seed %s: %d", name, w.Code)
		}
	}

	page := func(token string) ListEntitiesResponse {
		t.Helper()
		path := "/kinds/Task/entities?pageSize=2"
		if token != "" {
			path += "&pageToken=" + url.QueryEscape(token)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("list status = %d: %s", w.Code, w.Body.String())
		}
		var resp ListEntitiesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		return resp
	}

	first := page("")
	if len(first.Entities) != 2 || first.NextPageToken == "" {
		t.Fatalf("first page = %d entities, next=%q", len(first.Entities), first.NextPageToken)
	}
	second := page(first.NextPageToken)
	if len(second.Entities) != 1 || second.NextPageToken != "" {
		t.Fatalf("second page = %d entities, next=%q", len(second.Entities), second.NextPageToken)
	}
}

func TestListProperties(t *testing.T) {
	router := newTestRouter()
	ref := KeyRef{Path: []KeyElement{{Kind: "Task", Name: "a"}}}
	props := map[string]any{
		"name": map[string]any{"stringValue": "write"},
		"done": map[string]any{"booleanValue": true},
	}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: props})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", w.Code, w.Body.String())
	}

	pw := httptest.NewRecorder()
	router.ServeHTTP(pw, httptest.NewRequest(http.MethodGet, "/kinds/Task/properties", nil))
	if pw.Code != http.StatusOK {
		t.Fatalf("GET properties status = %d: %s", pw.Code, pw.Body.String())
	}
	var resp ListPropertiesResponse
	if err := json.Unmarshal(pw.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode properties: %v", err)
	}
	got := map[string][]string{}
	for _, p := range resp.Properties {
		got[p.Name] = p.Representations
	}
	if len(got) != 2 {
		t.Fatalf("properties = %v, want name/done", got)
	}
	if len(got["name"]) != 1 || got["name"][0] != "STRING" {
		t.Fatalf("name reps = %v", got["name"])
	}
	if len(got["done"]) != 1 || got["done"][0] != "BOOLEAN" {
		t.Fatalf("done reps = %v", got["done"])
	}
}

func TestRunGQLQuery(t *testing.T) {
	router := newTestRouter()
	ref := KeyRef{Path: []KeyElement{{Kind: "Task", Name: "a"}}}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{"n": map[string]any{"integerValue": "1"}}})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/entity", strings.NewReader(string(body))))
	if w.Code != http.StatusOK {
		t.Fatalf("seed: %d", w.Code)
	}

	qbody, _ := json.Marshal(GQLQueryRequest{QueryString: "SELECT * FROM Task"})
	qw := httptest.NewRecorder()
	router.ServeHTTP(qw, httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(qbody))))
	if qw.Code != http.StatusOK {
		t.Fatalf("POST /query status = %d: %s", qw.Code, qw.Body.String())
	}
	var resp QueryResponse
	if err := json.Unmarshal(qw.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode query: %v", err)
	}
	if resp.Total != 1 || resp.Entities[0].Kind != "Task" {
		t.Fatalf("query response = %+v", resp)
	}
}

func TestReservedKindWriteRejected(t *testing.T) {
	ref := KeyRef{Path: []KeyElement{{Kind: "__secret__", Name: "x"}}}
	body, _ := json.Marshal(UpsertEntityRequest{Key: ref, Properties: map[string]any{}})
	w := do(t, http.MethodPut, "/entity", string(body))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("reserved kind PUT status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

//go:build gcp_conformance

package gcpconformance

import "testing"

func TestMatchMethod(t *testing.T) {
	docs := loadDocs(t)
	ix := BuildMethodIndex(docs)

	cases := []struct {
		name        string
		httpMethod  string
		path        string
		wantService string
		wantMethod  string
	}{
		{
			name:       "storage object get strips query",
			httpMethod: "GET",
			path:       "/storage/v1/b/my-bucket/o/dir%2Ffile.txt?alt=media",
			wantMethod: "storage.objects.get",
		},
		{
			name:        "storage object insert via upload prefix",
			httpMethod:  "POST",
			path:        "/upload/storage/v1/b/my-bucket/o?uploadType=media&name=x",
			wantService: "storage",
			wantMethod:  "storage.objects.insert",
		},
		{
			name:        "pubsub custom method suffix",
			httpMethod:  "POST",
			path:        "/v1/projects/p/topics/t:publish",
			wantService: "pubsub",
			wantMethod:  "pubsub.projects.topics.publish",
		},
		{
			name:        "kms getIamPolicy beats bare get",
			httpMethod:  "GET",
			path:        "/v1/projects/p/locations/global/keyRings/r:getIamPolicy",
			wantService: "kms",
			wantMethod:  "cloudkms.projects.locations.keyRings.getIamPolicy",
		},
		{
			name:        "clouddns literal template",
			httpMethod:  "GET",
			path:        "/dns/v1/projects/p/managedZones/z/rrsets",
			wantService: "clouddns",
			wantMethod:  "dns.resourceRecordSets.list",
		},
		{
			name:        "bigquery dataset get",
			httpMethod:  "GET",
			path:        "/bigquery/v2/projects/p/datasets/ds",
			wantService: "bigquery",
			wantMethod:  "bigquery.datasets.get",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, m, ok := ix.MatchMethod(tc.httpMethod, tc.path)
			if !ok {
				t.Fatalf("no match for %s %s", tc.httpMethod, tc.path)
			}
			if m.ID != tc.wantMethod {
				t.Fatalf("matched %s, want %s", m.ID, tc.wantMethod)
			}
			if doc.Name == "" {
				t.Fatalf("matched doc has no name")
			}
		})
	}

	if _, _, ok := ix.MatchMethod("GET", "/totally/unknown/service/path"); ok {
		t.Fatal("expected no match for unknown path")
	}
}

// TestCommonLocationGetMatchesLocationSchema guards the synthetic
// projects.locations.get entry: a bare GetLocation request must validate
// against the Location schema, not fuzzy-match a flat /{version}/{+name}
// method (which produced false unknown_field divergences for displayName /
// locationId).
func TestCommonLocationGetMatchesLocationSchema(t *testing.T) {
	docs := loadDocs(t)
	ix := BuildMethodIndex(docs)
	doc, m, ok := ix.MatchMethodInService("functions", "GET", "/v1/projects/p/locations/us-central1")
	if !ok {
		t.Fatal("GetLocation path did not match any method")
	}
	if m.Response == nil || m.Response.Ref != "Location" {
		t.Fatalf("matched %s with response %+v, want Location", m.ID, m.Response)
	}
	if _, ok := doc.ResolveRef("Location"); !ok {
		t.Fatal("Location schema not resolvable")
	}
	// A deeper resource path under locations/{location} must not match the
	// synthetic GetLocation method.
	if _, m, ok := ix.MatchMethodInService("functions", "GET", "/v1/projects/p/locations/us-central1/functions"); ok && m.Response != nil && m.Response.Ref == "Location" {
		t.Fatalf("functions list matched the Location method: %s", m.ID)
	}
}

// TestMonitoringV1DashboardsMerged pins the multi-version snapshot merge: the
// v1 dashboards document is folded into the v3 "monitoring" document, so the
// dashboards methods are matchable under the same service key while the
// primary document keeps its version/rootUrl and every v3 method.
func TestMonitoringV1DashboardsMerged(t *testing.T) {
	docs := loadDocs(t)
	doc := docs["monitoring"]
	if doc == nil {
		t.Fatal("monitoring doc not loaded")
	}
	if doc.Version != "v3" {
		t.Fatalf("primary version = %q, want v3", doc.Version)
	}
	if _, ok := docs["monitoring@v1"]; ok {
		t.Fatal("versioned snapshot must merge into the primary, not stay a separate key")
	}

	ix := BuildMethodIndex(docs)
	cases := []struct{ method, path, want string }{
		{"GET", "/v1/projects/p/dashboards", "monitoring.projects.dashboards.list"},
		{"POST", "/v1/projects/p/dashboards", "monitoring.projects.dashboards.create"},
		{"GET", "/v1/projects/p/dashboards/d1", "monitoring.projects.dashboards.get"},
		{"PATCH", "/v1/projects/p/dashboards/d1", "monitoring.projects.dashboards.patch"},
		{"DELETE", "/v1/projects/p/dashboards/d1", "monitoring.projects.dashboards.delete"},
		// The v3 methods survive the merge and still match under monitoring.
		{"GET", "/v3/projects/p/alertPolicies", "monitoring.projects.alertPolicies.list"},
	}
	for _, tc := range cases {
		_, m, ok := ix.MatchMethodInService("monitoring", tc.method, tc.path)
		if !ok {
			t.Errorf("%s %s: no match under monitoring", tc.method, tc.path)
			continue
		}
		if m.ID != tc.want {
			t.Errorf("%s %s matched %s, want %s", tc.method, tc.path, m.ID, tc.want)
		}
	}
	// The merged schemas make the dashboard response validatable.
	for _, schema := range []string{"Dashboard", "ListDashboardsResponse", "GridLayout"} {
		if _, ok := doc.ResolveRef(schema); !ok {
			t.Errorf("merged document cannot resolve schema %q", schema)
		}
	}
	// Every merged method resolves against the primary document.
	doc.WalkMethods(func(m *Method) {
		if m.doc != doc {
			t.Errorf("method %s is not attached to the primary document", m.ID)
		}
	})
}

// TestMergeDiscoveryVersionPrefersPrimary covers the merge rule directly: a
// method or schema the primary already declares is never overwritten.
func TestMergeDiscoveryVersionPrefersPrimary(t *testing.T) {
	primary := &DiscoveryDoc{
		Version: "v3",
		Resources: map[string]*Resource{
			"projects": {Methods: map[string]*Method{"get": {ID: "primary.get"}}, Resources: map[string]*Resource{}},
		},
		Schemas: map[string]*Schema{"Shared": {Type: "primary"}},
	}
	secondary := &DiscoveryDoc{
		Version: "v1",
		Resources: map[string]*Resource{
			"projects": {
				Methods: map[string]*Method{"get": {ID: "secondary.get"}, "create": {ID: "secondary.create"}},
				Resources: map[string]*Resource{
					"dashboards": {Methods: map[string]*Method{"list": {ID: "secondary.dashboards.list"}}},
				},
			},
		},
		Schemas: map[string]*Schema{"Shared": {Type: "secondary"}, "Extra": {Type: "extra"}},
	}
	mergeDiscoveryVersion(primary, secondary)

	if got := primary.Resources["projects"].Methods["get"].ID; got != "primary.get" {
		t.Errorf("primary method overwritten: %s", got)
	}
	if got := primary.Resources["projects"].Methods["create"].ID; got != "secondary.create" {
		t.Errorf("secondary method not merged: %s", got)
	}
	if got := primary.Resources["projects"].Resources["dashboards"].Methods["list"].ID; got != "secondary.dashboards.list" {
		t.Errorf("nested resource not merged: %s", got)
	}
	if got := primary.Schemas["Shared"].Type; got != "primary" {
		t.Errorf("primary schema overwritten: %s", got)
	}
	if got := primary.Schemas["Extra"].Type; got != "extra" {
		t.Errorf("secondary schema not merged: %s", got)
	}
	if primary.Version != "v3" {
		t.Errorf("primary version changed: %s", primary.Version)
	}
	if primary.Resources["projects"].Methods["create"].doc != primary {
		t.Error("merged method not attached to the primary document")
	}
}

// TestMergeDiscoveryResourcesInitialisesNilChildMaps covers a nil destination
// map at both levels: a nil document resource map and a primary node whose
// nested resource map is nil must gain the secondary's children rather than
// panic on assignment.
func TestMergeDiscoveryResourcesInitialisesNilChildMaps(t *testing.T) {
	secondary := map[string]*Resource{
		"projects": {
			Methods: map[string]*Method{"get": {ID: "secondary.get"}},
			Resources: map[string]*Resource{
				"dashboards": {Methods: map[string]*Method{"list": {ID: "secondary.dashboards.list"}}},
			},
		},
	}
	primary := map[string]*Resource{
		// No Resources map: the secondary's dashboards must be merged in.
		"projects": {Methods: map[string]*Method{"get": {ID: "primary.get"}}},
	}
	merged := mergeDiscoveryResources(primary, secondary)
	if merged["projects"].Resources == nil {
		t.Fatal("nil child-resource map was not initialized")
	}
	if got := merged["projects"].Resources["dashboards"].Methods["list"].ID; got != "secondary.dashboards.list" {
		t.Fatalf("nested method not merged: %q", got)
	}
	if got := merged["projects"].Methods["get"].ID; got != "primary.get" {
		t.Fatalf("primary method overwritten: %q", got)
	}

	// A nil destination document map is initialized from the secondary.
	fromNil := mergeDiscoveryResources(nil, secondary)
	if fromNil == nil || fromNil["projects"] == nil {
		t.Fatal("nil destination map was not initialized")
	}
	if got := fromNil["projects"].Resources["dashboards"].Methods["list"].ID; got != "secondary.dashboards.list" {
		t.Fatalf("nil-destination merge lost the nested method: %q", got)
	}
	if mergeDiscoveryResources(nil, nil) != nil {
		t.Fatal("empty merge should stay nil")
	}
}

func TestResolveRefForms(t *testing.T) {
	docs := loadDocs(t)
	storage := docs["storage"]
	if storage == nil {
		t.Fatal("storage doc not loaded")
	}
	if _, ok := storage.ResolveRef("Object"); !ok {
		t.Error("short ref Object did not resolve")
	}
	if _, ok := storage.ResolveRef("storage.v1.Object"); !ok {
		t.Error("qualified ref storage.v1.Object did not resolve")
	}
	if _, ok := storage.ResolveRef("not.a.schema"); ok {
		t.Error("bogus ref resolved")
	}
}

func TestValidateValueBasic(t *testing.T) {
	docs := loadDocs(t)
	storage := docs["storage"]
	object, ok := storage.ResolveRef("Object")
	if !ok {
		t.Fatal("Object schema missing")
	}

	divs := ValidateValue(storage, object, map[string]any{
		"name":       "x",
		"size":       5,          // wrong type: Object.size is a string
		"bogusField": "surprise", // unknown field -> info
	}, "obj")
	if len(divs) != 2 {
		t.Fatalf("got %d divergences, want 2: %+v", len(divs), divs)
	}
	kinds := map[string]string{}
	for _, d := range divs {
		kinds[d.Kind] = d.Severity
	}
	if kinds[kindWrongType] != "high" {
		t.Errorf("size should be wrong_type/high, got %v", kinds)
	}
	if kinds[kindUnknownField] != "info" {
		t.Errorf("bogusField should be unknown_field/info, got %v", kinds)
	}
}

func TestValidateErrorEnvelope(t *testing.T) {
	good := []byte(`{"error":{"code":404,"message":"not found","status":"NOT_FOUND","errors":[{"reason":"notFound","domain":"global","message":"not found"}]}}`)
	if divs := ValidateErrorEnvelope(404, good); len(divs) != 0 {
		t.Fatalf("valid envelope reported divergences: %+v", divs)
	}

	bad := []byte(`{"error":{"code":200,"message":"ok"}}`)
	divs := ValidateErrorEnvelope(404, bad)
	if len(divs) == 0 {
		t.Fatal("expected divergences for mismatched code and missing status/errors")
	}
	var sawCode bool
	for _, d := range divs {
		if d.Path == "error.code" && d.Kind == kindBadErrorEnvelope {
			sawCode = true
		}
	}
	if !sawCode {
		t.Errorf("expected error.code mismatch divergence, got %+v", divs)
	}
}

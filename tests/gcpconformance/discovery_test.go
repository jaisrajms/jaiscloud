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

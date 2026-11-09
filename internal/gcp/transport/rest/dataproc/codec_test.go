package dataproc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCodecDeriveAction covers the REST path → action routing for the clusters
// and jobs surface, including the jobs.patch (UpdateJob) route and the
// custom-method forms.
func TestCodecDeriveAction(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/clusters", "CreateCluster"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/clusters", "ListClusters"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/clusters/c1", "GetCluster"},
		{http.MethodPatch, "/v1/projects/proj/regions/us-central1/clusters/c1", "UpdateCluster"},
		{http.MethodDelete, "/v1/projects/proj/regions/us-central1/clusters/c1", "DeleteCluster"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/clusters/c1:start", "StartCluster"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/clusters/c1:stop", "StopCluster"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/clusters/c1:diagnose", "DiagnoseCluster"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/jobs", "ListJobs"},
		{http.MethodGet, "/v1/projects/proj/regions/us-central1/jobs/j1", "GetJob"},
		{http.MethodPatch, "/v1/projects/proj/regions/us-central1/jobs/j1", "UpdateJob"},
		{http.MethodDelete, "/v1/projects/proj/regions/us-central1/jobs/j1", "DeleteJob"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/jobs:submit", "SubmitJob"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/jobs:submitAsOperation", "SubmitJobAsOperation"},
		{http.MethodPost, "/v1/projects/proj/regions/us-central1/jobs/j1:cancel", "CancelJob"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader("{}"))
		nr, err := c.Decode(req, []byte("{}"))
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if nr.Action != tc.want {
			t.Fatalf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.want)
		}
	}

	// jobs.patch extracts the job id and updateMask.
	nr, err := c.Decode(httptest.NewRequest(http.MethodPatch, "/v1/projects/proj/regions/us-central1/jobs/j1?updateMask=labels", nil), nil)
	if err != nil {
		t.Fatalf("decode patch: %v", err)
	}
	if nr.Params["jobId"] != "j1" || nr.Params["updateMask"] != "labels" || nr.Params["region"] != "us-central1" {
		t.Fatalf("params = %v", nr.Params)
	}
}

// TestCodecWorkflowTemplateIAM covers the three workflowTemplates IAM custom
// verbs: they are POSTs (Dataproc's Discovery surface, unlike metastore's GET
// getIamPolicy), map onto the WorkflowTemplate* actions, and produce a canonical
// iamName with the custom suffix stripped.
func TestCodecWorkflowTemplateIAM(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		verb string
		want string
	}{
		{"getIamPolicy", "WorkflowTemplateGetIamPolicy"},
		{"setIamPolicy", "WorkflowTemplateSetIamPolicy"},
		{"testIamPermissions", "WorkflowTemplateTestIamPermissions"},
	}
	for _, tc := range cases {
		path := "/v1/projects/proj/regions/us-central1/workflowTemplates/t1:" + tc.verb
		nr, err := c.Decode(httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")), []byte("{}"))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		if nr.Action != tc.want {
			t.Fatalf("POST %s: action = %q, want %q", path, nr.Action, tc.want)
		}
		if nr.Params["workflowTemplateId"] != "t1" {
			t.Fatalf("POST %s: workflowTemplateId = %v", path, nr.Params["workflowTemplateId"])
		}
		iamName, _ := nr.Params["iamName"].(string)
		if want := "projects/proj/regions/us-central1/workflowTemplates/t1"; iamName != want {
			t.Fatalf("POST %s: iamName = %q, want %q", path, iamName, want)
		}
		if strings.Contains(iamName, ":") {
			t.Fatalf("POST %s: iamName retained a custom suffix: %q", path, iamName)
		}
	}

	// A GET is not the dataproc IAM shape, and an unknown custom verb must not
	// fall through to the get/patch method switch.
	if _, err := c.Decode(httptest.NewRequest(http.MethodPost, "/v1/projects/proj/regions/us-central1/workflowTemplates/t1:frobnicate", nil), nil); err == nil {
		t.Fatal("unknown custom verb decoded successfully")
	}
}

// TestCodecClusterAndJobIAM covers the IAM mixin on the other two regional
// collections the provider's google_dataproc_{cluster,job}_iam_* resources use.
// They are POST custom verbs too; the canonical iamName keeps the collection so
// the core can tell a cluster policy from a job policy.
func TestCodecClusterAndJobIAM(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		path, action, iamName string
	}{
		{"/v1/projects/proj/regions/us-central1/clusters/c1:getIamPolicy",
			"ClusterGetIamPolicy", "projects/proj/regions/us-central1/clusters/c1"},
		{"/v1/projects/proj/regions/us-central1/clusters/c1:setIamPolicy",
			"ClusterSetIamPolicy", "projects/proj/regions/us-central1/clusters/c1"},
		{"/v1/projects/proj/regions/us-central1/clusters/c1:testIamPermissions",
			"ClusterTestIamPermissions", "projects/proj/regions/us-central1/clusters/c1"},
		{"/v1/projects/proj/regions/us-central1/jobs/9e0d4c31:getIamPolicy",
			"JobGetIamPolicy", "projects/proj/regions/us-central1/jobs/9e0d4c31"},
		{"/v1/projects/proj/regions/us-central1/jobs/9e0d4c31:setIamPolicy",
			"JobSetIamPolicy", "projects/proj/regions/us-central1/jobs/9e0d4c31"},
		{"/v1/projects/proj/regions/us-central1/jobs/9e0d4c31:testIamPermissions",
			"JobTestIamPermissions", "projects/proj/regions/us-central1/jobs/9e0d4c31"},
	}
	for _, tc := range cases {
		nr, err := c.Decode(httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader("{}")), []byte("{}"))
		if err != nil {
			t.Fatalf("POST %s: %v", tc.path, err)
		}
		if nr.Action != tc.action {
			t.Fatalf("POST %s: action = %q, want %q", tc.path, nr.Action, tc.action)
		}
		if got, _ := nr.Params["iamName"].(string); got != tc.iamName {
			t.Fatalf("POST %s: iamName = %q, want %q", tc.path, got, tc.iamName)
		}
	}

	// An unknown custom verb on a cluster or job must stay a 404, not fall
	// through to the DELETE/PATCH handler for the same path.
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/v1/projects/proj/regions/us-central1/clusters/c1:frobnicate"},
		{http.MethodPatch, "/v1/projects/proj/regions/us-central1/jobs/j1:frobnicate"},
	} {
		if _, err := c.Decode(httptest.NewRequest(tc.method, tc.path, nil), nil); err == nil {
			t.Fatalf("%s %s: unknown custom verb decoded successfully", tc.method, tc.path)
		}
	}
}

// The serverless paths must decode to a Dataproc action (so the provider can
// answer 501) rather than falling through as an unknown resource.
func TestDecodeServerlessPaths(t *testing.T) {
	c := NewCodec()
	cases := []struct {
		path, action, rpc string
		method            string
	}{
		{"/v1/projects/p/locations/us-central1/batches", "Serverless", "BatchController.CreateBatch", http.MethodPost},
		{"/v1/projects/p/locations/us-central1/batches", "Serverless", "BatchController.ListBatches", http.MethodGet},
		{"/v1/projects/p/locations/us-central1/batches/b1", "Serverless", "BatchController.GetBatch", http.MethodGet},
		{"/v1/projects/p/locations/us-central1/batches/b1", "Serverless", "BatchController.DeleteBatch", http.MethodDelete},
		{"/v1/projects/p/locations/us-central1/batches/b1:analyze", "Serverless", "BatchController.AnalyzeBatch", http.MethodPost},
		{"/v1/projects/p/locations/us-central1/sessions", "Serverless", "SessionController.CreateSession", http.MethodPost},
		{"/v1/projects/p/locations/us-central1/sessionTemplates", "Serverless", "SessionTemplateController.CreateSessionTemplate", http.MethodPost},
	}
	for _, tc := range cases {
		r, _ := http.NewRequest(tc.method, "http://localhost:8080"+tc.path, nil)
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if nr.Action != tc.action {
			t.Fatalf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if got := nr.Params["rpc"]; got != tc.rpc {
			t.Fatalf("%s %s: rpc = %v, want %q", tc.method, tc.path, got, tc.rpc)
		}
	}
}

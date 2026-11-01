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

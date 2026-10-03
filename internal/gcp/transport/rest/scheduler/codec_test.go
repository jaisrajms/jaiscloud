package scheduler

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCodecRoutesJobs(t *testing.T) {
	cases := []struct {
		method string
		path   string
		action string
	}{
		{http.MethodGet, "/v1/projects/p/locations/l/jobs", "JobsList"},
		{http.MethodPost, "/v1/projects/p/locations/l/jobs", "JobsCreate"},
		{http.MethodGet, "/v1/projects/p/locations/l/jobs/j1", "JobsGet"},
		{http.MethodPatch, "/v1/projects/p/locations/l/jobs/j1", "JobsPatch"},
		{http.MethodDelete, "/v1/projects/p/locations/l/jobs/j1", "JobsDelete"},
		{http.MethodPost, "/v1/projects/p/locations/l/jobs/j1:pause", "JobsPause"},
		{http.MethodPost, "/v1/projects/p/locations/l/jobs/j1:resume", "JobsResume"},
		{http.MethodPost, "/v1/projects/p/locations/l/jobs/j1:run", "JobsRun"},
	}
	codec := NewCodec()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			nr, err := codec.Decode(r, nil)
			if err != nil {
				t.Fatalf("Decode(%s %s): %v", tc.method, tc.path, err)
			}
			if nr.Action != tc.action {
				t.Fatalf("action = %q, want %q", nr.Action, tc.action)
			}
			if nr.Params["project"] != "p" || nr.Params["location"] != "l" {
				t.Fatalf("params = %v", nr.Params)
			}
			if tc.action == "JobsGet" || tc.action == "JobsPatch" || tc.action == "JobsDelete" ||
				tc.action == "JobsPause" || tc.action == "JobsResume" || tc.action == "JobsRun" {
				if nr.Params["job"] != "j1" {
					t.Fatalf("job param = %v", nr.Params["job"])
				}
			}
		})
	}
}

func TestCodecRejectsUnknown(t *testing.T) {
	codec := NewCodec()
	r := httptest.NewRequest(http.MethodGet, "/v1/projects/p/locations/l/nope", nil)
	if _, err := codec.Decode(r, nil); err == nil {
		t.Fatalf("expected error for unknown path")
	}
}

func TestJobFromBodyRoundTrip(t *testing.T) {
	body := map[string]any{
		"name":        "projects/p/locations/l/jobs/j1",
		"description": "d",
		"schedule":    "* * * * *",
		"timeZone":    "UTC",
		"httpTarget": map[string]any{
			"uri":        "http://example.test/hook",
			"httpMethod": "POST",
			"headers":    map[string]any{"X-Test": "1"},
			"body":       "aGk=",
		},
		"retryConfig": map[string]any{
			"retryCount":         float64(3),
			"minBackoffDuration": "5s",
		},
	}
	j, err := jobFromBody(body)
	if err != nil {
		t.Fatalf("jobFromBody: %v", err)
	}
	if j.Name != "j1" || j.Schedule != "* * * * *" || j.HTTP == nil || string(j.HTTP.Body) != "hi" {
		t.Fatalf("decoded = %+v", j)
	}
	if j.RetryConfig == nil || j.RetryConfig.RetryCount != 3 || j.RetryConfig.MinBackoffDuration.String() != "5s" {
		t.Fatalf("retryConfig = %+v", j.RetryConfig)
	}
	j.ProjectID = "p"
	j.Location = "l"
	out := jobToJSON(j)
	if out["name"] != "projects/p/locations/l/jobs/j1" || out["schedule"] != "* * * * *" {
		t.Fatalf("encoded = %v", out)
	}
}

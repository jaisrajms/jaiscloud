package scheduler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
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

func TestJobToJSONAlwaysEmitsHTTPHeaders(t *testing.T) {
	out := jobToJSON(schedstore.Job{
		Name:      "j1",
		ProjectID: "p",
		Location:  "l",
		Schedule:  "* * * * *",
		Target:    schedstore.TargetHTTP,
		HTTP:      &schedstore.HttpTarget{URI: "http://example.test/hook"},
	})
	ht, ok := out["httpTarget"].(map[string]any)
	if !ok {
		t.Fatalf("httpTarget = %T", out["httpTarget"])
	}
	headers, ok := ht["headers"].(map[string]string)
	if !ok || headers == nil {
		t.Fatalf("httpTarget.headers = %#v, want an empty object", ht["headers"])
	}
	if len(headers) != 0 {
		t.Fatalf("httpTarget.headers = %v, want empty", headers)
	}
}

func TestJobToJSONOmitsRunStateWhenPaused(t *testing.T) {
	// Real Cloud Scheduler removes the next run on pause and clears the last
	// attempt, so a paused job answers none of scheduleTime/lastAttemptTime/
	// status. The adapter drops them even if a record persisted before the core
	// cleared it still carries them (TF2-12-1).
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stale := schedstore.Job{
		Name:            "j1",
		ProjectID:       "p",
		Location:        "l",
		Schedule:        "* * * * *",
		Target:          schedstore.TargetHTTP,
		HTTP:            &schedstore.HttpTarget{URI: "http://example.test/hook"},
		ScheduleTime:    at,
		LastAttemptTime: at,
		UserUpdateTime:  at,
		Status:          &schedstore.Status{Code: 13, Message: "boom"},
	}
	paused := stale
	paused.State = schedstore.StatePaused
	out := jobToJSON(paused)
	for _, field := range []string{"scheduleTime", "lastAttemptTime", "status"} {
		if _, ok := out[field]; ok {
			t.Fatalf("paused job rendered %s = %v", field, out[field])
		}
	}
	// An enabled job still answers them.
	enabled := stale
	enabled.State = schedstore.StateEnabled
	out = jobToJSON(enabled)
	for _, field := range []string{"scheduleTime", "lastAttemptTime", "status"} {
		if _, ok := out[field]; !ok {
			t.Fatalf("enabled job did not render %s", field)
		}
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

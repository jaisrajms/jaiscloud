package tasks

import (
	"net/http"
	"net/http/httptest"
	"testing"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

func TestCodecRoutesQueuesAndTasks(t *testing.T) {
	cases := []struct {
		method string
		path   string
		action string
		queue  bool
		task   bool
	}{
		{http.MethodGet, "/v2/projects/p/locations/l/queues", "QueuesList", false, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues", "QueuesCreate", false, false},
		{http.MethodGet, "/v2/projects/p/locations/l/queues/q1", "QueuesGet", true, false},
		{http.MethodPatch, "/v2/projects/p/locations/l/queues/q1", "QueuesPatch", true, false},
		{http.MethodDelete, "/v2/projects/p/locations/l/queues/q1", "QueuesDelete", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:pause", "QueuesPause", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:resume", "QueuesResume", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:purge", "QueuesPurge", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:getIamPolicy", "QueuesGetIamPolicy", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:setIamPolicy", "QueuesSetIamPolicy", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1:testIamPermissions", "QueuesTestIamPermissions", true, false},
		{http.MethodGet, "/v2/projects/p/locations/l/queues/q1/tasks", "TasksList", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks", "TasksCreate", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks:buffer", "TasksBuffer", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks:batchCreate", "TasksBatchCreate", true, false},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks:batchDelete", "TasksBatchDelete", true, false},
		{http.MethodGet, "/v2/projects/p/locations/l/queues/q1/tasks/t1", "TasksGet", true, true},
		{http.MethodDelete, "/v2/projects/p/locations/l/queues/q1/tasks/t1", "TasksDelete", true, true},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks/t1:run", "TasksRun", true, true},
		{http.MethodPost, "/v2/projects/p/locations/l/queues/q1/tasks/t1:buffer", "TasksBuffer", true, true},
		{http.MethodGet, "/v2/projects/p/locations/l/cmekConfig", "GetCmekConfig", false, false},
		{http.MethodPatch, "/v2/projects/p/locations/l/cmekConfig", "UpdateCmekConfig", false, false},
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
			if tc.queue && nr.Params["queue"] != "q1" {
				t.Fatalf("queue param = %v", nr.Params["queue"])
			}
			if tc.task && nr.Params["task"] != "t1" {
				t.Fatalf("task param = %v", nr.Params["task"])
			}
		})
	}
}

func TestCodecRejectsUnknown(t *testing.T) {
	codec := NewCodec()
	for _, p := range []string{
		"/v2/projects/p/locations/l/nope",
		"/v2/projects/p/locations/l/queues/q1/tasks/t1:unknown",
	} {
		r := httptest.NewRequest(http.MethodGet, p, nil)
		if _, err := codec.Decode(r, nil); err == nil {
			t.Fatalf("expected error for %s", p)
		}
	}
	// The CMEK config accepts only GET and PATCH; any other verb keeps the 404.
	for _, m := range []string{http.MethodPost, http.MethodDelete, http.MethodPut} {
		r := httptest.NewRequest(m, "/v2/projects/p/locations/l/cmekConfig", nil)
		if _, err := codec.Decode(r, nil); err == nil {
			t.Fatalf("expected error for %s cmekConfig", m)
		}
	}
}

func TestQueueFromBodyRoundTrip(t *testing.T) {
	q, err := queueFromBody(map[string]any{
		"name": "projects/p/locations/l/queues/q1",
		"rateLimits": map[string]any{
			"maxDispatchesPerSecond": float64(12),
			"maxBurstSize":           float64(3),
		},
		"retryConfig": map[string]any{
			"maxAttempts": float64(7),
			"minBackoff":  "5s",
		},
	})
	if err != nil {
		t.Fatalf("queueFromBody: %v", err)
	}
	if q.Name != "q1" || q.RateLimits.MaxDispatchesPerSecond != 12 || q.RateLimits.MaxBurstSize != 3 {
		t.Fatalf("decoded = %+v", q)
	}
	if q.RetryConfig.MaxAttempts != 7 || q.RetryConfig.MinBackoff.String() != "5s" {
		t.Fatalf("retry decoded = %+v", q.RetryConfig)
	}
}

func TestTaskFromBodyRoundTrip(t *testing.T) {
	tk, err := taskFromBody(map[string]any{
		"name": "projects/p/locations/l/queues/q1/tasks/t1",
		"httpRequest": map[string]any{
			"url":        "http://example.test/hook",
			"httpMethod": "POST",
			"headers":    map[string]any{"X-Test": "1"},
			"body":       "aGk=",
		},
		"dispatchDeadline": "30s",
		"retryConfig": map[string]any{
			"maxAttempts": float64(3),
			"minBackoff":  "2s",
			"maxBackoff":  "1m",
		},
	})
	if err != nil {
		t.Fatalf("taskFromBody: %v", err)
	}
	if tk.Name != "t1" || tk.Target != tasksstore.TargetHTTP || tk.HTTP == nil || string(tk.HTTP.Body) != "hi" {
		t.Fatalf("decoded = %+v", tk)
	}
	if tk.DispatchDeadline.String() != "30s" {
		t.Fatalf("dispatchDeadline = %v", tk.DispatchDeadline)
	}
	if tk.RetryConfig == nil || tk.RetryConfig.MaxAttempts != 3 || tk.RetryConfig.MinBackoff.String() != "2s" || tk.RetryConfig.MaxBackoff.String() != "1m0s" {
		t.Fatalf("retryConfig = %+v", tk.RetryConfig)
	}
	// The task-level retryConfig is emitted back on the wire (a full override,
	// so its own values are reported, never a merge with the queue's).
	out := taskToJSON(tk)
	rc, ok := out["retryConfig"].(map[string]any)
	if !ok {
		t.Fatalf("taskToJSON omitted retryConfig: %v", out)
	}
	if rc["maxAttempts"] != int32(3) || rc["minBackoff"] != "2s" || rc["maxBackoff"] != "60s" {
		t.Fatalf("encoded retryConfig = %v", rc)
	}
	// An absent task-level config means "inherit the queue", so it is omitted.
	tk.RetryConfig = nil
	if _, ok := taskToJSON(tk)["retryConfig"]; ok {
		t.Fatalf("taskToJSON emitted an absent retryConfig")
	}
}

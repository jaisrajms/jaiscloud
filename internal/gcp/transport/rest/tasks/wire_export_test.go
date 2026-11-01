package tasks

import (
	"encoding/json"
	"testing"
)

// viaJSON is what a body looks like once it has crossed the wire.
func viaJSON(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The exported converters are what the console uses to talk to a plugin that owns the Tasks keys:
// what they encode must decode back to the same model.
func TestExportedQueueConvertersRoundTrip(t *testing.T) {
	q, err := QueueFromBody(map[string]any{
		"name":        "projects/p/locations/l/queues/q",
		"rateLimits":  map[string]any{"maxDispatchesPerSecond": float64(5), "maxConcurrentDispatches": float64(3)},
		"retryConfig": map[string]any{"maxAttempts": float64(4)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if q.RateLimits == nil || q.RetryConfig == nil {
		t.Fatalf("queue = %+v", q)
	}
	q.ProjectID, q.Location = "p", "l" // the body names them; the store model carries them separately
	back, err := QueueFromBody(viaJSON(t, QueueToJSON(q)))
	if err != nil {
		t.Fatal(err)
	}
	if back.RateLimits == nil || back.RateLimits.MaxDispatchesPerSecond != q.RateLimits.MaxDispatchesPerSecond ||
		back.RateLimits.MaxConcurrentDispatches != q.RateLimits.MaxConcurrentDispatches || back.RetryConfig == nil || back.RetryConfig.MaxAttempts != q.RetryConfig.MaxAttempts {
		t.Fatalf("queue after a round trip = %+v rate %+v retry %+v, want %+v rate %+v retry %+v", back, back.RateLimits, back.RetryConfig, q, q.RateLimits, q.RetryConfig)
	}
}

func TestExportedTaskConvertersRoundTrip(t *testing.T) {
	for name, body := range map[string]map[string]any{
		"http": {"name": "projects/p/locations/l/queues/q/tasks/t1", "httpRequest": map[string]any{"url": "http://x/y", "httpMethod": "POST", "headers": map[string]any{"a": "b"}}, "dispatchDeadline": "30s"},
		"appengine": {"name": "projects/p/locations/l/queues/q/tasks/t2", "appEngineHttpRequest": map[string]any{"relativeUri": "/w", "httpMethod": "GET",
			"appEngineRouting": map[string]any{"service": "svc", "version": "v1"}}},
	} {
		task, err := TaskFromBody(body)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		back, err := TaskFromBody(viaJSON(t, TaskToJSON(task)))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if back.Target != task.Target || back.DispatchDeadline != task.DispatchDeadline || (back.HTTP == nil) != (task.HTTP == nil) || (back.AppEngine == nil) != (task.AppEngine == nil) {
			t.Fatalf("%s: task after a round trip = %+v, want %+v", name, back, task)
		}
	}
}

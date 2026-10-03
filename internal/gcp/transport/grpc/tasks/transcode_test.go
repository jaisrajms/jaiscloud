package tasks

import (
	"testing"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

func TestQueueProtoRoundTrip(t *testing.T) {
	q := tasksstore.Queue{
		ProjectID: "p", Location: "l", Name: "q1",
		RateLimits:  &tasksstore.RateLimits{MaxDispatchesPerSecond: 12, MaxBurstSize: 3, MaxConcurrentDispatches: 4},
		RetryConfig: &tasksstore.RetryConfig{MaxAttempts: 7, MaxDoublings: 2},
		State:       tasksstore.StateRunning,
	}
	pb := queueToProto(q)
	if pb.GetName() != "projects/p/locations/l/queues/q1" || pb.GetState() != cloudtaskspb.Queue_RUNNING {
		t.Fatalf("proto = %+v", pb)
	}
	back := queueFromProto(pb)
	if back.Name != "q1" || back.RateLimits.MaxDispatchesPerSecond != 12 || back.RetryConfig.MaxAttempts != 7 {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestTaskProtoRoundTrip(t *testing.T) {
	tk := tasksstore.Task{
		ProjectID: "p", Location: "l", Queue: "q1", Name: "t1",
		Target: tasksstore.TargetHTTP,
		HTTP:   &tasksstore.HttpRequest{URL: "http://example.test/hook", HTTPMethod: "POST", Body: []byte("hi")},
	}
	pb := taskToProto(tk)
	if pb.GetHttpRequest().GetUrl() != "http://example.test/hook" || pb.GetHttpRequest().GetHttpMethod() != cloudtaskspb.HttpMethod_POST {
		t.Fatalf("proto = %+v", pb)
	}
	back := taskFromProto(pb)
	if back.Name != "t1" || back.Target != tasksstore.TargetHTTP || string(back.HTTP.Body) != "hi" {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestAppEngineTaskProtoRoundTrip(t *testing.T) {
	tk := tasksstore.Task{
		Name: "t1", Target: tasksstore.TargetAppEngine,
		AppEngine: &tasksstore.AppEngineHttpRequest{RelativeURI: "/do", HTTPMethod: "POST", AppEngineRouting: &tasksstore.AppEngineRouting{Service: "s"}},
	}
	back := taskFromProto(taskToProto(tk))
	if back.Target != tasksstore.TargetAppEngine || back.AppEngine.RelativeURI != "/do" || back.AppEngine.AppEngineRouting.Service != "s" {
		t.Fatalf("round trip = %+v", back)
	}
}

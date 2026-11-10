package scheduler

import (
	"testing"
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

func TestJobProtoRoundTrip(t *testing.T) {
	orig := schedstore.Job{
		ProjectID:       "p",
		Location:        "l",
		Name:            "j1",
		Description:     "d",
		Schedule:        "* * * * *",
		TimeZone:        "UTC",
		Target:          schedstore.TargetHTTP,
		HTTP:            &schedstore.HttpTarget{URI: "http://x", HTTPMethod: "POST", Body: []byte("hi"), Headers: map[string]string{"A": "b"}},
		RetryConfig:     &schedstore.RetryConfig{RetryCount: 2, MaxDoublings: 3, MinBackoffDuration: 5 * time.Second},
		AttemptDeadline: 30 * time.Second,
		State:           schedstore.StateEnabled,
		ScheduleTime:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UserUpdateTime:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Status:          &schedstore.Status{Code: 13, Message: "boom"},
	}
	pb := jobToProto(orig)
	if pb.GetName() != "projects/p/locations/l/jobs/j1" || pb.GetState() != schedulerpb.Job_ENABLED {
		t.Fatalf("proto = %v", pb)
	}
	if pb.GetHttpTarget().GetHttpMethod() != schedulerpb.HttpMethod_POST {
		t.Fatalf("method = %v", pb.GetHttpTarget().GetHttpMethod())
	}
	back := jobFromProto(pb)
	if back.Name != "j1" || back.Schedule != "* * * * *" || back.Target != schedstore.TargetHTTP {
		t.Fatalf("round trip = %+v", back)
	}
	if back.HTTP == nil || string(back.HTTP.Body) != "hi" || back.HTTP.HTTPMethod != "POST" {
		t.Fatalf("http = %+v", back.HTTP)
	}
	if back.RetryConfig == nil || back.RetryConfig.RetryCount != 2 || back.RetryConfig.MinBackoffDuration != 5*time.Second {
		t.Fatalf("retry = %+v", back.RetryConfig)
	}
	if back.AttemptDeadline != 30*time.Second {
		t.Fatalf("deadline = %v", back.AttemptDeadline)
	}
}

func TestJobProtoOmitsRunStateWhenPaused(t *testing.T) {
	// Real Cloud Scheduler removes the next run on pause and clears the last
	// attempt, so a paused job answers no scheduleTime/lastAttemptTime/status on
	// the gRPC wire either. Absent means the message field is unset, not an
	// empty timestamp (TF2-12-1).
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	stale := schedstore.Job{
		ProjectID:       "p",
		Location:        "l",
		Name:            "j1",
		Schedule:        "* * * * *",
		Target:          schedstore.TargetHTTP,
		HTTP:            &schedstore.HttpTarget{URI: "http://example.test/hook"},
		ScheduleTime:    at.Add(time.Minute),
		LastAttemptTime: at,
		UserUpdateTime:  at,
		Status:          &schedstore.Status{Code: 13, Message: "boom"},
	}
	paused := stale
	paused.State = schedstore.StatePaused
	pb := jobToProto(paused)
	if pb.GetScheduleTime() != nil || pb.GetLastAttemptTime() != nil || pb.GetStatus() != nil {
		t.Fatalf("paused job rendered run state: scheduleTime=%v lastAttemptTime=%v status=%v",
			pb.GetScheduleTime(), pb.GetLastAttemptTime(), pb.GetStatus())
	}
	// An enabled job still answers them.
	enabled := stale
	enabled.State = schedstore.StateEnabled
	pb = jobToProto(enabled)
	if pb.GetScheduleTime() == nil || pb.GetLastAttemptTime() == nil || pb.GetStatus() == nil {
		t.Fatalf("enabled job did not render run state: %v", pb)
	}
}

func TestJobProtoPubSubAndAppEngine(t *testing.T) {
	pub := jobToProto(schedstore.Job{Target: schedstore.TargetPubSub, PubSub: &schedstore.PubsubTarget{TopicName: "projects/p/topics/t"}})
	if pub.GetPubsubTarget().GetTopicName() != "projects/p/topics/t" {
		t.Fatalf("pubsub proto = %v", pub)
	}
	app := jobFromProto(&schedulerpb.Job{Target: &schedulerpb.Job_AppEngineHttpTarget{
		AppEngineHttpTarget: &schedulerpb.AppEngineHttpTarget{RelativeUri: "/x", HttpMethod: schedulerpb.HttpMethod_GET},
	}})
	if app.Target != schedstore.TargetAppEngine || app.AppEngine == nil || app.AppEngine.RelativeURI != "/x" {
		t.Fatalf("appengine = %+v", app)
	}
}

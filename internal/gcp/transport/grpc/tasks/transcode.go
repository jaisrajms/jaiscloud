// Package tasks is the gRPC transport for Cloud Tasks v2
// (google.cloud.tasks.v2.CloudTasks). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/tasks: it transcodes between
// the generated protobuf messages and the core's typed API, and maps core
// errors to gRPC status codes. It owns no business logic and no state beyond
// its default project.
package tasks

import (
	"time"

	cloudtaskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// queueToProto renders a core Queue as the proto Queue message.
func queueToProto(q tasksstore.Queue) *cloudtaskspb.Queue {
	out := &cloudtaskspb.Queue{
		Name:  core.QueueName(q.ProjectID, q.Location, q.Name),
		State: stateToProto(q.State),
	}
	if q.AppEngineRoutingOverride != nil {
		out.AppEngineRoutingOverride = routingToProto(q.AppEngineRoutingOverride)
	}
	if q.RateLimits != nil {
		out.RateLimits = &cloudtaskspb.RateLimits{
			MaxDispatchesPerSecond:  q.RateLimits.MaxDispatchesPerSecond,
			MaxBurstSize:            q.RateLimits.MaxBurstSize,
			MaxConcurrentDispatches: q.RateLimits.MaxConcurrentDispatches,
		}
	}
	if q.RetryConfig != nil {
		out.RetryConfig = retryToProto(q.RetryConfig)
	}
	if q.StackdriverLoggingConfig != nil {
		out.StackdriverLoggingConfig = &cloudtaskspb.StackdriverLoggingConfig{SamplingRatio: q.StackdriverLoggingConfig.SamplingRatio}
	}
	if !q.PurgeTime.IsZero() {
		out.PurgeTime = timestamppb.New(q.PurgeTime)
	}
	return out
}

// queueFromProto decodes a proto Queue into the store model.
func queueFromProto(pb *cloudtaskspb.Queue) tasksstore.Queue {
	var q tasksstore.Queue
	if pb == nil {
		return q
	}
	if name := pb.GetName(); name != "" {
		if _, _, id, ok := core.ParseQueueName(name); ok {
			q.Name = id
		} else {
			q.Name = name
		}
	}
	if r := pb.GetAppEngineRoutingOverride(); r != nil {
		q.AppEngineRoutingOverride = routingFromProto(r)
	}
	if rl := pb.GetRateLimits(); rl != nil {
		q.RateLimits = &tasksstore.RateLimits{
			MaxDispatchesPerSecond:  rl.GetMaxDispatchesPerSecond(),
			MaxBurstSize:            rl.GetMaxBurstSize(),
			MaxConcurrentDispatches: rl.GetMaxConcurrentDispatches(),
		}
	}
	if rc := pb.GetRetryConfig(); rc != nil {
		q.RetryConfig = &tasksstore.RetryConfig{
			MaxAttempts:      rc.GetMaxAttempts(),
			MaxRetryDuration: durationFromProto(rc.GetMaxRetryDuration()),
			MinBackoff:       durationFromProto(rc.GetMinBackoff()),
			MaxBackoff:       durationFromProto(rc.GetMaxBackoff()),
			MaxDoublings:     rc.GetMaxDoublings(),
		}
	}
	if sc := pb.GetStackdriverLoggingConfig(); sc != nil {
		q.StackdriverLoggingConfig = &tasksstore.StackdriverLoggingConfig{SamplingRatio: sc.GetSamplingRatio()}
	}
	return q
}

// taskToProto renders a core Task as the proto Task message.
func taskToProto(t tasksstore.Task) *cloudtaskspb.Task {
	out := &cloudtaskspb.Task{
		Name:          core.TaskName(t.ProjectID, t.Location, t.Queue, t.Name),
		DispatchCount: t.DispatchCount,
		ResponseCount: t.ResponseCount,
	}
	switch t.Target {
	case tasksstore.TargetHTTP:
		if t.HTTP != nil {
			out.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: httpRequestToProto(t.HTTP)}
		}
	case tasksstore.TargetAppEngine:
		if t.AppEngine != nil {
			out.MessageType = &cloudtaskspb.Task_AppEngineHttpRequest{AppEngineHttpRequest: appEngineRequestToProto(t.AppEngine)}
		}
	}
	if !t.ScheduleTime.IsZero() {
		out.ScheduleTime = timestamppb.New(t.ScheduleTime)
	}
	if !t.CreateTime.IsZero() {
		out.CreateTime = timestamppb.New(t.CreateTime)
	}
	if t.DispatchDeadline > 0 {
		out.DispatchDeadline = durationpb.New(t.DispatchDeadline)
	}
	if t.FirstAttempt != nil {
		out.FirstAttempt = attemptToProto(t.FirstAttempt)
	}
	if t.LastAttempt != nil {
		out.LastAttempt = attemptToProto(t.LastAttempt)
	}
	return out
}

// taskFromProto decodes a proto Task into the store model.
func taskFromProto(pb *cloudtaskspb.Task) tasksstore.Task {
	var t tasksstore.Task
	if pb == nil {
		return t
	}
	if name := pb.GetName(); name != "" {
		if _, _, _, id, ok := core.ParseTaskName(name); ok {
			t.Name = id
		} else {
			t.Name = name
		}
	}
	switch m := pb.MessageType.(type) {
	case *cloudtaskspb.Task_HttpRequest:
		t.HTTP = httpRequestFromProto(m.HttpRequest)
		t.Target = tasksstore.TargetHTTP
	case *cloudtaskspb.Task_AppEngineHttpRequest:
		t.AppEngine = appEngineRequestFromProto(m.AppEngineHttpRequest)
		t.Target = tasksstore.TargetAppEngine
	}
	if st := pb.GetScheduleTime(); st != nil {
		t.ScheduleTime = st.AsTime()
	}
	if dd := pb.GetDispatchDeadline(); dd != nil {
		t.DispatchDeadline = durationFromProto(dd)
	}
	return t
}

func httpRequestToProto(r *tasksstore.HttpRequest) *cloudtaskspb.HttpRequest {
	out := &cloudtaskspb.HttpRequest{
		Url:        r.URL,
		HttpMethod: methodToProto(r.HTTPMethod),
		Headers:    r.Headers,
		Body:       r.Body,
	}
	if r.OAuthToken != nil {
		out.AuthorizationHeader = &cloudtaskspb.HttpRequest_OauthToken{
			OauthToken: &cloudtaskspb.OAuthToken{ServiceAccountEmail: r.OAuthToken.ServiceAccountEmail, Scope: r.OAuthToken.Scope},
		}
	}
	if r.OidcToken != nil {
		out.AuthorizationHeader = &cloudtaskspb.HttpRequest_OidcToken{
			OidcToken: &cloudtaskspb.OidcToken{ServiceAccountEmail: r.OidcToken.ServiceAccountEmail, Audience: r.OidcToken.Audience},
		}
	}
	return out
}

func httpRequestFromProto(pb *cloudtaskspb.HttpRequest) *tasksstore.HttpRequest {
	if pb == nil {
		return nil
	}
	r := &tasksstore.HttpRequest{
		URL:        pb.GetUrl(),
		HTTPMethod: methodFromProto(pb.GetHttpMethod()),
		Headers:    pb.GetHeaders(),
		Body:       pb.GetBody(),
	}
	if o := pb.GetOauthToken(); o != nil {
		r.OAuthToken = &tasksstore.OAuthToken{ServiceAccountEmail: o.GetServiceAccountEmail(), Scope: o.GetScope()}
	}
	if o := pb.GetOidcToken(); o != nil {
		r.OidcToken = &tasksstore.OidcToken{ServiceAccountEmail: o.GetServiceAccountEmail(), Audience: o.GetAudience()}
	}
	return r
}

func appEngineRequestToProto(r *tasksstore.AppEngineHttpRequest) *cloudtaskspb.AppEngineHttpRequest {
	out := &cloudtaskspb.AppEngineHttpRequest{
		HttpMethod:  methodToProto(r.HTTPMethod),
		RelativeUri: r.RelativeURI,
		Headers:     r.Headers,
		Body:        r.Body,
	}
	if r.AppEngineRouting != nil {
		out.AppEngineRouting = routingToProto(r.AppEngineRouting)
	}
	return out
}

func appEngineRequestFromProto(pb *cloudtaskspb.AppEngineHttpRequest) *tasksstore.AppEngineHttpRequest {
	if pb == nil {
		return nil
	}
	r := &tasksstore.AppEngineHttpRequest{
		HTTPMethod:  methodFromProto(pb.GetHttpMethod()),
		RelativeURI: pb.GetRelativeUri(),
		Headers:     pb.GetHeaders(),
		Body:        pb.GetBody(),
	}
	if rt := pb.GetAppEngineRouting(); rt != nil {
		r.AppEngineRouting = routingFromProto(rt)
	}
	return r
}

func routingToProto(r *tasksstore.AppEngineRouting) *cloudtaskspb.AppEngineRouting {
	return &cloudtaskspb.AppEngineRouting{
		Service:  r.Service,
		Version:  r.Version,
		Instance: r.Instance,
		Host:     r.Host,
	}
}

func routingFromProto(pb *cloudtaskspb.AppEngineRouting) *tasksstore.AppEngineRouting {
	if pb == nil {
		return nil
	}
	return &tasksstore.AppEngineRouting{
		Service:  pb.GetService(),
		Version:  pb.GetVersion(),
		Instance: pb.GetInstance(),
		Host:     pb.GetHost(),
	}
}

func retryToProto(rc *tasksstore.RetryConfig) *cloudtaskspb.RetryConfig {
	out := &cloudtaskspb.RetryConfig{MaxAttempts: rc.MaxAttempts, MaxDoublings: rc.MaxDoublings}
	if rc.MaxRetryDuration > 0 {
		out.MaxRetryDuration = durationpb.New(rc.MaxRetryDuration)
	}
	if rc.MinBackoff > 0 {
		out.MinBackoff = durationpb.New(rc.MinBackoff)
	}
	if rc.MaxBackoff > 0 {
		out.MaxBackoff = durationpb.New(rc.MaxBackoff)
	}
	return out
}

func attemptToProto(a *tasksstore.Attempt) *cloudtaskspb.Attempt {
	out := &cloudtaskspb.Attempt{}
	if !a.ScheduleTime.IsZero() {
		out.ScheduleTime = timestamppb.New(a.ScheduleTime)
	}
	if !a.DispatchTime.IsZero() {
		out.DispatchTime = timestamppb.New(a.DispatchTime)
	}
	if !a.ResponseTime.IsZero() {
		out.ResponseTime = timestamppb.New(a.ResponseTime)
	}
	if a.ResponseStatus != nil {
		out.ResponseStatus = &statuspb.Status{Code: a.ResponseStatus.Code, Message: a.ResponseStatus.Message}
	}
	return out
}

func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func stateToProto(s tasksstore.QueueState) cloudtaskspb.Queue_State {
	switch s {
	case tasksstore.StateRunning:
		return cloudtaskspb.Queue_RUNNING
	case tasksstore.StatePaused:
		return cloudtaskspb.Queue_PAUSED
	case tasksstore.StateDisabled:
		return cloudtaskspb.Queue_DISABLED
	default:
		return cloudtaskspb.Queue_STATE_UNSPECIFIED
	}
}

var httpMethods = map[string]cloudtaskspb.HttpMethod{
	"POST":    cloudtaskspb.HttpMethod_POST,
	"GET":     cloudtaskspb.HttpMethod_GET,
	"HEAD":    cloudtaskspb.HttpMethod_HEAD,
	"PUT":     cloudtaskspb.HttpMethod_PUT,
	"DELETE":  cloudtaskspb.HttpMethod_DELETE,
	"PATCH":   cloudtaskspb.HttpMethod_PATCH,
	"OPTIONS": cloudtaskspb.HttpMethod_OPTIONS,
}

func methodToProto(m string) cloudtaskspb.HttpMethod {
	if v, ok := httpMethods[m]; ok {
		return v
	}
	return cloudtaskspb.HttpMethod_HTTP_METHOD_UNSPECIFIED
}

func methodFromProto(m cloudtaskspb.HttpMethod) string {
	for k, v := range httpMethods {
		if v == m {
			return k
		}
	}
	return ""
}

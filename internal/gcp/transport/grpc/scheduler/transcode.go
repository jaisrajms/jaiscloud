package scheduler

import (
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// jobToProto renders a core Job as the proto Job message.
func jobToProto(j schedstore.Job) *schedulerpb.Job {
	out := &schedulerpb.Job{
		Name:        core.JobName(j.ProjectID, j.Location, j.Name),
		Description: j.Description,
		Schedule:    j.Schedule,
		TimeZone:    j.TimeZone,
		State:       stateToProto(j.State),
	}
	switch j.Target {
	case schedstore.TargetHTTP:
		if j.HTTP != nil {
			out.Target = &schedulerpb.Job_HttpTarget{HttpTarget: httpTargetToProto(j.HTTP)}
		}
	case schedstore.TargetPubSub:
		if j.PubSub != nil {
			out.Target = &schedulerpb.Job_PubsubTarget{PubsubTarget: pubsubTargetToProto(j.PubSub)}
		}
	case schedstore.TargetAppEngine:
		if j.AppEngine != nil {
			out.Target = &schedulerpb.Job_AppEngineHttpTarget{AppEngineHttpTarget: appEngineTargetToProto(j.AppEngine)}
		}
	}
	if j.RetryConfig != nil {
		out.RetryConfig = retryToProto(j.RetryConfig)
	}
	if j.AttemptDeadline > 0 {
		out.AttemptDeadline = durationpb.New(j.AttemptDeadline)
	}
	if !j.ScheduleTime.IsZero() {
		out.ScheduleTime = timestamppb.New(j.ScheduleTime)
	}
	if !j.LastAttemptTime.IsZero() {
		out.LastAttemptTime = timestamppb.New(j.LastAttemptTime)
	}
	if !j.UserUpdateTime.IsZero() {
		out.UserUpdateTime = timestamppb.New(j.UserUpdateTime)
	}
	if j.Status != nil {
		out.Status = &statuspb.Status{Code: j.Status.Code, Message: j.Status.Message}
	}
	return out
}

// jobFromProto decodes a proto Job into the store model.
func jobFromProto(pb *schedulerpb.Job) schedstore.Job {
	var j schedstore.Job
	if pb == nil {
		return j
	}
	j.Description = pb.GetDescription()
	j.Schedule = pb.GetSchedule()
	j.TimeZone = pb.GetTimeZone()
	if name := pb.GetName(); name != "" {
		if _, _, id, ok := core.ParseJobName(name); ok {
			j.Name = id
		} else {
			j.Name = name
		}
	}
	switch t := pb.Target.(type) {
	case *schedulerpb.Job_HttpTarget:
		ht := t.HttpTarget
		j.HTTP = &schedstore.HttpTarget{
			URI:        ht.GetUri(),
			HTTPMethod: methodFromProto(ht.GetHttpMethod()),
			Headers:    ht.GetHeaders(),
			Body:       ht.GetBody(),
		}
		if o := ht.GetOauthToken(); o != nil {
			j.HTTP.OAuthToken = &schedstore.OAuthToken{Scope: o.GetScope(), ServiceAccountEmail: o.GetServiceAccountEmail()}
		}
		if o := ht.GetOidcToken(); o != nil {
			j.HTTP.OidcToken = &schedstore.OidcToken{ServiceAccountEmail: o.GetServiceAccountEmail(), Audience: o.GetAudience()}
		}
		j.Target = schedstore.TargetHTTP
	case *schedulerpb.Job_PubsubTarget:
		pt := t.PubsubTarget
		j.PubSub = &schedstore.PubsubTarget{TopicName: pt.GetTopicName(), Data: pt.GetData(), Attributes: pt.GetAttributes()}
		j.Target = schedstore.TargetPubSub
	case *schedulerpb.Job_AppEngineHttpTarget:
		at := t.AppEngineHttpTarget
		j.AppEngine = &schedstore.AppEngineTarget{
			HTTPMethod:  methodFromProto(at.GetHttpMethod()),
			RelativeURI: at.GetRelativeUri(),
			Headers:     at.GetHeaders(),
			Body:        at.GetBody(),
		}
		if r := at.GetAppEngineRouting(); r != nil {
			j.AppEngine.Routing = &schedstore.AppEngineRouting{
				Service:  r.GetService(),
				Version:  r.GetVersion(),
				Instance: r.GetInstance(),
				Host:     r.GetHost(),
			}
		}
		j.Target = schedstore.TargetAppEngine
	}
	if rc := pb.GetRetryConfig(); rc != nil {
		j.RetryConfig = &schedstore.RetryConfig{
			RetryCount:         rc.GetRetryCount(),
			MaxDoublings:       rc.GetMaxDoublings(),
			MaxRetryDuration:   durationFromProto(rc.GetMaxRetryDuration()),
			MinBackoffDuration: durationFromProto(rc.GetMinBackoffDuration()),
			MaxBackoffDuration: durationFromProto(rc.GetMaxBackoffDuration()),
		}
	}
	if ad := pb.GetAttemptDeadline(); ad != nil {
		j.AttemptDeadline = durationFromProto(ad)
	}
	return j
}

func httpTargetToProto(t *schedstore.HttpTarget) *schedulerpb.HttpTarget {
	out := &schedulerpb.HttpTarget{
		Uri:        t.URI,
		HttpMethod: methodToProto(t.HTTPMethod),
		Headers:    t.Headers,
		Body:       t.Body,
	}
	if t.OAuthToken != nil {
		out.AuthorizationHeader = &schedulerpb.HttpTarget_OauthToken{
			OauthToken: &schedulerpb.OAuthToken{Scope: t.OAuthToken.Scope, ServiceAccountEmail: t.OAuthToken.ServiceAccountEmail},
		}
	}
	if t.OidcToken != nil {
		out.AuthorizationHeader = &schedulerpb.HttpTarget_OidcToken{
			OidcToken: &schedulerpb.OidcToken{ServiceAccountEmail: t.OidcToken.ServiceAccountEmail, Audience: t.OidcToken.Audience},
		}
	}
	return out
}

func pubsubTargetToProto(t *schedstore.PubsubTarget) *schedulerpb.PubsubTarget {
	return &schedulerpb.PubsubTarget{TopicName: t.TopicName, Data: t.Data, Attributes: t.Attributes}
}

func appEngineTargetToProto(t *schedstore.AppEngineTarget) *schedulerpb.AppEngineHttpTarget {
	out := &schedulerpb.AppEngineHttpTarget{
		HttpMethod:  methodToProto(t.HTTPMethod),
		RelativeUri: t.RelativeURI,
		Headers:     t.Headers,
		Body:        t.Body,
	}
	if t.Routing != nil {
		out.AppEngineRouting = &schedulerpb.AppEngineRouting{
			Service:  t.Routing.Service,
			Version:  t.Routing.Version,
			Instance: t.Routing.Instance,
			Host:     t.Routing.Host,
		}
	}
	return out
}

func retryToProto(rc *schedstore.RetryConfig) *schedulerpb.RetryConfig {
	out := &schedulerpb.RetryConfig{RetryCount: rc.RetryCount, MaxDoublings: rc.MaxDoublings}
	if rc.MaxRetryDuration > 0 {
		out.MaxRetryDuration = durationpb.New(rc.MaxRetryDuration)
	}
	if rc.MinBackoffDuration > 0 {
		out.MinBackoffDuration = durationpb.New(rc.MinBackoffDuration)
	}
	if rc.MaxBackoffDuration > 0 {
		out.MaxBackoffDuration = durationpb.New(rc.MaxBackoffDuration)
	}
	return out
}

func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func stateToProto(s schedstore.State) schedulerpb.Job_State {
	switch s {
	case schedstore.StateEnabled:
		return schedulerpb.Job_ENABLED
	case schedstore.StatePaused:
		return schedulerpb.Job_PAUSED
	case schedstore.StateDisabled:
		return schedulerpb.Job_DISABLED
	case schedstore.StateUpdateFailed:
		return schedulerpb.Job_UPDATE_FAILED
	default:
		return schedulerpb.Job_STATE_UNSPECIFIED
	}
}

var httpMethods = map[string]schedulerpb.HttpMethod{
	"POST":    schedulerpb.HttpMethod_POST,
	"GET":     schedulerpb.HttpMethod_GET,
	"HEAD":    schedulerpb.HttpMethod_HEAD,
	"PUT":     schedulerpb.HttpMethod_PUT,
	"DELETE":  schedulerpb.HttpMethod_DELETE,
	"PATCH":   schedulerpb.HttpMethod_PATCH,
	"OPTIONS": schedulerpb.HttpMethod_OPTIONS,
}

func methodToProto(m string) schedulerpb.HttpMethod {
	if v, ok := httpMethods[m]; ok {
		return v
	}
	return schedulerpb.HttpMethod_HTTP_METHOD_UNSPECIFIED
}

func methodFromProto(m schedulerpb.HttpMethod) string {
	switch m {
	case schedulerpb.HttpMethod_POST:
		return "POST"
	case schedulerpb.HttpMethod_GET:
		return "GET"
	case schedulerpb.HttpMethod_HEAD:
		return "HEAD"
	case schedulerpb.HttpMethod_PUT:
		return "PUT"
	case schedulerpb.HttpMethod_DELETE:
		return "DELETE"
	case schedulerpb.HttpMethod_PATCH:
		return "PATCH"
	case schedulerpb.HttpMethod_OPTIONS:
		return "OPTIONS"
	default:
		return ""
	}
}

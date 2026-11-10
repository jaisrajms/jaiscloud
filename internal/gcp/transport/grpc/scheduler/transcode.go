package scheduler

import (
	"time"

	schedulerpb "cloud.google.com/go/scheduler/apiv1/schedulerpb"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	core "jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/gcp/timestamp"
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
	// Real Cloud Scheduler always answers a retryConfig; the core resolves the
	// documented defaults for a job whose create body carried none (or carried
	// the all-zero block the provider drops), so the read is a stable
	// retry_config for Terraform (TF3-4-2).
	out.RetryConfig = retryToProto(core.EffectiveRetryConfig(j))
	if j.AttemptDeadline > 0 {
		out.AttemptDeadline = durationpb.New(j.AttemptDeadline)
	}
	// A paused job answers no run state: pausing removes the next run and
	// clears the last attempt, so scheduleTime/lastAttemptTime/status stay
	// unset (TF2-12-1).
	if j.RunStateReported() {
		if !j.ScheduleTime.IsZero() {
			out.ScheduleTime = timestamp.Proto(j.ScheduleTime)
		}
		if !j.LastAttemptTime.IsZero() {
			out.LastAttemptTime = timestamp.Proto(j.LastAttemptTime)
		}
	}
	if !j.UserUpdateTime.IsZero() {
		out.UserUpdateTime = timestamp.Proto(j.UserUpdateTime)
	}
	if j.RunStateReported() && j.Status != nil {
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

// retryToProto renders a RetryConfig. Every field is set: the core resolves the
// documented defaults first, and real Cloud Scheduler answers the whole message
// (max_retry_duration included, at 0s when the retry duration is unlimited).
func retryToProto(rc *schedstore.RetryConfig) *schedulerpb.RetryConfig {
	return &schedulerpb.RetryConfig{
		RetryCount:         rc.RetryCount,
		MaxRetryDuration:   durationpb.New(rc.MaxRetryDuration),
		MinBackoffDuration: durationpb.New(rc.MinBackoffDuration),
		MaxBackoffDuration: durationpb.New(rc.MaxBackoffDuration),
		MaxDoublings:       rc.MaxDoublings,
	}
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

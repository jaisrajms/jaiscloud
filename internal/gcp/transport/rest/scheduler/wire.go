package scheduler

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

func parseJSON(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func strMap(v any) map[string]string {
	m, _ := v.(map[string]any)
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}

func decodeBytes(v any) ([]byte, error) {
	s, _ := v.(string)
	if s == "" {
		return nil, nil
	}
	return base64.StdEncoding.DecodeString(s)
}

func splitMask(v any) []string {
	s, _ := v.(string)
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// jobFromBody decodes a Discovery Job JSON object into the store model.
func jobFromBody(m map[string]any) (schedstore.Job, error) {
	var j schedstore.Job
	j.Description = str(m, "description")
	j.Schedule = str(m, "schedule")
	j.TimeZone = str(m, "timeZone")
	if raw := str(m, "name"); raw != "" {
		if strings.Contains(raw, "/") {
			_, _, id, ok := coreParseJobName(raw)
			if !ok {
				return j, model.NewProviderError("InvalidArgument", "invalid job name", 400)
			}
			j.Name = id
		} else {
			j.Name = raw
		}
	}

	if ht, ok := m["httpTarget"].(map[string]any); ok {
		t := &schedstore.HttpTarget{
			URI:        str(ht, "uri"),
			HTTPMethod: str(ht, "httpMethod"),
			Headers:    strMap(ht["headers"]),
		}
		body, err := decodeBytes(ht["body"])
		if err != nil {
			return j, model.NewProviderError("InvalidArgument", "httpTarget.body is not valid base64", 400)
		}
		t.Body = body
		if o, ok := ht["oauthToken"].(map[string]any); ok {
			t.OAuthToken = &schedstore.OAuthToken{Scope: str(o, "scope"), ServiceAccountEmail: str(o, "serviceAccountEmail")}
		}
		if o, ok := ht["oidcToken"].(map[string]any); ok {
			t.OidcToken = &schedstore.OidcToken{ServiceAccountEmail: str(o, "serviceAccountEmail"), Audience: str(o, "audience")}
		}
		j.HTTP = t
		j.Target = schedstore.TargetHTTP
	}
	if pt, ok := m["pubsubTarget"].(map[string]any); ok {
		t := &schedstore.PubsubTarget{
			TopicName:  str(pt, "topicName"),
			Attributes: strMap(pt["attributes"]),
		}
		data, err := decodeBytes(pt["data"])
		if err != nil {
			return j, model.NewProviderError("InvalidArgument", "pubsubTarget.data is not valid base64", 400)
		}
		t.Data = data
		j.PubSub = t
		j.Target = schedstore.TargetPubSub
	}
	if at, ok := m["appEngineHttpTarget"].(map[string]any); ok {
		t := &schedstore.AppEngineTarget{
			HTTPMethod:  str(at, "httpMethod"),
			RelativeURI: str(at, "relativeUri"),
			Headers:     strMap(at["headers"]),
		}
		body, err := decodeBytes(at["body"])
		if err != nil {
			return j, model.NewProviderError("InvalidArgument", "appEngineHttpTarget.body is not valid base64", 400)
		}
		t.Body = body
		if r, ok := at["appEngineRouting"].(map[string]any); ok {
			t.Routing = &schedstore.AppEngineRouting{
				Service:  str(r, "service"),
				Version:  str(r, "version"),
				Instance: str(r, "instance"),
				Host:     str(r, "host"),
			}
		}
		j.AppEngine = t
		j.Target = schedstore.TargetAppEngine
	}

	if rc, ok := m["retryConfig"].(map[string]any); ok {
		cfg := &schedstore.RetryConfig{RetryCount: int32(intFrom(rc["retryCount"])), MaxDoublings: int32(intFrom(rc["maxDoublings"]))}
		for key, dst := range map[string]*time.Duration{
			"maxRetryDuration":   &cfg.MaxRetryDuration,
			"minBackoffDuration": &cfg.MinBackoffDuration,
			"maxBackoffDuration": &cfg.MaxBackoffDuration,
		} {
			if s := str(rc, key); s != "" {
				d, err := time.ParseDuration(s)
				if err != nil {
					return j, model.NewProviderError("InvalidArgument", "invalid retryConfig."+key, 400)
				}
				*dst = d
			}
		}
		j.RetryConfig = cfg
	}
	if s := str(m, "attemptDeadline"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return j, model.NewProviderError("InvalidArgument", "invalid attemptDeadline", 400)
		}
		j.AttemptDeadline = d
	}
	return j, nil
}

// coreParseJobName mirrors core.ParseJobName without importing the core package
// (the codec is the wire adapter and must not depend on core internals beyond
// the typed model).
func coreParseJobName(name string) (project, location, job string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "jobs" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// ─── wire encoding ────────────────────────────────────────────────────────────

func jobToJSON(j schedstore.Job) map[string]any {
	out := map[string]any{
		"name":     jobResourceName(j),
		"schedule": j.Schedule,
		"state":    string(j.State),
	}
	if j.Description != "" {
		out["description"] = j.Description
	}
	if j.TimeZone != "" {
		out["timeZone"] = j.TimeZone
	}
	switch j.Target {
	case schedstore.TargetHTTP:
		out["httpTarget"] = httpTargetToJSON(j.HTTP)
	case schedstore.TargetPubSub:
		out["pubsubTarget"] = pubsubTargetToJSON(j.PubSub)
	case schedstore.TargetAppEngine:
		out["appEngineHttpTarget"] = appEngineTargetToJSON(j.AppEngine)
	}
	if j.RetryConfig != nil {
		out["retryConfig"] = retryToJSON(j.RetryConfig)
	}
	if j.AttemptDeadline > 0 {
		out["attemptDeadline"] = formatDuration(j.AttemptDeadline)
	}
	if !j.ScheduleTime.IsZero() {
		out["scheduleTime"] = j.ScheduleTime.UTC().Format(time.RFC3339Nano)
	}
	if !j.LastAttemptTime.IsZero() {
		out["lastAttemptTime"] = j.LastAttemptTime.UTC().Format(time.RFC3339Nano)
	}
	if !j.UserUpdateTime.IsZero() {
		out["userUpdateTime"] = j.UserUpdateTime.UTC().Format(time.RFC3339Nano)
	}
	if j.Status != nil {
		out["status"] = map[string]any{"code": j.Status.Code, "message": j.Status.Message}
	}
	return out
}

func jobResourceName(j schedstore.Job) string {
	return "projects/" + j.ProjectID + "/locations/" + j.Location + "/jobs/" + j.Name
}

func httpTargetToJSON(t *schedstore.HttpTarget) map[string]any {
	if t == nil {
		return nil
	}
	out := map[string]any{"uri": t.URI}
	if t.HTTPMethod != "" {
		out["httpMethod"] = t.HTTPMethod
	}
	if len(t.Headers) > 0 {
		out["headers"] = t.Headers
	}
	if len(t.Body) > 0 {
		out["body"] = base64.StdEncoding.EncodeToString(t.Body)
	}
	if t.OAuthToken != nil {
		tok := map[string]any{}
		if t.OAuthToken.Scope != "" {
			tok["scope"] = t.OAuthToken.Scope
		}
		if t.OAuthToken.ServiceAccountEmail != "" {
			tok["serviceAccountEmail"] = t.OAuthToken.ServiceAccountEmail
		}
		out["oauthToken"] = tok
	}
	if t.OidcToken != nil {
		tok := map[string]any{}
		if t.OidcToken.ServiceAccountEmail != "" {
			tok["serviceAccountEmail"] = t.OidcToken.ServiceAccountEmail
		}
		if t.OidcToken.Audience != "" {
			tok["audience"] = t.OidcToken.Audience
		}
		out["oidcToken"] = tok
	}
	return out
}

func pubsubTargetToJSON(t *schedstore.PubsubTarget) map[string]any {
	if t == nil {
		return nil
	}
	out := map[string]any{"topicName": t.TopicName}
	if len(t.Data) > 0 {
		out["data"] = base64.StdEncoding.EncodeToString(t.Data)
	}
	if len(t.Attributes) > 0 {
		out["attributes"] = t.Attributes
	}
	return out
}

func appEngineTargetToJSON(t *schedstore.AppEngineTarget) map[string]any {
	if t == nil {
		return nil
	}
	out := map[string]any{}
	if t.HTTPMethod != "" {
		out["httpMethod"] = t.HTTPMethod
	}
	if t.RelativeURI != "" {
		out["relativeUri"] = t.RelativeURI
	}
	if len(t.Headers) > 0 {
		out["headers"] = t.Headers
	}
	if len(t.Body) > 0 {
		out["body"] = base64.StdEncoding.EncodeToString(t.Body)
	}
	if t.Routing != nil {
		out["appEngineRouting"] = map[string]any{
			"service":  t.Routing.Service,
			"version":  t.Routing.Version,
			"instance": t.Routing.Instance,
			"host":     t.Routing.Host,
		}
	}
	return out
}

func retryToJSON(rc *schedstore.RetryConfig) map[string]any {
	out := map[string]any{"retryCount": rc.RetryCount}
	if rc.MaxDoublings != 0 {
		out["maxDoublings"] = rc.MaxDoublings
	}
	if rc.MaxRetryDuration > 0 {
		out["maxRetryDuration"] = formatDuration(rc.MaxRetryDuration)
	}
	if rc.MinBackoffDuration > 0 {
		out["minBackoffDuration"] = formatDuration(rc.MinBackoffDuration)
	}
	if rc.MaxBackoffDuration > 0 {
		out["maxBackoffDuration"] = formatDuration(rc.MaxBackoffDuration)
	}
	return out
}

// formatDuration renders a google.protobuf.Duration JSON value (seconds with a
// fractional part), never Go's "1h0m0s" form which the wire does not accept.
func formatDuration(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

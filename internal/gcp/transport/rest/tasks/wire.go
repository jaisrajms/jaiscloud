package tasks

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
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

func numFrom(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
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

func boolFrom(v any) bool {
	b, _ := v.(bool)
	return b
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

// queueFromBody decodes a Discovery Queue JSON object into the store model.
func queueFromBody(m map[string]any) (tasksstore.Queue, error) {
	var q tasksstore.Queue
	if raw := str(m, "name"); raw != "" {
		if strings.Contains(raw, "/") {
			_, _, id, ok := coreParseQueueName(raw)
			if !ok {
				return q, model.NewProviderError("InvalidArgument", "invalid queue name", 400)
			}
			q.Name = id
		} else {
			q.Name = raw
		}
	}
	if r, ok := m["appEngineRoutingOverride"].(map[string]any); ok {
		q.AppEngineRoutingOverride = &tasksstore.AppEngineRouting{
			Service:  str(r, "service"),
			Version:  str(r, "version"),
			Instance: str(r, "instance"),
			Host:     str(r, "host"),
		}
	}
	if rl, ok := m["rateLimits"].(map[string]any); ok {
		q.RateLimits = &tasksstore.RateLimits{
			MaxDispatchesPerSecond:  numFrom(rl["maxDispatchesPerSecond"]),
			MaxBurstSize:            int32(intFrom(rl["maxBurstSize"])),
			MaxConcurrentDispatches: int32(intFrom(rl["maxConcurrentDispatches"])),
		}
	}
	if rc, ok := m["retryConfig"].(map[string]any); ok {
		cfg := &tasksstore.RetryConfig{
			MaxAttempts:  int32(intFrom(rc["maxAttempts"])),
			MaxDoublings: int32(intFrom(rc["maxDoublings"])),
		}
		for key, dst := range map[string]*time.Duration{
			"maxRetryDuration": &cfg.MaxRetryDuration,
			"minBackoff":       &cfg.MinBackoff,
			"maxBackoff":       &cfg.MaxBackoff,
		} {
			if s := str(rc, key); s != "" {
				d, err := time.ParseDuration(s)
				if err != nil {
					return q, model.NewProviderError("InvalidArgument", "invalid retryConfig."+key, 400)
				}
				*dst = d
			}
		}
		q.RetryConfig = cfg
	}
	if sc, ok := m["stackdriverLoggingConfig"].(map[string]any); ok {
		q.StackdriverLoggingConfig = &tasksstore.StackdriverLoggingConfig{SamplingRatio: numFrom(sc["samplingRatio"])}
	}
	return q, nil
}

// taskFromBody decodes a Discovery Task JSON object into the store model.
func taskFromBody(m map[string]any) (tasksstore.Task, error) {
	var t tasksstore.Task
	if raw := str(m, "name"); raw != "" {
		if strings.Contains(raw, "/") {
			_, _, _, id, ok := coreParseTaskName(raw)
			if !ok {
				return t, model.NewProviderError("InvalidArgument", "invalid task name", 400)
			}
			t.Name = id
		} else {
			t.Name = raw
		}
	}
	if hr, ok := m["httpRequest"].(map[string]any); ok {
		r := &tasksstore.HttpRequest{
			URL:        str(hr, "url"),
			HTTPMethod: str(hr, "httpMethod"),
			Headers:    strMap(hr["headers"]),
		}
		body, err := decodeBytes(hr["body"])
		if err != nil {
			return t, model.NewProviderError("InvalidArgument", "httpRequest.body is not valid base64", 400)
		}
		r.Body = body
		if o, ok := hr["oauthToken"].(map[string]any); ok {
			r.OAuthToken = &tasksstore.OAuthToken{ServiceAccountEmail: str(o, "serviceAccountEmail"), Scope: str(o, "scope")}
		}
		if o, ok := hr["oidcToken"].(map[string]any); ok {
			r.OidcToken = &tasksstore.OidcToken{ServiceAccountEmail: str(o, "serviceAccountEmail"), Audience: str(o, "audience")}
		}
		t.HTTP = r
		t.Target = tasksstore.TargetHTTP
	}
	if ar, ok := m["appEngineHttpRequest"].(map[string]any); ok {
		r := &tasksstore.AppEngineHttpRequest{
			HTTPMethod:  str(ar, "httpMethod"),
			RelativeURI: str(ar, "relativeUri"),
			Headers:     strMap(ar["headers"]),
		}
		body, err := decodeBytes(ar["body"])
		if err != nil {
			return t, model.NewProviderError("InvalidArgument", "appEngineHttpRequest.body is not valid base64", 400)
		}
		r.Body = body
		if rt, ok := ar["appEngineRouting"].(map[string]any); ok {
			r.AppEngineRouting = &tasksstore.AppEngineRouting{
				Service:  str(rt, "service"),
				Version:  str(rt, "version"),
				Instance: str(rt, "instance"),
				Host:     str(rt, "host"),
			}
		}
		t.AppEngine = r
		t.Target = tasksstore.TargetAppEngine
	}
	if s := str(m, "scheduleTime"); s != "" {
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return t, model.NewProviderError("InvalidArgument", "invalid scheduleTime", 400)
		}
		t.ScheduleTime = ts
	}
	if s := str(m, "dispatchDeadline"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return t, model.NewProviderError("InvalidArgument", "invalid dispatchDeadline", 400)
		}
		t.DispatchDeadline = d
	}
	return t, nil
}

// coreParseQueueName mirrors core.ParseQueueName without importing the core
// package (the codec is the wire adapter and must not depend on core internals
// beyond the typed model).
func coreParseQueueName(name string) (project, location, queue string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 6 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "queues" {
		return "", "", "", false
	}
	return segs[1], segs[3], segs[5], true
}

// coreParseTaskName mirrors core.ParseTaskName.
func coreParseTaskName(name string) (project, location, queue, task string, ok bool) {
	segs := strings.Split(strings.Trim(name, "/"), "/")
	if len(segs) != 8 || segs[0] != "projects" || segs[2] != "locations" || segs[4] != "queues" || segs[6] != "tasks" {
		return "", "", "", "", false
	}
	return segs[1], segs[3], segs[5], segs[7], true
}

// ─── wire encoding ────────────────────────────────────────────────────────────

func queueToJSON(q tasksstore.Queue) map[string]any {
	out := map[string]any{
		"name":  queueResourceName(q),
		"state": string(q.State),
	}
	if q.AppEngineRoutingOverride != nil {
		out["appEngineRoutingOverride"] = appEngineRoutingToJSON(q.AppEngineRoutingOverride)
	}
	if q.RateLimits != nil {
		out["rateLimits"] = map[string]any{
			"maxDispatchesPerSecond":  q.RateLimits.MaxDispatchesPerSecond,
			"maxBurstSize":            q.RateLimits.MaxBurstSize,
			"maxConcurrentDispatches": q.RateLimits.MaxConcurrentDispatches,
		}
	}
	if q.RetryConfig != nil {
		rc := map[string]any{"maxAttempts": q.RetryConfig.MaxAttempts}
		if q.RetryConfig.MaxDoublings != 0 {
			rc["maxDoublings"] = q.RetryConfig.MaxDoublings
		}
		if q.RetryConfig.MaxRetryDuration > 0 {
			rc["maxRetryDuration"] = formatDuration(q.RetryConfig.MaxRetryDuration)
		}
		if q.RetryConfig.MinBackoff > 0 {
			rc["minBackoff"] = formatDuration(q.RetryConfig.MinBackoff)
		}
		if q.RetryConfig.MaxBackoff > 0 {
			rc["maxBackoff"] = formatDuration(q.RetryConfig.MaxBackoff)
		}
		out["retryConfig"] = rc
	}
	if q.StackdriverLoggingConfig != nil {
		out["stackdriverLoggingConfig"] = map[string]any{"samplingRatio": q.StackdriverLoggingConfig.SamplingRatio}
	}
	if !q.PurgeTime.IsZero() {
		out["purgeTime"] = q.PurgeTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func queueResourceName(q tasksstore.Queue) string {
	return "projects/" + q.ProjectID + "/locations/" + q.Location + "/queues/" + q.Name
}

func taskToJSON(t tasksstore.Task) map[string]any {
	out := map[string]any{"name": taskResourceName(t)}
	switch t.Target {
	case tasksstore.TargetHTTP:
		if t.HTTP != nil {
			out["httpRequest"] = httpRequestToJSON(t.HTTP)
		}
	case tasksstore.TargetAppEngine:
		if t.AppEngine != nil {
			out["appEngineHttpRequest"] = appEngineRequestToJSON(t.AppEngine)
		}
	}
	if !t.ScheduleTime.IsZero() {
		out["scheduleTime"] = t.ScheduleTime.UTC().Format(time.RFC3339Nano)
	}
	if !t.CreateTime.IsZero() {
		out["createTime"] = t.CreateTime.UTC().Format(time.RFC3339Nano)
	}
	if t.DispatchDeadline > 0 {
		out["dispatchDeadline"] = formatDuration(t.DispatchDeadline)
	}
	if t.DispatchCount != 0 {
		out["dispatchCount"] = t.DispatchCount
	}
	if t.ResponseCount != 0 {
		out["responseCount"] = t.ResponseCount
	}
	if t.FirstAttempt != nil {
		out["firstAttempt"] = attemptToJSON(t.FirstAttempt)
	}
	if t.LastAttempt != nil {
		out["lastAttempt"] = attemptToJSON(t.LastAttempt)
	}
	return out
}

func taskResourceName(t tasksstore.Task) string {
	return "projects/" + t.ProjectID + "/locations/" + t.Location + "/queues/" + t.Queue + "/tasks/" + t.Name
}

func httpRequestToJSON(r *tasksstore.HttpRequest) map[string]any {
	out := map[string]any{"url": r.URL}
	if r.HTTPMethod != "" {
		out["httpMethod"] = r.HTTPMethod
	}
	if len(r.Headers) > 0 {
		out["headers"] = r.Headers
	}
	if len(r.Body) > 0 {
		out["body"] = base64.StdEncoding.EncodeToString(r.Body)
	}
	if r.OAuthToken != nil {
		tok := map[string]any{}
		if r.OAuthToken.ServiceAccountEmail != "" {
			tok["serviceAccountEmail"] = r.OAuthToken.ServiceAccountEmail
		}
		if r.OAuthToken.Scope != "" {
			tok["scope"] = r.OAuthToken.Scope
		}
		out["oauthToken"] = tok
	}
	if r.OidcToken != nil {
		tok := map[string]any{}
		if r.OidcToken.ServiceAccountEmail != "" {
			tok["serviceAccountEmail"] = r.OidcToken.ServiceAccountEmail
		}
		if r.OidcToken.Audience != "" {
			tok["audience"] = r.OidcToken.Audience
		}
		out["oidcToken"] = tok
	}
	return out
}

func appEngineRequestToJSON(r *tasksstore.AppEngineHttpRequest) map[string]any {
	out := map[string]any{}
	if r.HTTPMethod != "" {
		out["httpMethod"] = r.HTTPMethod
	}
	if r.RelativeURI != "" {
		out["relativeUri"] = r.RelativeURI
	}
	if len(r.Headers) > 0 {
		out["headers"] = r.Headers
	}
	if len(r.Body) > 0 {
		out["body"] = base64.StdEncoding.EncodeToString(r.Body)
	}
	if r.AppEngineRouting != nil {
		out["appEngineRouting"] = appEngineRoutingToJSON(r.AppEngineRouting)
	}
	return out
}

func appEngineRoutingToJSON(r *tasksstore.AppEngineRouting) map[string]any {
	return map[string]any{
		"service":  r.Service,
		"version":  r.Version,
		"instance": r.Instance,
		"host":     r.Host,
	}
}

func attemptToJSON(a *tasksstore.Attempt) map[string]any {
	out := map[string]any{}
	if !a.ScheduleTime.IsZero() {
		out["scheduleTime"] = a.ScheduleTime.UTC().Format(time.RFC3339Nano)
	}
	if !a.DispatchTime.IsZero() {
		out["dispatchTime"] = a.DispatchTime.UTC().Format(time.RFC3339Nano)
	}
	if !a.ResponseTime.IsZero() {
		out["responseTime"] = a.ResponseTime.UTC().Format(time.RFC3339Nano)
	}
	if a.ResponseStatus != nil {
		out["responseStatus"] = map[string]any{"code": a.ResponseStatus.Code, "message": a.ResponseStatus.Message}
	}
	return out
}

// formatDuration renders a google.protobuf.Duration JSON value (seconds with a
// fractional part), never Go's "1h0m0s" form which the wire does not accept.
func formatDuration(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

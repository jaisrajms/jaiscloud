// Package tasks is the REST transport for Cloud Tasks v2
// (cloudtasks.googleapis.com/v2), which manages push queues:
//
//	GET    /v2/projects/{p}/locations/{l}/queues
//	POST   /v2/projects/{p}/locations/{l}/queues
//	GET    /v2/projects/{p}/locations/{l}/queues/{q}
//	PATCH  /v2/projects/{p}/locations/{l}/queues/{q}
//	DELETE /v2/projects/{p}/locations/{l}/queues/{q}
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:pause
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:resume
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:purge
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:getIamPolicy
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:setIamPolicy
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}:testIamPermissions
//	GET    /v2/projects/{p}/locations/{l}/queues/{q}/tasks
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}/tasks
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}/tasks:batchCreate
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}/tasks:batchDelete
//	GET    /v2/projects/{p}/locations/{l}/queues/{q}/tasks/{t}
//	DELETE /v2/projects/{p}/locations/{l}/queues/{q}/tasks/{t}
//	POST   /v2/projects/{p}/locations/{l}/queues/{q}/tasks/{t}:run
//
// The Codec is a NormalizedRequest adapter; the Provider holds the routes.
// Neither owns business logic — both delegate to the single core Service shared
// with the gRPC transport (see internal/gcp/service/tasks).
package tasks

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "tasks"

// Codec decodes Cloud Tasks v2 REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope.
type Codec struct{}

// NewCodec returns the Cloud Tasks REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v2 queues/tasks path into a NormalizedRequest. Params carry
// project, location, queue (queue/task paths), task (task paths), body
// (POST/PATCH), updateMask (PATCH), and any query parameters.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+3 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project or locations resource in path", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	if m, err := parseJSON(body); err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	} else if m != nil {
		nr.Params["body"] = m
	}

	rest := seg[pi+2:]
	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}
	if len(rest) < 3 || rest[0] != "locations" || rest[2] != "queues" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr.Params["location"] = rest[1]
	if len(rest) >= 4 {
		nr.Params["queue"] = rest[3]
	}
	if len(rest) >= 5 && rest[4] == "tasks" && len(rest) >= 6 {
		nr.Params["task"] = rest[5]
	}

	nr.Action = tasksAction(rest, custom, r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// tasksAction maps the path segments after projects/{project} (plus the custom
// verb and HTTP method) to the provider action name.
func tasksAction(rest []string, custom, method string) string {
	isTaskCollection := len(rest) == 5 && rest[4] == "tasks"
	isTaskItem := len(rest) == 6 && rest[4] == "tasks"
	switch {
	case len(rest) == 3 && custom == "" && method == http.MethodGet:
		return "QueuesList"
	case len(rest) == 3 && custom == "" && method == http.MethodPost:
		return "QueuesCreate"
	case len(rest) == 4 && custom == "" && method == http.MethodGet:
		return "QueuesGet"
	case len(rest) == 4 && custom == "" && method == http.MethodPatch:
		return "QueuesPatch"
	case len(rest) == 4 && custom == "" && method == http.MethodDelete:
		return "QueuesDelete"
	case len(rest) == 4 && custom == "pause" && method == http.MethodPost:
		return "QueuesPause"
	case len(rest) == 4 && custom == "resume" && method == http.MethodPost:
		return "QueuesResume"
	case len(rest) == 4 && custom == "purge" && method == http.MethodPost:
		return "QueuesPurge"
	case len(rest) == 4 && custom == "getIamPolicy" && method == http.MethodPost:
		return "QueuesGetIamPolicy"
	case len(rest) == 4 && custom == "setIamPolicy" && method == http.MethodPost:
		return "QueuesSetIamPolicy"
	case len(rest) == 4 && custom == "testIamPermissions" && method == http.MethodPost:
		return "QueuesTestIamPermissions"
	case isTaskCollection && custom == "" && method == http.MethodGet:
		return "TasksList"
	case isTaskCollection && custom == "" && method == http.MethodPost:
		return "TasksCreate"
	case isTaskCollection && custom == "batchCreate" && method == http.MethodPost:
		return "TasksBatchCreate"
	case isTaskCollection && custom == "batchDelete" && method == http.MethodPost:
		return "TasksBatchDelete"
	case isTaskItem && custom == "" && method == http.MethodGet:
		return "TasksGet"
	case isTaskItem && custom == "" && method == http.MethodDelete:
		return "TasksDelete"
	case isTaskItem && custom == "run" && method == http.MethodPost:
		return "TasksRun"
	}
	return ""
}

// Encode serialises a provider response as JSON.
func (c *Codec) Encode(_ *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCP error envelope.
func (c *Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	env := map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": perr.Message,
			"status":  statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

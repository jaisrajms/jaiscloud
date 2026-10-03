// Package scheduler is the REST transport for Cloud Scheduler v1
// (cloudscheduler.googleapis.com/v1), which manages cron jobs:
//
//	GET    /v1/projects/{project}/locations/{location}/jobs
//	POST   /v1/projects/{project}/locations/{location}/jobs
//	GET    /v1/projects/{project}/locations/{location}/jobs/{job}
//	PATCH  /v1/projects/{project}/locations/{location}/jobs/{job}
//	DELETE /v1/projects/{project}/locations/{location}/jobs/{job}
//	POST   /v1/projects/{project}/locations/{location}/jobs/{job}:pause
//	POST   /v1/projects/{project}/locations/{location}/jobs/{job}:resume
//	POST   /v1/projects/{project}/locations/{location}/jobs/{job}:run
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and this surface follows the vendored Discovery
// document. The Codec is a NormalizedRequest adapter; the Provider holds the
// routes. Neither owns business logic — both delegate to the single core
// Service shared with the gRPC transport (see internal/gcp/service/scheduler).
package scheduler

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "scheduler"

// Codec decodes Cloud Scheduler v1 REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope.
type Codec struct{}

// NewCodec returns the Cloud Scheduler REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 jobs path into a NormalizedRequest. Params carry project,
// location, job (item paths), body (POST/PATCH), and updateMask (PATCH), plus
// any query parameters (pageSize, pageToken).
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
	if len(rest) < 3 || rest[0] != "locations" || rest[2] != "jobs" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr.Params["location"] = rest[1]
	if len(rest) >= 4 {
		nr.Params["job"] = rest[3]
	}

	nr.Action = schedulerAction(rest, custom, r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// schedulerAction maps the path segments after projects/{project} (plus the
// custom verb and HTTP method) to the provider action name.
func schedulerAction(rest []string, custom, method string) string {
	switch {
	case len(rest) == 3 && custom == "" && method == http.MethodGet:
		return "JobsList"
	case len(rest) == 3 && custom == "" && method == http.MethodPost:
		return "JobsCreate"
	case len(rest) == 4 && custom == "" && method == http.MethodGet:
		return "JobsGet"
	case len(rest) == 4 && custom == "" && method == http.MethodPatch:
		return "JobsPatch"
	case len(rest) == 4 && custom == "" && method == http.MethodDelete:
		return "JobsDelete"
	case len(rest) == 4 && custom == "pause" && method == http.MethodPost:
		return "JobsPause"
	case len(rest) == 4 && custom == "resume" && method == http.MethodPost:
		return "JobsResume"
	case len(rest) == 4 && custom == "run" && method == http.MethodPost:
		return "JobsRun"
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

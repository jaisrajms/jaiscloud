// Package workflowexecutions is the REST transport for Cloud Workflow
// Executions v1 (workflowexecutions.googleapis.com).
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The four implemented methods are the REST mirrors of the
// cloud.google.com/go/workflows/executions/apiv1 Executions RPCs:
//
//	GET    /v1/projects/{p}/locations/{l}/workflows/{w}/executions             executions.list
//	POST   /v1/projects/{p}/locations/{l}/workflows/{w}/executions             executions.create
//	GET    /v1/projects/{p}/locations/{l}/workflows/{w}/executions/{e}         executions.get
//	POST   /v1/projects/{p}/locations/{l}/workflows/{w}/executions/{e}:cancel  executions.cancel
//
// The other v1 methods (triggerPubsubExecution, deleteExecutionHistory,
// exportData, callbacks.list, stepEntries.*) are not implemented and return 404
// NOT_FOUND for their unrecognized paths.
//
// The Codec is a NormalizedRequest adapter (HTTP path/body ↔ the core's typed
// API); the Provider holds the routes. Neither owns business logic — both
// delegate to the single core Service shared with the gRPC transport (see
// internal/gcp/service/workflowexecutions).
package workflowexecutions

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "workflowexecutions"

// Codec decodes Cloud Workflow Executions REST requests into a
// NormalizedRequest and encodes provider responses as the GCP JSON envelope. It
// satisfies adapter.Codec structurally (the adapter package imports this
// package, so this package must not import it).
type Codec struct{}

// NewCodec returns the Workflow Executions REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 executions path into a NormalizedRequest. Params carry
// project, location, workflowId, executionId (item paths), the relative resource
// name, body (POST), and any query parameters (including view).
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := "/" + strings.TrimLeft(r.URL.EscapedPath(), "/")
	const prefix = "/v1/projects/"
	if !strings.HasPrefix(path, prefix) {
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}
	// segs = [project, locations, {l}, workflows, {w}, executions, ...]
	segs := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(segs) < 6 || segs[0] == "" ||
		segs[1] != "locations" || segs[2] == "" ||
		segs[3] != "workflows" || segs[4] == "" ||
		segs[5] != "executions" {
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	// Query parameters are decoded FIRST; the authoritative path segments are
	// assigned afterwards so a crafted ?project=/?location=/?workflowId= cannot
	// override the resource addressed by the URL.
	queryToParams(r, nr.Params)

	project, location, workflowID := segs[0], segs[2], segs[4]
	collection := "locations/" + location + "/workflows/" + workflowID + "/executions"

	switch len(segs) {
	case 6:
		// The executions collection: list (GET) or create (POST).
		nr.Params["name"] = collection
		switch r.Method {
		case http.MethodGet:
			nr.Action = "ListExecutions"
		case http.MethodPost:
			nr.Action = "CreateExecution"
		default:
			return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
		}
	case 7:
		// An execution item, optionally a ":cancel" custom method.
		last := segs[6]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			if last[i+1:] != "cancel" || r.Method != http.MethodPost || last[:i] == "" {
				return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
			}
			nr.Params["executionId"] = last[:i]
			nr.Params["name"] = collection + "/" + last[:i]
			nr.Action = "CancelExecution"
			break
		}
		if r.Method != http.MethodGet || last == "" {
			return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
		}
		nr.Params["executionId"] = last
		nr.Params["name"] = collection + "/" + last
		nr.Action = "GetExecution"
	default:
		// callbacks/stepEntries and anything deeper are not implemented.
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}

	nr.Params["project"] = project
	nr.Params["location"] = location
	nr.Params["workflowId"] = workflowID
	nr.Params["apiVersion"] = "v1"
	if len(body) > 0 {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	}
	return nr, nil
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

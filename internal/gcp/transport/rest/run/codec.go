// Package run is the REST transport for Cloud Run Admin v2
// (run.googleapis.com), which manages services, revisions and operations:
//
//	POST   /v2/projects/{p}/locations/{l}/services?serviceId={id}
//	GET    /v2/projects/{p}/locations/{l}/services
//	GET    /v2/projects/{p}/locations/{l}/services/{id}
//	PATCH  /v2/projects/{p}/locations/{l}/services/{id}?updateMask=...
//	DELETE /v2/projects/{p}/locations/{l}/services/{id}
//	GET    /v2/projects/{p}/locations/{l}/services/{id}:getIamPolicy
//	POST   /v2/projects/{p}/locations/{l}/services/{id}:setIamPolicy
//	POST   /v2/projects/{p}/locations/{l}/services/{id}:testIamPermissions
//	GET    /v2/projects/{p}/locations/{l}/services/{id}/revisions
//	GET    /v2/projects/{p}/locations/{l}/services/{id}/revisions/{rev}
//	GET    /v2/projects/{p}/locations/{l}/operations/{operation-run-*}
//
// Cloud Run shares the canonical /v2/projects/{p}/locations/{l}/... path with
// Cloud Functions and Cloud Tasks on the single emulator origin, so the router
// disambiguates on the resource segment (services/revisions) and on Cloud Run's
// run-prefixed operation ids. Terraform/gcloud use a "/run" path prefix, which
// the codec also accepts. The Codec is a NormalizedRequest adapter and the
// Provider holds the routes; both delegate to the transport-neutral core
// (internal/gcp/service/run). REST-first: no gRPC server is registered (CR4).
package run

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "run"

// Codec decodes Cloud Run v2 REST requests into a NormalizedRequest and encodes
// provider responses as Cloud Run JSON.
type Codec struct{}

// NewCodec returns the Cloud Run REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a Cloud Run v2 path into a NormalizedRequest. Params carry
// project, location, service/revision/operation (item paths), body (POST/PATCH)
// and any query parameters.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+3 >= len(seg) || seg[pi+2] != "locations" {
		return nil, model.NewProviderError("InvalidRequest", "missing project or location in path", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	nr.Params["location"] = seg[pi+3]
	queryToParams(r, nr.Params)
	if m, err := parseJSON(body); err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	} else if m != nil {
		nr.Params["body"] = m
	}

	rest := seg[pi+4:]
	if len(rest) == 0 {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	action := runAction(rest, r.Method, nr.Params)
	if action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr.Action = action
	return nr, nil
}

// runAction maps the path segments after projects/{p}/locations/{l}, the HTTP
// method and (for item paths) the custom verb to the provider action. It also
// fills the resource-name params.
func runAction(rest []string, method string, params map[string]any) string {
	switch rest[0] {
	case "services":
		return serviceAction(rest, method, params)
	case "operations":
		return operationAction(rest, method, params)
	}
	return ""
}

func serviceAction(rest []string, method string, params map[string]any) string {
	switch len(rest) {
	case 1:
		switch method {
		case http.MethodGet:
			return "ListServices"
		case http.MethodPost:
			return "CreateService"
		}
	case 2:
		id, verb := splitVerb(rest[1])
		params["service"] = id
		switch verb {
		case "getIamPolicy":
			return "GetIamPolicy"
		case "setIamPolicy":
			return "SetIamPolicy"
		case "testIamPermissions":
			return "TestIamPermissions"
		case "":
			switch method {
			case http.MethodGet:
				return "GetService"
			case http.MethodPatch:
				return "UpdateService"
			case http.MethodDelete:
				return "DeleteService"
			}
		}
	case 3:
		if rest[2] == "revisions" && method == http.MethodGet {
			params["service"] = rest[1]
			return "ListRevisions"
		}
	case 4:
		if rest[2] == "revisions" {
			params["service"] = rest[1]
			id, _ := splitVerb(rest[3])
			params["revision"] = id
			if method == http.MethodGet {
				return "GetRevision"
			}
		}
	}
	return ""
}

func operationAction(rest []string, method string, params map[string]any) string {
	switch len(rest) {
	case 1:
		if method == http.MethodGet {
			return "ListOperations"
		}
	case 2:
		id, verb := splitVerb(rest[1])
		params["operation"] = id
		switch {
		case verb == "wait" && method == http.MethodPost:
			return "WaitOperation"
		case verb == "cancel" && method == http.MethodPost:
			return "CancelOperation"
		case verb == "" && method == http.MethodGet:
			return "GetOperation"
		case verb == "" && method == http.MethodDelete:
			return "DeleteOperation"
		}
	}
	return ""
}

// Encode serialises a provider response as Cloud Run JSON.
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

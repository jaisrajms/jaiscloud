// Package resourcemanager is the REST transport for the Cloud Resource Manager
// v1 project surface (cloudresourcemanager.googleapis.com/v1) that the
// hashicorp/google Terraform and Pulumi Google providers require:
//
//	GET  /v1/projects/{project}                      (projects.get)
//	POST /v1/projects/{project}:getIamPolicy         (projects.getIamPolicy)
//	POST /v1/projects/{project}:setIamPolicy         (projects.setIamPolicy)
//	POST /v1/projects/{project}:testIamPermissions   (projects.testIamPermissions)
//
// Real GCP's proto-defined Resource Manager surface is v3 and is served over
// both gRPC and REST, but this legacy v1 REST surface has no v3 transcode: the
// Codec/Provider are a thin v1 <-> core adapter over the canonical v3 semantics
// in internal/gcp/service/resourcemanager (projectId, projectNumber, name =
// displayName, lifecycleState = state). Neither owns business logic.
//
// A dedicated codec is required because project-level IAM has no resource
// segment after the project: the custom verb attaches directly to the project
// segment, so the generic JSONCodec (which derives resourceType/name from the
// segments after projects/{project}) has nothing to work with.
package resourcemanager

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "resourcemanager"

// Codec decodes Cloud Resource Manager v1 REST requests into a
// NormalizedRequest and encodes provider responses as the GCP JSON envelope. It
// satisfies adapter.Codec structurally (the adapter package imports this
// package, so this package must not import it).
type Codec struct {
	Service string
}

// NewCodec returns the Cloud Resource Manager REST codec.
func NewCodec() *Codec { return &Codec{Service: ServiceName} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return c.Service }

// Decode parses a v1 Resource Manager path into a NormalizedRequest.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+1 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}
	// The router only claims the project-last shape, but stay defensive: a
	// trailing resource segment is not part of the supported surface.
	if pi+1 != len(seg)-1 {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	projectSeg := seg[pi+1]
	project := projectSeg
	custom := ""
	if i := strings.IndexByte(projectSeg, ':'); i >= 0 {
		project = projectSeg[:i]
		custom = projectSeg[i+1:]
	}
	if project == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 404)
	}

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = project
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// The project IAM custom methods are POST-only per Discovery; a bare
	// project read is GET.
	switch {
	case custom == "getIamPolicy" && r.Method == http.MethodPost:
		nr.Action = "ProjectGetIamPolicy"
	case custom == "setIamPolicy" && r.Method == http.MethodPost:
		nr.Action = "ProjectSetIamPolicy"
	case custom == "testIamPermissions" && r.Method == http.MethodPost:
		nr.Action = "ProjectTestIamPermissions"
	case custom == "" && r.Method == http.MethodGet:
		nr.Action = "ProjectGet"
	default:
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// Encode serialises a provider response as JSON.
func (c *Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *Codec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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

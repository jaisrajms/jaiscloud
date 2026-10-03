// Package serviceusage is the REST transport for Service Usage v1
// (serviceusage.googleapis.com/v1), which manages a project's enabled APIs:
//
//	GET  /v1/projects/{project}/services
//	GET  /v1/projects/{project}/services/{service}
//	POST /v1/projects/{project}/services:batchEnable
//	POST /v1/projects/{project}/services/{service}:enable
//	POST /v1/projects/{project}/services/{service}:disable
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and this surface follows the vendored Discovery
// document. The Codec is a NormalizedRequest adapter (HTTP path/body ↔ the
// core's typed API); the Provider holds the routes. Neither owns business logic
// — both delegate to the single core Service shared with the gRPC transport
// (see internal/gcp/service/serviceusage).
//
// A dedicated codec is used rather than the generic JSONCodec for two reasons:
// the custom verb attaches to the "services" collection segment
// (services:batchEnable) rather than to a resource id, and the "services"
// resource type also appears beneath Dataproc Metastore's
// locations/{location}/services — teaching detectResourceType about it would
// make that generic scan ambiguous. The router (detectV1Service) claims these
// paths before the GCS raw-media fallback.
package serviceusage

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "serviceusage"

// Codec decodes Service Usage v1 REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope. It satisfies
// adapter.Codec structurally (the adapter package imports this package, so this
// package must not import it).
type Codec struct{}

// NewCodec returns the Service Usage REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 services path into a NormalizedRequest. Params carry
// project, name, service (item paths), and body (POST), plus any query
// parameters (filter, pageSize, pageToken).
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+2 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project or services resource in path", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	rest := seg[pi+2:]
	// Strip a trailing custom-method suffix (":batchEnable", ":enable",
	// ":disable") so the "services" collection marker and the service id are
	// the only remaining segments.
	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}
	if len(rest) == 0 || rest[0] != "services" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	nr.Params["name"] = strings.Join(rest, "/")
	if len(rest) >= 2 {
		nr.Params["service"] = rest[1]
	}

	nr.Action = serviceUsageAction(rest, custom, r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// serviceUsageAction maps the path segments after projects/{project} (plus the
// custom verb and HTTP method) to the provider action name.
func serviceUsageAction(rest []string, custom, method string) string {
	switch {
	case len(rest) == 1 && custom == "" && method == http.MethodGet:
		return "ServicesList"
	case len(rest) == 1 && custom == "batchEnable" && method == http.MethodPost:
		return "ServicesBatchEnable"
	case len(rest) == 2 && custom == "" && method == http.MethodGet:
		return "ServicesGet"
	case len(rest) == 2 && custom == "enable" && method == http.MethodPost:
		return "ServicesEnable"
	case len(rest) == 2 && custom == "disable" && method == http.MethodPost:
		return "ServicesDisable"
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

// Package serviceusage is the REST transport for Service Usage v1
// (serviceusage.googleapis.com/v1), which manages a project's enabled APIs:
//
//	GET  /v1/projects/{project}/services
//	GET  /v1/projects/{project}/services:batchGet
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

// Decode parses a v1 (or v1beta1) services path into a NormalizedRequest.
// Params carry project, name, service (item paths), and body (POST), plus any
// query parameters (filter, pageSize, pageToken). The top-level v1beta1
// operations path (GET /v1beta1/operations/{id}) decodes as OperationsGet.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	// Service Usage v1beta1's generateServiceIdentity returns a top-level
	// operation (operations/{id}) that the client polls at the v1beta1 base URL.
	// That path has no project segment, so it is decoded before the project scan.
	if len(seg) == 3 && seg[0] == "v1beta1" && seg[1] == "operations" && r.Method == http.MethodGet {
		nr.Params["name"] = "operations/" + seg[2]
		nr.Action = "OperationsGet"
		return nr, nil
	}
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

	nr.Params["project"] = seg[pi+1]
	// `names` is a repeated query parameter (services:batchGet); keep every
	// value, not just the first, so the handler sees the whole batch.
	if names, ok := r.URL.Query()["names"]; ok {
		nr.Params["names"] = names
	}
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	version := ""
	if len(seg) > 0 {
		version = seg[0]
	}
	rest := seg[pi+2:]
	// Strip a trailing custom-method suffix (":batchEnable", ":enable",
	// ":disable", ":generateServiceIdentity") so the "services" collection marker
	// and the service id are the only remaining segments.
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

	nr.Action = serviceUsageAction(version, rest, custom, r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// serviceUsageAction maps the path segments after projects/{project} (plus the
// version, the custom verb and the HTTP method) to the provider action name.
// The emulator serves the enable/disable/get/list family on v1 only, and
// generateServiceIdentity on v1beta1 only (the v1 Discovery declares no such
// method; real GCP answers 404 for a v1 path). Both versions therefore resolve
// exactly the paths the router claims, and a wrong-version request stays
// unsupported rather than being quietly served with the other version's shape.
func serviceUsageAction(version string, rest []string, custom, method string) string {
	switch {
	case version != "v1" && version != "v1beta1":
		return ""
	case len(rest) == 1 && custom == "" && method == http.MethodGet && version == "v1":
		return "ServicesList"
	case len(rest) == 1 && custom == "batchGet" && method == http.MethodGet && version == "v1":
		return "ServicesBatchGet"
	case len(rest) == 1 && custom == "batchEnable" && method == http.MethodPost && version == "v1":
		return "ServicesBatchEnable"
	case len(rest) == 2 && custom == "" && method == http.MethodGet && version == "v1":
		return "ServicesGet"
	case len(rest) == 2 && custom == "enable" && method == http.MethodPost && version == "v1":
		return "ServicesEnable"
	case len(rest) == 2 && custom == "disable" && method == http.MethodPost && version == "v1":
		return "ServicesDisable"
	case len(rest) == 2 && custom == "generateServiceIdentity" && method == http.MethodPost && version == "v1beta1":
		return "ServicesGenerateServiceIdentity"
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
//
// A provider may carry the legacy pre-google.rpc errors[] entry in
// ProviderError.Data["reason"] (the same convention the GCS codec uses). Real
// Service Usage includes it on the 403 for an unknown project, and the
// Google apiary client surfaces it as *googleapi.Error.Errors — its Error()
// string then ends with the reason ("…, forbidden"), which the Terraform
// Google provider matches (TF4-1-1).
func (c *Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	statusStr, _ := gcperr.Resolve(perr)
	errObj := map[string]any{
		"code":    status,
		"message": perr.Message,
		"status":  statusStr,
	}
	if reason, _ := perr.Data["reason"].(string); reason != "" {
		errObj["errors"] = []map[string]any{{
			"domain":  "global",
			"reason":  reason,
			"message": perr.Message,
		}}
	}
	env := map[string]any{"error": errObj}
	out, _ := json.Marshal(env)
	return status, headers, out
}

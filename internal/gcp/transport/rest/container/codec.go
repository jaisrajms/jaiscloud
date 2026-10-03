// Package container is the REST transport for Google Kubernetes Engine (GKE) v1
// (container.googleapis.com), which manages clusters:
//
//	GET    /v1/projects/{project}/locations/{location}/clusters
//	POST   /v1/projects/{project}/locations/{location}/clusters
//	GET    /v1/projects/{project}/locations/{location}/clusters/{cluster}
//	DELETE /v1/projects/{project}/locations/{location}/clusters/{cluster}
//	GET    /v1/projects/{project}/locations/{location}/operations
//	GET    /v1/projects/{project}/locations/{location}/operations/{operation}
//
// GKE shares the canonical /v1/projects/{project}/locations/{location}/clusters
// path with Managed Kafka on the single emulator origin, so the two are
// disambiguated by host: a request whose Host's first DNS label is "container"
// is GKE (container.googleapis.com), and Terraform/gcloud reach the same surface
// under a "/container" path prefix. The codec accepts both forms — it locates
// the projects/{project} segment regardless of what precedes it.
//
// Real GKE defaults to gRPC (google.container.v1.ClusterManager); this surface is
// deliberately REST-only (matching floci-gcp and the HttpJson Java suite), so no
// gRPC server is registered. The Codec is a NormalizedRequest adapter and the
// Provider holds the routes; both delegate to the transport-neutral core Service
// (internal/gcp/service/container).
package container

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "container"

// Codec decodes GKE v1 REST requests into a NormalizedRequest and encodes
// provider responses as GKE JSON.
type Codec struct{}

// NewCodec returns the GKE REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a GKE v1 path into a NormalizedRequest. Params carry project,
// location, cluster/operation (item paths), body (POST), plus any query
// parameters (pageSize, pageToken).
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	pi := -1
	for i, s := range seg {
		if s == "projects" {
			pi = i
			break
		}
	}
	if pi < 0 || pi+4 >= len(seg) {
		return nil, model.NewProviderError("InvalidRequest", "missing project or locations resource in path", 404)
	}
	if seg[pi+2] != "locations" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
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

	resource := seg[pi+4]
	if len(seg) >= pi+6 {
		nr.Params[itemParam(resource)] = seg[pi+5]
	}

	nr.Action = containerAction(resource, len(seg)-(pi+4), r.Method)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// itemParam maps a collection segment to the path-parameter name.
func itemParam(resource string) string {
	if resource == "operations" {
		return "operation"
	}
	return "cluster"
}

// containerAction maps the collection segment after projects/{p}/locations/{l},
// its item depth, and the HTTP method to the provider action.
func containerAction(resource string, depth int, method string) string {
	switch resource {
	case "clusters":
		switch {
		case depth == 1 && method == http.MethodGet:
			return "ListClusters"
		case depth == 1 && method == http.MethodPost:
			return "CreateCluster"
		case depth == 2 && method == http.MethodGet:
			return "GetCluster"
		case depth == 2 && method == http.MethodDelete:
			return "DeleteCluster"
		}
	case "operations":
		switch {
		case depth == 1 && method == http.MethodGet:
			return "ListOperations"
		case depth == 2 && method == http.MethodGet:
			return "GetOperation"
		}
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

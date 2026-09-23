// Package datastore is the REST transport for the Cloud Datastore v1 service.
//
// Real GCP serves Datastore's proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document: eight data methods, each a POST to a project-segment
// custom verb —
//
//	POST /v1/projects/{projectId}:lookup
//	POST /v1/projects/{projectId}:runQuery
//	POST /v1/projects/{projectId}:runAggregationQuery
//	POST /v1/projects/{projectId}:beginTransaction
//	POST /v1/projects/{projectId}:commit
//	POST /v1/projects/{projectId}:rollback
//	POST /v1/projects/{projectId}:allocateIds
//	POST /v1/projects/{projectId}:reserveIds
//
// The Codec is a NormalizedRequest adapter (HTTP path/body ↔ the core's typed
// API); the Provider holds the routes. Neither owns business logic — both
// delegate to the single core Service shared with the gRPC transport (see
// internal/gcp/service/datastore).
package datastore

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "datastore"

// verbActions maps the REST custom verb (path suffix) to the provider action.
var verbActions = map[string]string{
	"lookup":              "Lookup",
	"runQuery":            "RunQuery",
	"runAggregationQuery": "RunAggregationQuery",
	"beginTransaction":    "BeginTransaction",
	"commit":              "Commit",
	"rollback":            "Rollback",
	"allocateIds":         "AllocateIds",
	"reserveIds":          "ReserveIds",
}

// Codec decodes Datastore REST requests into a NormalizedRequest and encodes
// provider responses as the GCP JSON envelope. It satisfies adapter.Codec
// structurally (the adapter package imports this package, so this package must
// not import it).
type Codec struct{}

// NewCodec returns the Datastore REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a POST /v1/projects/{projectId}:{verb} path into a
// NormalizedRequest. Params carry project, apiVersion, verb, and body.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	if r.Method != http.MethodPost {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	path := "/" + strings.TrimLeft(r.URL.Path, "/")
	rest := strings.TrimPrefix(path, "/v1/projects/")
	if rest == path {
		return nil, model.NewProviderError("InvalidRequest", "missing project in resource path", 400)
	}
	idx := strings.IndexByte(rest, ':')
	if idx <= 0 {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	project, verb := rest[:idx], rest[idx+1:]
	action, ok := verbActions[verb]
	if !ok {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = project
	nr.Params["apiVersion"] = "v1"
	nr.Params["verb"] = verb
	if len(body) > 0 {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	}
	nr.Action = action
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

// Package logging is the REST transport for Cloud Logging v2.
//
// Real GCP serves Logging's proto-defined v2 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The ga data methods implemented are:
//
//	POST   /v2/entries:write                          entries.write
//	POST   /v2/entries:list                           entries.list
//	GET    /v2/{parent}/logs                          logs.list
//	DELETE /v2/{logName}                              logs.delete
//	GET    /v2/monitoredResourceDescriptors           monitoredResourceDescriptors.list
//	GET/POST        /v2/{parent}/sinks                sinks.list / sinks.create
//	GET/PUT/PATCH/DELETE /v2/{sinkName}               sinks.get/update/patch/delete
//	GET/POST        /v2/{parent}/exclusions           exclusions.list / exclusions.create
//	GET/PATCH/DELETE /v2/{name}                       exclusions.get/patch/delete
//	GET/POST        /v2/{parent}/metrics              metrics.list / metrics.create
//	GET/PUT/DELETE  /v2/{metricName}                  metrics.get/update/delete
//
// entries.tail is bidirectional-streaming and stays gRPC-only; the
// settings/buckets/views and entries.copy families are not implemented (they are
// out of scope for this phase).
//
// The Codec is a NormalizedRequest adapter (HTTP path/body ↔ the core's typed
// API); the Provider holds the routes. Neither owns business logic — both
// delegate to the single core Service shared with the gRPC transport (see
// internal/gcp/service/logging).
package logging

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	core "jaiscloud/internal/gcp/service/logging"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "logging"

// Codec decodes Cloud Logging REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope. It satisfies
// adapter.Codec structurally (the adapter package imports this package, so this
// package must not import it).
type Codec struct{}

// NewCodec returns the Logging REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// knownUnimplemented are real Logging v2 methods the emulator deliberately does
// not serve over REST: entries.tail is bidirectional-streaming (gRPC-only) and
// entries.copy is out of scope for this phase. They are reported as 501
// UNIMPLEMENTED, distinct from an unknown path (404 NOT_FOUND).
var knownUnimplemented = map[string]bool{
	"entries:tail": true,
	"entries:copy": true,
}

// Decode parses a Logging v2 REST path into a NormalizedRequest. Params carry
// apiVersion, body (POST), the parsed parent/logName, and any query parameters.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := "/" + strings.TrimLeft(r.URL.EscapedPath(), "/")
	if !strings.HasPrefix(path, "/v2/") {
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}
	rest := strings.TrimPrefix(path, "/v2/")

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	switch {
	case r.Method == http.MethodPost && rest == "entries:write":
		nr.Action = "EntryWrite"
	case r.Method == http.MethodPost && rest == "entries:list":
		nr.Action = "EntryList"
	case r.Method == http.MethodGet && rest == "monitoredResourceDescriptors":
		nr.Action = "MonitoredResourceDescriptorList"
	case decodeMetricsPath(r, rest, nr):
		// Action/params are set by the helper. Checked before the log family so
		// a (non-canonical) raw-slash metric id containing a "logs" segment is
		// not misrouted.
	case r.Method == http.MethodGet && strings.HasSuffix(rest, "/logs"):
		nr.Action = "LogList"
		nr.Params["parent"] = strings.TrimSuffix(rest, "/logs")
	case r.Method == http.MethodDelete && strings.Contains(rest, "/logs/"):
		nr.Action = "LogDelete"
		nr.Params["logName"] = rest
	case decodeConfigPath(r, rest, nr):
		// Action/params are set by the helper.
	case r.Method == http.MethodPost && knownUnimplemented[rest]:
		return nil, model.NewProviderError("Unimplemented", "method not implemented over REST", 501)
	default:
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}

	nr.Params["apiVersion"] = "v2"
	if len(body) > 0 {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	}
	queryToParams(r, nr.Params)
	return nr, nil
}

// decodeMetricsPath recognises the logs-based metric paths and sets the action +
// resource params on nr. It returns false when rest is not a metric path (or
// uses an unsupported method on one).
//
//	GET    /v2/{parent}/metrics            metrics.list
//	POST   /v2/{parent}/metrics            metrics.create
//	GET    /v2/{metricName}                metrics.get
//	PUT    /v2/{metricName}                metrics.update
//	DELETE /v2/{metricName}                metrics.delete
//
// The metric id may itself contain slashes; clients percent-encode them as
// %2F (the id is one path segment on the wire), so the item id is reassembled
// from every remaining segment and URL-decoded.
func decodeMetricsPath(r *http.Request, rest string, nr *model.NormalizedRequest) bool {
	parts := strings.Split(rest, "/")
	if len(parts) < 3 || !core.IsLogScope(parts[0]) || parts[1] == "" || parts[2] != "metrics" {
		return false
	}
	parent := parts[0] + "/" + parts[1]
	if len(parts) == 3 {
		switch r.Method {
		case http.MethodGet:
			nr.Action, nr.Params["parent"] = "MetricList", parent
		case http.MethodPost:
			nr.Action, nr.Params["parent"] = "MetricCreate", parent
		default:
			return false
		}
		return true
	}
	decoded, err := url.PathUnescape(strings.Join(parts[3:], "/"))
	if err != nil || decoded == "" {
		return false
	}
	full := parent + "/metrics/" + url.PathEscape(decoded)
	switch r.Method {
	case http.MethodGet:
		nr.Action, nr.Params["metricName"] = "MetricGet", full
	case http.MethodPut:
		nr.Action, nr.Params["metricName"] = "MetricUpdate", full
	case http.MethodDelete:
		nr.Action, nr.Params["metricName"] = "MetricDelete", full
	default:
		return false
	}
	return true
}

// decodeConfigPath recognises the Logging config-plane paths and sets the
// action + resource params on nr. It returns false when rest is not a
// sink/exclusion path (or uses an unsupported method on one), so Decode can
// fall through to the not-found/unimplemented cases.
//
//	GET    /v2/{parent}/sinks               sinks.list
//	POST   /v2/{parent}/sinks               sinks.create
//	GET    /v2/{sinkName}                   sinks.get
//	PUT    /v2/{sinkName}                   sinks.update
//	PATCH  /v2/{sinkName}                   sinks.patch
//	DELETE /v2/{sinkName}                   sinks.delete
//	GET    /v2/{parent}/exclusions          exclusions.list
//	POST   /v2/{parent}/exclusions          exclusions.create
//	GET    /v2/{name}                       exclusions.get
//	PATCH  /v2/{name}                       exclusions.patch
//	DELETE /v2/{name}                       exclusions.delete
func decodeConfigPath(r *http.Request, rest string, nr *model.NormalizedRequest) bool {
	parts := strings.Split(rest, "/")
	if len(parts) < 3 || len(parts) > 4 {
		return false
	}
	if !core.IsLogScope(parts[0]) || parts[1] == "" {
		return false
	}
	collection := parts[2]
	if collection != "sinks" && collection != "exclusions" {
		return false
	}
	parent := parts[0] + "/" + parts[1]
	if len(parts) == 3 {
		switch {
		case r.Method == http.MethodGet && collection == "sinks":
			nr.Action, nr.Params["parent"] = "SinkList", parent
		case r.Method == http.MethodPost && collection == "sinks":
			nr.Action, nr.Params["parent"] = "SinkCreate", parent
		case r.Method == http.MethodGet && collection == "exclusions":
			nr.Action, nr.Params["parent"] = "ExclusionList", parent
		case r.Method == http.MethodPost && collection == "exclusions":
			nr.Action, nr.Params["parent"] = "ExclusionCreate", parent
		default:
			return false
		}
		return true
	}
	if parts[3] == "" {
		return false
	}
	full := parent + "/" + collection + "/" + parts[3]
	switch {
	case collection == "sinks" && r.Method == http.MethodGet:
		nr.Action, nr.Params["sinkName"] = "SinkGet", full
	case collection == "sinks" && r.Method == http.MethodPut:
		nr.Action, nr.Params["sinkName"] = "SinkUpdate", full
	case collection == "sinks" && r.Method == http.MethodPatch:
		nr.Action, nr.Params["sinkName"] = "SinkPatch", full
	case collection == "sinks" && r.Method == http.MethodDelete:
		nr.Action, nr.Params["sinkName"] = "SinkDelete", full
	case collection == "exclusions" && r.Method == http.MethodGet:
		nr.Action, nr.Params["name"] = "ExclusionGet", full
	case collection == "exclusions" && r.Method == http.MethodPatch:
		nr.Action, nr.Params["name"] = "ExclusionPatch", full
	case collection == "exclusions" && r.Method == http.MethodDelete:
		nr.Action, nr.Params["name"] = "ExclusionDelete", full
	default:
		return false
	}
	return true
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

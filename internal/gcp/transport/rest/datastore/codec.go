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
//
// Like real Datastore v1 REST, the codec accepts both application/json and
// application/x-protobuf request bodies (the official google-cloud-datastore
// HTTP transport sends protobuf) and replies in the negotiated encoding. The
// protobuf path is bridged through the protos' canonical JSON mapping so the
// existing JSON→core transcoding in wire.go remains the single REST builder of
// core inputs.
package datastore

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ServiceName is the wire service name.
const ServiceName = "datastore"

// protoResponseKey marks a NormalizedRequest whose response (and error envelope)
// must be encoded as protobuf rather than JSON.
const protoResponseKey = "jaiscloud:protoResponse"

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

// verbProtos maps a REST verb to its Datastore v1 request and response
// messages, used to decode an application/x-protobuf body and re-encode the
// provider's Discovery-shaped result as protobuf.
var verbProtos = map[string]struct {
	request  func() proto.Message
	response func() proto.Message
}{
	"lookup": {
		request:  func() proto.Message { return &datastorepb.LookupRequest{} },
		response: func() proto.Message { return &datastorepb.LookupResponse{} },
	},
	"runQuery": {
		request:  func() proto.Message { return &datastorepb.RunQueryRequest{} },
		response: func() proto.Message { return &datastorepb.RunQueryResponse{} },
	},
	"runAggregationQuery": {
		request:  func() proto.Message { return &datastorepb.RunAggregationQueryRequest{} },
		response: func() proto.Message { return &datastorepb.RunAggregationQueryResponse{} },
	},
	"beginTransaction": {
		request:  func() proto.Message { return &datastorepb.BeginTransactionRequest{} },
		response: func() proto.Message { return &datastorepb.BeginTransactionResponse{} },
	},
	"commit": {
		request:  func() proto.Message { return &datastorepb.CommitRequest{} },
		response: func() proto.Message { return &datastorepb.CommitResponse{} },
	},
	"rollback": {
		request:  func() proto.Message { return &datastorepb.RollbackRequest{} },
		response: func() proto.Message { return &datastorepb.RollbackResponse{} },
	},
	"allocateIds": {
		request:  func() proto.Message { return &datastorepb.AllocateIdsRequest{} },
		response: func() proto.Message { return &datastorepb.AllocateIdsResponse{} },
	},
	"reserveIds": {
		request:  func() proto.Message { return &datastorepb.ReserveIdsRequest{} },
		response: func() proto.Message { return &datastorepb.ReserveIdsResponse{} },
	},
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
// NormalizedRequest. Params carry project, apiVersion, verb, and body. An
// application/x-protobuf body is decoded through the datastorepb request and
// its canonical JSON mapping, so the provider sees the same body shape the JSON
// path produces.
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
		if isProtoContentType(r.Header.Get("Content-Type")) {
			m, err := decodeProtoBody(verb, body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed protobuf body", 400)
			}
			nr.Params["body"] = m
		} else {
			var m map[string]any
			if err := json.Unmarshal(body, &m); err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		}
	}
	if protoResponseRequested(r) {
		nr.Params[protoResponseKey] = true
	}
	nr.Action = action
	return nr, nil
}

// isProtoContentType reports whether a Content-Type selects the protobuf
// encoding. Parameters (e.g. "; charset=UTF-8") are ignored.
func isProtoContentType(ct string) bool {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	switch strings.ToLower(strings.TrimSpace(ct)) {
	case "application/x-protobuf", "application/protobuf":
		return true
	default:
		return false
	}
}

// protoResponseRequested reports whether the response must be encoded as
// protobuf: the request body is protobuf, alt=proto is set, or Accept prefers
// protobuf over JSON. Accept is honoured with q-values (RFC 7231): the highest
// q wins, ties go to the first-listed supported type.
func protoResponseRequested(r *http.Request) bool {
	if isProtoContentType(r.Header.Get("Content-Type")) {
		return true
	}
	if strings.EqualFold(r.URL.Query().Get("alt"), "proto") {
		return true
	}
	bestQ := -1.0
	bestProto := false
	for _, part := range strings.Split(strings.ToLower(r.Header.Get("Accept")), ",") {
		fields := strings.Split(part, ";")
		protoWanted, ok := acceptKind(strings.TrimSpace(fields[0]))
		if !ok {
			continue
		}
		q := 1.0
		for _, f := range fields[1:] {
			f = strings.TrimSpace(f)
			if v, err := strconv.ParseFloat(strings.TrimPrefix(f, "q="), 64); err == nil && strings.HasPrefix(f, "q=") {
				q = v
			}
		}
		if q > bestQ {
			bestQ, bestProto = q, protoWanted
		}
	}
	return bestQ >= 0 && bestProto
}

// acceptKind classifies an Accept media range. The +json suffix is checked
// before the "protobuf" substring so a vendor type like
// application/x-protobuf+json is treated as JSON.
func acceptKind(mt string) (protoWanted bool, ok bool) {
	switch {
	case mt == "application/json" || strings.HasSuffix(mt, "+json"):
		return false, true
	case mt == "application/x-protobuf" || mt == "application/protobuf":
		return true, true
	case strings.Contains(mt, "protobuf"):
		return true, true
	default:
		return false, false
	}
}

// decodeProtoBody unmarshals a datastorepb request and renders it through
// protojson, yielding the same map[string]any shape the JSON path feeds the
// provider. This keeps wire.go the only REST builder of core inputs.
func decodeProtoBody(verb string, body []byte) (map[string]any, error) {
	def, ok := verbProtos[verb]
	if !ok {
		return nil, fmt.Errorf("no protobuf request for verb %q", verb)
	}
	msg := def.request()
	if err := proto.Unmarshal(body, msg); err != nil {
		return nil, err
	}
	j, err := protojson.Marshal(msg)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(j, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// sanitizeNonFinite replaces non-finite float64 values with the strings
// protojson uses for them ("NaN"/"Infinity"/"-Infinity"). encoding/json cannot
// marshal NaN/±Inf, so without this a protobuf response carrying one would fail
// to encode. The replacement is valid protojson input, so the JSON fallback
// (should it ever run) stays well-formed too.
func sanitizeNonFinite(v any) any {
	switch x := v.(type) {
	case float64:
		switch {
		case math.IsNaN(x):
			return "NaN"
		case math.IsInf(x, 1):
			return "Infinity"
		case math.IsInf(x, -1):
			return "-Infinity"
		}
		return x
	case map[string]any:
		for k, vv := range x {
			x[k] = sanitizeNonFinite(vv)
		}
		return x
	case []any:
		for i, vv := range x {
			x[i] = sanitizeNonFinite(vv)
		}
		return x
	default:
		return v
	}
}

// protoResponseWanted reports whether Decode selected the protobuf response
// encoding for nr.
func protoResponseWanted(nr *model.NormalizedRequest) bool {
	if nr == nil {
		return false
	}
	b, _ := nr.Params[protoResponseKey].(bool)
	return b
}

// encodeProtoResponse converts a provider's Discovery-shaped result into the
// protobuf response for the request verb.
func encodeProtoResponse(nr *model.NormalizedRequest, data map[string]any) ([]byte, bool) {
	verb, _ := nr.Params["verb"].(string)
	def, ok := verbProtos[verb]
	if !ok {
		return nil, false
	}
	raw, err := json.Marshal(sanitizeNonFinite(data))
	if err != nil {
		return nil, false
	}
	msg := def.response()
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, msg); err != nil {
		return nil, false
	}
	out, err := proto.Marshal(msg)
	if err != nil {
		return nil, false
	}
	return out, true
}

// Encode serialises a provider response as protobuf when the request selected
// it, else as JSON.
func (c *Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	if protoResponseWanted(nr) {
		if out, ok := encodeProtoResponse(nr, resp.Data); ok {
			headers.Set("Content-Type", "application/x-protobuf")
			return status, headers, out
		}
		// The client negotiated protobuf: never answer with a mismatched JSON
		// body, so surface the encode failure as a protobuf google.rpc.Status.
		headers.Set("Content-Type", "application/x-protobuf")
		if out, err := proto.Marshal(&statuspb.Status{Code: int32(codes.Internal), Message: "encode failure"}); err == nil {
			return http.StatusInternalServerError, headers, out
		}
		return http.StatusInternalServerError, headers, nil
	}
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

// EncodeError serialises a ProviderError as a GCP error envelope — a
// google.rpc.Status when the request selected protobuf, else the JSON envelope.
func (c *Codec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	statusStr, _ := gcperr.Resolve(perr)
	if protoResponseWanted(nr) {
		headers.Set("Content-Type", "application/x-protobuf")
		code, ok := gcperr.GRPCCodeForStatus(statusStr)
		if !ok {
			code = codes.Internal
		}
		out, err := proto.Marshal(&statuspb.Status{Code: int32(code), Message: perr.Message})
		if err == nil {
			return status, headers, out
		}
	}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
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

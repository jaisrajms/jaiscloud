// Package metastore is the REST transport for the Dataproc Metastore v1
// control plane (metastore.googleapis.com/v1).
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The Codec is a NormalizedRequest adapter (HTTP path/body
// ↔ the core's typed API); the Provider holds the routes. Neither owns business
// logic — both delegate to the single core Service shared with the gRPC
// transport (see internal/gcp/service/metastore).
//
// Resources live under
// /v1/projects/{project}/locations/{location}/services[/{id}[/backups[/{bid}]|
// /metadataImports[/{mid}]]] plus the long-running operations surface at
// .../locations/{location}/operations[/{id}]. Create/Update/Delete return a
// done google.longrunning.Operation (Dataproc shape); Get/List return the
// resource inline. Custom-method verbs
// (:exportMetadata, :restore, :queryMetadata, :moveTableToDatabase,
// :alterLocation) are deferred to Unimplemented handlers.
package metastore

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ServiceName is the wire service name.
const ServiceName = "metastore"

// Codec decodes Dataproc Metastore REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope. It satisfies
// adapter.Codec structurally (the adapter package imports this package, so this
// package must not import it).
type Codec struct{}

// NewCodec returns the Dataproc Metastore REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// Decode parses a v1 Dataproc Metastore path into a NormalizedRequest.
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

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	// Query parameters are decoded FIRST; the authoritative path segments are
	// assigned afterwards so a crafted ?project=/?location= cannot override the
	// resource addressed by the URL.
	queryToParams(r, nr.Params)
	nr.Params["project"] = seg[pi+1]
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	rest := seg[pi+2:]
	if len(rest) < 3 || rest[0] != "locations" {
		return nil, model.NewProviderError("InvalidRequest", "expected locations/{location}/services in metastore path", 404)
	}
	nr.Params["location"] = rest[1]
	tail := rest[2:]

	if len(tail) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "missing metastore resource", 404)
	}

	// Strip a trailing custom-method suffix from the last segment.
	custom := ""
	last := tail[len(tail)-1]
	if i := strings.IndexByte(last, ':'); i >= 0 {
		custom = last[i+1:]
		tail[len(tail)-1] = last[:i]
	}

	var resourceType string
	isCollection := false
	switch {
	case tail[0] == "operations":
		resourceType = "operations"
		if len(tail) >= 2 {
			nr.Params["operationId"] = tail[1]
		}
		isCollection = len(tail) == 1
	case tail[0] == "services":
		resourceType = "services"
		switch len(tail) {
		case 1:
			isCollection = true
		case 2:
			nr.Params["serviceId"] = tail[1]
		case 3:
			// services/{id}/backups or services/{id}/metadataImports
			resourceType = tail[2]
			nr.Params["serviceId"] = tail[1]
			isCollection = true
		case 4:
			resourceType = tail[2]
			nr.Params["serviceId"] = tail[1]
			if tail[2] == "backups" {
				nr.Params["backupId"] = tail[3]
			} else if tail[2] == "metadataImports" {
				nr.Params["metadataImportId"] = tail[3]
			}
		default:
			return nil, model.NewProviderError("InvalidRequest", "unrecognized metastore path", 404)
		}
	default:
		return nil, model.NewProviderError("InvalidRequest", "expected services or operations in metastore path", 404)
	}

	nr.Params["resourceType"] = resourceType
	nr.Params["name"] = strings.Join(rest, "/")

	nr.Action = deriveMetastoreAction(resourceType, isCollection, r.Method, custom)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// deriveMetastoreAction maps (resourceType, isCollection, method, custom) to the
// action name. Create is POST on a collection; delete is DELETE on a resource;
// update is PATCH; the custom-method verbs defer to Unimplemented handlers.
func deriveMetastoreAction(resourceType string, isCollection bool, method, custom string) string {
	if custom != "" {
		switch resourceType {
		case "services":
			switch custom {
			case "exportMetadata":
				return "ExportMetadata"
			case "restore":
				return "RestoreService"
			case "queryMetadata":
				return "QueryMetadata"
			case "moveTableToDatabase":
				return "MoveTableToDatabase"
			case "alterLocation":
				return "AlterMetadataResourceLocation"
			}
		}
	}

	switch resourceType {
	case "services":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateService"
		case isCollection && method == http.MethodGet:
			return "ListServices"
		case method == http.MethodGet:
			return "GetService"
		case method == http.MethodPatch:
			return "UpdateService"
		case method == http.MethodDelete:
			return "DeleteService"
		}
	case "backups":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateBackup"
		case isCollection && method == http.MethodGet:
			return "ListBackups"
		case method == http.MethodGet:
			return "GetBackup"
		case method == http.MethodDelete:
			return "DeleteBackup"
		}
	case "metadataImports":
		switch {
		case isCollection && method == http.MethodPost:
			return "CreateMetadataImport"
		case isCollection && method == http.MethodGet:
			return "ListMetadataImports"
		case method == http.MethodGet:
			return "GetMetadataImport"
		case method == http.MethodPatch:
			return "UpdateMetadataImport"
		}
	case "operations":
		switch {
		case isCollection && method == http.MethodGet:
			return "ListOperations"
		case method == http.MethodGet:
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

package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// BigQueryCodec decodes the BigQuery v2 REST surface
// (bigquery.googleapis.com/bigquery/v2). Resources live under
// /bigquery/v2/projects/{project}/... (datasets, tables, jobs, queries, and the
// projects.serviceAccount endpoint). The codec also accepts the
// WithEndpoint-stripped form /projects/{project}/... that the apiary client
// emits when its endpoint option drops the bigquery/v2/ servicePath: both forms
// share the same "projects/{project}" tail, so the segment scan below resolves
// them identically. tabledata lives as trailing methods on a table path
// (.../tables/{id}/insertAll and .../tables/{id}/data); jobs.delete is a DELETE
// on .../jobs/{id}/delete (not a bare DELETE). The dataset-scoped routines and
// the table-scoped rowAccessPolicies resources are declared resource CRUD, so
// each method resolves to its own action; the dataset-scoped models resource is
// an explicit 501 (models are ML training artifacts the emulator does not
// model), and the routines/rowAccessPolicies IAM custom methods resolve to an
// Unimplemented action so the provider fails loud.
type BigQueryCodec struct {
	Service string
}

func (c *BigQueryCodec) ServiceName() string { return c.Service }

func (c *BigQueryCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// The discovery document carries no project or resource segment, so it is
	// claimed before the resource-path scan below (which would otherwise reject
	// the request for a missing project).
	if isBigQueryDiscoveryPath(r.URL.Path) {
		return c.decodeDiscovery(r)
	}
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

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Params["project"] = seg[pi+1]
	queryToParams(r, nr.Params)

	rest := seg[pi+2:]
	// The jobs.insert media-upload endpoint (load_table_from_file) carries a job
	// resource plus raw file bytes rather than a plain JSON body. Route it before
	// the JSON body parse, which would otherwise reject the upload framing.
	if isBigQueryUploadPath(r.URL.Path) {
		return c.decodeUpload(r, body, nr, rest)
	}

	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	action, datasetID, tableID, jobID, routineID, policyID := bigQueryAction(rest, r.Method)
	if action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported bigquery operation", 404)
	}
	if datasetID != "" {
		nr.Params["datasetId"] = datasetID
	}
	if tableID != "" {
		nr.Params["tableId"] = tableID
	}
	if jobID != "" {
		nr.Params["jobId"] = jobID
	}
	if routineID != "" {
		nr.Params["routineId"] = routineID
	}
	if policyID != "" {
		nr.Params["policyId"] = policyID
	}
	nr.Action = action
	return nr, nil
}

// isBigQueryDiscoveryPath reports whether path is one of the two discovery
// document probes the bq CLI and the official SDKs use: the current
// {endpoint}/$discovery/rest form (google-cloud-sdk >= 469, with
// ?version=v2) and the legacy /discovery/v1/apis/bigquery/v2/rest form. Leading
// slashes are collapsed so the endpoint/version concatenation the clients do
// cannot defeat the match.
func isBigQueryDiscoveryPath(path string) bool {
	p := "/" + strings.TrimLeft(path, "/")
	return p == "/$discovery/rest" || p == "/discovery/v1/apis/bigquery/v2/rest"
}

// decodeDiscovery resolves a discovery-document probe. version defaults to v2
// (the only version the emulator serves) and is validated by the provider.
func (c *BigQueryCodec) decodeDiscovery(r *http.Request) (*model.NormalizedRequest, error) {
	if r.Method != http.MethodGet {
		return nil, model.NewProviderError("InvalidRequest", "the bigquery discovery document is served only for GET", http.StatusMethodNotAllowed)
	}
	version := r.URL.Query().Get("version")
	if version == "" {
		version = "v2"
	}
	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	nr.Action = "GetDiscovery"
	nr.Params["version"] = version
	// The provider rewrites the document's rootUrl/baseUrl to the origin the
	// client called, so it must be absolute and reflect any proxy scheme.
	nr.Params[wire.BaseURLKey] = baseURLFromRequest(r)
	return nr, nil
}

// isBigQueryUploadPath reports whether path is a BigQuery jobs.insert media
// upload endpoint. The clients use both the /upload/bigquery/v2/ form (the
// Discovery `simple` media path, with uploadType=multipart|resumable) and the
// /resumable/upload/bigquery/v2/ form (the Discovery `resumable` media path).
func isBigQueryUploadPath(path string) bool {
	return strings.HasPrefix(path, "/upload/bigquery/v2/projects/") ||
		strings.HasPrefix(path, "/resumable/upload/bigquery/v2/projects/")
}

// decodeUpload handles the jobs.insert media-upload endpoint. A multipart
// upload is a single multipart/related POST (JSON job resource part, then the
// file bytes); a resumable upload is a POST that returns a session Location,
// followed by PUT chunks carrying Content-Range, and is finalized by the chunk
// that completes the source. Both feed the same configuration.load decoder as
// the gs:// source path; the job resource is a full Job (jobReference +
// configuration.load) exactly as a jobs.insert body.
func (c *BigQueryCodec) decodeUpload(r *http.Request, body []byte, nr *model.NormalizedRequest, rest []string) (*model.NormalizedRequest, error) {
	if len(rest) != 1 || rest[0] != "jobs" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported bigquery upload path", 404)
	}
	// The SDK expects an absolute Location header back, so pass the request base
	// URL down for the provider to build the resumable session URI.
	nr.Params[wire.BaseURLKey] = baseURLFromRequest(r)

	uploadType, _ := nr.Params["uploadType"].(string)
	if uploadType == "" && strings.HasPrefix(r.URL.Path, "/resumable/upload/") {
		uploadType = "resumable"
	}
	switch uploadType {
	case "multipart":
		nr.Action = "InsertJob"
		if err := parseMultipart(r, body, nr.Params); err != nil {
			return nil, err
		}
	case "resumable":
		if id, _ := nr.Params["upload_id"].(string); id != "" {
			// A chunk upload. The gateway buffers the chunk body (resumable is
			// not in the streaming-upload detection), so the bytes arrive whole.
			nr.Action = "InsertJobResumable"
			nr.Params[wire.MediaKey] = body
			if cr := r.Header.Get("Content-Range"); cr != "" {
				nr.Params["contentRange"] = cr
			}
			break
		}
		// Session start: the body is the Job resource, and X-Upload-Content-Type
		// declares the media type (informational for a load job).
		nr.Action = "InsertJobResumableStart"
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON job resource", 400)
		}
		nr.Params["body"] = m
		if ct := r.Header.Get("X-Upload-Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
	default:
		// uploadType=media cannot carry a configuration.load job resource, so it
		// is not a usable load path; fail loud rather than silently mis-loading.
		return nil, model.NewProviderError("InvalidRequest",
			"unsupported uploadType "+uploadType+" for a BigQuery load job (use multipart or resumable)", 400)
	}
	return nr, nil
}

// bigQueryAction maps the path segments after projects/{project} (plus the HTTP
// method) to the provider action name and the dataset/table/job/routine/policy
// IDs it carries. The apiary Datasets.Update/Tables.Update use PUT on the same
// resource paths as the PATCH-based Patch methods; both are routed to the
// merge-equivalent UpdateDataset/UpdateTable handlers (full-replace PUT
// semantics are out of scope for the emulator). routines and rowAccessPolicies
// are declared as resource CRUD in the Discovery document (list/get/insert/
// update/delete, plus rowAccessPolicies:batchDelete); their IAM custom methods
// resolve to an explicit Unimplemented action so the provider fails loud rather
// than the codec 404-ing them. models keeps its coarse action: every models
// method is an explicit 501.
func bigQueryAction(seg []string, method string) (action, datasetID, tableID, jobID, routineID, policyID string) {
	if len(seg) == 0 {
		return "", "", "", "", "", ""
	}
	switch seg[0] {
	case "datasets":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "CreateDataset", "", "", "", "", ""
			}
			if method == http.MethodGet {
				return "ListDatasets", "", "", "", "", ""
			}
		case 2:
			switch method {
			case http.MethodGet:
				return "GetDataset", seg[1], "", "", "", ""
			case http.MethodPatch:
				return "UpdateDataset", seg[1], "", "", "", ""
			case http.MethodPut:
				return "UpdateDataset", seg[1], "", "", "", ""
			case http.MethodDelete:
				return "DeleteDataset", seg[1], "", "", "", ""
			}
		case 3:
			switch seg[2] {
			case "tables":
				if method == http.MethodPost {
					return "CreateTable", seg[1], "", "", "", ""
				}
				if method == http.MethodGet {
					return "ListTables", seg[1], "", "", "", ""
				}
			case "routines":
				if method == http.MethodPost {
					return "InsertRoutine", seg[1], "", "", "", ""
				}
				if method == http.MethodGet {
					return "ListRoutines", seg[1], "", "", "", ""
				}
			case "models":
				return "Models", seg[1], "", "", "", ""
			}
		case 4:
			switch seg[2] {
			case "tables":
				switch method {
				case http.MethodGet:
					return "GetTable", seg[1], seg[3], "", "", ""
				case http.MethodPatch:
					return "UpdateTable", seg[1], seg[3], "", "", ""
				case http.MethodPut:
					return "UpdateTable", seg[1], seg[3], "", "", ""
				case http.MethodDelete:
					return "DeleteTable", seg[1], seg[3], "", "", ""
				}
			case "routines":
				if resourceSegment(seg[3]) != seg[3] {
					// routines.getIamPolicy / setIamPolicy / testIamPermissions.
					if method == http.MethodPost {
						return "RoutinesIam", seg[1], "", "", "", ""
					}
					return "", "", "", "", "", ""
				}
				switch method {
				case http.MethodGet:
					return "GetRoutine", seg[1], "", "", seg[3], ""
				case http.MethodPut:
					return "UpdateRoutine", seg[1], "", "", seg[3], ""
				case http.MethodDelete:
					return "DeleteRoutine", seg[1], "", "", seg[3], ""
				}
			case "models":
				return "Models", seg[1], "", "", "", ""
			}
		case 5:
			if seg[2] == "tables" {
				switch resourceSegment(seg[4]) {
				case "insertAll":
					if method == http.MethodPost {
						return "InsertAll", seg[1], seg[3], "", "", ""
					}
				case "data":
					if method == http.MethodGet {
						return "ListRows", seg[1], seg[3], "", "", ""
					}
				case "rowAccessPolicies":
					if method == http.MethodPost && seg[4] == "rowAccessPolicies:batchDelete" {
						return "BatchDeleteRowAccessPolicies", seg[1], seg[3], "", "", ""
					}
					if seg[4] != "rowAccessPolicies" {
						// rowAccessPolicies.getIamPolicy / setIamPolicy /
						// testIamPermissions on the policy path.
						if method == http.MethodPost {
							return "RowAccessPoliciesIam", seg[1], seg[3], "", "", ""
						}
						return "", "", "", "", "", ""
					}
					switch method {
					case http.MethodGet:
						return "ListRowAccessPolicies", seg[1], seg[3], "", "", ""
					case http.MethodPost:
						return "InsertRowAccessPolicy", seg[1], seg[3], "", "", ""
					}
				}
			}
		case 6:
			if seg[2] == "tables" && seg[4] == "rowAccessPolicies" {
				if resourceSegment(seg[5]) != seg[5] {
					if method == http.MethodPost {
						return "RowAccessPoliciesIam", seg[1], seg[3], "", "", ""
					}
					return "", "", "", "", "", ""
				}
				switch method {
				case http.MethodGet:
					return "GetRowAccessPolicy", seg[1], seg[3], "", "", seg[5]
				case http.MethodPut:
					return "UpdateRowAccessPolicy", seg[1], seg[3], "", "", seg[5]
				case http.MethodDelete:
					return "DeleteRowAccessPolicy", seg[1], seg[3], "", "", seg[5]
				}
			}
		}
	case "jobs":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "InsertJob", "", "", "", "", ""
			}
			if method == http.MethodGet {
				return "ListJobs", "", "", "", "", ""
			}
		case 2:
			if method == http.MethodGet {
				return "GetJob", "", "", seg[1], "", ""
			}
		case 3:
			switch seg[2] {
			case "delete":
				if method == http.MethodDelete {
					return "DeleteJob", "", "", seg[1], "", ""
				}
			case "cancel":
				if method == http.MethodPost {
					return "CancelJob", "", "", seg[1], "", ""
				}
			}
		}
	case "queries":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "Query", "", "", "", "", ""
			}
		case 2:
			if method == http.MethodGet {
				return "GetQueryResults", "", "", seg[1], "", ""
			}
		}
	case "serviceAccount":
		if len(seg) == 1 && method == http.MethodGet {
			return "GetServiceAccount", "", "", "", "", ""
		}
	}
	return "", "", "", "", "", ""
}

// resourceSegment strips a trailing custom-method suffix (for example
// "rowAccessPolicies:batchDelete") from a path segment, leaving the resource
// type used to derive the action name.
func resourceSegment(seg string) string {
	if i := strings.IndexByte(seg, ':'); i >= 0 {
		return seg[:i]
	}
	return seg
}

// Encode serialises a provider response as JSON.
func (c *BigQueryCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	// The discovery document is served from an embedded snapshot that only
	// changes with a release, so no client may cache it across an upgrade.
	if nr != nil && nr.Action == "GetDiscovery" {
		headers.Set("Cache-Control", "no-store")
	}
	// A resumable-upload session start returns its session URI in Location (and
	// no body); an incomplete chunk returns 308 with a Range header. These share
	// the GCS resumable wire contract (see internal/gcp/wire).
	if loc, ok := resp.Data[wire.LocationKey].(string); ok && loc != "" {
		headers.Set("Location", loc)
		return status, headers, nil
	}
	if rng, ok := resp.Data[wire.RangeKey].(string); ok && rng != "" {
		headers.Set("Range", rng)
		if so, ok := resp.Data[wire.StatusOverrideKey].(string); ok && so != "" {
			headers.Set("X-Http-Status-Code-Override", so)
		}
		return status, headers, nil
	}
	if so, ok := resp.Data[wire.StatusOverrideKey].(string); ok && so != "" {
		headers.Set("X-Http-Status-Code-Override", so)
		return status, headers, nil
	}
	// A 308 Resume Incomplete with no Range (empty session) must return an empty
	// body, not the "{}" the generic JSON marshal would emit.
	if status == http.StatusPermanentRedirect {
		return status, headers, nil
	}

	if raw, ok := resp.Data[wire.RawJSONKey].(json.RawMessage); ok {
		return status, headers, raw
	}
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as the BigQuery error envelope. Real
// BigQuery (unlike the modern gRPC-transcoded REST APIs) carries the legacy
// Errors array alongside the google.rpc status: the official client SDKs read
// error.errors[0].reason to populate their typed error (the Java
// BigQueryException builds its whole error list from that reason), so a
// response that omits it surfaces as a null error.
func (c *BigQueryCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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
			"errors": []any{
				map[string]any{
					"domain":  "global",
					"reason":  bigQueryReason(perr.Code),
					"message": perr.Message,
				},
			},
			"status": statusStr,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

// bigQueryReason maps a ProviderError code to the ErrorProto.reason value the
// BigQuery clients surface (for example a duplicate dataset is reason
// "duplicate", not the HTTP-derived "alreadyExists"). Unknown codes fall back
// to a non-empty generic reason so the envelope always stays SDK-parseable.
func bigQueryReason(code string) string {
	switch code {
	case "NotFound":
		return "notFound"
	case "AlreadyExists", "Conflict":
		return "duplicate"
	case "InvalidArgument", "InvalidRequest", "InvalidParameter", "FailedPrecondition":
		return "invalid"
	case "PreconditionFailed":
		return "conditionNotMet"
	case "InvalidQuery":
		return "invalidQuery"
	case "PermissionDenied":
		return "accessDenied"
	case "UnsupportedOperation", "Unimplemented":
		return "unsupported"
	default:
		return "internalError"
	}
}

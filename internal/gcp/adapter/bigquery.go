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
// models resources and the table-scoped rowAccessPolicies resource are deferred
// by the emulator; they resolve to their own action names here so the provider
// can fail loud with Unimplemented rather than the codec 404-ing them.
type BigQueryCodec struct {
	Service string
}

func (c *BigQueryCodec) ServiceName() string { return c.Service }

func (c *BigQueryCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
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

	action, datasetID, tableID, jobID := bigQueryAction(rest, r.Method)
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
	nr.Action = action
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
// method) to the provider action name and the dataset/table/job IDs it carries.
// The apiary Datasets.Update/Tables.Update use PUT on the same resource paths
// as the PATCH-based Patch methods; both are routed to the merge-equivalent
// UpdateDataset/UpdateTable handlers (full-replace PUT semantics are out of
// scope for the emulator).
func bigQueryAction(seg []string, method string) (action, datasetID, tableID, jobID string) {
	if len(seg) == 0 {
		return "", "", "", ""
	}
	switch seg[0] {
	case "datasets":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "CreateDataset", "", "", ""
			}
			if method == http.MethodGet {
				return "ListDatasets", "", "", ""
			}
		case 2:
			switch method {
			case http.MethodGet:
				return "GetDataset", seg[1], "", ""
			case http.MethodPatch:
				return "UpdateDataset", seg[1], "", ""
			case http.MethodPut:
				return "UpdateDataset", seg[1], "", ""
			case http.MethodDelete:
				return "DeleteDataset", seg[1], "", ""
			}
		case 3:
			switch seg[2] {
			case "tables":
				if method == http.MethodPost {
					return "CreateTable", seg[1], "", ""
				}
				if method == http.MethodGet {
					return "ListTables", seg[1], "", ""
				}
			case "routines":
				return "Routines", seg[1], "", ""
			case "models":
				return "Models", seg[1], "", ""
			}
		case 4:
			switch seg[2] {
			case "tables":
				switch method {
				case http.MethodGet:
					return "GetTable", seg[1], seg[3], ""
				case http.MethodPatch:
					return "UpdateTable", seg[1], seg[3], ""
				case http.MethodPut:
					return "UpdateTable", seg[1], seg[3], ""
				case http.MethodDelete:
					return "DeleteTable", seg[1], seg[3], ""
				}
			case "routines":
				return "Routines", seg[1], "", ""
			case "models":
				return "Models", seg[1], "", ""
			}
		case 5:
			if seg[2] == "tables" {
				switch resourceSegment(seg[4]) {
				case "insertAll":
					if method == http.MethodPost {
						return "InsertAll", seg[1], seg[3], ""
					}
				case "data":
					if method == http.MethodGet {
						return "ListRows", seg[1], seg[3], ""
					}
				case "rowAccessPolicies":
					return "RowAccessPolicies", seg[1], seg[3], ""
				}
			}
		case 6:
			if seg[2] == "tables" && resourceSegment(seg[4]) == "rowAccessPolicies" {
				return "RowAccessPolicies", seg[1], seg[3], ""
			}
		}
	case "jobs":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "InsertJob", "", "", ""
			}
			if method == http.MethodGet {
				return "ListJobs", "", "", ""
			}
		case 2:
			if method == http.MethodGet {
				return "GetJob", "", "", seg[1]
			}
		case 3:
			switch seg[2] {
			case "delete":
				if method == http.MethodDelete {
					return "DeleteJob", "", "", seg[1]
				}
			case "cancel":
				if method == http.MethodPost {
					return "CancelJob", "", "", seg[1]
				}
			}
		}
	case "queries":
		switch len(seg) {
		case 1:
			if method == http.MethodPost {
				return "Query", "", "", ""
			}
		case 2:
			if method == http.MethodGet {
				return "GetQueryResults", "", "", seg[1]
			}
		}
	case "serviceAccount":
		if len(seg) == 1 && method == http.MethodGet {
			return "GetServiceAccount", "", "", ""
		}
	}
	return "", "", "", ""
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

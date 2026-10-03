package gcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/model"
)

// IcebergCodec decodes the BigLake Metastore Iceberg REST Catalog surface
// mounted at /iceberg/v1/... (Spark points uri=http://host:port/iceberg/, so
// real requests arrive as /iceberg/v1/...). BigLake Metastore is GCP's
// managed-Iceberg product, and its catalog is the standard Apache Iceberg REST
// catalog surface. It is a plain method+path → action mapping — no projects
// segment, no custom-method :suffix — with the body placed verbatim in
// nr.Params["body"] and query params (pageToken/pageSize) folded in via
// queryToParams.
//
// Path shape (after stripping the /iceberg mount):
//
//	/v1/{prefix}/config
//	/v1/{prefix}/namespaces[/{namespace}][/properties]
//	/v1/{prefix}/namespaces/{namespace}/tables[/{table}][/metrics]
//	/v1/{prefix}/tables/rename
//
// The {prefix} is the warehouse id (optional, a single segment); {namespace}
// is one-or-more segments joined by "/". Because namespace levels and the
// trailing tables/properties/metrics keywords are ambiguous, the namespace
// branch splits at the LAST such marker. That means a namespace whose FINAL
// level is a marker keyword (e.g. the namespace "a/tables") is misrouted to
// the "a" tables collection rather than treated as a namespace level (see
// decodeNamespaceRoute below).
type IcebergCodec struct{}

func (c *IcebergCodec) ServiceName() string { return "iceberg" }

func (c *IcebergCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())
	// Strip the /iceberg mount (first segment is "iceberg").
	if len(seg) > 0 && seg[0] == "iceberg" {
		seg = seg[1:]
	}
	// Drop empty segments so a trailing slash (e.g. /iceberg/v1/namespaces/a/)
	// does not produce a spurious empty namespace level.
	seg = dropEmptySegments(seg)
	if len(seg) == 0 || seg[0] != "v1" {
		return nil, model.NewProviderError("BadRequestException", "expected /iceberg/v1/... path", 404)
	}
	rest := seg[1:]

	nr := &model.NormalizedRequest{Service: "iceberg", Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	// Decode the body with UseNumber so int64 snapshot ids (~1e18, > 2^53) are
	// not silently corrupted into float64.
	var m map[string]any
	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			return nil, model.NewProviderError("BadRequestException", "malformed JSON body", 400)
		}
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// Optional single-segment prefix (warehouse id). If the next segment is a
	// dispatch keyword, the prefix is empty.
	if len(rest) == 0 {
		return nil, model.NewProviderError("BadRequestException", "missing Iceberg resource", 404)
	}
	prefix := ""
	if !icebergDispatchKeyword(rest[0]) {
		prefix = rest[0]
		rest = rest[1:]
	}
	nr.Params["prefix"] = prefix
	if len(rest) == 0 {
		return nil, model.NewProviderError("BadRequestException", "missing Iceberg resource", 404)
	}

	switch rest[0] {
	case "config":
		if len(rest) != 1 || r.Method != http.MethodGet {
			return nil, model.NewProviderError("BadRequestException", "unsupported config operation", 404)
		}
		nr.Action = "GetConfig"
	case "tables":
		// The only top-level tables route is POST /tables/rename.
		if len(rest) == 2 && rest[1] == "rename" && r.Method == http.MethodPost {
			nr.Action = "RenameTable"
		} else {
			return nil, model.NewProviderError("BadRequestException", "unsupported tables operation", 404)
		}
	case "namespaces":
		action, err := c.decodeNamespaceRoute(r, rest[1:], nr.Params)
		if err != nil {
			return nil, err
		}
		nr.Action = action
	default:
		return nil, model.NewProviderError("BadRequestException", "unrecognized Iceberg resource", 404)
	}

	return nr, nil
}

// icebergDispatchKeyword reports whether a path segment is one of the top-level
// dispatch keywords (config/namespaces/tables) rather than a warehouse prefix.
func icebergDispatchKeyword(s string) bool {
	return s == "config" || s == "namespaces" || s == "tables"
}

// dropEmptySegments removes empty path segments introduced by a trailing slash
// (or doubled slash), so a namespace never gains a spurious empty level.
func dropEmptySegments(seg []string) []string {
	out := seg[:0]
	for _, s := range seg {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// decodeNamespaceRoute maps the segments after "namespaces" to an action. The
// namespace is the join of segments before the last tables/properties/metrics
// marker. Because the split happens at the LAST marker, a namespace whose FINAL
// level is a marker keyword (e.g. the namespace "a/tables") is misrouted to the
// "a" tables collection — it cannot be disambiguated from a plain namespace
// level on this path shape. metrics always appears as tables/{table}/metrics
// and is checked first.
func (c *IcebergCodec) decodeNamespaceRoute(r *http.Request, seg []string, params map[string]any) (string, error) {
	// Collection: /namespaces
	if len(seg) == 0 {
		switch r.Method {
		case http.MethodGet:
			return "ListNamespaces", nil
		case http.MethodPost:
			return "CreateNamespace", nil
		}
		return "", model.NewProviderError("BadRequestException", "unsupported namespaces operation", 404)
	}

	// metrics: POST .../tables/{table}/metrics (the Iceberg REST reportMetrics
	// endpoint; the spec defines only POST, returning 204).
	if len(seg) >= 3 && seg[len(seg)-1] == "metrics" && seg[len(seg)-3] == "tables" {
		if r.Method == http.MethodPost {
			params["namespace"] = strings.Join(seg[:len(seg)-3], "/")
			params["table"] = seg[len(seg)-2]
			return "TableMetrics", nil
		}
		return "", model.NewProviderError("BadRequestException", "unsupported metrics operation", 404)
	}

	// tables collection: .../tables
	if seg[len(seg)-1] == "tables" {
		ns := strings.Join(seg[:len(seg)-1], "/")
		params["namespace"] = ns
		switch r.Method {
		case http.MethodGet:
			return "ListTables", nil
		case http.MethodPost:
			return "CreateTable", nil
		}
		return "", model.NewProviderError("BadRequestException", "unsupported tables operation", 404)
	}

	// properties: .../properties
	if seg[len(seg)-1] == "properties" {
		if r.Method == http.MethodPost {
			params["namespace"] = strings.Join(seg[:len(seg)-1], "/")
			return "UpdateNamespaceProperties", nil
		}
		return "", model.NewProviderError("BadRequestException", "unsupported properties operation", 404)
	}

	// table resource: .../tables/{table}
	if len(seg) >= 2 && seg[len(seg)-2] == "tables" {
		params["namespace"] = strings.Join(seg[:len(seg)-2], "/")
		params["table"] = seg[len(seg)-1]
		switch r.Method {
		case http.MethodGet:
			return "LoadTable", nil
		case http.MethodPost:
			return "CommitTable", nil
		case http.MethodDelete:
			return "DropTable", nil
		}
		return "", model.NewProviderError("BadRequestException", "unsupported table operation", 404)
	}

	// namespace resource: .../namespaces/{namespace}
	params["namespace"] = strings.Join(seg, "/")
	switch r.Method {
	case http.MethodGet:
		return "GetNamespace", nil
	case http.MethodHead:
		return "NamespaceExists", nil
	case http.MethodDelete:
		return "DropNamespace", nil
	}
	return "", model.NewProviderError("BadRequestException", "unsupported namespace operation", 404)
}

// Encode serialises a provider response as JSON (Iceberg REST bodies are
// plain JSON objects).
func (c *IcebergCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"message":"encode failure","type":"ServiceUnavailableException","code":500}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as Iceberg's ErrorResponse shape
// ({"error":{"message","type","code"}}), which differs from the GCP error
// envelope — Iceberg has its own wire format.
func (c *IcebergCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	// perr.Code carries the Iceberg exception name (e.g. NoSuchTableException);
	// fall back to a generic name when absent.
	typ := perr.Code
	if typ == "" {
		typ = "ServiceUnavailableException"
	}
	env := map[string]any{
		"error": map[string]any{
			"message": perr.Message,
			"type":    typ,
			"code":    status,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

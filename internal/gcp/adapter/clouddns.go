package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// CloudDNSCodec decodes the Cloud DNS v1 REST surface
// (dns.googleapis.com/dns/v1). Unlike the shared /v1/projects/{project}/...
// services, Cloud DNS's method paths embed the "dns/v1/" service prefix, so the
// API is identified by the "/dns/v1/" path prefix and decoded here. Resources
// live under /dns/v1/projects/{project}[/managedZones[/{zone}[/rrsets[/{name}/{type}]|
// /changes[/{id}]]]]. The {project} resource itself (projects.get) carries no
// further segments. DNSSEC keys, policies, response policies, and the IAM
// custom methods resolve to an Unimplemented action so the provider fails loud
// with 501 rather than the codec 404-ing them.
type CloudDNSCodec struct {
	Service string
}

func (c *CloudDNSCodec) ServiceName() string { return c.Service }

func (c *CloudDNSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
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
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	rest := seg[pi+2:]

	// Strip a trailing custom-method suffix (":getIamPolicy", ...) from the
	// last segment. Cloud DNS's custom methods are all IAM, which is deferred.
	custom := ""
	if len(rest) > 0 {
		last := rest[len(rest)-1]
		if i := strings.IndexByte(last, ':'); i >= 0 {
			custom = last[i+1:]
			rest[len(rest)-1] = last[:i]
		}
	}
	if custom != "" {
		nr.Action = "Unimplemented"
		return nr, nil
	}

	nr.Action = cloudDNSAction(rest, r.Method, nr)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// cloudDNSAction maps the path segments after projects/{project} (plus the HTTP
// method) to the provider action name, recording the managed-zone, rrset, and
// change identifiers it carries into nr.Params.
func cloudDNSAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		if method == http.MethodGet {
			return "ProjectGet"
		}
		return ""
	}
	switch rest[0] {
	case "managedZones":
		return managedZoneAction(rest[1:], method, nr)
	case "policies", "responsePolicies":
		return "Unimplemented"
	}
	return ""
}

func managedZoneAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "ManagedZoneList"
		case http.MethodPost:
			return "ManagedZoneCreate"
		}
		return ""
	}

	zone := rest[0]
	nr.Params["managedZone"] = zone
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "ManagedZoneGet"
		case http.MethodPatch:
			return "ManagedZonePatch"
		case http.MethodPut:
			return "ManagedZoneUpdate"
		case http.MethodDelete:
			return "ManagedZoneDelete"
		}
		return ""
	}

	switch rest[1] {
	case "rrsets":
		switch len(rest) {
		case 2:
			switch method {
			case http.MethodGet:
				return "ResourceRecordSetList"
			case http.MethodPost:
				return "ResourceRecordSetCreate"
			case http.MethodDelete:
				// Legacy query-parameter delete form:
				// DELETE .../rrsets?name=&type=
				nr.Params["rrsetName"], _ = nr.Params["name"].(string)
				nr.Params["rrsetType"], _ = nr.Params["type"].(string)
				return "ResourceRecordSetDelete"
			}
		case 4:
			nr.Params["rrsetName"] = rest[2]
			nr.Params["rrsetType"] = rest[3]
			switch method {
			case http.MethodGet:
				return "ResourceRecordSetGet"
			case http.MethodPatch, http.MethodPut:
				return "ResourceRecordSetPatch"
			case http.MethodDelete:
				return "ResourceRecordSetDelete"
			}
		}
	case "changes":
		switch len(rest) {
		case 2:
			switch method {
			case http.MethodGet:
				return "ChangeList"
			case http.MethodPost:
				return "ChangeCreate"
			}
		case 3:
			nr.Params["changeId"] = rest[2]
			if method == http.MethodGet {
				return "ChangeGet"
			}
		}
	case "dnsKeys", "operations":
		return "Unimplemented"
	}
	return ""
}

// Encode serialises a provider response as JSON.
func (c *CloudDNSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *CloudDNSCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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

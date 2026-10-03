package gcp

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// CloudSQLCodec decodes the Cloud SQL Admin v1beta4 REST surface
// (sqladmin.googleapis.com/sql/v1beta4). Unlike the shared
// /v1/projects/{project}/... services, Cloud SQL's method paths embed the
// "sql/v1beta4/" service prefix, so the API is identified by the "/sql/" path
// prefix and decoded here. Resources live under
// /sql/v1beta4/projects/{project}/{instances[/{instance}[/restart|/databases[/{db}]|
// /users[/{name}]|/connectSettings]]|operations[/{op}]|tiers} plus the
// project-less /sql/v1beta4/flags discovery path.
//
// Deferred surfaces (sslCerts, backupRuns, clone, failover, promoteReplica,
// import/export, demote, resetSslConfig, operation cancel, and every instance
// custom method such as instances:generateEphemeralCert) resolve to an
// Unimplemented action so the provider fails loud with 501 rather than the
// codec 404-ing them.
type CloudSQLCodec struct {
	Service string
}

func (c *CloudSQLCodec) ServiceName() string { return c.Service }

func (c *CloudSQLCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(r.URL.EscapedPath())

	nr := &model.NormalizedRequest{Service: c.Service, Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	if m != nil {
		nr.Params["body"] = m
	}

	// flags.list is project-less: GET /sql/v1beta4/flags.
	if len(seg) > 0 && seg[len(seg)-1] == "flags" {
		if r.Method == http.MethodGet {
			nr.Action = "FlagsList"
			return nr, nil
		}
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}

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
	nr.Params["project"] = seg[pi+1]
	rest := seg[pi+2:]

	// Strip a trailing custom-method suffix (":generateEphemeralCert", ...).
	// Every Cloud SQL custom method is deferred, so a non-empty custom method
	// resolves to Unimplemented.
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

	nr.Action = cloudSQLAction(rest, r.Method, nr)
	if nr.Action == "" {
		return nil, model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
	}
	return nr, nil
}

// cloudSQLAction maps the path segments after projects/{project} (plus the HTTP
// method) to the provider action name, recording the instance, database, user,
// and operation identifiers it carries into nr.Params.
func cloudSQLAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		return ""
	}
	switch rest[0] {
	case "instances":
		return instanceAction(rest[1:], method, nr)
	case "operations":
		return operationAction(rest[1:], method, nr)
	case "tiers":
		if len(rest) == 1 && method == http.MethodGet {
			return "TiersList"
		}
	case "flags":
		if len(rest) == 1 && method == http.MethodGet {
			return "FlagsList"
		}
	}
	return ""
}

func instanceAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "InstancesList"
		case http.MethodPost:
			return "InstancesInsert"
		}
		return ""
	}

	instance := rest[0]
	nr.Params["instance"] = instance
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "InstancesGet"
		case http.MethodPut:
			return "InstancesUpdate"
		case http.MethodPatch:
			return "InstancesPatch"
		case http.MethodDelete:
			return "InstancesDelete"
		}
		return ""
	}

	switch rest[1] {
	case "restart":
		if len(rest) == 2 && method == http.MethodPost {
			return "InstancesRestart"
		}
	case "databases":
		return databaseAction(rest[2:], method, nr)
	case "users":
		return userAction(rest[2:], method, nr)
	case "connectSettings":
		if len(rest) == 2 && method == http.MethodGet {
			return "ConnectGet"
		}
	default:
		// sslCerts, backupRuns, clone, failover, promoteReplica, import,
		// export, demote, resetSslConfig, restoreBackup, ... — deferred.
		return "Unimplemented"
	}
	return ""
}

func databaseAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "DatabasesList"
		case http.MethodPost:
			return "DatabasesInsert"
		}
		return ""
	}
	nr.Params["database"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "DatabasesGet"
		case http.MethodPut:
			return "DatabasesUpdate"
		case http.MethodPatch:
			return "DatabasesPatch"
		case http.MethodDelete:
			return "DatabasesDelete"
		}
	}
	return ""
}

func userAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "UsersList"
		case http.MethodPost:
			return "UsersInsert"
		case http.MethodPut:
			return "UsersUpdate"
		case http.MethodDelete:
			return "UsersDelete"
		}
		return ""
	}
	nr.Params["user"] = rest[0]
	if len(rest) == 1 && method == http.MethodGet {
		return "UsersGet"
	}
	return ""
}

func operationAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		if method == http.MethodGet {
			return "OperationsList"
		}
		return ""
	}
	nr.Params["operation"] = rest[0]
	if len(rest) == 1 && method == http.MethodGet {
		return "OperationsGet"
	}
	if len(rest) == 2 && rest[1] == "cancel" && method == http.MethodPost {
		return "Unimplemented"
	}
	return ""
}

// Encode serialises a provider response as JSON.
func (c *CloudSQLCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *CloudSQLCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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

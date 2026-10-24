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
// Real GKE defaults to gRPC (google.container.v1.ClusterManager), so this REST
// surface is paired with the gRPC adapter in internal/gcp/transport/grpc/container
// over the same core. The Codec is a NormalizedRequest adapter and the
// Provider holds the routes; both delegate to the transport-neutral core Service
// (internal/gcp/service/container).
package container

import (
	"encoding/json"
	"net/http"
	"strings"

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
// location, cluster/nodePool/operation (item paths), body (POST/PUT), plus any
// query parameters (pageSize, pageToken).
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

	action, err := routeContainer(seg[pi+4:], r.Method, nr.Params)
	if err != nil {
		return nil, err
	}
	nr.Action = action
	return nr, nil
}

// routeContainer maps the path segments after projects/{p}/locations/{l} and
// the HTTP method to a provider action, filling the cluster/nodePool/operation
// path params.
func routeContainer(rel []string, method string, params map[string]any) (string, error) {
	switch rel[0] {
	case "clusters":
		return routeClusters(rel[1:], method, params)
	case "operations":
		return routeOperations(rel[1:], method, params)
	}
	return "", model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
}

func routeClusters(rel []string, method string, params map[string]any) (string, error) {
	if len(rel) == 0 {
		switch method {
		case http.MethodGet:
			return "ListClusters", nil
		case http.MethodPost:
			return "CreateCluster", nil
		}
		return "", unsupported()
	}
	clusterID, suffix := splitAction(rel[0])

	// clusters/{cluster}:action custom methods.
	if suffix != "" {
		if len(rel) != 1 {
			return "", unsupported()
		}
		params["cluster"] = clusterID
		if action := clusterCustomAction(suffix); action != "" && method == http.MethodPost {
			return action, nil
		}
		return "", unsupported()
	}

	params["cluster"] = clusterID
	switch {
	case len(rel) == 1:
		switch method {
		case http.MethodGet:
			return "GetCluster", nil
		case http.MethodDelete:
			return "DeleteCluster", nil
		case http.MethodPut:
			return "UpdateCluster", nil
		}
	case len(rel) == 2 && rel[1] == "nodePools":
		switch method {
		case http.MethodGet:
			return "ListNodePools", nil
		case http.MethodPost:
			return "CreateNodePool", nil
		}
	case len(rel) == 3 && rel[1] == "nodePools":
		poolID, poolSuffix := splitAction(rel[2])
		params["nodepool"] = poolID
		if poolSuffix != "" {
			if action := nodePoolCustomAction(poolSuffix); action != "" && method == http.MethodPost {
				return action, nil
			}
			return "", unsupported()
		}
		switch method {
		case http.MethodGet:
			return "GetNodePool", nil
		case http.MethodDelete:
			return "DeleteNodePool", nil
		case http.MethodPut:
			return "UpdateNodePool", nil
		}
	}
	return "", unsupported()
}

func routeOperations(rel []string, method string, params map[string]any) (string, error) {
	if len(rel) == 0 {
		if method == http.MethodGet {
			return "ListOperations", nil
		}
		return "", unsupported()
	}
	opID, suffix := splitAction(rel[0])
	params["operation"] = opID
	if suffix != "" {
		if suffix == "cancel" && method == http.MethodPost {
			return "CancelOperation", nil
		}
		return "", unsupported()
	}
	if len(rel) == 1 && method == http.MethodGet {
		return "GetOperation", nil
	}
	return "", unsupported()
}

// splitAction splits a "{id}:{action}" segment.
func splitAction(seg string) (id, action string) {
	if i := strings.LastIndex(seg, ":"); i >= 0 {
		return seg[:i], seg[i+1:]
	}
	return seg, ""
}

// clusterCustomAction maps a custom cluster method's action name to a provider
// action.
func clusterCustomAction(action string) string {
	switch action {
	case "setAddons":
		return "SetAddonsConfig"
	case "setResourceLabels":
		return "SetLabels"
	case "setLegacyAbac":
		return "SetLegacyAbac"
	case "setLocations":
		return "SetLocations"
	case "setLogging":
		return "SetLoggingService"
	case "setMonitoring":
		return "SetMonitoringService"
	case "setNetworkPolicy":
		return "SetNetworkPolicy"
	case "setMaintenancePolicy":
		return "SetMaintenancePolicy"
	case "setMasterAuth":
		return "SetMasterAuth"
	case "updateMaster":
		return "UpdateMaster"
	case "startIpRotation":
		return "StartIPRotation"
	case "completeIpRotation":
		return "CompleteIPRotation"
	}
	return ""
}

// nodePoolCustomAction maps a custom node-pool method's action name to a
// provider action.
func nodePoolCustomAction(action string) string {
	switch action {
	case "setAutoscaling":
		return "SetNodePoolAutoscaling"
	case "setSize":
		return "SetNodePoolSize"
	case "setManagement":
		return "SetNodePoolManagement"
	case "rollback":
		return "RollbackNodePoolUpgrade"
	}
	return ""
}

func unsupported() error {
	return model.NewProviderError("UnsupportedOperation", "unsupported operation", 404)
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

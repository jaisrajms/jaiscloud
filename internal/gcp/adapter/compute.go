package gcp

import (
	"encoding/json"
	"net/http"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// ComputeCodec decodes the Compute Engine v1 REST surface
// (compute.googleapis.com/compute/v1). Unlike the shared
// /v1/projects/{project}/... services, Compute Engine's method paths embed the
// "compute/v1/" service prefix, so the API is identified by the "/compute/v1/"
// path prefix and decoded here. Resources live under
// /compute/v1/projects/{project}/{zones/{zone}|regions/{region}|global}/{resource}
// plus the project-less aggregated and bare zone/region discovery forms.
//
// The path carries BOTH the scope (a zone, a region, or "global") and the
// resource type before the resource id, so the codec records project, scope,
// and scopeType in Params for the provider to use as the store scope.
//
// Every /compute/v1/ path that parses a project but is not one of the supported
// resources resolves to an Unimplemented action so the provider fails loud with
// 501 rather than the codec 404-ing.
type ComputeCodec struct {
	Service string
}

func (c *ComputeCodec) ServiceName() string { return c.Service }

func (c *ComputeCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
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

	nr.Action = computeAction(rest, r.Method, nr)
	return nr, nil
}

// computeAction maps the path segments after projects/{project} (plus the HTTP
// method) to the provider action name, recording the scope (zone/region/global)
// and the resource identifiers it carries into nr.Params. Unrecognized paths
// map to Unimplemented so the provider can fail loud with 501.
func computeAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		return "Unimplemented"
	}
	switch rest[0] {
	case "aggregated":
		if len(rest) == 2 && rest[1] == "instances" && method == http.MethodGet {
			return "InstancesAggregatedList"
		}
		return "Unimplemented"
	case "zones":
		if len(rest) == 1 && method == http.MethodGet {
			return "ZonesList"
		}
		if len(rest) == 2 && method == http.MethodGet {
			nr.Params["zone"] = rest[1]
			return "ZonesGet"
		}
		if len(rest) >= 3 {
			return scopedAction(rest[2:], rest[1], "zones", method, nr)
		}
		return "Unimplemented"
	case "regions":
		if len(rest) == 1 && method == http.MethodGet {
			return "RegionsList"
		}
		if len(rest) == 2 && method == http.MethodGet {
			nr.Params["region"] = rest[1]
			return "RegionsGet"
		}
		if len(rest) >= 3 {
			return scopedAction(rest[2:], rest[1], "regions", method, nr)
		}
		return "Unimplemented"
	case "global":
		if len(rest) >= 2 {
			return scopedAction(rest[1:], "global", "global", method, nr)
		}
		return "Unimplemented"
	}
	return "Unimplemented"
}

// scopedAction records the store scope and dispatches on the resource type.
func scopedAction(rest []string, scope, scopeType, method string, nr *model.NormalizedRequest) string {
	nr.Params["scope"] = scope
	nr.Params["scopeType"] = scopeType
	switch scopeType {
	case "zones":
		nr.Params["zone"] = scope
	case "regions":
		nr.Params["region"] = scope
	}
	if len(rest) == 0 {
		return "Unimplemented"
	}
	switch rest[0] {
	case "instances":
		return computeInstanceAction(rest[1:], method, nr)
	case "disks":
		return computeDiskAction(rest[1:], method, nr)
	case "networks":
		return computeNetworkAction(rest[1:], method, nr)
	case "firewalls":
		return computeFirewallAction(rest[1:], method, nr)
	case "subnetworks":
		return computeSubnetworkAction(rest[1:], method, nr)
	case "machineTypes":
		return computeMachineTypeAction(rest[1:], method, nr)
	case "operations":
		return computeOperationAction(rest[1:], method, nr)
	}
	return "Unimplemented"
}

func computeInstanceAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "InstancesList"
		case http.MethodPost:
			return "InstancesInsert"
		}
		return "Unimplemented"
	}
	nr.Params["instance"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "InstancesGet"
		case http.MethodDelete:
			return "InstancesDelete"
		}
		return "Unimplemented"
	}
	if len(rest) == 2 && method == http.MethodPost {
		switch rest[1] {
		case "start":
			return "InstancesStart"
		case "stop":
			return "InstancesStop"
		case "reset":
			return "InstancesReset"
		}
	}
	return "Unimplemented"
}

func computeDiskAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "DisksList"
		case http.MethodPost:
			return "DisksInsert"
		}
		return "Unimplemented"
	}
	nr.Params["disk"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "DisksGet"
		case http.MethodDelete:
			return "DisksDelete"
		}
	}
	return "Unimplemented"
}

func computeNetworkAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "NetworksList"
		case http.MethodPost:
			return "NetworksInsert"
		}
		return "Unimplemented"
	}
	nr.Params["network"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "NetworksGet"
		case http.MethodDelete:
			return "NetworksDelete"
		}
	}
	return "Unimplemented"
}

func computeFirewallAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "FirewallsList"
		case http.MethodPost:
			return "FirewallsInsert"
		}
		return "Unimplemented"
	}
	nr.Params["firewall"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "FirewallsGet"
		case http.MethodDelete:
			return "FirewallsDelete"
		}
	}
	return "Unimplemented"
}

func computeSubnetworkAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		switch method {
		case http.MethodGet:
			return "SubnetworksList"
		case http.MethodPost:
			return "SubnetworksInsert"
		}
		return "Unimplemented"
	}
	nr.Params["subnetwork"] = rest[0]
	if len(rest) == 1 {
		switch method {
		case http.MethodGet:
			return "SubnetworksGet"
		case http.MethodDelete:
			return "SubnetworksDelete"
		}
	}
	return "Unimplemented"
}

func computeMachineTypeAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		if method == http.MethodGet {
			return "MachineTypesList"
		}
		return "Unimplemented"
	}
	nr.Params["machineType"] = rest[0]
	if len(rest) == 1 && method == http.MethodGet {
		return "MachineTypesGet"
	}
	return "Unimplemented"
}

func computeOperationAction(rest []string, method string, nr *model.NormalizedRequest) string {
	if len(rest) == 0 {
		if method == http.MethodGet {
			return "OperationsList"
		}
		return "Unimplemented"
	}
	nr.Params["operation"] = rest[0]
	if len(rest) == 1 && method == http.MethodGet {
		return "OperationsGet"
	}
	return "Unimplemented"
}

// Encode serialises a provider response as JSON.
func (c *ComputeCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
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
func (c *ComputeCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
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

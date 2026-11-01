// Package monitoring is the REST transport for Cloud Monitoring.
//
// Real GCP serves Monitoring's proto-defined v3 API over both gRPC and REST
// (grpc-gateway transcoding), and the REST surface here follows the vendored
// Discovery document. The implemented data methods are the REST mirrors of the
// 34 gRPC RPCs across MetricService, AlertPolicyService,
// NotificationChannelService, and ServiceMonitoringService:
//
//	projects.metricDescriptors.{list,get,create,delete}
//	projects.monitoredResourceDescriptors.{list,get}
//	projects.timeSeries.{list,create,createService}
//	projects.alertPolicies.{list,get,create,patch,delete}
//	projects.notificationChannels.{list,get,create,patch,delete}
//	projects.notificationChannels.{sendVerificationCode,getVerificationCode,verify}
//	projects.notificationChannelDescriptors.{list,get}
//	projects.services.{list,get,create,patch,delete}
//	projects.services.serviceLevelObjectives.{list,get,create,patch,delete}
//
// The REST-only v1 dashboards API (projects.dashboards.{list,get,create,patch,
// delete}, official v1 Discovery document revision 20261022) is served from the
// same provider over the shared core; dashboards have no gRPC surface in the
// real API.
//
// The rest of the v3 surface (snoozes, uptime checks, groups) is not implemented
// and returns 501 UNIMPLEMENTED when it is a recognized v3 collection, or 404
// NOT_FOUND for an unknown path.
//
// The Codec is a NormalizedRequest adapter (HTTP path/body ↔ the core's typed
// API); the Provider holds the routes. Neither owns business logic — both
// delegate to the single core Service shared with the gRPC transport (see
// internal/gcp/service/monitoring).
package monitoring

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"

	core "jaiscloud/internal/gcp/service/monitoring"
)

// ServiceName is the wire service name.
const ServiceName = "monitoring"

// Codec decodes Cloud Monitoring REST requests into a NormalizedRequest and
// encodes provider responses as the GCP JSON envelope. It satisfies
// adapter.Codec structurally (the adapter package imports this package, so this
// package must not import it).
type Codec struct{}

// NewCodec returns the Monitoring REST codec.
func NewCodec() *Codec { return &Codec{} }

// ServiceName implements adapter.Codec.
func (c *Codec) ServiceName() string { return ServiceName }

// knownUnimplemented are real v3 collections the emulator does not serve over
// REST. They are reported as 501 UNIMPLEMENTED, distinct from an unknown path
// (404 NOT_FOUND). Dashboards are a v1 API served by this transport (see
// routeDashboards); a /v3/ dashboards path is not a real v3 collection and
// stays 404.
var knownUnimplemented = []string{
	"/snoozes", "/uptimeCheckConfigs",
	"/groups", "/alerts", "/collectdTimeSeries", "uptimeCheckIps",
}

// Decode parses a Cloud Monitoring REST path into a NormalizedRequest. Params
// carry apiVersion, project/name, body (POST/PATCH), and any query parameters.
// The v3 surface is the proto-defined Monitoring API; the v1 surface is the
// REST-only dashboards API (projects.dashboards), which the official v1
// Discovery document declares on the same host
// (https://monitoring.googleapis.com/$discovery/rest?version=v1). Every other
// /v1/ path stays 404.
func (c *Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := "/" + strings.TrimLeft(r.URL.EscapedPath(), "/")
	version := ""
	switch {
	case strings.HasPrefix(path, "/v3/"):
		version = "v3"
	case strings.HasPrefix(path, "/v1/"):
		version = "v1"
	default:
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}
	rest := strings.TrimPrefix(path, "/"+version+"/")

	nr := &model.NormalizedRequest{Service: ServiceName, Params: map[string]any{}, Raw: r}
	action, ok := route(version, rest, r.Method, nr)
	if !ok {
		if version == "v3" {
			for _, collection := range knownUnimplemented {
				if strings.Contains(rest, collection) {
					return nil, model.NewProviderError("Unimplemented", "method not implemented over REST", 501)
				}
			}
		}
		return nil, model.NewProviderError("NotFound", "unsupported operation", 404)
	}
	nr.Action = action

	nr.Params["apiVersion"] = version
	if len(body) > 0 {
		var m map[string]any
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	}
	queryToParams(r, nr.Params)
	// Keep the request body's original bytes as well: the map above is a
	// float64 round-trip, which would silently re-render integers beyond 2^53
	// and precise decimals. The opaque-document surfaces (v1 dashboards) read
	// this value so the client's document is stored verbatim. Set after
	// queryToParams so a query parameter cannot spoof the key.
	if len(body) > 0 {
		nr.Params["rawBody"] = json.RawMessage(body)
	}
	return nr, nil
}

// route maps an escaped path (without the leading /v3/ or /v1/) and HTTP method
// to a provider action, populating nr.Params with the resolved project or
// resource name. It returns ok=false for paths the emulator does not serve.
func route(version, rest, method string, nr *model.NormalizedRequest) (string, bool) {
	// Dashboards are the only v1 API. They share the projects/{p}/... namespace
	// with the v3 surface but not the URL version, so they are routed by version
	// first.
	if version == "v1" {
		return routeDashboards(rest, method, nr)
	}
	// Service Monitoring is checked before the MetricService paths because an
	// SLO name ("projects/{p}/services/{s}/serviceLevelObjectives/{id}") also
	// contains "/services/".
	switch {
	// Service level objectives. The collection is nested under a service, so
	// the parent is "projects/{p}/services/{s}".
	case strings.HasSuffix(rest, "/serviceLevelObjectives"):
		parent := strings.TrimSuffix(rest, "/serviceLevelObjectives")
		project, service, ok := core.SplitServiceName(parent)
		if !ok {
			return "", false
		}
		nr.Params["parent"] = parent
		nr.Params["project"] = project
		nr.Params["service"] = service
		switch method {
		case http.MethodGet:
			return "ListServiceLevelObjectives", true
		case http.MethodPost:
			return "CreateServiceLevelObjective", true
		}
		return "", false
	case strings.Contains(rest, "/serviceLevelObjectives/"):
		nr.Params["name"] = rest
		switch method {
		case http.MethodGet:
			return "GetServiceLevelObjective", true
		case http.MethodPatch:
			return "UpdateServiceLevelObjective", true
		case http.MethodDelete:
			return "DeleteServiceLevelObjective", true
		}
		return "", false

	// Services (Service Monitoring).
	case strings.HasSuffix(rest, "/services"):
		if !setParent(nr, rest, "/services") {
			return "", false
		}
		switch method {
		case http.MethodGet:
			return "ListServices", true
		case http.MethodPost:
			return "CreateService", true
		}
		return "", false
	case strings.Contains(rest, "/services/"):
		nr.Params["name"] = rest
		switch method {
		case http.MethodGet:
			return "GetService", true
		case http.MethodPatch:
			return "UpdateService", true
		case http.MethodDelete:
			return "DeleteService", true
		}
		return "", false
	}

	// Metric descriptors. A metric type may contain slashes, so get/delete are
	// distinguished from list/create by whether anything follows the collection.
	switch {
	case strings.HasSuffix(rest, "/metricDescriptors"):
		if !setParent(nr, rest, "/metricDescriptors") {
			return "", false
		}
		switch method {
		case http.MethodGet:
			return "ListMetricDescriptors", true
		case http.MethodPost:
			return "CreateMetricDescriptor", true
		}
		return "", false
	case strings.Contains(rest, "/metricDescriptors/"):
		nr.Params["name"] = rest
		switch method {
		case http.MethodGet:
			return "GetMetricDescriptor", true
		case http.MethodDelete:
			return "DeleteMetricDescriptor", true
		}
		return "", false

	case strings.HasSuffix(rest, "/monitoredResourceDescriptors"):
		if !setParent(nr, rest, "/monitoredResourceDescriptors") {
			return "", false
		}
		if method == http.MethodGet {
			return "ListMonitoredResourceDescriptors", true
		}
		return "", false
	case strings.Contains(rest, "/monitoredResourceDescriptors/"):
		nr.Params["name"] = rest
		if method == http.MethodGet {
			return "GetMonitoredResourceDescriptor", true
		}
		return "", false

	case strings.HasSuffix(rest, "/timeSeries:createService"):
		if !setParent(nr, rest, "/timeSeries:createService") {
			return "", false
		}
		if method == http.MethodPost {
			return "CreateServiceTimeSeries", true
		}
		return "", false
	case strings.HasSuffix(rest, "/timeSeries"):
		// timeSeries.list is the one Monitoring method declared on the
		// folders scope as well as projects; the create sibling stays
		// project-only, so the two methods resolve their parent differently.
		if !setTimeSeriesParent(nr, rest, method) {
			return "", false
		}
		switch method {
		case http.MethodGet:
			return "ListTimeSeries", true
		case http.MethodPost:
			return "CreateTimeSeries", true
		}
		return "", false

	case strings.HasSuffix(rest, "/alertPolicies"):
		if !setParent(nr, rest, "/alertPolicies") {
			return "", false
		}
		switch method {
		case http.MethodGet:
			return "ListAlertPolicies", true
		case http.MethodPost:
			return "CreateAlertPolicy", true
		}
		return "", false
	case strings.Contains(rest, "/alertPolicies/"):
		nr.Params["name"] = rest
		switch method {
		case http.MethodGet:
			return "GetAlertPolicy", true
		case http.MethodPatch:
			return "UpdateAlertPolicy", true
		case http.MethodDelete:
			return "DeleteAlertPolicy", true
		}
		return "", false

	case strings.HasSuffix(rest, "/notificationChannelDescriptors"):
		if !setParent(nr, rest, "/notificationChannelDescriptors") {
			return "", false
		}
		if method == http.MethodGet {
			return "ListNotificationChannelDescriptors", true
		}
		return "", false
	case strings.Contains(rest, "/notificationChannelDescriptors/"):
		nr.Params["name"] = rest
		if method == http.MethodGet {
			return "GetNotificationChannelDescriptor", true
		}
		return "", false

	case strings.HasSuffix(rest, ":sendVerificationCode"):
		nr.Params["name"] = strings.TrimSuffix(rest, ":sendVerificationCode")
		if method == http.MethodPost {
			return "SendNotificationChannelVerificationCode", true
		}
		return "", false
	case strings.HasSuffix(rest, ":getVerificationCode"):
		nr.Params["name"] = strings.TrimSuffix(rest, ":getVerificationCode")
		if method == http.MethodPost {
			return "GetNotificationChannelVerificationCode", true
		}
		return "", false
	case strings.HasSuffix(rest, ":verify"):
		nr.Params["name"] = strings.TrimSuffix(rest, ":verify")
		if method == http.MethodPost {
			return "VerifyNotificationChannel", true
		}
		return "", false

	case strings.HasSuffix(rest, "/notificationChannels"):
		if !setParent(nr, rest, "/notificationChannels") {
			return "", false
		}
		switch method {
		case http.MethodGet:
			return "ListNotificationChannels", true
		case http.MethodPost:
			return "CreateNotificationChannel", true
		}
		return "", false
	case strings.Contains(rest, "/notificationChannels/"):
		nr.Params["name"] = rest
		switch method {
		case http.MethodGet:
			return "GetNotificationChannel", true
		case http.MethodPatch:
			return "UpdateNotificationChannel", true
		case http.MethodDelete:
			return "DeleteNotificationChannel", true
		}
		return "", false
	}
	return "", false
}

// routeDashboards maps the REST-only v1 dashboards surface
// (projects.dashboards.{list,get,create,patch,delete}, official v1 Discovery
// document revision 20261022). Every other v1 path is unsupported (404). The
// document's flatPath binds both the collection ("v1/projects/{projectsId}/
// dashboards") and the item ("v1/projects/{projectsId}/dashboards/
// {dashboardsId}"), so the parent/name split is on the literal "dashboards"
// segment.
func routeDashboards(rest, method string, nr *model.NormalizedRequest) (string, bool) {
	if strings.HasSuffix(rest, "/dashboards") {
		parent := strings.TrimSuffix(rest, "/dashboards")
		project, ok := core.SplitDashboardParent(parent)
		if !ok {
			return "", false
		}
		nr.Params["parent"] = parent
		nr.Params["project"] = project
		switch method {
		case http.MethodGet:
			return "ListDashboards", true
		case http.MethodPost:
			return "CreateDashboard", true
		}
		return "", false
	}
	if strings.Contains(rest, "/dashboards/") {
		project, _, ok := core.SplitDashboardName(rest)
		if !ok {
			return "", false
		}
		nr.Params["name"] = rest
		nr.Params["project"] = project
		switch method {
		case http.MethodGet:
			return "GetDashboard", true
		case http.MethodPatch:
			return "UpdateDashboard", true
		case http.MethodDelete:
			return "DeleteDashboard", true
		}
	}
	return "", false
}

// setParent extracts the parent resource name ("projects/{p}") preceding
// collection and records the project id. It returns false when the parent is
// not a project.
func setParent(nr *model.NormalizedRequest, rest, collection string) bool {
	parent := strings.TrimSuffix(rest, collection)
	project := core.ProjectFromResourceName(parent)
	if project == "" {
		return false
	}
	nr.Params["parent"] = parent
	nr.Params["project"] = project
	return true
}

// setTimeSeriesParent resolves the parent of the timeSeries collection: a
// projects/{p} parent for both verbs, or a folders/{f} parent for list (the
// folders arm is read-only). The store scope key lands in "project".
func setTimeSeriesParent(nr *model.NormalizedRequest, rest, method string) bool {
	parent := strings.TrimSuffix(rest, "/timeSeries")
	scope, ok := core.ScopeFromResourceName(parent)
	if !ok {
		return false
	}
	if strings.HasPrefix(scope, "folders/") && method != http.MethodGet {
		return false
	}
	nr.Params["parent"] = parent
	nr.Params["project"] = scope
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

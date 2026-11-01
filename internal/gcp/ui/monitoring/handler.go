package monitoringui

import (
	"encoding/json"
	"net/http"
	"net/url"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/monitoring"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Monitoring UI API requests by calling the Monitoring REST
// provider. It is a thin translation layer over the provider's Discovery-shaped
// JSON.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// scope returns the project scope parent ("projects/{p}") for a request.
func (h *Handler) scope(r *http.Request) string {
	return resource.ResourceID(h.account(r))("project", "")
}

// resourceName builds "projects/{p}/{collection}/{id}" for a decoded trailing
// path parameter, re-encoding it so unusual ids round-trip.
func (h *Handler) resourceName(r *http.Request, collection string) (string, bool) {
	id := uihelper.PathParam(r, "id")
	if id == "" {
		return "", false
	}
	return h.scope(r) + "/" + collection + "/" + url.PathEscape(id), true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return false
	}
	return true
}

// ─── metrics ──────────────────────────────────────────────────────────────────

// GET /metricDescriptors lists metric descriptors for the project.
func (h *Handler) ListMetricDescriptors(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListMetricDescriptors", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)
	if f := r.URL.Query().Get("filter"); f != "" {
		nr.Params["filter"] = f
	}

	resp, err := h.provider.ListMetricDescriptors(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /timeSeries lists time series matching the filter and interval.
func (h *Handler) ListTimeSeries(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListTimeSeries", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)
	q := r.URL.Query()
	if f := q.Get("filter"); f != "" {
		nr.Params["filter"] = f
	}
	if s := q.Get("startTime"); s != "" {
		nr.Params["interval.startTime"] = s
	}
	if e := q.Get("endTime"); e != "" {
		nr.Params["interval.endTime"] = e
	}

	resp, err := h.provider.ListTimeSeries(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── alert policies ───────────────────────────────────────────────────────────

// GET /alertPolicies lists alert policies.
func (h *Handler) ListAlertPolicies(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListAlertPolicies", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ListAlertPolicies(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /alertPolicies/{id} returns one alert policy.
func (h *Handler) GetAlertPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "alertPolicies")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid alert policy name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.GetAlertPolicy", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetAlertPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /alertPolicies creates an alert policy.
func (h *Handler) CreateAlertPolicy(w http.ResponseWriter, r *http.Request) {
	var req AlertPolicyRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.CreateAlertPolicy", "global", h.account(r))
	nr.Params["body"] = req.body()

	resp, err := h.provider.CreateAlertPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /alertPolicies/{id} updates an alert policy.
func (h *Handler) UpdateAlertPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "alertPolicies")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid alert policy name", http.StatusBadRequest)
		return
	}
	var req AlertPolicyRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.UpdateAlertPolicy", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.UpdateAlertPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /alertPolicies/{id} deletes an alert policy.
func (h *Handler) DeleteAlertPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "alertPolicies")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid alert policy name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.DeleteAlertPolicy", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.DeleteAlertPolicy(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── notification channels ────────────────────────────────────────────────────

// GET /notificationChannels lists notification channels.
func (h *Handler) ListNotificationChannels(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListNotificationChannels", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ListNotificationChannels(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /notificationChannels/{id} returns one channel.
func (h *Handler) GetNotificationChannel(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "notificationChannels")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid notification channel name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.GetNotificationChannel", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetNotificationChannel(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /notificationChannels creates a channel.
func (h *Handler) CreateNotificationChannel(w http.ResponseWriter, r *http.Request) {
	var req NotificationChannelRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.CreateNotificationChannel", "global", h.account(r))
	nr.Params["body"] = req.body()

	resp, err := h.provider.CreateNotificationChannel(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /notificationChannels/{id} updates a channel.
func (h *Handler) UpdateNotificationChannel(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "notificationChannels")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid notification channel name", http.StatusBadRequest)
		return
	}
	var req NotificationChannelRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.UpdateNotificationChannel", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.UpdateNotificationChannel(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /notificationChannels/{id} deletes a channel.
func (h *Handler) DeleteNotificationChannel(w http.ResponseWriter, r *http.Request) {
	name, ok := h.resourceName(r, "notificationChannels")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid notification channel name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.DeleteNotificationChannel", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.DeleteNotificationChannel(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /notificationChannelDescriptors lists the channel types the create dialog
// can offer.
func (h *Handler) ListNotificationChannelDescriptors(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListNotificationChannelDescriptors", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ListNotificationChannelDescriptors(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── service monitoring: services ─────────────────────────────────────────────

// serviceName builds "projects/{p}/services/{id}" from the {service} path
// parameter, rejecting an id that decoded to more than one path segment.
func (h *Handler) serviceName(r *http.Request) (string, bool) {
	id, ok := uihelper.Segment(r, "service")
	if !ok {
		return "", false
	}
	return core.ServiceName(h.account(r), id), true
}

// sloName builds
// "projects/{p}/services/{service}/serviceLevelObjectives/{slo}" from the path
// parameters.
func (h *Handler) sloName(r *http.Request) (string, bool) {
	service, ok := uihelper.Segment(r, "service")
	if !ok {
		return "", false
	}
	id, ok := uihelper.Segment(r, "slo")
	if !ok {
		return "", false
	}
	return core.ServiceLevelObjectiveName(h.account(r), service, id), true
}

// GET /services lists Service Monitoring services for the project.
func (h *Handler) ListServices(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListServices", "global", h.account(r))
	nr.Params["project"] = h.account(r)
	uihelper.PageParams(r, nr.Params)
	if f := r.URL.Query().Get("filter"); f != "" {
		nr.Params["filter"] = f
	}

	resp, err := h.provider.ListServices(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /services/{service} returns one service.
func (h *Handler) GetService(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.GetService", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetService(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /services creates a service. ?serviceId= supplies the id; the core
// generates one when it is omitted.
func (h *Handler) CreateService(w http.ResponseWriter, r *http.Request) {
	var req ServiceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.CreateService", "global", h.account(r))
	nr.Params["project"] = h.account(r)
	if id := r.URL.Query().Get("serviceId"); id != "" {
		nr.Params["serviceId"] = id
	}
	nr.Params["body"] = req.body()

	resp, err := h.provider.CreateService(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /services/{service} updates a service.
func (h *Handler) UpdateService(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service name", http.StatusBadRequest)
		return
	}
	var req ServiceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.UpdateService", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.UpdateService(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /services/{service} deletes a service and the SLOs it owns.
func (h *Handler) DeleteService(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.DeleteService", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.DeleteService(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── service monitoring: service level objectives ─────────────────────────────

// GET /services/{service}/serviceLevelObjectives lists a service's SLOs.
func (h *Handler) ListServiceLevelObjectives(w http.ResponseWriter, r *http.Request) {
	service, ok := uihelper.Segment(r, "service")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.ListServiceLevelObjectives", "global", h.account(r))
	nr.Params["project"] = h.account(r)
	nr.Params["service"] = service
	uihelper.PageParams(r, nr.Params)
	if f := r.URL.Query().Get("filter"); f != "" {
		nr.Params["filter"] = f
	}

	resp, err := h.provider.ListServiceLevelObjectives(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /services/{service}/serviceLevelObjectives/{slo} returns one SLO.
func (h *Handler) GetServiceLevelObjective(w http.ResponseWriter, r *http.Request) {
	name, ok := h.sloName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service level objective name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.GetServiceLevelObjective", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.GetServiceLevelObjective(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /services/{service}/serviceLevelObjectives creates an SLO.
// ?serviceLevelObjectiveId= supplies the id; the core generates one when it is
// omitted.
func (h *Handler) CreateServiceLevelObjective(w http.ResponseWriter, r *http.Request) {
	service, ok := uihelper.Segment(r, "service")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service name", http.StatusBadRequest)
		return
	}
	var req ServiceLevelObjectiveRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.CreateServiceLevelObjective", "global", h.account(r))
	nr.Params["project"] = h.account(r)
	nr.Params["service"] = service
	if id := r.URL.Query().Get("serviceLevelObjectiveId"); id != "" {
		nr.Params["serviceLevelObjectiveId"] = id
	}
	nr.Params["body"] = req.body()

	resp, err := h.provider.CreateServiceLevelObjective(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /services/{service}/serviceLevelObjectives/{slo} updates an SLO.
func (h *Handler) UpdateServiceLevelObjective(w http.ResponseWriter, r *http.Request) {
	name, ok := h.sloName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service level objective name", http.StatusBadRequest)
		return
	}
	var req ServiceLevelObjectiveRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.UpdateServiceLevelObjective", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.UpdateServiceLevelObjective(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /services/{service}/serviceLevelObjectives/{slo} deletes an SLO.
func (h *Handler) DeleteServiceLevelObjective(w http.ResponseWriter, r *http.Request) {
	name, ok := h.sloName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service level objective name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "monitoring", "Monitoring.DeleteServiceLevelObjective", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.DeleteServiceLevelObjective(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

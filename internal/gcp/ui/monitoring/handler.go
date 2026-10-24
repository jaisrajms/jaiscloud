package monitoringui

import (
	"encoding/json"
	"net/http"
	"net/url"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
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

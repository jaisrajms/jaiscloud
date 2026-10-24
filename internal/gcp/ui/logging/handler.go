package loggingui

import (
	"encoding/json"
	"net/http"
	"net/url"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Logging UI API requests by calling the Logging REST
// provider. It is a thin translation layer: request parameters are turned into
// NormalizedRequest params and the provider's Discovery-shaped JSON is returned
// to the console.
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

// metricName builds "projects/{p}/metrics/{id}", percent-encoding an id that may
// itself contain slashes (e.g. "nginx/requests").
func (h *Handler) metricName(r *http.Request) (string, bool) {
	id := uihelper.PathParam(r, "metric")
	if id == "" {
		return "", false
	}
	return h.scope(r) + "/metrics/" + url.PathEscape(id), true
}

// named builds "{scope}/{collection}/{id}" for sinks/exclusions, where id is the
// decoded trailing path parameter. The URL path parameter is re-encoded so a
// slash-containing id round-trips.
func (h *Handler) named(r *http.Request, collection string) (string, bool) {
	id := uihelper.PathParam(r, "id")
	if id == "" {
		return "", false
	}
	return h.scope(r) + "/" + collection + "/" + url.PathEscape(id), true
}

// decodeJSON decodes the request body into v, writing a 400 on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return false
	}
	return true
}

// ─── entries + logs ───────────────────────────────────────────────────────────

// GET /entries (also POST) returns log entries matching the filter.
func (h *Handler) ListEntries(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.EntryList", "global", h.account(r))
	body := map[string]any{"resourceNames": []any{h.scope(r)}}
	q := r.URL.Query()
	for _, key := range []string{"filter", "orderBy", "pageSize", "pageToken"} {
		if v := q.Get(key); v != "" {
			body[key] = v
		}
	}
	nr.Params["body"] = body

	resp, err := h.provider.EntryList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /logs lists the distinct log names under the project.
func (h *Handler) ListLogs(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.LogList", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.LogList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /logs/{log} deletes every entry of a log.
func (h *Handler) DeleteLog(w http.ResponseWriter, r *http.Request) {
	log := uihelper.PathParam(r, "log")
	if log == "" {
		uihelper.UIError(w, "BadRequest", "invalid log name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.LogDelete", "global", h.account(r))
	nr.Params["logName"] = h.scope(r) + "/logs/" + url.PathEscape(log)

	if _, err := h.provider.LogDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── logs-based metrics ───────────────────────────────────────────────────────

// GET /metrics lists logs-based metrics.
func (h *Handler) ListMetrics(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.MetricList", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.MetricList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /metrics/{metric} returns one metric.
func (h *Handler) GetMetric(w http.ResponseWriter, r *http.Request) {
	name, ok := h.metricName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid metric name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.MetricGet", "global", h.account(r))
	nr.Params["metricName"] = name

	resp, err := h.provider.MetricGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /metrics creates a logs-based metric.
func (h *Handler) CreateMetric(w http.ResponseWriter, r *http.Request) {
	var req MetricRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.MetricCreate", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	nr.Params["body"] = req.body()

	resp, err := h.provider.MetricCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PUT /metrics/{metric} updates (or upserts) a logs-based metric.
func (h *Handler) UpdateMetric(w http.ResponseWriter, r *http.Request) {
	name, ok := h.metricName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid metric name", http.StatusBadRequest)
		return
	}
	var req MetricRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.MetricUpdate", "global", h.account(r))
	nr.Params["metricName"] = name
	nr.Params["body"] = req.body()

	resp, err := h.provider.MetricUpdate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /metrics/{metric} deletes a logs-based metric.
func (h *Handler) DeleteMetric(w http.ResponseWriter, r *http.Request) {
	name, ok := h.metricName(r)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid metric name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.MetricDelete", "global", h.account(r))
	nr.Params["metricName"] = name

	if _, err := h.provider.MetricDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── sinks ────────────────────────────────────────────────────────────────────

// GET /sinks lists log sinks.
func (h *Handler) ListSinks(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.SinkList", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.SinkList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /sinks/{id} returns one sink.
func (h *Handler) GetSink(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "sinks")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid sink name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.SinkGet", "global", h.account(r))
	nr.Params["sinkName"] = name

	resp, err := h.provider.SinkGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /sinks creates a log sink.
func (h *Handler) CreateSink(w http.ResponseWriter, r *http.Request) {
	var req SinkRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.SinkCreate", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	nr.Params["body"] = req.body()

	resp, err := h.provider.SinkCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /sinks/{id} updates a log sink.
func (h *Handler) UpdateSink(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "sinks")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid sink name", http.StatusBadRequest)
		return
	}
	var req SinkRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.SinkPatch", "global", h.account(r))
	nr.Params["sinkName"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.SinkPatch(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /sinks/{id} deletes a log sink.
func (h *Handler) DeleteSink(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "sinks")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid sink name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.SinkDelete", "global", h.account(r))
	nr.Params["sinkName"] = name

	if _, err := h.provider.SinkDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── exclusions ───────────────────────────────────────────────────────────────

// GET /exclusions lists resource-level exclusions.
func (h *Handler) ListExclusions(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.ExclusionList", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ExclusionList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /exclusions/{id} returns one exclusion.
func (h *Handler) GetExclusion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "exclusions")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid exclusion name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.ExclusionGet", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.ExclusionGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /exclusions creates a resource-level exclusion.
func (h *Handler) CreateExclusion(w http.ResponseWriter, r *http.Request) {
	var req ExclusionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.ExclusionCreate", "global", h.account(r))
	nr.Params["parent"] = h.scope(r)
	nr.Params["body"] = req.body()

	resp, err := h.provider.ExclusionCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// PATCH /exclusions/{id} updates an exclusion.
func (h *Handler) UpdateExclusion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "exclusions")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid exclusion name", http.StatusBadRequest)
		return
	}
	var req ExclusionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.ExclusionPatch", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = req.body()
	if mask := r.URL.Query().Get("updateMask"); mask != "" {
		nr.Params["updateMask"] = mask
	}

	resp, err := h.provider.ExclusionPatch(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /exclusions/{id} deletes an exclusion.
func (h *Handler) DeleteExclusion(w http.ResponseWriter, r *http.Request) {
	name, ok := h.named(r, "exclusions")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid exclusion name", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "logging", "Logging.ExclusionDelete", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.ExclusionDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

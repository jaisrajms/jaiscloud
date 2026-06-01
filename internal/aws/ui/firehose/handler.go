package firehoseui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves Firehose UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /streams
func (h *Handler) ListDeliveryStreams(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firehose", "ListDeliveryStreams", region, account)

	resp, err := h.provider.ListDeliveryStreams(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawNames, _ := resp.Data["DeliveryStreamNames"].([]string)
	items := make([]DeliveryStream, 0, len(rawNames))
	for _, name := range rawNames {
		items = append(items, DeliveryStream{Name: name})
	}
	uihelper.WriteJSON(w, ListDeliveryStreamsResponse{Items: items, Total: len(items)})
}

// POST /streams  body: { "name": "...", "type": "DirectPut" }
func (h *Handler) CreateDeliveryStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firehose", "CreateDeliveryStream", region, account)
	nr.Params["DeliveryStreamName"] = req.Name
	if req.Type != "" {
		nr.Params["DeliveryStreamType"] = req.Type
	}

	resp, err := h.provider.CreateDeliveryStream(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /streams/{name}
func (h *Handler) DeleteDeliveryStream(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "firehose", "DeleteDeliveryStream", region, account)
	nr.Params["DeliveryStreamName"] = name

	if _, err := h.provider.DeleteDeliveryStream(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

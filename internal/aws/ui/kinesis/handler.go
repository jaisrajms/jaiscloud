package kinesisui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves Kinesis UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /streams
func (h *Handler) ListStreams(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "kinesis", "ListStreams", region, account)

	resp, err := h.provider.ListStreams(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	// ListStreams returns StreamSummaries with name/arn/status/mode
	rawSummaries, _ := resp.Data["StreamSummaries"].([]map[string]any)
	items := make([]Stream, 0, len(rawSummaries))
	for _, s := range rawSummaries {
		st := Stream{
			Name:   strAny(s, "StreamName"),
			ARN:    strAny(s, "StreamARN"),
			Status: strAny(s, "StreamStatus"),
		}
		if md, ok := s["StreamModeDetails"].(map[string]any); ok {
			st.Mode = strAny(md, "StreamMode")
		}
		items = append(items, st)
	}

	// Fall back to StreamNames if StreamSummaries is empty
	if len(items) == 0 {
		rawNames, _ := resp.Data["StreamNames"].([]any)
		for _, raw := range rawNames {
			if name, ok := raw.(string); ok {
				items = append(items, Stream{Name: name})
			}
		}
	}

	nextToken, _ := resp.Data["NextToken"].(string)
	uihelper.WriteJSON(w, ListStreamsResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /streams  body: { "name": "...", "shardCount": 1 }
func (h *Handler) CreateStream(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		ShardCount int    `json:"shardCount"`
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
	nr := uihelper.NR(r.Context(), h.cfg, "kinesis", "CreateStream", region, account)
	nr.Params["StreamName"] = req.Name
	if req.ShardCount > 0 {
		nr.Params["ShardCount"] = req.ShardCount
	}

	resp, err := h.provider.CreateStream(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /streams/{name}
func (h *Handler) DeleteStream(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "kinesis", "DeleteStream", region, account)
	nr.Params["StreamName"] = name

	if _, err := h.provider.DeleteStream(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

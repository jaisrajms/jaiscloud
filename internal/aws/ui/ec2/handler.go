package ec2ui

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves EC2 UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /instances
func (h *Handler) ListInstances(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ec2", "DescribeInstances", region, account)

	resp, err := h.provider.DescribeInstances(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	// Flatten Reservations → Instances
	var items []Instance
	if rawReservations, ok := resp.Data["Reservations"].([]any); ok {
		for _, rawRes := range rawReservations {
			if res, ok := rawRes.(map[string]any); ok {
				if rawInsts, ok := res["Instances"].([]any); ok {
					for _, rawInst := range rawInsts {
						if m, ok := rawInst.(map[string]any); ok {
							items = append(items, mapInstance(m))
						}
					}
				}
			}
		}
	}
	if items == nil {
		items = []Instance{}
	}
	nextToken, _ := resp.Data["NextToken"].(string)
	uihelper.WriteJSON(w, ListInstancesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// DELETE /instances/{id}
func (h *Handler) TerminateInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ec2", "TerminateInstances", region, account)
	nr.Params["InstanceId.1"] = id

	if _, err := h.provider.TerminateInstances(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /instances/{id}/start
func (h *Handler) StartInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ec2", "StartInstances", region, account)
	nr.Params["InstanceId.1"] = id

	if _, err := h.provider.StartInstances(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /instances/{id}/stop
func (h *Handler) StopInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ec2", "StopInstances", region, account)
	nr.Params["InstanceId.1"] = id

	if _, err := h.provider.StopInstances(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapInstance(m map[string]any) Instance {
	inst := Instance{
		ID:           strAny(m, "InstanceId"),
		ImageID:      strAny(m, "ImageId"),
		InstanceType: strAny(m, "InstanceType"),
		LaunchTime:   strAny(m, "LaunchTime"),
	}
	if state, ok := m["State"].(map[string]any); ok {
		inst.State = strAny(state, "Name")
	}
	inst.PublicIP = strAny(m, "PublicIpAddress")
	inst.PrivateIP = strAny(m, "PrivateIpAddress")
	return inst
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

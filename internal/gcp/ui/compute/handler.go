package computeui

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/model"
)

// Handler serves Compute Engine UI API requests by calling the Compute Engine
// provider.
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

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func mapAt(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	return v
}

// lastSegment returns the final "/"-separated segment of a resource URL, or the
// input unchanged when it has no slash.
func lastSegment(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// stringMapAt extracts a string-valued map, dropping any non-string entries.
func stringMapAt(m map[string]any, key string) map[string]string {
	raw := mapAt(m, key)
	if raw == nil {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// segment returns the decoded, single-segment value of a URL path parameter.
// A decoded '/' is rejected: zones and instance ids are single path segments.
func segment(r *http.Request, key string) (string, bool) {
	v := uihelper.PathParam(r, key)
	if v == "" || strings.Contains(v, "/") {
		return "", false
	}
	return v, true
}

// instanceFromMap converts a provider instance map into the UI summary. zone is
// the fallback zone used when the instance body carries no zone selfLink (the
// aggregated list's group key).
func instanceFromMap(m map[string]any, zone string) Instance {
	inst := Instance{
		Name:              str(m, "name"),
		Zone:              lastSegment(str(m, "zone")),
		Status:            str(m, "status"),
		MachineType:       lastSegment(str(m, "machineType")),
		CPUPlatform:       str(m, "cpuPlatform"),
		CreationTimestamp: str(m, "creationTimestamp"),
		Labels:            stringMapAt(m, "labels"),
	}
	if inst.Zone == "" {
		inst.Zone = zone
	}
	if nics := uihelper.AsSlice(m["networkInterfaces"]); len(nics) > 0 {
		if nic, ok := nics[0].(map[string]any); ok {
			inst.InternalIP = str(nic, "networkIP")
			if cfgs := uihelper.AsSlice(nic["accessConfigs"]); len(cfgs) > 0 {
				if cfg, ok := cfgs[0].(map[string]any); ok {
					inst.ExternalIP = str(cfg, "natIP")
				}
			}
		}
	}
	return inst
}

// detailFromMap builds the full detail view, passing the provider's metadata /
// disks / networkInterfaces JSON through unchanged.
func detailFromMap(m map[string]any) InstanceDetail {
	d := InstanceDetail{
		Instance:    instanceFromMap(m, lastSegment(str(m, "zone"))),
		ID:          str(m, "id"),
		SelfLink:    str(m, "selfLink"),
		Description: str(m, "description"),
		Metadata:    mapAt(m, "metadata"),
	}
	if disks := uihelper.AsSlice(m["disks"]); disks != nil {
		d.Disks = disks
	}
	if nics := uihelper.AsSlice(m["networkInterfaces"]); nics != nil {
		d.NetworkInterfaces = nics
	}
	return d
}

// ─── Instances ───────────────────────────────────────────────────────────────

// GET /instances
func (h *Handler) ListInstances(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "compute", "Compute.InstancesAggregatedList", "global", account)

	resp, err := h.provider.InstancesAggregatedList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	instances := make([]Instance, 0)
	for key, group := range mapAt(resp.Data, "items") {
		zone := lastSegment(key)
		groupMap, _ := group.(map[string]any)
		for _, raw := range uihelper.AsSlice(groupMap["instances"]) {
			if m, ok := raw.(map[string]any); ok {
				instances = append(instances, instanceFromMap(m, zone))
			}
		}
	}
	sort.Slice(instances, func(i, j int) bool {
		if instances[i].Zone != instances[j].Zone {
			return instances[i].Zone < instances[j].Zone
		}
		return instances[i].Name < instances[j].Name
	})
	uihelper.WriteJSON(w, ListInstancesResponse{Instances: instances, Total: len(instances)})
}

// GET /instances/{zone}/{instance}
func (h *Handler) GetInstance(w http.ResponseWriter, r *http.Request) {
	zone, instance, ok := h.target(w, r)
	if !ok {
		return
	}
	nr := h.zonalNR(r, "Compute.InstancesGet", zone, instance)

	resp, err := h.provider.InstancesGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, detailFromMap(resp.Data))
}

// POST /instances/{zone}/{instance}/start
func (h *Handler) StartInstance(w http.ResponseWriter, r *http.Request) {
	h.setState(w, r, "Compute.InstancesStart", h.provider.InstancesStart)
}

// POST /instances/{zone}/{instance}/stop
func (h *Handler) StopInstance(w http.ResponseWriter, r *http.Request) {
	h.setState(w, r, "Compute.InstancesStop", h.provider.InstancesStop)
}

// DELETE /instances/{zone}/{instance}
func (h *Handler) DeleteInstance(w http.ResponseWriter, r *http.Request) {
	zone, instance, ok := h.target(w, r)
	if !ok {
		return
	}
	nr := h.zonalNR(r, "Compute.InstancesDelete", zone, instance)

	if _, err := h.provider.InstancesDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {zone}/{instance} path parameters, writing a
// 400 and returning false when either is missing or malformed.
func (h *Handler) target(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	zone, ok := segment(r, "zone")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid zone", http.StatusBadRequest)
		return "", "", false
	}
	instance, ok := segment(r, "instance")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid instance", http.StatusBadRequest)
		return "", "", false
	}
	return zone, instance, true
}

// zonalNR builds a NormalizedRequest scoped to a zone, since instances are
// zonal and the provider reads the store scope from Params["scope"].
func (h *Handler) zonalNR(r *http.Request, action, zone, instance string) *model.NormalizedRequest {
	nr := uihelper.NR(r.Context(), h.cfg, "compute", action, zone, h.account(r))
	nr.Params["scope"] = zone
	nr.Params["instance"] = instance
	return nr
}

// setState runs a start/stop action and returns the provider's compute#operation
// envelope.
func (h *Handler) setState(w http.ResponseWriter, r *http.Request, action string, fn func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)) {
	zone, instance, ok := h.target(w, r)
	if !ok {
		return
	}
	resp, err := fn(r.Context(), h.zonalNR(r, action, zone, instance))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

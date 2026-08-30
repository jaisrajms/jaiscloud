package emroneksui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves EMR on EKS UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /virtual-clusters?state=...
func (h *Handler) ListVirtualClusters(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "ListVirtualClusters", region, account)
	if state := r.URL.Query().Get("state"); state != "" {
		nr.Params["states"] = state
	}

	resp, err := h.provider.ListVirtualClusters(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawVCs, _ := resp.Data["virtualClusters"].([]any)
	items := make([]VirtualCluster, 0, len(rawVCs))
	for _, raw := range rawVCs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapVC(m))
		}
	}

	uihelper.WriteJSON(w, ListVirtualClustersResponse{Items: items, Total: len(items)})
}

// POST /virtual-clusters  body: { "name": "...", "eksClusterId": "...", "namespace": "..." }
func (h *Handler) CreateVirtualCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string `json:"name"`
		EksClusterID string `json:"eksClusterId"`
		Namespace    string `json:"namespace"`
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

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "CreateVirtualCluster", region, account)
	nr.Params["name"] = req.Name
	if req.EksClusterID != "" {
		nr.Params["containerProvider"] = map[string]any{
			"id":   req.EksClusterID,
			"type": "EKS",
			"info": map[string]any{
				"eksInfo": map[string]any{"namespace": ns},
			},
		}
	}

	resp, err := h.provider.CreateVirtualCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /virtual-clusters/{id}
func (h *Handler) DescribeVirtualCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "DescribeVirtualCluster", region, account)
	nr.Params["virtualClusterId"] = id

	resp, err := h.provider.DescribeVirtualCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	vc, _ := resp.Data["virtualCluster"].(map[string]any)
	if vc == nil {
		vc = map[string]any{}
	}
	uihelper.WriteJSON(w, mapVC(vc))
}

// DELETE /virtual-clusters/{id}
func (h *Handler) DeleteVirtualCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "DeleteVirtualCluster", region, account)
	nr.Params["virtualClusterId"] = id

	if _, err := h.provider.DeleteVirtualCluster(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /virtual-clusters/{vcId}/jobs
func (h *Handler) ListJobRuns(w http.ResponseWriter, r *http.Request) {
	vcID := chi.URLParam(r, "vcId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "ListJobRuns", region, account)
	nr.Params["virtualClusterId"] = vcID

	resp, err := h.provider.ListJobRuns(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawJobs, _ := resp.Data["jobRuns"].([]any)
	items := make([]JobRun, 0, len(rawJobs))
	for _, raw := range rawJobs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapJobRun(m))
		}
	}

	uihelper.WriteJSON(w, ListJobRunsResponse{Items: items, Total: len(items)})
}

// POST /virtual-clusters/{vcId}/jobs  body: { "name": "...", "releaseLabel": "...", "executionRoleArn": "..." }
func (h *Handler) StartJobRun(w http.ResponseWriter, r *http.Request) {
	vcID := chi.URLParam(r, "vcId")

	var req struct {
		Name             string `json:"name"`
		ReleaseLabel     string `json:"releaseLabel"`
		ExecutionRoleArn string `json:"executionRoleArn"`
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

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "StartJobRun", region, account)
	nr.Params["virtualClusterId"] = vcID
	nr.Params["name"] = req.Name
	if req.ReleaseLabel != "" {
		nr.Params["releaseLabel"] = req.ReleaseLabel
	}
	if req.ExecutionRoleArn != "" {
		nr.Params["executionRoleArn"] = req.ExecutionRoleArn
	}

	resp, err := h.provider.StartJobRun(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /virtual-clusters/{vcId}/jobs/{jobId}
func (h *Handler) DescribeJobRun(w http.ResponseWriter, r *http.Request) {
	vcID := chi.URLParam(r, "vcId")
	jobID := chi.URLParam(r, "jobId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "DescribeJobRun", region, account)
	nr.Params["virtualClusterId"] = vcID
	nr.Params["id"] = jobID

	resp, err := h.provider.DescribeJobRun(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	job, _ := resp.Data["jobRun"].(map[string]any)
	if job == nil {
		job = map[string]any{}
	}
	uihelper.WriteJSON(w, mapJobRun(job))
}

// DELETE /virtual-clusters/{vcId}/jobs/{jobId}
func (h *Handler) CancelJobRun(w http.ResponseWriter, r *http.Request) {
	vcID := chi.URLParam(r, "vcId")
	jobID := chi.URLParam(r, "jobId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr-containers", "CancelJobRun", region, account)
	nr.Params["virtualClusterId"] = vcID
	nr.Params["id"] = jobID

	if _, err := h.provider.CancelJobRun(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapVC(m map[string]any) VirtualCluster {
	vc := VirtualCluster{
		ID:    strAny(m, "id"),
		Name:  strAny(m, "name"),
		State: strAny(m, "state"),
		ARN:   strAny(m, "arn"),
	}
	if cp, ok := m["containerProvider"].(map[string]any); ok {
		vc.EksCluster = strAny(cp, "id")
		if info, ok := cp["info"].(map[string]any); ok {
			if eks, ok := info["eksInfo"].(map[string]any); ok {
				vc.Namespace = strAny(eks, "namespace")
			}
		}
	}
	return vc
}

func mapJobRun(m map[string]any) JobRun {
	return JobRun{
		ID:               strAny(m, "id"),
		Name:             strAny(m, "name"),
		VirtualClusterID: strAny(m, "virtualClusterId"),
		State:            strAny(m, "state"),
		ReleaseLabel:     strAny(m, "releaseLabel"),
		ARN:              strAny(m, "arn"),
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

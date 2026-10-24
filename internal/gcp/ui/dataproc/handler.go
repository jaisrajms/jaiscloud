package dataprocui

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"jaiscloud/internal/config"
	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Dataproc UI API requests by calling the Dataproc core.
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

// formatTime renders a stored timestamp as RFC3339, or "" when zero.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// clusterHistory converts a stored cluster status history for the console.
func clusterHistory(h []dpstore.ClusterStatus) []StatusEvent {
	if len(h) == 0 {
		return nil
	}
	out := make([]StatusEvent, 0, len(h))
	for _, s := range h {
		out = append(out, StatusEvent{State: s.State, Detail: s.Detail, StartTime: formatTime(s.StateStartTime)})
	}
	return out
}

// jobHistory converts a stored job status history for the console.
func jobHistory(h []dpstore.JobStatus) []StatusEvent {
	if len(h) == 0 {
		return nil
	}
	out := make([]StatusEvent, 0, len(h))
	for _, s := range h {
		out = append(out, StatusEvent{State: s.State, Detail: s.Details, StartTime: formatTime(s.StateStartTime)})
	}
	return out
}

// renderCluster flattens a stored cluster into the console row. The full
// config (Config/VirtualClusterConfig) and the Kubernetes namespace placement
// (effective + requested) are included only on the detail read to keep the list
// payload small.
func renderCluster(project string, c dpstore.Cluster, full bool) Cluster {
	out := Cluster{
		ID:            c.Name,
		Name:          dataproccore.ClusterName(project, c.Region, c.Name),
		Region:        c.Region,
		Status:        c.Status.State,
		StatusDetail:  c.Status.Detail,
		StatusHistory: clusterHistory(c.StatusHistory),
		ClusterUUID:   c.ClusterUUID,
		Labels:        c.Labels,
		GKEBacked:     c.IsGKEBacked(),
		CreateTime:    formatTime(c.CreateTime),
		UpdateTime:    formatTime(c.UpdateTime),
	}
	if full {
		out.Config = c.Config
		out.VirtualClusterConfig = c.VirtualClusterConfig
		out.Namespace = c.Namespace
		out.NamespaceOwned = c.NamespaceOwned
		out.KubernetesNamespace = dataproccore.KubernetesNamespaceFromVirtualClusterConfig(c.VirtualClusterConfig)
	}
	return out
}

// renderJob flattens a stored job into the console row. The type-specific body
// is included only on the detail read.
func renderJob(project string, j dpstore.Job, full bool) Job {
	out := Job{
		ID:                      j.JobID,
		Name:                    dataproccore.JobName(project, j.Region, j.JobID),
		Region:                  j.Region,
		ClusterName:             j.PlacementClusterName,
		Type:                    j.Type,
		Status:                  j.Status.State,
		StatusDetail:            j.Status.Details,
		Substate:                j.Status.Substate,
		StatusHistory:           jobHistory(j.StatusHistory),
		Labels:                  j.Labels,
		DriverOutputResourceURI: j.DriverOutputResourceURI,
		DriverControlFilesURI:   j.DriverControlFilesURI,
		JobUUID:                 j.JobUUID,
		CreateTime:              formatTime(j.CreateTime),
	}
	if full {
		out.TypeJob = j.TypeJob
	}
	return out
}

// renderTemplate flattens a stored workflow template into the console row. The
// full definition is included only on the detail read.
func renderTemplate(project string, t dpstore.WorkflowTemplate, full bool) WorkflowTemplate {
	out := WorkflowTemplate{
		ID:         t.TemplateID,
		Name:       dataproccore.WorkflowTemplateName(project, t.Region, t.TemplateID),
		Region:     t.Region,
		Version:    t.Version,
		CreateTime: formatTime(t.CreateTime),
		UpdateTime: formatTime(t.UpdateTime),
	}
	if full {
		out.Definition = t.Definition
	}
	return out
}

// ─── Clusters ────────────────────────────────────────────────────────────────

// GET /clusters
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	clusters, err := h.provider.ListAllClusters(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Cluster, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, renderCluster(project, c, false))
	}
	uihelper.WriteJSON(w, ListClustersResponse{Clusters: out, Total: len(out)})
}

// POST /clusters
func (h *Handler) CreateCluster(w http.ResponseWriter, r *http.Request) {
	var in CreateClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return
	}
	if in.Region == "" || in.Name == "" {
		uihelper.UIError(w, "InvalidArgument", "region and name are required", http.StatusBadRequest)
		return
	}
	project := h.account(r)
	c, err := h.provider.CreateCluster(r.Context(), project, in.Region, in.Name, dataproccore.ClusterInput{
		Labels:               in.Labels,
		Config:               in.Config,
		VirtualClusterConfig: in.VirtualClusterConfig,
	})
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderCluster(project, c, true))
}

// GET /clusters/{region}/{cluster}
func (h *Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	region, name, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	c, err := h.provider.GetCluster(r.Context(), project, region, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderCluster(project, c, true))
}

// POST /clusters/{region}/{cluster}/start
func (h *Handler) StartCluster(w http.ResponseWriter, r *http.Request) {
	h.mutateCluster(w, r, h.provider.StartCluster)
}

// POST /clusters/{region}/{cluster}/stop
func (h *Handler) StopCluster(w http.ResponseWriter, r *http.Request) {
	h.mutateCluster(w, r, h.provider.StopCluster)
}

// DELETE /clusters/{region}/{cluster}
func (h *Handler) DeleteCluster(w http.ResponseWriter, r *http.Request) {
	region, name, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	if err := h.provider.DeleteCluster(r.Context(), h.account(r), region, name); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) mutateCluster(w http.ResponseWriter, r *http.Request, fn func(ctx context.Context, project, region, name string) (dpstore.Cluster, error)) {
	region, name, ok := h.target(w, r, "cluster")
	if !ok {
		return
	}
	project := h.account(r)
	c, err := fn(r.Context(), project, region, name)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderCluster(project, c, true))
}

// ─── Jobs ────────────────────────────────────────────────────────────────────

// GET /jobs
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	jobs, err := h.provider.ListAllJobs(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, renderJob(project, j, false))
	}
	uihelper.WriteJSON(w, ListJobsResponse{Jobs: out, Total: len(out)})
}

// GET /jobs/{region}/{job}
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	region, id, ok := h.target(w, r, "job")
	if !ok {
		return
	}
	project := h.account(r)
	j, err := h.provider.GetJob(r.Context(), project, region, id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderJob(project, j, true))
}

// POST /jobs/{region}/{job}/cancel
func (h *Handler) CancelJob(w http.ResponseWriter, r *http.Request) {
	region, id, ok := h.target(w, r, "job")
	if !ok {
		return
	}
	project := h.account(r)
	j, err := h.provider.CancelJob(r.Context(), project, region, id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderJob(project, j, true))
}

// ─── Workflow templates ──────────────────────────────────────────────────────

// GET /workflow-templates
func (h *Handler) ListWorkflowTemplates(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	templates, err := h.provider.ListAllWorkflowTemplates(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]WorkflowTemplate, 0, len(templates))
	for _, t := range templates {
		out = append(out, renderTemplate(project, t, false))
	}
	uihelper.WriteJSON(w, ListWorkflowTemplatesResponse{Templates: out, Total: len(out)})
}

// GET /workflow-templates/{region}/{template}
func (h *Handler) GetWorkflowTemplate(w http.ResponseWriter, r *http.Request) {
	region, id, ok := h.target(w, r, "template")
	if !ok {
		return
	}
	project := h.account(r)
	t, err := h.provider.GetWorkflowTemplate(r.Context(), project, region, id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderTemplate(project, t, true))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {region}/{<name>} path parameters, writing a
// 400 and returning false when either is missing or malformed. key is the
// chi parameter name of the resource id segment ("cluster", "job", "template").
func (h *Handler) target(w http.ResponseWriter, r *http.Request, key string) (string, string, bool) {
	region, ok := uihelper.Segment(r, "region")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid region", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, key)
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid "+key, http.StatusBadRequest)
		return "", "", false
	}
	return region, id, true
}

package workflowsui

import (
	"encoding/json"
	"net/http"
	"time"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Workflows UI API requests by calling the Workflows and
// Workflow Executions cores.
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

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// workflowName is the canonical Cloud Workflows resource name.
func workflowName(project, location, id string) string {
	return resource.ResourceID(project)("workflow", location+"/"+id)
}

// executionName is the canonical Workflow Executions resource name.
func executionName(project, location, workflowID, id string) string {
	return workflowName(project, location, workflowID) + "/executions/" + id
}

// renderWorkflow flattens a stored workflow into the console row.
func renderWorkflow(project string, w workflowsstore.Workflow) Workflow {
	return Workflow{
		ID:             w.ID,
		Location:       w.Location,
		Name:           workflowName(project, w.Location, w.ID),
		Description:    w.Description,
		State:          w.State,
		RevisionID:     w.RevisionID,
		ServiceAccount: w.ServiceAccount,
		SourceContents: w.SourceContents,
		CallLogLevel:   w.CallLogLevel,
		Labels:         w.Labels,
		UserEnvVars:    w.UserEnvVars,
		CreateTime:     formatTime(w.CreateTime),
		UpdateTime:     formatTime(w.UpdateTime),
	}
}

// renderExecution flattens a stored execution into the console row.
func renderExecution(project string, e workflowsstore.Execution) Execution {
	out := Execution{
		ID:                 e.ID,
		WorkflowID:         e.WorkflowID,
		Location:           e.Location,
		Name:               executionName(project, e.Location, e.WorkflowID, e.ID),
		State:              e.State,
		Argument:           e.Argument,
		Result:             e.Result,
		StartTime:          formatTime(e.StartTime),
		EndTime:            formatTime(e.EndTime),
		Duration:           e.Duration,
		WorkflowRevisionID: e.WorkflowRevisionID,
		CallLogLevel:       e.CallLogLevel,
		Labels:             e.Labels,
	}
	if e.Error != nil {
		out.Error = &ExecutionError{Payload: e.Error.Payload, Context: e.Error.Context}
	}
	for _, s := range e.CurrentSteps {
		out.CurrentSteps = append(out.CurrentSteps, Step{Routine: s.Routine, Step: s.Step})
	}
	return out
}

// decodeInput decodes a JSON request body, writing a 400 on failure.
func decodeInput[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var in T
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return in, false
	}
	return in, true
}

// ─── Workflows ───────────────────────────────────────────────────────────────

// GET /workflows
func (h *Handler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	project := h.account(r)
	wfs, err := h.provider.ListWorkflowsByProject(r.Context(), project)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Workflow, 0, len(wfs))
	for _, w := range wfs {
		out = append(out, renderWorkflow(project, w))
	}
	uihelper.WriteJSON(w, ListWorkflowsResponse{Workflows: out, Total: len(out)})
}

// POST /workflows
func (h *Handler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeInput[WorkflowInput](w, r)
	if !ok {
		return
	}
	if in.Location == "" {
		uihelper.UIError(w, "InvalidArgument", "location is required", http.StatusBadRequest)
		return
	}
	if in.ID == "" {
		uihelper.UIError(w, "InvalidArgument", "workflow id is required", http.StatusBadRequest)
		return
	}
	created, err := h.provider.CreateWorkflow(r.Context(), h.account(r), in.Location, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderWorkflow(h.account(r), created))
}

// GET /workflows/{location}/{workflow}
func (h *Handler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	location, id, ok := h.targetWorkflow(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	wf, err := h.provider.GetWorkflow(r.Context(), project, location, id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderWorkflow(project, wf))
}

// PUT /workflows/{location}/{workflow}
func (h *Handler) UpdateWorkflow(w http.ResponseWriter, r *http.Request) {
	location, id, ok := h.targetWorkflow(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[WorkflowUpdateInput](w, r)
	if !ok {
		return
	}
	updated, err := h.provider.UpdateWorkflow(r.Context(), h.account(r), location, id, in)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderWorkflow(h.account(r), updated))
}

// DELETE /workflows/{location}/{workflow}
func (h *Handler) DeleteWorkflow(w http.ResponseWriter, r *http.Request) {
	location, id, ok := h.targetWorkflow(w, r)
	if !ok {
		return
	}
	if err := h.provider.DeleteWorkflow(r.Context(), h.account(r), location, id); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Executions ──────────────────────────────────────────────────────────────

// GET /workflows/{location}/{workflow}/executions
func (h *Handler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	location, workflow, ok := h.targetWorkflow(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	pageSize := uihelper.PageSizeFrom(r, 500, 1000)
	execs, next, err := h.provider.ListExecutions(r.Context(), project, location, workflow, pageSize, r.URL.Query().Get("pageToken"))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	out := make([]Execution, 0, len(execs))
	for _, e := range execs {
		out = append(out, renderExecution(project, e))
	}
	uihelper.WriteJSON(w, ListExecutionsResponse{Executions: out, Total: len(out), NextPageToken: next})
}

// POST /workflows/{location}/{workflow}/executions
func (h *Handler) RunWorkflow(w http.ResponseWriter, r *http.Request) {
	location, workflow, ok := h.targetWorkflow(w, r)
	if !ok {
		return
	}
	in, ok := decodeInput[RunExecutionInput](w, r)
	if !ok {
		return
	}
	project := h.account(r)
	exec, err := h.provider.RunWorkflow(r.Context(), project, location, workflow, in.Argument, in.CallLogLevel, in.Labels)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, renderExecution(project, exec))
}

// GET /workflows/{location}/{workflow}/executions/{execution}
func (h *Handler) GetExecution(w http.ResponseWriter, r *http.Request) {
	location, workflow, execution, ok := h.targetExecution(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	exec, err := h.provider.GetExecution(r.Context(), project, location, workflow, execution)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderExecution(project, exec))
}

// POST /workflows/{location}/{workflow}/executions/{execution}/cancel
func (h *Handler) CancelExecution(w http.ResponseWriter, r *http.Request) {
	location, workflow, execution, ok := h.targetExecution(w, r)
	if !ok {
		return
	}
	project := h.account(r)
	exec, err := h.provider.CancelExecution(r.Context(), project, location, workflow, execution)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, renderExecution(project, exec))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// targetWorkflow reads and validates the {location}/{workflow} path parameters,
// writing a 400 and returning false when either is missing or malformed.
func (h *Handler) targetWorkflow(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	location, ok := uihelper.Segment(r, "location")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid location", http.StatusBadRequest)
		return "", "", false
	}
	id, ok := uihelper.Segment(r, "workflow")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid workflow", http.StatusBadRequest)
		return "", "", false
	}
	return location, id, true
}

// targetExecution reads and validates the {location}/{workflow}/{execution}
// path parameters.
func (h *Handler) targetExecution(w http.ResponseWriter, r *http.Request) (string, string, string, bool) {
	location, workflow, ok := h.targetWorkflow(w, r)
	if !ok {
		return "", "", "", false
	}
	id, ok := uihelper.Segment(r, "execution")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid execution", http.StatusBadRequest)
		return "", "", "", false
	}
	return location, workflow, id, true
}

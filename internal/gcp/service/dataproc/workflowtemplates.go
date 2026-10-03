package dataproc

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// WorkflowTemplateInput carries the caller-supplied fields of a workflow
// template create/update. Definition is the full dataproc.v1.WorkflowTemplate
// wire object (jobs, placement, parameters, labels, dagTimeout, ...).
type WorkflowTemplateInput struct {
	ID         string
	Version    int32
	Definition map[string]any
}

// CreateWorkflowTemplate stores a new workflow template at version 1. The
// template id is caller-supplied (dataproc.v1.WorkflowTemplate.id is an input
// field). The definition is validated structurally (placement present, jobs
// with unique step ids, resolvable acyclic prerequisites).
func (s *Service) CreateWorkflowTemplate(ctx context.Context, project, region string, in WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	if region == "" {
		return dpstore.WorkflowTemplate{}, invalidArgument("missing region")
	}
	if in.ID == "" {
		return dpstore.WorkflowTemplate{}, invalidArgument("missing workflow template id")
	}
	if err := validateWorkflowDefinition(in.Definition); err != nil {
		return dpstore.WorkflowTemplate{}, err
	}
	def, err := json.Marshal(in.Definition)
	if err != nil {
		return dpstore.WorkflowTemplate{}, invalidArgument("malformed workflow template")
	}
	now := clock.Now().UTC()
	t := dpstore.WorkflowTemplate{
		ProjectID:  project,
		Region:     region,
		TemplateID: in.ID,
		Version:    1,
		Definition: def,
		CreateTime: now,
		UpdateTime: now,
	}
	if err := s.store.CreateWorkflowTemplate(ctx, project, region, t); err != nil {
		return dpstore.WorkflowTemplate{}, mapErr(err)
	}
	return t, nil
}

// GetWorkflowTemplate returns a stored workflow template. version 0 returns the
// latest; an explicit version must match it (only the latest version is
// retained — see the store doc).
func (s *Service) GetWorkflowTemplate(ctx context.Context, project, region, id string, version int32) (dpstore.WorkflowTemplate, error) {
	if region == "" || id == "" {
		return dpstore.WorkflowTemplate{}, invalidArgument("missing region or workflow template id")
	}
	t, err := s.store.GetWorkflowTemplate(ctx, project, region, id)
	if err != nil {
		return dpstore.WorkflowTemplate{}, mapErr(err)
	}
	if version != 0 && version != t.Version {
		return dpstore.WorkflowTemplate{}, model.NewProviderError("NotFound", "workflow template version not found", 404)
	}
	return t, nil
}

// ListWorkflowTemplates returns a cursor page of the workflow templates in a
// region.
func (s *Service) ListWorkflowTemplates(ctx context.Context, project, region string, pageSize int, pageToken string) ([]dpstore.WorkflowTemplate, string, error) {
	if region == "" {
		return nil, "", invalidArgument("missing region")
	}
	all, err := s.store.ListWorkflowTemplates(ctx, project, region)
	if err != nil {
		return nil, "", err
	}
	page, next := paging.Page(all, func(t dpstore.WorkflowTemplate) string { return t.TemplateID }, pageParams(pageSize, pageToken))
	return page, next, nil
}

// UpdateWorkflowTemplate replaces a template's definition and bumps its
// version. Real GCP requires the supplied version to match the current server
// version (ABORTED on mismatch); the definition is re-validated.
func (s *Service) UpdateWorkflowTemplate(ctx context.Context, project, region string, in WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	if region == "" || in.ID == "" {
		return dpstore.WorkflowTemplate{}, invalidArgument("missing region or workflow template id")
	}
	if err := validateWorkflowDefinition(in.Definition); err != nil {
		return dpstore.WorkflowTemplate{}, err
	}
	def, err := json.Marshal(in.Definition)
	if err != nil {
		return dpstore.WorkflowTemplate{}, invalidArgument("malformed workflow template")
	}
	now := clock.Now().UTC()
	updated, err := s.store.UpdateWorkflowTemplateAtomic(ctx, project, region, in.ID, func(cur dpstore.WorkflowTemplate) (dpstore.WorkflowTemplate, error) {
		if in.Version == 0 || in.Version != cur.Version {
			return cur, &model.ProviderError{
				Code:       "Aborted",
				Message:    "workflow template version mismatch",
				HTTPStatus: 409,
				Status:     "ABORTED",
			}
		}
		cur.Definition = def
		cur.Version++
		cur.UpdateTime = now
		return cur, nil
	})
	if err != nil {
		return dpstore.WorkflowTemplate{}, mapErr(err)
	}
	return updated, nil
}

// DeleteWorkflowTemplate removes a workflow template. version 0 deletes it
// entirely; an explicit version must match the current one (only the latest
// version is retained).
func (s *Service) DeleteWorkflowTemplate(ctx context.Context, project, region, id string, version int32) error {
	if region == "" || id == "" {
		return invalidArgument("missing region or workflow template id")
	}
	if version != 0 {
		cur, err := s.store.GetWorkflowTemplate(ctx, project, region, id)
		if err != nil {
			return mapErr(err)
		}
		if cur.Version != version {
			return model.NewProviderError("NotFound", "workflow template version not found", 404)
		}
	}
	if err := s.store.DeleteWorkflowTemplate(ctx, project, region, id); err != nil {
		return mapErr(err)
	}
	return nil
}

// validateWorkflowDefinition enforces the structural invariants of a
// dataproc.v1.WorkflowTemplate: a placement, at least one job, unique step ids,
// exactly one job type per job, resolvable prerequisites, and an acyclic
// prerequisite graph. Job types the emulator cannot run are accepted at create
// (real GCP supports them) and fail loud at instantiate.
func validateWorkflowDefinition(def map[string]any) error {
	if def == nil {
		return invalidArgument("missing workflow template")
	}
	placement, _ := def["placement"].(map[string]any)
	if len(placement) == 0 {
		return invalidArgument("workflow template placement is required")
	}
	jobsRaw, _ := def["jobs"].([]any)
	if len(jobsRaw) == 0 {
		return invalidArgument("workflow template must define at least one job")
	}
	stepIDs := make(map[string]bool, len(jobsRaw))
	ids := make([]string, 0, len(jobsRaw))
	prereqs := make(map[string][]string, len(jobsRaw))
	for _, jr := range jobsRaw {
		j, ok := jr.(map[string]any)
		if !ok {
			return invalidArgument("workflow template job must be an object")
		}
		stepID, _ := j["stepId"].(string)
		if stepID == "" {
			return invalidArgument("workflow template job stepId is required")
		}
		if stepIDs[stepID] {
			return invalidArgument("duplicate workflow stepId " + stepID)
		}
		stepIDs[stepID] = true
		ids = append(ids, stepID)

		types := 0
		for _, k := range jobTypeKeys {
			if _, ok := j[k]; ok {
				types++
			}
		}
		if types == 0 {
			return invalidArgument("workflow job " + stepID + " has no job type")
		}
		if types > 1 {
			return invalidArgument("workflow job " + stepID + " has multiple job types")
		}

		var ps []string
		if arr, ok := j["prerequisiteStepIds"].([]any); ok {
			for _, p := range arr {
				if s, ok := p.(string); ok {
					ps = append(ps, s)
				}
			}
		}
		prereqs[stepID] = ps
	}
	for _, id := range ids {
		for _, p := range prereqs[id] {
			if !stepIDs[p] {
				return invalidArgument("workflow job " + id + " has unknown prerequisite " + p)
			}
		}
	}
	return assertAcyclic(ids, prereqs)
}

// assertAcyclic reports whether the prerequisite graph is a DAG (Kahn's
// algorithm). A cycle is InvalidArgument, matching real Dataproc's create-time
// validation.
func assertAcyclic(ids []string, prereqs map[string][]string) error {
	indegree := make(map[string]int, len(ids))
	adjacency := make(map[string][]string, len(ids))
	for _, id := range ids {
		indegree[id] = 0
	}
	for _, id := range ids {
		for _, p := range prereqs[id] {
			indegree[id]++
			adjacency[p] = append(adjacency[p], id)
		}
	}
	queue := make([]string, 0, len(ids))
	for _, id := range ids {
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}
	seen := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		seen++
		for _, m := range adjacency[n] {
			indegree[m]--
			if indegree[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	if seen != len(ids) {
		return invalidArgument("workflow template has a prerequisite cycle")
	}
	return nil
}

// WorkflowTemplateJSON renders a stored template as a dataproc.v1.WorkflowTemplate
// wire map: the stored definition plus the server-managed name/version/timestamps.
func WorkflowTemplateJSON(t dpstore.WorkflowTemplate) map[string]any {
	out := map[string]any{}
	if len(t.Definition) > 0 {
		_ = json.Unmarshal(t.Definition, &out)
	}
	out["id"] = t.TemplateID
	out["name"] = WorkflowTemplateName(t.ProjectID, t.Region, t.TemplateID)
	out["version"] = t.Version
	out["createTime"] = formatTimestamp(t.CreateTime)
	out["updateTime"] = formatTimestamp(t.UpdateTime)
	return out
}

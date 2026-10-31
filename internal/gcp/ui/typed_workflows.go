package ui

import (
	"context"
	"encoding/json"

	workflowscore "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	workflowsui "jaiscloud/internal/gcp/ui/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedWorkflows serves the Cloud Workflows console through the plugin chain when
// a plugin covers the key (see typed.go).
type typedWorkflows struct {
	workflowsui.ProviderInterface
	c typedCaller
}

func routeWorkflows(c typedCaller, p workflowsui.ProviderInterface) workflowsui.ProviderInterface {
	return typedWorkflows{ProviderInterface: p, c: c}
}

func decodeWorkflow(m map[string]any) workflowsstore.Workflow {
	parts := nameParts(strField(m, "name"))
	return workflowsstore.Workflow{
		ID:                 parts["workflows"],
		Location:           parts["locations"],
		Description:        strField(m, "description"),
		Labels:             strMapField(m, "labels"),
		ServiceAccount:     strField(m, "serviceAccount"),
		SourceContents:     strField(m, "sourceContents"),
		State:              strField(m, "state"),
		RevisionID:         strField(m, "revisionId"),
		CreateTime:         timeField(m, "createTime"),
		UpdateTime:         timeField(m, "updateTime"),
		RevisionCreateTime: timeField(m, "revisionCreateTime"),
		CallLogLevel:       strField(m, "callLogLevel"),
		UserEnvVars:        strMapField(m, "userEnvVars"),
		Tags:               strMapField(m, "tags"),
	}
}

// operationWorkflow reads the workflow a create or update operation carries.
func operationWorkflow(m map[string]any) workflowsstore.Workflow {
	if r, ok := m["response"].(map[string]any); ok {
		return decodeWorkflow(r)
	}
	return decodeWorkflow(m)
}

func decodeExecution(m map[string]any) workflowsstore.Execution {
	parts := nameParts(strField(m, "name"))
	e := workflowsstore.Execution{
		ID:                 parts["executions"],
		WorkflowID:         parts["workflows"],
		Location:           parts["locations"],
		State:              strField(m, "state"),
		Argument:           strField(m, "argument"),
		Result:             strField(m, "result"),
		StartTime:          timeField(m, "startTime"),
		EndTime:            timeField(m, "endTime"),
		Duration:           strField(m, "duration"),
		WorkflowRevisionID: strField(m, "workflowRevisionId"),
		CallLogLevel:       strField(m, "callLogLevel"),
		Labels:             strMapField(m, "labels"),
	}
	if raw, ok := m["error"]; ok {
		if b, err := json.Marshal(raw); err == nil {
			var ee workflowsstore.ExecutionError
			if json.Unmarshal(b, &ee) == nil {
				e.Error = &ee
			}
		}
	}
	if st, ok := m["status"].(map[string]any); ok {
		for _, s := range items(st, "currentSteps") {
			e.CurrentSteps = append(e.CurrentSteps, workflowsstore.Step{Routine: strField(s, "routine"), Step: strField(s, "step")})
		}
	}
	return e
}

func (p typedWorkflows) wfName(project, location, id string) string {
	return workflowscore.WorkflowName(project, location, id)
}

func (p typedWorkflows) ListWorkflowsByProject(ctx context.Context, project string) ([]workflowsstore.Workflow, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		ws, err := p.ProviderInterface.ListWorkflowsByProject(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(ws))
		for _, w := range ws {
			out = append(out, workflowscore.WorkflowJSON(w, project))
		}
		return provider.OK(map[string]any{"workflows": out}), nil
	}
	data, covered, err := p.c.do(ctx, "Workflow.ListWorkflows", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListWorkflowsByProject(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []workflowsstore.Workflow
	for _, m := range items(data, "workflows") {
		out = append(out, decodeWorkflow(m))
	}
	return out, nil
}

func (p typedWorkflows) GetWorkflow(ctx context.Context, project, location, id string) (workflowsstore.Workflow, error) {
	data, covered, err := p.c.do(ctx, "Workflow.GetWorkflow", project, location,
		map[string]any{"project": project, "location": location, "name": p.wfName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.GetWorkflow(ctx, project, location, id)
	}
	if err != nil {
		return workflowsstore.Workflow{}, err
	}
	return decodeWorkflow(data), nil
}

func (p typedWorkflows) CreateWorkflow(ctx context.Context, project, location string, in workflowsui.WorkflowInput) (workflowsstore.Workflow, error) {
	body := map[string]any{"description": in.Description, "serviceAccount": in.ServiceAccount, "sourceContents": in.SourceContents, "callLogLevel": in.CallLogLevel}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if in.UserEnvVars != nil {
		body["userEnvVars"] = in.UserEnvVars
	}
	data, covered, err := p.c.do(ctx, "Workflow.CreateWorkflow", project, location,
		map[string]any{"project": project, "location": location, "workflowId": in.ID, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.CreateWorkflow(ctx, project, location, in)
	}
	if err != nil {
		return workflowsstore.Workflow{}, err
	}
	return operationWorkflow(data), nil
}

func (p typedWorkflows) UpdateWorkflow(ctx context.Context, project, location, id string, in workflowsui.WorkflowUpdateInput) (workflowsstore.Workflow, error) {
	mask := in.UpdateMask
	if mask == "" {
		mask = workflowsui.WorkflowManagedMask
	}
	body := map[string]any{"description": in.Description, "serviceAccount": in.ServiceAccount, "sourceContents": in.SourceContents, "callLogLevel": in.CallLogLevel}
	if in.Labels != nil {
		body["labels"] = in.Labels
	}
	if in.UserEnvVars != nil {
		body["userEnvVars"] = in.UserEnvVars
	}
	data, covered, err := p.c.do(ctx, "Workflow.UpdateWorkflow", project, location,
		map[string]any{"project": project, "location": location, "name": p.wfName(project, location, id), "updateMask": mask, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.UpdateWorkflow(ctx, project, location, id, in)
	}
	if err != nil {
		return workflowsstore.Workflow{}, err
	}
	return operationWorkflow(data), nil
}

func (p typedWorkflows) DeleteWorkflow(ctx context.Context, project, location, id string) error {
	_, covered, err := p.c.do(ctx, "Workflow.DeleteWorkflow", project, location,
		map[string]any{"project": project, "location": location, "name": p.wfName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.DeleteWorkflow(ctx, project, location, id)
	}
	return err
}

func (p typedWorkflows) ListExecutions(ctx context.Context, project, location, workflowID string, pageSize int, pageToken string) ([]workflowsstore.Execution, string, error) {
	params := map[string]any{"project": project, "location": location, "workflowId": workflowID, "view": "FULL"}
	if pageSize > 0 {
		params["pageSize"] = float64(pageSize)
	}
	if pageToken != "" {
		params["pageToken"] = pageToken
	}
	data, covered, err := p.c.do(ctx, "WorkflowExecution.ListExecutions", project, location, params, nil)
	if !covered {
		return p.ProviderInterface.ListExecutions(ctx, project, location, workflowID, pageSize, pageToken)
	}
	if err != nil {
		return nil, "", err
	}
	var out []workflowsstore.Execution
	for _, m := range items(data, "executions") {
		out = append(out, decodeExecution(m))
	}
	next, _ := data["nextPageToken"].(string)
	return out, next, nil
}

func (p typedWorkflows) execution(ctx context.Context, key, project, location, workflowID, executionID string, direct func() (workflowsstore.Execution, error)) (workflowsstore.Execution, error) {
	data, covered, err := p.c.do(ctx, key, project, location,
		map[string]any{"project": project, "location": location, "workflowId": workflowID, "executionId": executionID, "view": "FULL"}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return workflowsstore.Execution{}, err
	}
	return decodeExecution(data), nil
}

func (p typedWorkflows) GetExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	return p.execution(ctx, "WorkflowExecution.GetExecution", project, location, workflowID, executionID, func() (workflowsstore.Execution, error) {
		return p.ProviderInterface.GetExecution(ctx, project, location, workflowID, executionID)
	})
}

func (p typedWorkflows) CancelExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	return p.execution(ctx, "WorkflowExecution.CancelExecution", project, location, workflowID, executionID, func() (workflowsstore.Execution, error) {
		return p.ProviderInterface.CancelExecution(ctx, project, location, workflowID, executionID)
	})
}

func (p typedWorkflows) RunWorkflow(ctx context.Context, project, location, workflowID, argument, callLogLevel string, labels map[string]string) (workflowsstore.Execution, error) {
	body := map[string]any{"argument": argument, "callLogLevel": callLogLevel}
	if labels != nil {
		body["labels"] = labels
	}
	data, covered, err := p.c.do(ctx, "WorkflowExecution.CreateExecution", project, location,
		map[string]any{"project": project, "location": location, "workflowId": workflowID, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.RunWorkflow(ctx, project, location, workflowID, argument, callLogLevel, labels)
	}
	if err != nil {
		return workflowsstore.Execution{}, err
	}
	return decodeExecution(data), nil
}

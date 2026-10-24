package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
)

// Dataproc workflow node states (dataproc.v1.WorkflowNode.NodeState). The
// emulator reports BLOCKED (prerequisites unmet), RUNNING (job submitted) and
// the COMPLETED/FAILED terminals; RUNNABLE is transient and not persisted
// because a ready node is submitted in the same read.
const (
	workflowNodeBlocked   = "BLOCKED"
	workflowNodeRunning   = "RUNNING"
	workflowNodeCompleted = "COMPLETED"
	workflowNodeFailed    = "FAILED"
)

// WorkflowMetadata states (dataproc.v1.WorkflowMetadata.State): the emulator
// reports RUNNING until every node is terminal, then DONE. Per-node success or
// failure is carried by the graph, exactly as in real Dataproc.
const (
	workflowStateRunning = "RUNNING"
	workflowStateDone    = "DONE"
)

// workflowStep is one post-substitution OrderedJob, retained so the executor can
// (re)submit a ready node.
type workflowStep struct {
	StepID string         `json:"stepId"`
	Job    map[string]any `json:"job"`
}

// workflowNode is the per-step execution state.
type workflowNode struct {
	StepID  string   `json:"stepId"`
	Prereqs []string `json:"prerequisiteStepIds,omitempty"`
	JobID   string   `json:"jobId,omitempty"`
	State   string   `json:"state"`
	Error   string   `json:"error,omitempty"`
}

// workflowExec is the internal DAG execution state, persisted in the workflow
// operation's Target (an internal-only field never rendered on the wire).
type workflowExec struct {
	TemplateName string            `json:"templateName,omitempty"`
	Version      int32             `json:"version,omitempty"`
	ClusterName  string            `json:"clusterName"`
	OwnedCluster bool              `json:"ownedCluster,omitempty"`
	Parameters   map[string]string `json:"parameters,omitempty"`
	Steps        []workflowStep    `json:"steps"`
	Nodes        []workflowNode    `json:"nodes"`
}

// InstantiateWorkflowTemplate starts a workflow from a stored template. The
// returned operation is not done; polling it (GetOperation) advances the DAG.
func (s *Service) InstantiateWorkflowTemplate(ctx context.Context, project, region, templateID string, version int32, parameters map[string]string) (dpstore.Operation, error) {
	if region == "" || templateID == "" {
		return dpstore.Operation{}, invalidArgument("missing region or workflow template id")
	}
	t, err := s.GetWorkflowTemplate(ctx, project, region, templateID, version)
	if err != nil {
		return dpstore.Operation{}, err
	}
	def := map[string]any{}
	if err := json.Unmarshal(t.Definition, &def); err != nil {
		return dpstore.Operation{}, model.NewProviderError("Internal", "stored workflow template is malformed", 500)
	}
	return s.instantiateWorkflow(ctx, project, region, def, WorkflowTemplateName(project, region, templateID), t.Version, parameters)
}

// InstantiateInlineWorkflowTemplate starts a workflow from an inline template
// (equivalent to create + instantiate + delete in real GCP).
func (s *Service) InstantiateInlineWorkflowTemplate(ctx context.Context, project, region string, definition map[string]any) (dpstore.Operation, error) {
	return s.instantiateWorkflow(ctx, project, region, definition, "", 0, nil)
}

// instantiateWorkflow validates the template, resolves its placement to a
// cluster, substitutes parameters, and persists an in-flight workflow operation.
func (s *Service) instantiateWorkflow(ctx context.Context, project, region string, def map[string]any, templateName string, version int32, params map[string]string) (dpstore.Operation, error) {
	if region == "" {
		return dpstore.Operation{}, invalidArgument("missing region")
	}
	if err := validateWorkflowDefinition(def); err != nil {
		return dpstore.Operation{}, err
	}
	placement, _ := def["placement"].(map[string]any)
	cluster, owned, err := s.resolveWorkflowCluster(ctx, project, region, placement)
	if err != nil {
		return dpstore.Operation{}, err
	}

	jobsRaw, _ := def["jobs"].([]any)
	steps := make([]workflowStep, 0, len(jobsRaw))
	nodes := make([]workflowNode, 0, len(jobsRaw))
	for _, jr := range jobsRaw {
		job, _ := jr.(map[string]any)
		if len(params) > 0 {
			job = substituteParams(job, params).(map[string]any)
		}
		stepID, _ := job["stepId"].(string)
		steps = append(steps, workflowStep{StepID: stepID, Job: job})
		nodes = append(nodes, workflowNode{StepID: stepID, Prereqs: stringSlice(job["prerequisiteStepIds"]), State: workflowNodeBlocked})
	}

	exec := workflowExec{
		TemplateName: templateName,
		Version:      version,
		ClusterName:  cluster,
		OwnedCluster: owned,
		Parameters:   params,
		Steps:        steps,
		Nodes:        nodes,
	}
	execJSON, err := json.Marshal(exec)
	if err != nil {
		return dpstore.Operation{}, model.NewProviderError("Internal", "failed to persist workflow", 500)
	}
	now := clock.Now().UTC()
	op := dpstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Region:     region,
		Done:       false,
		Verb:       "workflow",
		Target:     string(execJSON),
		CreateTime: now,
	}
	if meta, err := json.Marshal(renderWorkflowMetadata(exec, workflowStateRunning, now, time.Time{})); err == nil {
		op.Metadata = string(meta)
	}
	s.sweepOperations(ctx)
	if err := s.store.CreateOperation(ctx, project, region, op); err != nil {
		return dpstore.Operation{}, err
	}
	return op, nil
}

// advanceWorkflowOperation is the read-time half of the workflow LRO: it submits
// ready nodes through the job core, settles running nodes, and closes the
// operation when every node is terminal.
func (s *Service) advanceWorkflowOperation(ctx context.Context, op dpstore.Operation) (dpstore.Operation, error) {
	var exec workflowExec
	if op.Target != "" {
		if err := json.Unmarshal([]byte(op.Target), &exec); err != nil {
			return op, nil
		}
	}

	// Submit ready nodes (all prerequisites completed).
	for i := range exec.Nodes {
		n := &exec.Nodes[i]
		if n.State != workflowNodeBlocked || !prereqsCompleted(exec.Nodes, n.Prereqs) {
			continue
		}
		jobID := randomHex(16)
		if _, err := s.SubmitJob(ctx, op.ProjectID, op.Region, workflowJobInput(exec.Steps[i].Job, jobID, exec.ClusterName)); err != nil {
			n.State = workflowNodeFailed
			n.Error = err.Error()
			continue
		}
		n.JobID = jobID
		n.State = workflowNodeRunning
	}

	// Advance running nodes one hop each.
	anyFailed := false
	for i := range exec.Nodes {
		n := &exec.Nodes[i]
		if n.State != workflowNodeRunning {
			continue
		}
		j, err := s.advanceJob(ctx, op.ProjectID, op.Region, n.JobID)
		if err != nil {
			n.State = workflowNodeFailed
			n.Error = err.Error()
			anyFailed = true
			continue
		}
		switch j.Status.State {
		case jobStateDone:
			n.State = workflowNodeCompleted
		case jobStateError, jobStateCancelled:
			n.State = workflowNodeFailed
			n.Error = j.Status.Details
			anyFailed = true
		}
	}
	// A failed node leaves its dependents blocked forever; fail them so the DAG
	// terminates (real GCP skips them).
	if anyFailed {
		for i := range exec.Nodes {
			if exec.Nodes[i].State == workflowNodeBlocked {
				exec.Nodes[i].State = workflowNodeFailed
				exec.Nodes[i].Error = "upstream step failed"
			}
		}
	}

	allTerminal := true
	for _, n := range exec.Nodes {
		if n.State == workflowNodeBlocked || n.State == workflowNodeRunning {
			allTerminal = false
			break
		}
	}

	now := clock.Now().UTC()
	state := workflowStateRunning
	if allTerminal {
		state = workflowStateDone
	}
	execJSON, _ := json.Marshal(exec)
	metaJSON, _ := json.Marshal(renderWorkflowMetadata(exec, state, op.CreateTime, now))

	updated, err := s.store.UpdateOperationAtomic(ctx, op.ProjectID, op.Region, op.ID, func(cur dpstore.Operation) (dpstore.Operation, error) {
		if cur.Done {
			return cur, nil
		}
		cur.Target = string(execJSON)
		cur.Metadata = string(metaJSON)
		if allTerminal {
			cur.Done = true
			cur.EndTime = now
			cur.Response = "{}"
		}
		return cur, nil
	})
	if err != nil {
		return op, err
	}
	if allTerminal && exec.OwnedCluster {
		// A workflow-owned managed cluster is deleted once the workflow ends.
		if derr := s.store.DeleteCluster(ctx, op.ProjectID, op.Region, exec.ClusterName); derr != nil && !errors.Is(derr, dpstore.ErrNoSuchCluster) {
			slog.Warn("dataproc: workflow-owned cluster delete failed", "cluster", exec.ClusterName, "err", derr)
		}
	}
	return updated, nil
}

// resolveWorkflowCluster resolves a placement to a concrete cluster. A
// managedCluster is created (RUNNING, workflow-owned) if absent; a
// clusterSelector matches an existing cluster's labels.
func (s *Service) resolveWorkflowCluster(ctx context.Context, project, region string, placement map[string]any) (string, bool, error) {
	if mc, ok := placement["managedCluster"].(map[string]any); ok {
		name := bodyString(mc, "clusterName")
		if name == "" {
			return "", false, invalidArgument("managedCluster.clusterName is required")
		}
		if _, err := s.store.GetCluster(ctx, project, region, name); err == nil {
			return name, false, nil
		} else if !errors.Is(err, dpstore.ErrNoSuchCluster) {
			return "", false, err
		}
		now := clock.Now().UTC()
		ns, nsOwned := s.resolveClusterNamespace(ctx, project, region, name, ClusterInput{})
		c := dpstore.Cluster{
			ProjectID:      project,
			Region:         region,
			Name:           name,
			Labels:         stringMap(mc["labels"]),
			Status:         dpstore.ClusterStatus{State: "RUNNING", StateStartTime: now},
			ClusterUUID:    randomHex(16),
			CreateTime:     now,
			UpdateTime:     now,
			Namespace:      ns,
			NamespaceOwned: nsOwned,
		}
		if err := s.store.CreateCluster(ctx, project, region, c); err != nil && !errors.Is(err, dpstore.ErrAlreadyExists) {
			s.rollbackNamespace(ctx, ns, nsOwned)
			return "", false, err
		} else if err == nil {
			s.registerNamespacePatcher(ns)
		}
		return name, true, nil
	}
	if cs, ok := placement["clusterSelector"].(map[string]any); ok {
		want := stringMap(cs["clusterLabels"])
		clusters, err := s.store.ListClusters(ctx, project, region)
		if err != nil {
			return "", false, err
		}
		for _, c := range clusters {
			if labelsSubset(c.Labels, want) {
				return c.Name, false, nil
			}
		}
		return "", false, model.NewProviderError("FailedPrecondition", "no cluster matches the workflow placement", 400)
	}
	return "", false, invalidArgument("workflow placement must set managedCluster or clusterSelector")
}

// workflowJobInput builds the job core's JobInput from a workflow step and the
// resolved cluster.
func workflowJobInput(stepJob map[string]any, jobID, cluster string) JobInput {
	body := map[string]any{
		"reference": map[string]any{"jobId": jobID},
		"placement": map[string]any{"clusterName": cluster},
	}
	for _, k := range jobTypeKeys {
		if v, ok := stepJob[k]; ok {
			body[k] = v
			break
		}
	}
	if labels, ok := stepJob["labels"].(map[string]any); ok {
		body["labels"] = labels
	}
	return JobInputFromMap(body)
}

// renderWorkflowMetadata renders the dataproc.v1.WorkflowMetadata wire object.
func renderWorkflowMetadata(exec workflowExec, state string, start, end time.Time) map[string]any {
	nodes := make([]any, 0, len(exec.Nodes))
	for _, n := range exec.Nodes {
		m := map[string]any{"stepId": n.StepID, "state": n.State}
		if len(n.Prereqs) > 0 {
			m["prerequisiteStepIds"] = n.Prereqs
		}
		if n.JobID != "" {
			m["jobId"] = n.JobID
		}
		if n.Error != "" {
			m["error"] = n.Error
		}
		nodes = append(nodes, m)
	}
	out := map[string]any{
		"@type":       "type.googleapis.com/google.cloud.dataproc.v1.WorkflowMetadata",
		"graph":       map[string]any{"nodes": nodes},
		"state":       state,
		"clusterName": exec.ClusterName,
		"startTime":   formatTimestamp(start),
	}
	if exec.TemplateName != "" {
		out["template"] = exec.TemplateName
	}
	if exec.Version != 0 {
		out["version"] = exec.Version
	}
	if len(exec.Parameters) > 0 {
		out["parameters"] = exec.Parameters
	}
	if !end.IsZero() {
		out["endTime"] = formatTimestamp(end)
	}
	return out
}

// prereqsCompleted reports whether every prerequisite node has completed.
func prereqsCompleted(nodes []workflowNode, prereqs []string) bool {
	for _, p := range prereqs {
		done := false
		for _, n := range nodes {
			if n.StepID == p && n.State == workflowNodeCompleted {
				done = true
				break
			}
		}
		if !done {
			return false
		}
	}
	return true
}

// substituteParams recursively replaces {name} placeholders with the supplied
// workflow parameter values.
func substituteParams(v any, params map[string]string) any {
	switch t := v.(type) {
	case string:
		out := t
		for k, val := range params {
			out = strings.ReplaceAll(out, "{"+k+"}", val)
		}
		return out
	case map[string]any:
		for k, vv := range t {
			t[k] = substituteParams(vv, params)
		}
		return t
	case []any:
		for i, vv := range t {
			t[i] = substituteParams(vv, params)
		}
		return t
	}
	return v
}

// stringSlice coerces a decoded JSON array of strings.
func stringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// stringMap coerces a decoded JSON object of strings.
func stringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, e := range m {
		if s, ok := e.(string); ok {
			out[k] = s
		}
	}
	return out
}

// labelsSubset reports whether every wanted label is present (and equal) in have.
func labelsSubset(have map[string]string, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

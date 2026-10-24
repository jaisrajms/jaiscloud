// Package workflowsui serves the Cloud Workflows UI API. It spans the Workflows
// management service and the Workflow Executions service, which share the
// workflows store, and calls their transport-neutral cores directly
// (in-process) rather than over the wire.
//
// The console is location-optional: the workflow list aggregates every workflow
// in the project across all locations (no location picker) and shows the
// location as a read-only field. Detail/actions link back with the location
// carried from the list row, so a workflow id never has to be disambiguated by
// hand. Executions are nested under their workflow, matching the store's
// per-workflow execution listing.
package workflowsui

// Workflow is the console rendering of a Cloud Workflows workflow.
type Workflow struct {
	ID             string            `json:"id"`
	Location       string            `json:"location"`
	Name           string            `json:"name"`
	Description    string            `json:"description,omitempty"`
	State          string            `json:"state,omitempty"`
	RevisionID     string            `json:"revisionId,omitempty"`
	ServiceAccount string            `json:"serviceAccount,omitempty"`
	SourceContents string            `json:"sourceContents,omitempty"`
	CallLogLevel   string            `json:"callLogLevel,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	UserEnvVars    map[string]string `json:"userEnvVars,omitempty"`
	CreateTime     string            `json:"createTime,omitempty"`
	UpdateTime     string            `json:"updateTime,omitempty"`
}

// ListWorkflowsResponse is the response for GET /workflows.
type ListWorkflowsResponse struct {
	Workflows []Workflow `json:"workflows"`
	Total     int        `json:"total"`
}

// WorkflowInput is the create form body. Location and id are required.
type WorkflowInput struct {
	ID             string            `json:"id"`
	Location       string            `json:"location"`
	Description    string            `json:"description"`
	ServiceAccount string            `json:"serviceAccount"`
	SourceContents string            `json:"sourceContents"`
	CallLogLevel   string            `json:"callLogLevel"`
	Labels         map[string]string `json:"labels"`
	UserEnvVars    map[string]string `json:"userEnvVars"`
}

// WorkflowUpdateInput is the update form body. The path carries the identity.
// UpdateMask is optional: when empty the provider applies the fields the form
// manages (description, sourceContents, serviceAccount, callLogLevel, labels,
// userEnvVars), preserving the rest.
type WorkflowUpdateInput struct {
	Description    string            `json:"description"`
	ServiceAccount string            `json:"serviceAccount"`
	SourceContents string            `json:"sourceContents"`
	CallLogLevel   string            `json:"callLogLevel"`
	Labels         map[string]string `json:"labels"`
	UserEnvVars    map[string]string `json:"userEnvVars"`
	UpdateMask     string            `json:"updateMask"`
}

// Step is one routine/step pair in an execution's current steps.
type Step struct {
	Routine string `json:"routine,omitempty"`
	Step    string `json:"step,omitempty"`
}

// ExecutionError describes why an execution terminated.
type ExecutionError struct {
	Payload string `json:"payload"`
	Context string `json:"context,omitempty"`
}

// Execution is the console rendering of a workflow execution.
type Execution struct {
	ID                 string            `json:"id"`
	WorkflowID         string            `json:"workflowId"`
	Location           string            `json:"location"`
	Name               string            `json:"name"`
	State              string            `json:"state,omitempty"`
	Argument           string            `json:"argument,omitempty"`
	Result             string            `json:"result,omitempty"`
	Error              *ExecutionError   `json:"error,omitempty"`
	StartTime          string            `json:"startTime,omitempty"`
	EndTime            string            `json:"endTime,omitempty"`
	Duration           string            `json:"duration,omitempty"`
	WorkflowRevisionID string            `json:"workflowRevisionId,omitempty"`
	CallLogLevel       string            `json:"callLogLevel,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`
	CurrentSteps       []Step            `json:"currentSteps,omitempty"`
}

// ListExecutionsResponse is the response for
// GET /workflows/{location}/{workflow}/executions.
type ListExecutionsResponse struct {
	Executions    []Execution `json:"executions"`
	Total         int         `json:"total"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// RunExecutionInput is the run-execution form body.
type RunExecutionInput struct {
	Argument     string            `json:"argument"`
	CallLogLevel string            `json:"callLogLevel"`
	Labels       map[string]string `json:"labels"`
}

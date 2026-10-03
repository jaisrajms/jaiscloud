// Package workflows provides the Cloud Workflows store (management workflows,
// their executions, and the long-running operations returned by create/update/
// delete). Workflows are project+location scoped; the canonical resource name is
// projects/{project}/locations/{location}/workflows/{workflow}, and an execution
// is nested under its workflow:
// projects/{project}/locations/{location}/workflows/{workflow}/executions/{id}.
package workflows

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchWorkflow  = errors.New("NoSuchWorkflow")
	ErrAlreadyExists   = errors.New("AlreadyExists")
	ErrNoSuchExecution = errors.New("NoSuchExecution")
	ErrNoSuchOperation = errors.New("NoSuchOperation")
)

// Workflow is the deployed workflow metadata (mirrors workflows.v1.Workflow).
type Workflow struct {
	ID             string            // workflow ID (last segment of name)
	Location       string            // region
	Description    string            // user-provided description
	Labels         map[string]string // user labels
	ServiceAccount string            // runtime identity
	SourceContents string            // YAML source, stored verbatim
	State          string            // "ACTIVE"
	RevisionID     string            // output-only revision (e.g. "000001-a4d")
	CreateTime     time.Time
	UpdateTime     time.Time
	CallLogLevel   string            // CALL_LOG_LEVEL_UNSPECIFIED / LOG_*_CALLS / LOG_NONE
	UserEnvVars    map[string]string // user-defined environment variables (workflow revision)
	Tags           map[string]string // immutable input-only tags (echoed verbatim)
}

// Position is a source-code position within a stack trace element.
type Position struct {
	Line   int64 `json:"line,omitempty"`
	Column int64 `json:"column,omitempty"`
	Length int64 `json:"length,omitempty"`
}

// StackTraceElement is one frame of an execution error stack trace.
type StackTraceElement struct {
	Step     string    `json:"step,omitempty"`
	Routine  string    `json:"routine,omitempty"`
	Position *Position `json:"position,omitempty"`
}

// StackTrace is the detailed error location (mirrors executions.v1.StackTrace).
type StackTrace struct {
	Elements []StackTraceElement `json:"elements,omitempty"`
}

// ExecutionError describes why an execution terminated (executions.v1.Error).
type ExecutionError struct {
	Payload    string      `json:"payload"`
	Context    string      `json:"context,omitempty"`
	StackTrace *StackTrace `json:"stackTrace,omitempty"`
}

// Step is a single routine/step pair in an execution status.
type Step struct {
	Routine string `json:"routine,omitempty"`
	Step    string `json:"step,omitempty"`
}

// Execution is one running/finished instance of a workflow.
type Execution struct {
	ID                 string          // execution ID (last segment of name)
	WorkflowID         string          // owning workflow ID
	Location           string          // region
	State              string          // ACTIVE/SUCCEEDED/FAILED/CANCELLED
	Argument           string          // JSON input, stored verbatim
	Result             string          // JSON output, stored verbatim (SUCCEEDED only)
	Error              *ExecutionError // nil unless FAILED/CANCELLED
	StartTime          time.Time
	EndTime            time.Time
	Duration           string            // proto Duration JSON (e.g. "0.5s")
	WorkflowRevisionID string            // revision of the workflow that ran
	CallLogLevel       string            // execution-level call log level
	Labels             map[string]string // execution labels
	CurrentSteps       []Step            // status.currentSteps (last attempted step)
}

// Operation is a long-running operation (done=true) for create/update/delete.
type Operation struct {
	ID         string // operation ID (last segment of name)
	ProjectID  string // owning project
	Location   string // region
	Done       bool   // always true in the emulator
	Response   string // JSON-encoded response body, stored verbatim
	Verb       string // "create"/"update"/"delete"
	Target     string // full workflow resource name
	CreateTime time.Time
	EndTime    time.Time
}

// Store is the Cloud Workflows store.
type Store interface {
	CreateWorkflow(ctx context.Context, projectID, location, id string, w Workflow) error
	GetWorkflow(ctx context.Context, projectID, location, id string) (Workflow, error)
	UpdateWorkflow(ctx context.Context, projectID, location, id string, w Workflow) error
	// UpdateWorkflowAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current workflow and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetWorkflow followed
	// by UpdateWorkflow, this is atomic with respect to concurrent updates on
	// the same workflow, so a masked PATCH that merges only a subset of
	// fields can't lose a concurrent PATCH's changes to other fields.
	UpdateWorkflowAtomic(ctx context.Context, projectID, location, id string, mutate func(Workflow) (Workflow, error)) (Workflow, error)
	DeleteWorkflow(ctx context.Context, projectID, location, id string) error
	ListWorkflows(ctx context.Context, projectID, location string) ([]Workflow, error)

	CreateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error
	GetExecution(ctx context.Context, projectID, location, workflowID, id string) (Execution, error)
	UpdateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error
	ListExecutions(ctx context.Context, projectID, location, workflowID string) ([]Execution, error)

	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, id string) (Operation, error)

	Reset(ctx context.Context)
}

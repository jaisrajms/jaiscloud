// Package workflows provides the Cloud Workflows store (management workflows,
// their executions, and the long-running operations returned by create/update/
// delete). Workflows are project+location scoped; the canonical resource name is
// projects/{project}/locations/{location}/workflows/{workflow}, and an execution
// is nested under its workflow:
// projects/{project}/locations/{location}/workflows/{workflow}/executions/{id}.
// Every workflow also keeps an ordered revision history (ListWorkflows'
// ListWorkflowRevisions), recorded on create and on each revision-changing
// update.
package workflows

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchWorkflow  = errors.New("NoSuchWorkflow")
	ErrNoSuchRevision  = errors.New("NoSuchRevision")
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
	// RevisionCreateTime is when the current revision was created. Unlike
	// UpdateTime it does not advance on a workflow-wide update (description,
	// labels) that does not mint a revision. Zero on legacy records; readers
	// fall back to UpdateTime.
	RevisionCreateTime time.Time
	CallLogLevel       string            // CALL_LOG_LEVEL_UNSPECIFIED / LOG_*_CALLS / LOG_NONE
	UserEnvVars        map[string]string // user-defined environment variables (workflow revision)
	Tags               map[string]string // immutable input-only tags (echoed verbatim)
}

// Revision is an immutable snapshot of a workflow's revision-scoped fields at
// the moment that revision was created. A workflow has one revision on create
// and a new one whenever sourceContents or serviceAccount changes (mirroring
// the proto's revision_id contract). Workflow-wide fields (name, description,
// labels, create/update time) are not tied to a revision and are reported from
// the live workflow when a revision is rendered.
type Revision struct {
	RevisionID         string            // output-only revision (e.g. "000001-a4d")
	RevisionCreateTime time.Time         // when this revision was created
	State              string            // deployment state at this revision
	SourceContents     string            // workflow YAML, stored verbatim
	ServiceAccount     string            // runtime identity
	CallLogLevel       string            // CALL_LOG_LEVEL_UNSPECIFIED / LOG_*_CALLS / LOG_NONE
	UserEnvVars        map[string]string // user-defined environment variables
}

// revisionOf builds the immutable snapshot recorded for a workflow version.
// The revision's creation time is the version's update time (create time on
// first write, the bump time on a revision-changing update).
func revisionOf(w Workflow) Revision {
	return Revision{
		RevisionID:         w.RevisionID,
		RevisionCreateTime: revisionCreateTime(w),
		State:              w.State,
		SourceContents:     w.SourceContents,
		ServiceAccount:     w.ServiceAccount,
		CallLogLevel:       w.CallLogLevel,
		UserEnvVars:        copyStringMap(w.UserEnvVars),
	}
}

// revisionCreateTime returns the workflow's stored revision creation time,
// falling back to UpdateTime for a legacy record that predates the field.
func revisionCreateTime(w Workflow) time.Time {
	if !w.RevisionCreateTime.IsZero() {
		return w.RevisionCreateTime
	}
	return w.UpdateTime
}

// copyStringMap returns a shallow copy so a recorded revision cannot be mutated
// through the live workflow's map.
func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
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
	// ListRevisions returns a workflow's revision history, newest first. The
	// store records a revision on create and on every revision-changing update
	// (a sourceContents or serviceAccount change), so callers never append one
	// explicitly. An unknown workflow yields an empty list.
	ListRevisions(ctx context.Context, projectID, location, workflowID string) ([]Revision, error)
	// GetRevision returns one revision of a workflow, or ErrNoSuchRevision when
	// that revision (or the workflow) does not exist.
	GetRevision(ctx context.Context, projectID, location, workflowID, revisionID string) (Revision, error)
	ListWorkflows(ctx context.Context, projectID, location string) ([]Workflow, error)
	// ListWorkflowsByProject returns every workflow in a project across all
	// locations, sorted by location then workflow ID. It backs the
	// location-optional console list, which aggregates locations and shows the
	// location as a read-only row field.
	ListWorkflowsByProject(ctx context.Context, projectID string) ([]Workflow, error)

	CreateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error
	GetExecution(ctx context.Context, projectID, location, workflowID, id string) (Execution, error)
	UpdateExecution(ctx context.Context, projectID, location, workflowID, id string, e Execution) error
	ListExecutions(ctx context.Context, projectID, location, workflowID string) ([]Execution, error)

	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, id string) (Operation, error)
	// ListOperations returns a location's persisted operations, ordered by ID
	// (the deterministic cursor order ListOperations pages over).
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)
	// DeleteOperation removes an operation, or ErrNoSuchOperation when the id
	// is unknown.
	DeleteOperation(ctx context.Context, projectID, location, id string) error

	Reset(ctx context.Context)
}

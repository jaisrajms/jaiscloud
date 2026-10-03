package workflowexecutions

import (
	"time"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	core "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// viewFromProto maps the proto ExecutionView to the core view, falling back to
// def for the unset/unspecified value (FULL for get, BASIC for list, matching
// the API contract).
func viewFromProto(v executionspb.ExecutionView, def core.View) core.View {
	switch v {
	case executionspb.ExecutionView_BASIC:
		return core.ViewBasic
	case executionspb.ExecutionView_FULL:
		return core.ViewFull
	default:
		return def
	}
}

// callLogLevelName renders a proto call-log-level enum as the stored enum name,
// or "" for the unset value (so it is omitted, matching protojson).
func callLogLevelName(v executionspb.Execution_CallLogLevel) string {
	if v == executionspb.Execution_CALL_LOG_LEVEL_UNSPECIFIED {
		return ""
	}
	return v.String()
}

// callLogLevelProto resolves a stored enum name back to the proto enum, or
// UNSPECIFIED for an unknown/empty name.
func callLogLevelProto(name string) executionspb.Execution_CallLogLevel {
	if n, ok := executionspb.Execution_CallLogLevel_value[name]; ok {
		return executionspb.Execution_CallLogLevel(n)
	}
	return executionspb.Execution_CALL_LOG_LEVEL_UNSPECIFIED
}

// executionToProto renders a stored execution as the proto Execution.
func executionToProto(e workflowsstore.Execution, project string) *executionspb.Execution {
	out := &executionspb.Execution{
		Name:               core.ExecutionName(project, e.Location, e.WorkflowID, e.ID),
		State:              executionspb.Execution_State(executionspb.Execution_State_value[e.State]),
		Argument:           e.Argument,
		Result:             e.Result,
		WorkflowRevisionId: e.WorkflowRevisionID,
		CallLogLevel:       callLogLevelProto(e.CallLogLevel),
		Labels:             e.Labels,
	}
	if !e.StartTime.IsZero() {
		out.StartTime = timestamppb.New(e.StartTime)
	}
	if !e.EndTime.IsZero() {
		out.EndTime = timestamppb.New(e.EndTime)
	}
	if e.Duration != "" {
		if d, err := time.ParseDuration(e.Duration); err == nil {
			out.Duration = durationpb.New(d)
		}
	}
	if e.Error != nil {
		out.Error = errorToProto(e.Error)
	}
	if len(e.CurrentSteps) > 0 {
		steps := make([]*executionspb.Execution_Status_Step, 0, len(e.CurrentSteps))
		for _, s := range e.CurrentSteps {
			steps = append(steps, &executionspb.Execution_Status_Step{Routine: s.Routine, Step: s.Step})
		}
		out.Status = &executionspb.Execution_Status{CurrentSteps: steps}
	}
	return out
}

// errorToProto converts a stored execution error (including an optional stack
// trace) to the proto Execution.Error.
func errorToProto(err *workflowsstore.ExecutionError) *executionspb.Execution_Error {
	out := &executionspb.Execution_Error{Payload: err.Payload, Context: err.Context}
	if err.StackTrace != nil {
		elements := make([]*executionspb.Execution_StackTraceElement, 0, len(err.StackTrace.Elements))
		for _, el := range err.StackTrace.Elements {
			pe := &executionspb.Execution_StackTraceElement{Step: el.Step, Routine: el.Routine}
			if el.Position != nil {
				pe.Position = &executionspb.Execution_StackTraceElement_Position{
					Line:   el.Position.Line,
					Column: el.Position.Column,
					Length: el.Position.Length,
				}
			}
			elements = append(elements, pe)
		}
		out.StackTrace = &executionspb.Execution_StackTrace{Elements: elements}
	}
	return out
}

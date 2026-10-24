package workflows

import (
	"strings"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	core "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/gcp/timestamp"
)

// stateToProto maps a stored workflow state string to the proto enum.
func stateToProto(s string) workflowspb.Workflow_State {
	switch s {
	case "ACTIVE":
		return workflowspb.Workflow_ACTIVE
	case "UNAVAILABLE":
		return workflowspb.Workflow_UNAVAILABLE
	default:
		return workflowspb.Workflow_STATE_UNSPECIFIED
	}
}

// callLogLevelName maps the proto enum to the string the shared store persists
// (matching the REST wire enum name).
func callLogLevelName(l workflowspb.Workflow_CallLogLevel) string {
	switch l {
	case workflowspb.Workflow_LOG_ALL_CALLS:
		return "LOG_ALL_CALLS"
	case workflowspb.Workflow_LOG_ERRORS_ONLY:
		return "LOG_ERRORS_ONLY"
	case workflowspb.Workflow_LOG_NONE:
		return "LOG_NONE"
	default:
		return ""
	}
}

// callLogLevelToProto maps the stored string back to the proto enum.
func callLogLevelToProto(s string) workflowspb.Workflow_CallLogLevel {
	switch s {
	case "LOG_ALL_CALLS":
		return workflowspb.Workflow_LOG_ALL_CALLS
	case "LOG_ERRORS_ONLY":
		return workflowspb.Workflow_LOG_ERRORS_ONLY
	case "LOG_NONE":
		return workflowspb.Workflow_LOG_NONE
	default:
		return workflowspb.Workflow_CALL_LOG_LEVEL_UNSPECIFIED
	}
}

// workflowToProto renders a stored workflow as the proto Workflow. An unset
// service account is reported as the project default, matching real GCP (and
// the REST transport).
func workflowToProto(w workflowsstore.Workflow, project string) *workflowspb.Workflow {
	revisionCreateTime := w.RevisionCreateTime
	if revisionCreateTime.IsZero() {
		revisionCreateTime = w.UpdateTime
	}
	out := &workflowspb.Workflow{
		Name:               core.WorkflowName(project, w.Location, w.ID),
		Description:        w.Description,
		State:              stateToProto(w.State),
		RevisionId:         w.RevisionID,
		CreateTime:         timestamp.Proto(w.CreateTime),
		UpdateTime:         timestamp.Proto(w.UpdateTime),
		RevisionCreateTime: timestamp.Proto(revisionCreateTime),
		Labels:             w.Labels,
		UserEnvVars:        w.UserEnvVars,
		Tags:               w.Tags,
		CallLogLevel:       callLogLevelToProto(w.CallLogLevel),
	}
	if w.SourceContents != "" {
		out.SourceCode = &workflowspb.Workflow_SourceContents{SourceContents: w.SourceContents}
	}
	if w.ServiceAccount != "" {
		out.ServiceAccount = w.ServiceAccount
	} else {
		out.ServiceAccount = core.DefaultServiceAccount(project)
	}
	return out
}

// revisionToProto renders a core revision as the proto Workflow. The
// workflow-wide fields come from workflowToProto (the live workflow); the
// revision-scoped fields are overridden. For the live revision the result is
// identical to workflowToProto.
func revisionToProto(r core.Revision, project string) *workflowspb.Workflow {
	out := workflowToProto(r.Workflow, project)
	out.State = stateToProto(r.State)
	out.RevisionId = r.RevisionID
	out.RevisionCreateTime = timestamp.Proto(r.RevisionCreateTime)
	if r.SourceContents != "" {
		out.SourceCode = &workflowspb.Workflow_SourceContents{SourceContents: r.SourceContents}
	} else {
		out.SourceCode = nil
	}
	out.UserEnvVars = r.UserEnvVars
	out.CallLogLevel = callLogLevelToProto(r.CallLogLevel)
	if r.ServiceAccount != "" {
		out.ServiceAccount = r.ServiceAccount
	} else {
		out.ServiceAccount = core.DefaultServiceAccount(project)
	}
	return out
}

// operationToProto renders a stored operation as the proto
// google.longrunning.Operation, packing the workflows.v1.OperationMetadata and
// the caller-supplied response message (a Workflow, or Empty for delete) as
// typed Any values. The result and metadata.endTime are only attached once the
// operation is done: real GCP omits them while in flight, and a done operation
// keeps the original output exactly.
func operationToProto(op workflowsstore.Operation, project string, response proto.Message) (*longrunningpb.Operation, error) {
	meta := &workflowspb.OperationMetadata{
		CreateTime: timestamp.Proto(op.CreateTime),
		Target:     op.Target,
		Verb:       op.Verb,
		ApiVersion: "v1",
	}
	if op.Done {
		meta.EndTime = timestamp.Proto(op.EndTime)
	}
	metadata, err := anypb.New(meta)
	if err != nil {
		return nil, mapError(err)
	}
	out := &longrunningpb.Operation{
		Name:     core.OperationName(project, op.Location, op.ID),
		Metadata: metadata,
		Done:     op.Done,
	}
	if op.Done && response != nil {
		resp, err := anypb.New(response)
		if err != nil {
			return nil, mapError(err)
		}
		out.Result = &longrunningpb.Operation_Response{Response: resp}
	}
	return out, nil
}

// operationResponse reconstructs the typed result message for a settled
// operation from its stored Discovery-shape JSON: create/update carry a
// Workflow and delete carries google.protobuf.Empty. Any other verb, an empty
// body, or a body that cannot be decoded falls back to Empty so a poll never
// fails on a malformed record.
func operationResponse(op workflowsstore.Operation) proto.Message {
	if op.Verb != "create" && op.Verb != "update" {
		return emptyResponse()
	}
	if op.Response == "" {
		return emptyResponse()
	}
	w := &workflowspb.Workflow{}
	// The stored body carries the Workflow @type discriminator, which protojson
	// does not expect on the message itself, so unknown fields are discarded.
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(op.Response), w); err != nil {
		return emptyResponse()
	}
	return w
}

// emptyResponse builds the google.protobuf.Empty result a delete operation
// carries.
func emptyResponse() proto.Message { return &emptypb.Empty{} }

// createInputFromProto builds a core CreateInput from a request Workflow.
func createInputFromProto(w *workflowspb.Workflow, id string) core.CreateInput {
	in := core.CreateInput{ID: id}
	if w != nil {
		in.Description = w.GetDescription()
		in.Labels = w.GetLabels()
		in.ServiceAccount = w.GetServiceAccount()
		in.SourceContents = w.GetSourceContents()
		in.CallLogLevel = callLogLevelName(w.GetCallLogLevel())
		in.UserEnvVars = w.GetUserEnvVars()
		in.Tags = w.GetTags()
	}
	return in
}

// updateInputFromProto builds a core UpdateInput from a request Workflow and
// its field mask. An absent or empty mask means every field is applied.
func updateInputFromProto(w *workflowspb.Workflow, mask *fieldmaskpb.FieldMask, id string) core.UpdateInput {
	in := core.UpdateInput{ID: id, UpdateMask: strings.Join(mask.GetPaths(), ",")}
	if w != nil {
		in.Description = w.GetDescription()
		in.Labels = w.GetLabels()
		in.UserEnvVars = w.GetUserEnvVars()
		in.ServiceAccount = w.GetServiceAccount()
		in.SourceContents = w.GetSourceContents()
		in.CallLogLevel = callLogLevelName(w.GetCallLogLevel())
	}
	return in
}

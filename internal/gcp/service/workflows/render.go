package workflows

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/gcp/timestamp"
)

// operationMetadataType is the @type of the Cloud Workflows v1 OperationMetadata.
const operationMetadataType = "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata"

// WorkflowJSON renders a stored Workflow as the workflows.v1.Workflow wire map
// (Discovery/REST JSON shape). Fields that are empty are omitted, matching
// protojson.
func WorkflowJSON(w workflowsstore.Workflow, project string) map[string]any {
	out := map[string]any{
		"name":               WorkflowName(project, w.Location, w.ID),
		"state":              w.State,
		"revisionId":         w.RevisionID,
		"revisionCreateTime": timestamp.Canonical(liveRevisionCreateTime(w)),
		"createTime":         timestamp.Canonical(w.CreateTime),
		"updateTime":         timestamp.Canonical(w.UpdateTime),
	}
	if w.Description != "" {
		out["description"] = w.Description
	}
	if w.SourceContents != "" {
		out["sourceContents"] = w.SourceContents
	}
	if w.Labels != nil {
		out["labels"] = stringMapToAny(w.Labels)
	}
	if w.UserEnvVars != nil {
		out["userEnvVars"] = stringMapToAny(w.UserEnvVars)
	}
	if w.Tags != nil {
		out["tags"] = stringMapToAny(w.Tags)
	}
	if w.ServiceAccount != "" {
		out["serviceAccount"] = w.ServiceAccount
	} else {
		// Real GCP defaults an unset serviceAccount to the project's Compute
		// Engine default service account. The exact identity is normalized in
		// the differential harness, so a synthetic default is sufficient.
		out["serviceAccount"] = DefaultServiceAccount(project)
	}
	if w.CallLogLevel != "" {
		out["callLogLevel"] = w.CallLogLevel
	}
	return out
}

// liveRevisionCreateTime returns the workflow's revision creation time, falling
// back to UpdateTime for a legacy record that predates the stored field.
func liveRevisionCreateTime(w workflowsstore.Workflow) time.Time {
	if !w.RevisionCreateTime.IsZero() {
		return w.RevisionCreateTime
	}
	return w.UpdateTime
}

// RevisionJSON renders a workflow revision as the workflows.v1.Workflow wire
// map: the workflow-wide fields (name, description, labels, create/update time,
// tags) come from the live workflow, while state, revisionId,
// revisionCreateTime, sourceContents, serviceAccount, userEnvVars and
// callLogLevel come from the revision. For the live revision the output is
// byte-for-byte WorkflowJSON.
func RevisionJSON(r Revision, project string) map[string]any {
	out := WorkflowJSON(r.Workflow, project)
	out["state"] = r.State
	out["revisionId"] = r.RevisionID
	out["revisionCreateTime"] = timestamp.Canonical(r.RevisionCreateTime)
	if r.SourceContents != "" {
		out["sourceContents"] = r.SourceContents
	} else {
		delete(out, "sourceContents")
	}
	if r.UserEnvVars != nil {
		out["userEnvVars"] = stringMapToAny(r.UserEnvVars)
	} else {
		delete(out, "userEnvVars")
	}
	if r.ServiceAccount != "" {
		out["serviceAccount"] = r.ServiceAccount
	} else {
		out["serviceAccount"] = DefaultServiceAccount(project)
	}
	if r.CallLogLevel != "" {
		out["callLogLevel"] = r.CallLogLevel
	} else {
		delete(out, "callLogLevel")
	}
	return out
}

// OperationJSON renders a stored Operation as a google.longrunning.Operation
// wire map. The response body and metadata.endTime are only present once the
// operation is done: real GCP omits them while in flight. A done operation
// keeps the original shape exactly.
func OperationJSON(op workflowsstore.Operation, project string) map[string]any {
	name := OperationName(project, op.Location, op.ID)
	metadata := map[string]any{
		"@type":      operationMetadataType,
		"createTime": timestamp.Canonical(op.CreateTime),
		"target":     op.Target,
		"verb":       op.Verb,
		"apiVersion": "v1",
	}
	out := map[string]any{
		"name":     name,
		"metadata": metadata,
		"done":     op.Done,
	}
	if op.Done {
		metadata["endTime"] = timestamp.Canonical(op.EndTime)
		var response any = map[string]any{}
		if op.Response != "" {
			_ = json.Unmarshal([]byte(op.Response), &response)
		}
		out["response"] = response
	}
	return out
}

// storeOperation persists a google.longrunning.Operation and returns it. In the
// default synchronous mode it is stored done=true with EndTime=now, exactly as
// before. In async mode it is stored done=false with a zero EndTime; the settle
// helper derives completion on read. Marshalling the plain maps this package
// builds cannot fail, so a marshal error is ignored and surfaces as an empty
// JSON object on read-back.
func (s *Service) storeOperation(ctx context.Context, project, location, verb, target string, response map[string]any) (workflowsstore.Operation, error) {
	now := clock.Now().UTC()
	respJSON, _ := json.Marshal(response)
	op := workflowsstore.Operation{
		ID:         randomHex(12),
		ProjectID:  project,
		Location:   location,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		Response:   string(respJSON),
	}
	if s.lroMode.Async() {
		op.Done = false
	} else {
		op.Done = true
		op.EndTime = now
	}
	if err := s.workflows.CreateOperation(ctx, project, location, op); err != nil {
		return workflowsstore.Operation{}, err
	}
	return op, nil
}

// settle derives the rendered state of a persisted operation from its stored
// done flag and the configured timing mode. An in-flight operation (done=false)
// becomes done once the delay has elapsed, with a deterministic EndTime of
// createTime+delay (a zero delay yields createTime).
//
// The flip is derived on read rather than written back: the workflows store has
// no UpdateOperation (adding one would be a store migration for no behavioral
// gain), and the persisted flag is an input while the settled state is a pure
// function of it and the clock. A store that later needs the settled flag to be
// visible to other readers can add the write without changing this contract.
func (s *Service) settle(op workflowsstore.Operation) workflowsstore.Operation {
	if op.Done || s.lroMode.Pending(op.CreateTime) {
		return op
	}
	op.Done = true
	op.EndTime = op.CreateTime.Add(s.lroMode.Delay)
	s.tracker.EmitOperation("workflows", op.ID, op.ID, op.Verb, op.ProjectID)
	return op
}

// workflowOpResponse is the Workflow payload embedded in a create/update LRO's
// response field, which real GCP tags with the Workflow @type discriminator.
func workflowOpResponse(w workflowsstore.Workflow, project string) map[string]any {
	resp := WorkflowJSON(w, project)
	resp["@type"] = "type.googleapis.com/google.cloud.workflows.v1.Workflow"
	return resp
}

// nextRevision derives the output-only revision ID (format "000001-a4d": a
// zero-padded six-digit ordinal, a hyphen, and three hexadecimal characters)
// for a newly created or source-changing updated workflow.
func nextRevision(prev string) string {
	n := 1
	if prev != "" {
		if parts := strings.SplitN(prev, "-", 2); len(parts) == 2 {
			if v, err := strconv.Atoi(parts[0]); err == nil {
				n = v + 1
			}
		}
	}
	return fmt.Sprintf("%06d-%s", n, randomHex(3))
}

// randomHex returns n random hexadecimal characters.
func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand is not expected to fail; a zeroed suffix keeps the value
		// well-formed on the impossible path.
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// stringMapToAny converts a map[string]string to the JSON-compatible
// map[string]any wire form so read-back matches what the JSON encoder emits.
func stringMapToAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Package workflows implements the Cloud Workflows v1 management provider
// (workflows.googleapis.com): List/Get/Create/Update/DeleteWorkflow plus the
// GetOperation surface for the long-running operations those methods return.
// Create/Update/Delete complete synchronously and return a done=true
// google.longrunning.Operation (mirroring the GCP REST LRO convention used by
// Firestore CreateIndex).
package workflows

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Cloud Workflows v1 workflow resources.
type Provider struct {
	workflows workflowsstore.Store
}

// New returns a Provider backed by the given store.
func New(workflows workflowsstore.Store) *Provider {
	return &Provider{workflows: workflows}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Workflow.ListWorkflows":  p.ListWorkflows,
		"Workflow.GetWorkflow":    p.GetWorkflow,
		"Workflow.CreateWorkflow": p.CreateWorkflow,
		"Workflow.UpdateWorkflow": p.UpdateWorkflow,
		"Workflow.DeleteWorkflow": p.DeleteWorkflow,
		"Workflow.GetOperation":   p.GetOperation,
	}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

func bodyStringMap(body map[string]any, key string) map[string]string {
	if body == nil {
		return nil
	}
	m, ok := body[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
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

// workflowID extracts the workflow ID from a relative name
// ("locations/{l}/workflows/{id}") or a full GCP name.
func workflowID(name string) string {
	if i := strings.Index(name, "/workflows/"); i >= 0 {
		return name[i+len("/workflows/"):]
	}
	return strings.TrimPrefix(name, "workflows/")
}

// operationID extracts the operation ID from a name ending in /operations/{id}.
func operationID(name string) string {
	if i := strings.Index(name, "/operations/"); i >= 0 {
		return name[i+len("/operations/"):]
	}
	return strings.TrimPrefix(name, "operations/")
}

// locationFromName extracts the location segment from a name of the form
// "locations/{l}/...".
func locationFromName(name string) string {
	for i, s := range strings.Split(name, "/") {
		if s == "locations" {
			parts := strings.Split(name, "/")
			if i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}
	return ""
}

func mapErr(err error) error {
	if errors.Is(err, workflowsstore.ErrNoSuchWorkflow) {
		return model.NewProviderError("NotFound", "workflow not found", 404)
	}
	if errors.Is(err, workflowsstore.ErrNoSuchOperation) {
		return model.NewProviderError("NotFound", "operation not found", 404)
	}
	return err
}

// workflowToMap renders a store Workflow as a workflows.v1.Workflow wire map.
func (p *Provider) workflowToMap(nr *model.NormalizedRequest, w workflowsstore.Workflow) map[string]any {
	out := map[string]any{
		"name":        nr.ResourceID("workflow", w.Location+"/"+w.ID),
		"state":       w.State,
		"revisionId":  w.RevisionID,
		"createTime":  w.CreateTime.Format(time.RFC3339Nano),
		"updateTime":  w.UpdateTime.Format(time.RFC3339Nano),
		"description": w.Description,
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
	}
	if w.CallLogLevel != "" {
		out["callLogLevel"] = w.CallLogLevel
	}
	return out
}

// workflowFromBody builds the store Workflow from a request body.
func workflowFromBody(body map[string]any, location, id string) workflowsstore.Workflow {
	now := clock.Now().UTC()
	return workflowsstore.Workflow{
		ID:             id,
		Location:       location,
		Description:    bodyString(body, "description"),
		Labels:         bodyStringMap(body, "labels"),
		ServiceAccount: bodyString(body, "serviceAccount"),
		SourceContents: bodyString(body, "sourceContents"),
		State:          "ACTIVE",
		RevisionID:     nextRevision(""),
		CreateTime:     now,
		UpdateTime:     now,
		CallLogLevel:   bodyString(body, "callLogLevel"),
		UserEnvVars:    bodyStringMap(body, "userEnvVars"),
		Tags:           bodyStringMap(body, "tags"),
	}
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
		// crypto/rand is not expected to fail; a zeroed suffix keeps the
		// value well-formed on the impossible path.
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// operationMap renders a stored Operation as a google.longrunning.Operation.
func (p *Provider) operationMap(nr *model.NormalizedRequest, location string, op workflowsstore.Operation) map[string]any {
	name := nr.ResourceID("workflow-operation", location+"/"+op.ID)
	var response any = map[string]any{}
	if op.Response != "" {
		_ = json.Unmarshal([]byte(op.Response), &response)
	}
	return map[string]any{
		"name": name,
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata",
			"createTime": op.CreateTime.Format(time.RFC3339Nano),
			"endTime":    op.EndTime.Format(time.RFC3339Nano),
			"target":     op.Target,
			"verb":       op.Verb,
			"apiVersion": "v1",
		},
		"done":     op.Done,
		"response": response,
	}
}

// storeOperation persists a done operation and returns its wire map.
func (p *Provider) storeOperation(ctx context.Context, nr *model.NormalizedRequest, location, verb string, target string, responseData map[string]any) (map[string]any, error) {
	now := clock.Now().UTC()
	op := workflowsstore.Operation{
		ID:         randomHex(12),
		ProjectID:  nr.AccountID,
		Location:   location,
		Done:       true,
		Verb:       verb,
		Target:     target,
		CreateTime: now,
		EndTime:    now,
	}
	respJSON, _ := json.Marshal(responseData)
	op.Response = string(respJSON)
	if err := p.workflows.CreateOperation(ctx, nr.AccountID, location, op); err != nil {
		return nil, err
	}
	return p.operationMap(nr, location, op), nil
}

func (p *Provider) ListWorkflows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	// filter and orderBy are accepted but ignored (documented limitation).
	wfs, err := p.workflows.ListWorkflows(ctx, nr.AccountID, location)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(wfs, func(w workflowsstore.Workflow) string { return w.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, w := range page {
		items = append(items, p.workflowToMap(nr, w))
	}
	resp := map[string]any{"workflows": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = locationFromName(name)
	}
	if name == "" || location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflow name", 400)
	}
	w, err := p.workflows.GetWorkflow(ctx, nr.AccountID, location, workflowID(name))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.workflowToMap(nr, w)), nil
}

func (p *Provider) CreateWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	location := strParam(nr, "location")
	if location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing location", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	id := strParam(nr, "workflowId")
	if id == "" {
		if n := bodyString(body, "name"); n != "" {
			id = workflowID(n)
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflowId", 400)
	}
	w := workflowFromBody(body, location, id)
	if err := p.workflows.CreateWorkflow(ctx, nr.AccountID, location, id, w); err != nil {
		if errors.Is(err, workflowsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "workflow already exists", 409)
		}
		return nil, err
	}
	target := nr.ResourceID("workflow", location+"/"+id)
	op, err := p.storeOperation(ctx, nr, location, "create", target, p.workflowToMap(nr, w))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) UpdateWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = locationFromName(name)
	}
	if name == "" || location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflow name", 400)
	}
	id := workflowID(name)
	body, _ := nr.Params["body"].(map[string]any)
	mask := strParam(nr, "updateMask")

	apply := func(field string) bool {
		return mask == "" || strings.Contains(","+mask+",", ","+field+",")
	}
	w, err := p.workflows.UpdateWorkflowAtomic(ctx, nr.AccountID, location, id, func(w workflowsstore.Workflow) (workflowsstore.Workflow, error) {
		sourceChanged := false
		if apply("description") {
			w.Description = bodyString(body, "description")
		}
		if apply("labels") {
			if labels := bodyStringMap(body, "labels"); labels != nil {
				w.Labels = labels
			}
		}
		if apply("userEnvVars") {
			if vars := bodyStringMap(body, "userEnvVars"); vars != nil {
				w.UserEnvVars = vars
			}
		}
		if apply("serviceAccount") {
			if v := bodyString(body, "serviceAccount"); v != w.ServiceAccount {
				w.ServiceAccount = v
				sourceChanged = true
			}
		}
		if apply("sourceContents") {
			if v := bodyString(body, "sourceContents"); v != w.SourceContents {
				w.SourceContents = v
				sourceChanged = true
			}
		}
		if apply("callLogLevel") {
			w.CallLogLevel = bodyString(body, "callLogLevel")
		}
		if sourceChanged {
			w.RevisionID = nextRevision(w.RevisionID)
		}
		w.UpdateTime = clock.Now().UTC()
		return w, nil
	})
	if err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("workflow", location+"/"+id)
	op, err := p.storeOperation(ctx, nr, location, "update", target, p.workflowToMap(nr, w))
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) DeleteWorkflow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = locationFromName(name)
	}
	if name == "" || location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing workflow name", 400)
	}
	id := workflowID(name)
	if err := p.workflows.DeleteWorkflow(ctx, nr.AccountID, location, id); err != nil {
		return nil, mapErr(err)
	}
	target := nr.ResourceID("workflow", location+"/"+id)
	// DeleteWorkflow's LRO response type is google.protobuf.Empty.
	op, err := p.storeOperation(ctx, nr, location, "delete", target, map[string]any{})
	if err != nil {
		return nil, err
	}
	return provider.OK(op), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name := strParam(nr, "name")
	location := strParam(nr, "location")
	if location == "" {
		location = locationFromName(name)
	}
	if name == "" || location == "" {
		return nil, model.NewProviderError("InvalidArgument", "missing operation name", 400)
	}
	op, err := p.workflows.GetOperation(ctx, nr.AccountID, location, operationID(name))
	if err != nil {
		return nil, mapErr(err)
	}
	return provider.OK(p.operationMap(nr, location, op)), nil
}

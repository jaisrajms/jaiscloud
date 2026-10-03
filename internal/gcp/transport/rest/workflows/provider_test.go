package workflows

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

const source = "main:\n  steps:\n    - r:\n        return: 1\n"

func newProvider(t *testing.T) *Provider {
	t.Helper()
	return NewProvider(core.NewService(workflowsstore.NewMemoryStore()), "default-proj")
}

func nr(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{AccountID: "proj", Params: params}
}

// TestProviderRoundTrip exercises the REST handlers over the shared core:
// create (LRO) → get → list → update (masked) → getOperation → delete.
func TestProviderRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	create := nr(map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1",
		"body": map[string]any{
			"description":    "hello",
			"sourceContents": source,
			"labels":         map[string]any{"k": "v"},
		},
	})
	cresp, err := p.CreateWorkflow(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if cresp.Data["done"] != true {
		t.Fatalf("create not done: %v", cresp.Data)
	}
	opName, _ := cresp.Data["name"].(string)
	if !strings.HasPrefix(opName, "projects/proj/locations/us-central1/operations/") {
		t.Fatalf("operation name = %q", opName)
	}
	response, _ := cresp.Data["response"].(map[string]any)
	if response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Fatalf("operation response name = %v", response["name"])
	}
	wantSA := "projects/proj/serviceAccounts/" + resource.ProjectNumber("proj") + "-compute@developer.gserviceaccount.com"
	if response["serviceAccount"] != wantSA {
		t.Fatalf("serviceAccount = %v, want %v", response["serviceAccount"], wantSA)
	}

	// Duplicate → AlreadyExists.
	if _, err := p.CreateWorkflow(ctx, create); err == nil {
		t.Fatalf("expected AlreadyExists on duplicate create")
	}

	get := nr(map[string]any{"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	gresp, err := p.GetWorkflow(ctx, get)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gresp.Data["sourceContents"] != source || gresp.Data["state"] != "ACTIVE" {
		t.Fatalf("unexpected get data: %v", gresp.Data)
	}

	list := nr(map[string]any{"project": "proj", "location": "us-central1"})
	lresp, err := p.ListWorkflows(ctx, list)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := lresp.Data["workflows"].([]any)
	if len(items) != 1 {
		t.Fatalf("list len = %d, want 1", len(items))
	}

	upd := nr(map[string]any{
		"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1",
		"updateMask": "description",
		"body":       map[string]any{"description": "new"},
	})
	uresp, err := p.UpdateWorkflow(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	updResp, _ := uresp.Data["response"].(map[string]any)
	if updResp["description"] != "new" {
		t.Fatalf("update description = %v", updResp["description"])
	}
	if updResp["sourceContents"] != source {
		t.Fatalf("update should preserve sourceContents: %v", updResp["sourceContents"])
	}

	getOp := nr(map[string]any{"project": "proj", "location": "us-central1", "name": opName})
	oresp, err := p.GetOperation(ctx, getOp)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if oresp.Data["name"] != opName {
		t.Fatalf("get operation name = %v, want %v", oresp.Data["name"], opName)
	}

	del := nr(map[string]any{"project": "proj", "location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	dresp, err := p.DeleteWorkflow(ctx, del)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if dresp.Data["done"] != true {
		t.Fatalf("delete not done: %v", dresp.Data)
	}

	if _, err := p.GetWorkflow(ctx, get); err == nil {
		t.Fatalf("expected NotFound after delete")
	}
}

// TestProviderUsesDefaultProjectWhenAbsent verifies the configured default is
// used when neither the path nor the account scope names a project.
func TestProviderUsesDefaultProjectWhenAbsent(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	_, err := p.ListWorkflows(ctx, &model.NormalizedRequest{Params: map[string]any{"location": "us-central1"}})
	if err != nil {
		t.Fatalf("list with default project: %v", err)
	}
}

// stubOperationResolver reports handled when opID matches want, returning body.
type stubOperationResolver struct {
	want    string
	body    map[string]any
	handled bool
}

func (s stubOperationResolver) ResolveOperation(_ context.Context, _, _, opID string) (map[string]any, bool, error) {
	if s.handled && opID == s.want {
		return s.body, true, nil
	}
	return nil, false, nil
}

// TestProviderResolvesForeignLocationOperation verifies that an unknown
// Workflows operation name falls through to the wired cross-service resolvers
// (the opt-in async path for Metastore/Managed Kafka) and that a resolver that
// declines still yields the canonical NotFound.
func TestProviderResolvesForeignLocationOperation(t *testing.T) {
	ctx := context.Background()
	get := nr(map[string]any{
		"project": "proj", "location": "us-central1",
		"name": "projects/proj/locations/us-central1/operations/abc123",
	})

	// No resolvers → the Workflows NotFound stands.
	p := newProvider(t)
	if _, err := p.GetOperation(ctx, get); err == nil || !core.IsNotFound(err) {
		t.Fatalf("expected NotFound without resolvers, got %v", err)
	}

	// A declining resolver does not change that.
	declining := newProvider(t)
	declining.SetOperationResolvers(stubOperationResolver{want: "abc123", handled: false})
	if _, err := declining.GetOperation(ctx, get); err == nil || !core.IsNotFound(err) {
		t.Fatalf("expected NotFound with a declining resolver, got %v", err)
	}

	// An owning resolver serves its own JSON.
	owning := newProvider(t)
	owning.SetOperationResolvers(stubOperationResolver{
		want:    "abc123",
		handled: true,
		body:    map[string]any{"name": get.Params["name"], "done": true},
	})
	resp, err := owning.GetOperation(ctx, get)
	if err != nil {
		t.Fatalf("resolved operation: %v", err)
	}
	if resp.Data["done"] != true || resp.Data["name"] != get.Params["name"] {
		t.Fatalf("resolved data = %v", resp.Data)
	}
}

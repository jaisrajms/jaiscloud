package workflows

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/resource"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

const simpleSource = "main:\n  steps:\n    - r:\n        return: 1\n"

// revisionRE matches the GCP revision_id format "000001-a4d": a zero-padded
// six-digit ordinal, a hyphen, and exactly three hexadecimal characters.
var revisionRE = regexp.MustCompile(`^[0-9]{6}-[0-9a-f]{3}$`)

// delayedGetStore wraps a workflowsstore.Store, delaying every GetWorkflow
// call to widen a TOCTOU race window in tests.
type delayedGetStore struct {
	workflowsstore.Store
	delay time.Duration
}

func (d *delayedGetStore) GetWorkflow(ctx context.Context, projectID, location, id string) (workflowsstore.Workflow, error) {
	w, err := d.Store.GetWorkflow(ctx, projectID, location, id)
	time.Sleep(d.delay)
	return w, err
}

// TestUpdateWorkflowConcurrentDisjointFieldsNoLostUpdate proves that
// UpdateWorkflow's get-merge-write cycle is atomic with respect to other
// concurrent masked-PATCH requests. Without atomicity, a PATCH updating only
// "description" (via updateMask) reads a stale full copy of the workflow
// (taken before a concurrent "callLogLevel"-only PATCH committed), then
// writes that stale copy back — silently reverting the callLogLevel change
// even though the description PATCH's mask never named it. 25 goroutines
// each PATCH only "description" to a unique value and 25 PATCH only
// "callLogLevel" to a unique value; a delayed-Get store wrapper widens the
// TOCTOU window reliably (the real in-memory round trip otherwise completes
// in nanoseconds, too fast to overlap deterministically).
func TestUpdateWorkflowConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	p := New(&delayedGetStore{Store: workflowsstore.NewMemoryStore(), delay: 5 * time.Millisecond})

	createNR := newNR(map[string]any{
		"location":   "us-central1",
		"workflowId": "wf1",
		"body": map[string]any{
			"description":    "d0",
			"sourceContents": simpleSource,
			"callLogLevel":   "LOG_ALL_CALLS",
		},
	})
	if _, err := p.CreateWorkflow(ctx, createNR); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/workflows/wf1",
				"updateMask": "description",
				"body":       map[string]any{"description": fmt.Sprintf("vDesc%d", i)},
			})
			_, errs[i] = p.UpdateWorkflow(ctx, nr)
		}(i)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/workflows/wf1",
				"updateMask": "callLogLevel",
				"body":       map[string]any{"callLogLevel": fmt.Sprintf("LEVEL%d", i)},
			})
			_, errs[perField+i] = p.UpdateWorkflow(ctx, nr)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := p.GetWorkflow(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/workflows/wf1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	description, _ := got.Data["description"].(string)
	callLogLevel, _ := got.Data["callLogLevel"].(string)
	if description == "d0" || !strings.HasPrefix(description, "vDesc") {
		t.Errorf("description reverted to a stale value instead of one of the 25 concurrent writers': got %q", description)
	}
	if callLogLevel == "LOG_ALL_CALLS" || !strings.HasPrefix(callLogLevel, "LEVEL") {
		t.Errorf("callLogLevel reverted to a stale value instead of one of the 25 concurrent writers': got %q", callLogLevel)
	}
}

func TestWorkflowCRUDAndLRO(t *testing.T) {
	ctx := context.Background()
	p := New(workflowsstore.NewMemoryStore())

	// Create (LRO).
	nr := newNR(map[string]any{
		"location":   "us-central1",
		"workflowId": "wf1",
		"body": map[string]any{
			"description":    "hello",
			"sourceContents": simpleSource,
			"labels":         map[string]any{"k": "v"},
			"userEnvVars":    map[string]any{"MY_VAR": "hello"},
			"tags":           map[string]any{"env": "test"},
		},
	})
	resp, err := p.CreateWorkflow(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("expected done=true, got %v", resp.Data["done"])
	}
	name, _ := resp.Data["name"].(string)
	if !strings.HasPrefix(name, "projects/proj/locations/us-central1/operations/") {
		t.Errorf("unexpected operation name: %q", name)
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md == nil || md["verb"] != "create" || md["target"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Errorf("unexpected metadata: %v", md)
	}
	if md["@type"] != "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata" {
		t.Errorf("expected OperationMetadata @type, got %v", md["@type"])
	}
	response, _ := resp.Data["response"].(map[string]any)
	if response == nil || response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Errorf("unexpected operation response: %v", response)
	}
	if !revisionRE.MatchString(response["revisionId"].(string)) {
		t.Errorf("revisionId not in NNNNNN-XXX format: %v", response["revisionId"])
	}

	// Duplicate → 409.
	if _, err := p.CreateWorkflow(ctx, nr); err == nil {
		t.Errorf("expected AlreadyExists on duplicate create")
	}

	// Get.
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	resp, err = p.GetWorkflow(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["sourceContents"] != simpleSource {
		t.Errorf("sourceContents not preserved: %v", resp.Data["sourceContents"])
	}
	if resp.Data["state"] != "ACTIVE" || resp.Data["revisionId"] == "" {
		t.Errorf("missing state/revision: %v", resp.Data)
	}
	if !revisionRE.MatchString(resp.Data["revisionId"].(string)) {
		t.Errorf("revisionId not in NNNNNN-XXX format on get: %v", resp.Data["revisionId"])
	}
	// userEnvVars and tags must round-trip (not silently dropped).
	ev, _ := resp.Data["userEnvVars"].(map[string]any)
	if ev == nil || ev["MY_VAR"] != "hello" {
		t.Errorf("userEnvVars not preserved: %v", resp.Data["userEnvVars"])
	}
	tg, _ := resp.Data["tags"].(map[string]any)
	if tg == nil || tg["env"] != "test" {
		t.Errorf("tags not preserved: %v", resp.Data["tags"])
	}

	// GetOperation round-trips the stored operation.
	getOpNR := newNR(map[string]any{"location": "us-central1", "name": name})
	if _, err := p.GetOperation(ctx, getOpNR); err != nil {
		t.Fatalf("get operation: %v", err)
	}

	// GetOperation on a missing op → 404.
	missingOp := newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/operations/deadbeef"})
	if _, err := p.GetOperation(ctx, missingOp); err == nil {
		t.Errorf("expected NotFound for missing operation")
	}

	// List.
	nr = newNR(map[string]any{"location": "us-central1"})
	resp, err = p.ListWorkflows(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["workflows"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(items))
	}

	// Delete (LRO, response is Empty).
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	resp, err = p.DeleteWorkflow(ctx, nr)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if resp.Data["done"] != true {
		t.Errorf("expected done=true on delete")
	}

	// Gone after delete.
	nr = newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	if _, err := p.GetWorkflow(ctx, nr); err == nil {
		t.Errorf("expected NotFound after delete")
	}
}

func TestUpdateWorkflowMask(t *testing.T) {
	ctx := context.Background()
	p := New(workflowsstore.NewMemoryStore())

	nr := newNR(map[string]any{
		"location":   "us-central1",
		"workflowId": "wf1",
		"body": map[string]any{
			"description":    "orig",
			"sourceContents": simpleSource,
			"labels":         map[string]any{"k": "v"},
		},
	})
	if _, err := p.CreateWorkflow(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update only description (updateMask=description): source + labels kept.
	upd := newNR(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/workflows/wf1",
		"updateMask": "description",
		"body":       map[string]any{"description": "new"},
	})
	resp, err := p.UpdateWorkflow(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	response, _ := resp.Data["response"].(map[string]any)
	if response["description"] != "new" {
		t.Errorf("description not updated: %v", response)
	}
	if response["sourceContents"] != simpleSource {
		t.Errorf("sourceContents should be preserved: %v", response)
	}
	labels, _ := response["labels"].(map[string]any)
	if labels["k"] != "v" {
		t.Errorf("labels should be preserved: %v", labels)
	}

	// Update sourceContents: revision must bump.
	oldRev, _ := response["revisionId"].(string)
	upd2 := newNR(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/workflows/wf1",
		"updateMask": "sourceContents",
		"body":       map[string]any{"sourceContents": "main:\n  steps:\n    - r:\n        return: 2\n"},
	})
	resp, err = p.UpdateWorkflow(ctx, upd2)
	if err != nil {
		t.Fatalf("update source: %v", err)
	}
	response2, _ := resp.Data["response"].(map[string]any)
	newRev, _ := response2["revisionId"].(string)
	if newRev == oldRev {
		t.Errorf("revisionId must change when sourceContents changes")
	}
}

func TestUpdateWorkflowUserEnvVars(t *testing.T) {
	ctx := context.Background()
	p := New(workflowsstore.NewMemoryStore())

	nr := newNR(map[string]any{
		"location":   "us-central1",
		"workflowId": "wf1",
		"body": map[string]any{
			"sourceContents": simpleSource,
			"userEnvVars":    map[string]any{"A": "1"},
		},
	})
	if _, err := p.CreateWorkflow(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}

	upd := newNR(map[string]any{
		"location":   "us-central1",
		"name":       "locations/us-central1/workflows/wf1",
		"updateMask": "userEnvVars",
		"body":       map[string]any{"userEnvVars": map[string]any{"B": "2"}},
	})
	resp, err := p.UpdateWorkflow(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	response, _ := resp.Data["response"].(map[string]any)
	ev, _ := response["userEnvVars"].(map[string]any)
	if ev == nil || ev["B"] != "2" {
		t.Errorf("userEnvVars not updated: %v", response["userEnvVars"])
	}

	// Read back reflects the update.
	get := newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/workflows/wf1"})
	gresp, err := p.GetWorkflow(ctx, get)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	ev, _ = gresp.Data["userEnvVars"].(map[string]any)
	if ev == nil || ev["B"] != "2" {
		t.Errorf("userEnvVars not persisted: %v", gresp.Data["userEnvVars"])
	}
}

func TestCreateWorkflowMissingLocation(t *testing.T) {
	ctx := context.Background()
	p := New(workflowsstore.NewMemoryStore())
	nr := newNR(map[string]any{"workflowId": "wf1", "body": map[string]any{}})
	if _, err := p.CreateWorkflow(ctx, nr); err == nil {
		t.Errorf("expected InvalidArgument for missing location")
	}
}

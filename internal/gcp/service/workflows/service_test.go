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
)

const simpleSource = "main:\n  steps:\n    - r:\n        return: 1\n"

// revisionRE matches the GCP revision_id format "000001-a4d": a zero-padded
// six-digit ordinal, a hyphen, and exactly three hexadecimal characters.
var revisionRE = regexp.MustCompile(`^[0-9]{6}-[0-9a-f]{3}$`)

// delayedGetStore wraps a workflowsstore.Store, delaying every GetWorkflow call
// to widen a TOCTOU race window in tests.
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
// concurrent masked updates. Without atomicity, an update touching only
// "description" reads a stale full copy of the workflow (taken before a
// concurrent "callLogLevel"-only update committed), then writes that stale copy
// back — silently reverting the callLogLevel change even though the description
// update's mask never named it.
func TestUpdateWorkflowConcurrentDisjointFieldsNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	s := NewService(&delayedGetStore{Store: workflowsstore.NewMemoryStore(), delay: 5 * time.Millisecond})

	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{
		ID:             "wf1",
		Description:    "d0",
		SourceContents: simpleSource,
		CallLogLevel:   "LOG_ALL_CALLS",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, _, errs[i] = s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
				ID: "wf1", UpdateMask: "description", Description: fmt.Sprintf("vDesc%d", i),
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _, errs[perField+i] = s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
				ID: "wf1", UpdateMask: "callLogLevel", CallLogLevel: fmt.Sprintf("LEVEL%d", i),
			})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	got, err := s.GetWorkflow(ctx, "proj", "us-central1", "wf1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Description == "d0" || !strings.HasPrefix(got.Description, "vDesc") {
		t.Errorf("description reverted to a stale value instead of one of the 25 concurrent writers': got %q", got.Description)
	}
	if got.CallLogLevel == "LOG_ALL_CALLS" || !strings.HasPrefix(got.CallLogLevel, "LEVEL") {
		t.Errorf("callLogLevel reverted to a stale value instead of one of the 25 concurrent writers': got %q", got.CallLogLevel)
	}
}

func TestWorkflowCRUDAndLRO(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())

	// Create (LRO).
	w, op, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{
		ID:             "wf1",
		Description:    "hello",
		SourceContents: simpleSource,
		Labels:         map[string]string{"k": "v"},
		UserEnvVars:    map[string]string{"MY_VAR": "hello"},
		Tags:           map[string]string{"env": "test"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !op.Done {
		t.Errorf("expected done=true")
	}
	if !strings.HasPrefix(OperationName("proj", "us-central1", op.ID), "projects/proj/locations/us-central1/operations/") {
		t.Errorf("unexpected operation name: %q", op.ID)
	}
	opJSON := OperationJSON(op, "proj")
	if opJSON["done"] != true {
		t.Errorf("expected done=true, got %v", opJSON["done"])
	}
	md, _ := opJSON["metadata"].(map[string]any)
	if md == nil || md["verb"] != "create" || md["target"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Errorf("unexpected metadata: %v", md)
	}
	if md["@type"] != "type.googleapis.com/google.cloud.workflows.v1.OperationMetadata" {
		t.Errorf("expected OperationMetadata @type, got %v", md["@type"])
	}
	response, _ := opJSON["response"].(map[string]any)
	if response == nil || response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Errorf("unexpected operation response: %v", response)
	}
	if !revisionRE.MatchString(w.RevisionID) {
		t.Errorf("revisionId not in NNNNNN-XXX format: %v", w.RevisionID)
	}

	// Duplicate → 409.
	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf1", SourceContents: simpleSource}); err == nil {
		t.Errorf("expected AlreadyExists on duplicate create")
	}

	// Get.
	got, err := s.GetWorkflow(ctx, "proj", "us-central1", "wf1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SourceContents != simpleSource {
		t.Errorf("sourceContents not preserved: %v", got.SourceContents)
	}
	if got.State != "ACTIVE" || got.RevisionID == "" {
		t.Errorf("missing state/revision: %+v", got)
	}
	if !revisionRE.MatchString(got.RevisionID) {
		t.Errorf("revisionId not in NNNNNN-XXX format on get: %v", got.RevisionID)
	}
	if got.UserEnvVars["MY_VAR"] != "hello" {
		t.Errorf("userEnvVars not preserved: %v", got.UserEnvVars)
	}
	if got.Tags["env"] != "test" {
		t.Errorf("tags not preserved: %v", got.Tags)
	}

	// GetOperation round-trips the stored operation.
	if _, err := s.GetOperation(ctx, "proj", "us-central1", op.ID); err != nil {
		t.Fatalf("get operation: %v", err)
	}

	// GetOperation on a missing op → NotFound.
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "deadbeef"); err == nil {
		t.Errorf("expected NotFound for missing operation")
	}

	// List.
	page, _, err := s.ListWorkflows(ctx, "proj", "us-central1", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 1 {
		t.Errorf("expected 1 workflow, got %d", len(page))
	}

	// Delete (LRO, response is Empty).
	if _, err := s.DeleteWorkflow(ctx, "proj", "us-central1", "wf1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Gone after delete.
	if _, err := s.GetWorkflow(ctx, "proj", "us-central1", "wf1"); err == nil {
		t.Errorf("expected NotFound after delete")
	}
}

func TestUpdateWorkflowMask(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())

	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{
		ID: "wf1", Description: "orig", SourceContents: simpleSource, Labels: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Update only description (updateMask=description): source + labels kept.
	w, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "description", Description: "new",
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if w.Description != "new" {
		t.Errorf("description not updated: %v", w.Description)
	}
	if w.SourceContents != simpleSource {
		t.Errorf("sourceContents should be preserved: %v", w.SourceContents)
	}
	if w.Labels["k"] != "v" {
		t.Errorf("labels should be preserved: %v", w.Labels)
	}

	// Update sourceContents: revision must bump.
	oldRev := w.RevisionID
	w2, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "sourceContents", SourceContents: "main:\n  steps:\n    - r:\n        return: 2\n",
	})
	if err != nil {
		t.Fatalf("update source: %v", err)
	}
	if w2.RevisionID == oldRev {
		t.Errorf("revisionId must change when sourceContents changes")
	}
}

func TestUpdateWorkflowUserEnvVars(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())

	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{
		ID: "wf1", SourceContents: simpleSource, UserEnvVars: map[string]string{"A": "1"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	w, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "userEnvVars", UserEnvVars: map[string]string{"B": "2"},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if w.UserEnvVars["B"] != "2" {
		t.Errorf("userEnvVars not updated: %v", w.UserEnvVars)
	}

	got, err := s.GetWorkflow(ctx, "proj", "us-central1", "wf1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserEnvVars["B"] != "2" {
		t.Errorf("userEnvVars not persisted: %v", got.UserEnvVars)
	}
}

func TestCreateWorkflowMissingLocation(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())
	if _, _, err := s.CreateWorkflow(ctx, "proj", "", CreateInput{ID: "wf1"}); err == nil {
		t.Errorf("expected InvalidArgument for missing location")
	}
}

func TestWorkflowJSONServiceAccountDefault(t *testing.T) {
	w := workflowsstore.Workflow{ID: "wf1", Location: "us-central1", State: "ACTIVE", RevisionID: "000001-a4d"}
	out := WorkflowJSON(w, "proj")
	want := "projects/proj/serviceAccounts/" + resource.ProjectNumber("proj") + "-compute@developer.gserviceaccount.com"
	if out["serviceAccount"] != want {
		t.Errorf("serviceAccount = %v, want %v", out["serviceAccount"], want)
	}
}

// TestUpdateWorkflowMaskAcceptsProtoAndJSONPaths verifies the mask accepts the
// gRPC proto snake_case path and the oneof path, and fails loud on an unknown
// path instead of silently ignoring it.
func TestUpdateWorkflowMaskAcceptsProtoAndJSONPaths(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())
	if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{
		ID: "wf1", Description: "orig", SourceContents: simpleSource,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Proto snake_case path updates the description.
	w, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "description", Description: "snake",
	})
	if err != nil {
		t.Fatalf("snake_case update: %v", err)
	}
	if w.Description != "snake" {
		t.Errorf("description = %q, want snake", w.Description)
	}

	// Oneof path bumps the revision and replaces the source.
	oldRev := w.RevisionID
	w2, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "source_code.source_contents",
		SourceContents: "main:\n  steps:\n    - r:\n        return: 2\n",
	})
	if err != nil {
		t.Fatalf("oneof update: %v", err)
	}
	if w2.SourceContents == simpleSource {
		t.Errorf("source not updated via oneof path")
	}
	if w2.RevisionID == oldRev {
		t.Errorf("revision should bump when source_contents changes")
	}

	// Unknown path fails loud.
	if _, _, err := s.UpdateWorkflow(ctx, "proj", "us-central1", UpdateInput{
		ID: "wf1", UpdateMask: "bogusField", Description: "x",
	}); err == nil {
		t.Errorf("expected error for unknown updateMask field")
	}
}

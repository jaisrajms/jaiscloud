package eventarc

import (
	"context"
	"encoding/json"
	"testing"

	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// maskStatus returns the HTTP status of a ProviderError, or 0 for any other
// error (including nil).
func maskStatus(err error) int {
	if perr, ok := err.(*model.ProviderError); ok {
		return perr.HTTPStatus
	}
	return 0
}

func TestMaskHelpers(t *testing.T) {
	if maskPaths("") != nil {
		t.Fatal("empty mask should be nil")
	}
	if got := maskPaths(" labels , serviceAccount "); len(got) != 2 || got[0] != "labels" {
		t.Fatalf("maskPaths = %v", got)
	}
	if maskRoot("destination.cloudRun") != "destination" || maskRoot("labels") != "labels" {
		t.Fatal("maskRoot failed")
	}
	if normalizeMaskField("event_filters") != "eventfilters" {
		t.Fatal("normalizeMaskField failed")
	}
	// apply* with an empty mask merges every incoming field.
	merged, err := applyTriggerMask(map[string]any{"a": 1}, map[string]any{"b": 2}, nil)
	if err != nil || merged["a"] != 1 || merged["b"] != 2 {
		t.Fatalf("empty-mask merge = %v, %v", merged, err)
	}
	// A mask root that names no field is a client error (400 InvalidArgument).
	if _, err := applyTriggerMask(nil, nil, []string{"bogus"}); maskStatus(err) != 400 {
		t.Fatalf("unknown trigger mask err = %v, want 400", err)
	}
	if _, err := applyChannelMask(nil, nil, []string{"bogus"}); maskStatus(err) != 400 {
		t.Fatalf("unknown channel mask err = %v, want 400", err)
	}
	// A real-but-unimplemented (output-only) field stays Unimplemented (501).
	if _, err := applyTriggerMask(nil, nil, []string{"conditions"}); maskStatus(err) != 501 {
		t.Fatalf("output-only trigger mask err = %v, want 501", err)
	}
	if _, err := applyChannelMask(nil, nil, []string{"state"}); maskStatus(err) != 501 {
		t.Fatalf("output-only channel mask err = %v, want 501", err)
	}
	if checkEtag("", "x") != nil || checkEtag("x", "x") != nil {
		t.Fatal("checkEtag rejected an empty/matching etag")
	}
	if err := checkEtag("x", "y"); err == nil {
		t.Fatal("checkEtag accepted a mismatched etag")
	}
}

// TestCoreTriggerRoundTrip is a core-level smoke test: create -> get -> list,
// with reference validation against a seeded workflow.
func TestCoreTriggerRoundTrip(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	workflows := workflowsstore.NewMemoryStore()
	if err := workflows.CreateWorkflow(ctx, "proj", "us-central1", "w1", workflowsstore.Workflow{}); err != nil {
		t.Fatalf("seed workflow: %v", err)
	}
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflows)

	body := json.RawMessage(`{"destination":{"workflow":"projects/proj/locations/us-central1/workflows/w1"},
		"eventFilters":[{"attribute":"type","value":"x"}]}`)
	trig, op, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", body, false)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if trig.UID == "" || trig.Etag == "" {
		t.Fatalf("uid/etag not populated: %+v", trig)
	}
	if op.Verb != "create" || op.Target != TriggerName("proj", "us-central1", "t1") {
		t.Fatalf("operation = %+v", op)
	}
	got, err := svc.GetTrigger(ctx, "proj", "us-central1", "t1")
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if got.Name != "t1" || got.UID != trig.UID {
		t.Fatalf("get = %+v", got)
	}
	page, _, err := svc.ListTriggers(ctx, "proj", "us-central1", "", "", 0, "")
	if err != nil || len(page) != 1 {
		t.Fatalf("ListTriggers = %v, %v", page, err)
	}
}

// TestCoreChannelRoundTrip covers channel create/get/delete and provider
// discovery.
func TestCoreChannelRoundTrip(t *testing.T) {
	ctx := context.Background()
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())

	if _, _, err := svc.CreateChannel(ctx, "proj", "us-central1", "c1", json.RawMessage(`{}`), false); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	c, err := svc.GetChannel(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if c.ActivationToken == "" {
		t.Fatalf("activation token not populated: %+v", c)
	}
	if _, _, err := svc.DeleteChannel(ctx, "proj", "us-central1", "c1", "", false); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	if _, err := svc.GetChannel(ctx, "proj", "us-central1", "c1"); err == nil {
		t.Fatal("expected NotFound after delete")
	}
	providers, _, err := svc.ListProviders(ctx, "proj", "us-central1", "", "", 0, "")
	if err != nil || len(providers) == 0 {
		t.Fatalf("ListProviders = %v, %v", providers, err)
	}
	if _, err := svc.GetProvider(ctx, "proj", "us-central1", "pubsub.googleapis.com"); err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	if _, err := svc.GetProvider(ctx, "proj", "us-central1", "nope"); err == nil {
		t.Fatal("expected NotFound for unknown provider")
	}
}

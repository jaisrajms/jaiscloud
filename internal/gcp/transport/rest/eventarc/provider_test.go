package eventarc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/gcp/resource"
	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"github.com/google/uuid"
)

// rtTopic mirrors the core's Pub/Sub topic resource type for test seeding.
const rtTopic = "gcp_topic"

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func newProvider() (*Provider, *store.MemoryResourceStore, *workflowsstore.MemoryStore) {
	resources := store.NewMemoryResourceStore()
	workflows := workflowsstore.NewMemoryStore()
	core := eventarccore.NewService(eventarcstore.NewMemoryStore(), resources, workflows)
	return NewProvider(core, "proj"), resources, workflows
}

func createWorkflow(t *testing.T, workflows *workflowsstore.MemoryStore, location, id string) {
	t.Helper()
	if err := workflows.CreateWorkflow(context.Background(), "proj", location, id, workflowsstore.Workflow{}); err != nil {
		t.Fatalf("create workflow: %v", err)
	}
}

func createTopic(t *testing.T, resources *store.MemoryResourceStore, topicID string) {
	t.Helper()
	if err := resources.Create(context.Background(), "proj", store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: topicID, Data: []byte(`{}`)}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

func triggerBody(workflowName string) map[string]any {
	return map[string]any{
		"destination":  map[string]any{"workflow": workflowName},
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.workflows.workflow.v1.executed"}},
	}
}

func TestTriggerCRUD(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	wfName := "projects/proj/locations/us-central1/workflows/w1"
	createResp, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1", "body": triggerBody(wfName),
	}))
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	if createResp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", createResp.Data["done"])
	}
	created, _ := createResp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/triggers/t1"
	if created["name"] != wantName {
		t.Errorf("name = %v, want %v", created["name"], wantName)
	}
	if created["@type"] != eventarccore.TriggerTypeURL {
		t.Errorf("response @type = %v, want %v", created["@type"], eventarccore.TriggerTypeURL)
	}
	if created["uid"] == "" || created["etag"] == "" {
		t.Errorf("uid/etag not populated: %v", created)
	}

	// Get echoes the trigger with a stable uid/etag.
	got, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if got.Data["name"] != wantName || got.Data["uid"] != created["uid"] || got.Data["etag"] != created["etag"] {
		t.Errorf("get = %v, want name=%v uid=%v etag=%v", got.Data, wantName, created["uid"], created["etag"])
	}

	// List.
	listResp, err := p.ListTriggers(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListTriggers: %v", err)
	}
	if items, _ := listResp.Data["triggers"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(items))
	}

	// Update (labels merge).
	updResp, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("UpdateTrigger: %v", err)
	}
	updated, _ := updResp.Data["response"].(map[string]any)
	if labels, _ := updated["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("updated labels = %v, want env=prod", updated["labels"])
	}

	// Delete.
	delResp, err := p.DeleteTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	if delResp.Data["done"] != true {
		t.Errorf("delete done = %v, want true", delResp.Data["done"])
	}
	deleted, _ := delResp.Data["response"].(map[string]any)
	if deleted["name"] != wantName || deleted["@type"] != eventarccore.TriggerTypeURL {
		t.Errorf("delete response = %v, want name=%v @type=%v", deleted, wantName, eventarccore.TriggerTypeURL)
	}
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

func TestTriggerValidation(t *testing.T) {
	ctx := context.Background()
	p, resources, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	createTopic(t, resources, "my-topic")

	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"missing eventFilters", map[string]any{"destination": map[string]any{"workflow": "projects/proj/locations/us-central1/workflows/w1"}}, "InvalidArgument"},
		{"missing destination", map[string]any{"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}}}, "InvalidArgument"},
		{"filter without type", map[string]any{
			"destination":  map[string]any{"workflow": "projects/proj/locations/us-central1/workflows/w1"},
			"eventFilters": []any{map[string]any{"attribute": "source", "value": "x"}},
		}, "InvalidArgument"},
		{"filter with empty type value", map[string]any{
			"destination":  map[string]any{"workflow": "projects/proj/locations/us-central1/workflows/w1"},
			"eventFilters": []any{map[string]any{"attribute": "type", "value": ""}},
		}, "InvalidArgument"},
		{"destination oneof conflict", map[string]any{
			"destination": map[string]any{
				"cloudFunction": "projects/proj/locations/us-central1/functions/f",
				"cloudRun":      map[string]any{"service": "svc"},
			},
			"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}},
		}, "InvalidArgument"},
		{"malformed cloudFunction name", map[string]any{
			"destination":  map[string]any{"cloudFunction": "bogus"},
			"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}},
		}, "InvalidArgument"},
		{"workflow not found", triggerBody("projects/proj/locations/us-central1/workflows/missing"), "NotFound"},
		{"topic not found", map[string]any{
			"destination":  map[string]any{"cloudRun": map[string]any{"service": "svc", "region": "us-central1"}},
			"eventFilters": []any{map[string]any{"attribute": "type", "value": "x"}},
			"transport":    map[string]any{"pubsub": map[string]any{"topic": "projects/proj/topics/missing"}},
		}, "NotFound"},
	}
	for _, tc := range cases {
		_, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": tc.body}))
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != tc.code {
			t.Errorf("%s: expected %s, got %v", tc.name, tc.code, err)
		}
	}
}

func TestTriggerWithPubsubTopicSucceeds(t *testing.T) {
	ctx := context.Background()
	p, resources, _ := newProvider()
	createTopic(t, resources, "my-topic")

	body := map[string]any{
		"destination":  map[string]any{"cloudRun": map[string]any{"service": "svc", "region": "us-central1"}},
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		"transport":    map[string]any{"pubsub": map[string]any{"topic": "projects/proj/topics/my-topic"}},
	}
	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": body})); err != nil {
		t.Fatalf("CreateTrigger with existing topic: %v", err)
	}
}

func TestTriggerWithCloudFunctionDestination(t *testing.T) {
	ctx := context.Background()
	p, resources, _ := newProvider()
	createTopic(t, resources, "my-topic")

	body := map[string]any{
		"destination":  map[string]any{"cloudFunction": "projects/proj/locations/us-central1/functions/a-function"},
		"eventFilters": []any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		"transport":    map[string]any{"pubsub": map[string]any{"topic": "projects/proj/topics/my-topic"}},
	}
	// destination.cloudFunction is a documented Eventarc destination and is
	// accepted; with no FunctionExister wired the existence check is skipped.
	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": body})); err != nil {
		t.Fatalf("CreateTrigger with a cloudFunction destination: %v", err)
	}
}

func TestTriggerMissingParams(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1"})); err == nil {
		t.Fatal("expected InvalidArgument for missing triggerId, got nil")
	}
}

func TestTriggerAlreadyExists(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	params := map[string]any{"location": "us-central1", "triggerId": "t1", "body": triggerBody("projects/proj/locations/us-central1/workflows/w1")}
	if _, err := p.CreateTrigger(ctx, newNR(params)); err != nil {
		t.Fatalf("first CreateTrigger: %v", err)
	}
	_, err := p.CreateTrigger(ctx, newNR(params))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "AlreadyExists" {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
}

func TestChannelCRUD(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	createResp, err := p.CreateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "channelId": "c1",
		"body": map[string]any{"provider": "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	}))
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if createResp.Data["done"] != true {
		t.Fatalf("expected done=true, got %v", createResp.Data["done"])
	}
	created, _ := createResp.Data["response"].(map[string]any)
	wantName := "projects/proj/locations/us-central1/channels/c1"
	if created["name"] != wantName {
		t.Errorf("name = %v, want %v", created["name"], wantName)
	}
	if created["@type"] != eventarccore.ChannelTypeURL {
		t.Errorf("response @type = %v, want %v", created["@type"], eventarccore.ChannelTypeURL)
	}
	if created["uid"] == "" || created["activationToken"] == "" || created["pubsubTopic"] == "" || created["state"] != "PENDING" {
		t.Errorf("output-only channel fields missing/incorrect: %v", created)
	}
	if created["etag"] == "" {
		t.Errorf("channel etag not populated: %v", created)
	}

	got, err := p.GetChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"}))
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if got.Data["activationToken"] != created["activationToken"] {
		t.Errorf("activationToken not stable across reads: %v", got.Data)
	}

	listResp, err := p.ListChannels(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListChannels: %v", err)
	}
	if items, _ := listResp.Data["channels"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(items))
	}

	delResp, err := p.DeleteChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"}))
	if err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	deleted, _ := delResp.Data["response"].(map[string]any)
	if deleted["name"] != wantName || deleted["@type"] != eventarccore.ChannelTypeURL {
		t.Errorf("delete response = %v, want name=%v @type=%v", deleted, wantName, eventarccore.ChannelTypeURL)
	}
	if _, err := p.GetChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"})); err == nil {
		t.Fatal("expected NotFound after delete, got nil error")
	}
}

func TestProviders(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()

	listResp, err := p.ListProviders(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	providers, _ := listResp.Data["providers"].([]any)
	if len(providers) < 2 {
		t.Fatalf("expected at least 2 providers, got %d", len(providers))
	}
	first, _ := providers[0].(map[string]any)
	if first["name"] != "projects/proj/locations/us-central1/providers/pubsub.googleapis.com" {
		t.Errorf("first provider name = %v", first["name"])
	}
	if first["displayName"] != "Cloud Pub/Sub" {
		t.Errorf("first provider displayName = %v", first["displayName"])
	}

	got, err := p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/providers/storage.googleapis.com"}))
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}
	if got.Data["displayName"] != "Cloud Storage" {
		t.Errorf("displayName = %v, want Cloud Storage", got.Data["displayName"])
	}

	if _, err := p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/providers/does-not-exist.googleapis.com"})); err == nil {
		t.Fatal("expected NotFound for unknown provider, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func TestTriggerIamTrio(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1",
		"body": triggerBody("projects/proj/locations/us-central1/workflows/w1"),
	})); err != nil {
		t.Fatalf("seed trigger: %v", err)
	}
	key := map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}

	// Default empty policy for an existing trigger.
	got, err := p.TriggerGetIamPolicy(ctx, newNR(key))
	if err != nil {
		t.Fatalf("TriggerGetIamPolicy: %v", err)
	}
	if got.Data["etag"] == "" {
		t.Errorf("default policy etag empty: %v", got.Data)
	}

	// Set then read back.
	setBody := map[string]any{"policy": map[string]any{"bindings": []any{
		map[string]any{"role": "roles/eventarc.viewer", "members": []any{"user:a@example.com"}},
	}}}
	if _, err := p.TriggerSetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1", "body": setBody})); err != nil {
		t.Fatalf("TriggerSetIamPolicy: %v", err)
	}
	got, err = p.TriggerGetIamPolicy(ctx, newNR(key))
	if err != nil {
		t.Fatalf("TriggerGetIamPolicy after set: %v", err)
	}
	bindings, _ := got.Data["bindings"].([]any)
	if len(bindings) != 1 {
		t.Errorf("expected 1 binding, got %v", got.Data["bindings"])
	}

	// testIamPermissions echoes the requested permissions.
	perm, err := p.TriggerTestIamPermissions(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"permissions": []any{"eventarc.triggers.get", "eventarc.triggers.update"}},
	}))
	if err != nil {
		t.Fatalf("TriggerTestIamPermissions: %v", err)
	}
	if perms, _ := perm.Data["permissions"].([]string); len(perms) != 2 {
		t.Errorf("expected 2 permissions, got %v", perm.Data["permissions"])
	}

	// Missing trigger → NotFound; missing name → InvalidArgument.
	if _, err := p.TriggerGetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/missing"})); err == nil {
		t.Error("expected NotFound for missing trigger, got nil")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "NotFound" {
		t.Errorf("expected NotFound, got %v", err)
	}
	if _, err := p.TriggerGetIamPolicy(ctx, newNR(nil)); err == nil {
		t.Error("expected InvalidArgument for missing name, got nil")
	}
}

func TestChannelIamTrio(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()
	if _, err := p.CreateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "channelId": "c1",
		"body": map[string]any{"provider": "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	})); err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	key := map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"}

	if _, err := p.ChannelGetIamPolicy(ctx, newNR(key)); err != nil {
		t.Fatalf("ChannelGetIamPolicy: %v", err)
	}
	setBody := map[string]any{"policy": map[string]any{"bindings": []any{
		map[string]any{"role": "roles/eventarc.viewer", "members": []any{"user:a@example.com"}},
	}}}
	if _, err := p.ChannelSetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1", "body": setBody})); err != nil {
		t.Fatalf("ChannelSetIamPolicy: %v", err)
	}
	got, err := p.ChannelGetIamPolicy(ctx, newNR(key))
	if err != nil {
		t.Fatalf("ChannelGetIamPolicy after set: %v", err)
	}
	if bindings, _ := got.Data["bindings"].([]any); len(bindings) != 1 {
		t.Errorf("expected 1 binding, got %v", got.Data["bindings"])
	}
	perm, err := p.ChannelTestIamPermissions(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"body": map[string]any{"permissions": []any{"eventarc.channels.get"}},
	}))
	if err != nil {
		t.Fatalf("ChannelTestIamPermissions: %v", err)
	}
	if perms, _ := perm.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("expected 1 permission, got %v", perm.Data["permissions"])
	}
	if _, err := p.ChannelSetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/missing"})); err == nil {
		t.Error("expected NotFound for missing channel, got nil")
	}
	if _, err := p.ChannelTestIamPermissions(ctx, newNR(nil)); err == nil {
		t.Error("expected InvalidArgument for missing name, got nil")
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")

	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t1", "body": triggerBody("projects/proj/locations/us-central1/workflows/w1")})); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	p.Reset(ctx)
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("expected NotFound after Reset, got nil error")
	}
}

func TestRoutes_AllHandlersRegistered(t *testing.T) {
	p, _, _ := newProvider()
	routes := p.Routes()
	want := []string{
		"Eventarc.CreateTrigger", "Eventarc.GetTrigger", "Eventarc.ListTriggers",
		"Eventarc.UpdateTrigger", "Eventarc.DeleteTrigger",
		"Eventarc.CreateChannel", "Eventarc.GetChannel", "Eventarc.ListChannels",
		"Eventarc.UpdateChannel", "Eventarc.DeleteChannel",
		"Eventarc.ListProviders", "Eventarc.GetProvider",
		"Eventarc.TriggerGetIamPolicy", "Eventarc.TriggerSetIamPolicy", "Eventarc.TriggerTestIamPermissions",
		"Eventarc.ChannelGetIamPolicy", "Eventarc.ChannelSetIamPolicy", "Eventarc.ChannelTestIamPermissions",
	}
	for _, k := range want {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("got %d routes, want %d", len(routes), len(want))
	}
}

// --- helpers for the review-fix tests ---

func respResource(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	r, ok := resp.Data["response"].(map[string]any)
	if !ok {
		t.Fatalf("LRO carried no response resource: %v", resp.Data)
	}
	return r
}

func seedTrigger(t *testing.T, p *Provider, workflows *workflowsstore.MemoryStore) map[string]any {
	t.Helper()
	ctx := context.Background()
	createWorkflow(t, workflows, "us-central1", "w1")
	resp, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1",
		"body": map[string]any{
			"destination":    map[string]any{"workflow": "projects/proj/locations/us-central1/workflows/w1"},
			"eventFilters":   []any{map[string]any{"attribute": "type", "value": "x"}},
			"serviceAccount": "orig@proj.iam.gserviceaccount.com",
		},
	}))
	if err != nil {
		t.Fatalf("seed trigger: %v", err)
	}
	return respResource(t, resp)
}

func seedChannel(t *testing.T, p *Provider) map[string]any {
	t.Helper()
	resp, err := p.CreateChannel(context.Background(), newNR(map[string]any{
		"location": "us-central1", "channelId": "c1",
		"body": map[string]any{"provider": "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	}))
	if err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	return respResource(t, resp)
}

func expectProviderError(t *testing.T, err error, code string, status int) {
	t.Helper()
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *ProviderError %s, got %T: %v", code, err, err)
	}
	if perr.Code != code || perr.HTTPStatus != status {
		t.Fatalf("expected %s/%d, got %s/%d (%v)", code, status, perr.Code, perr.HTTPStatus, err)
	}
}

// TestUpdateTriggerMasked proves a masked PATCH applies only the named field:
// the body's unmasked destination must not clobber the stored destination.
func TestUpdateTriggerMasked(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	seedTrigger(t, p, workflows)

	for _, mask := range []string{"serviceAccount", "service_account"} {
		resp, err := p.UpdateTrigger(ctx, newNR(map[string]any{
			"location": "us-central1", "name": "locations/us-central1/triggers/t1",
			"updateMask": mask,
			"body": map[string]any{
				"serviceAccount": "new@proj.iam.gserviceaccount.com",
				"destination":    map[string]any{"cloudRun": map[string]any{"service": "other", "region": "us-central1"}},
			},
		}))
		if err != nil {
			t.Fatalf("masked UpdateTrigger (%s): %v", mask, err)
		}
		updated := respResource(t, resp)
		if updated["serviceAccount"] != "new@proj.iam.gserviceaccount.com" {
			t.Fatalf("masked serviceAccount not applied: %v", updated)
		}
		dest, _ := updated["destination"].(map[string]any)
		if wf, _ := dest["workflow"].(string); wf != "projects/proj/locations/us-central1/workflows/w1" {
			t.Fatalf("unmasked destination was clobbered by the body: %v", dest)
		}
	}
}

func TestUpdateTriggerMaskUnsupported(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	seedTrigger(t, p, workflows)
	_, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"updateMask": "uid",
		"body":       map[string]any{"serviceAccount": "x"},
	}))
	expectProviderError(t, err, "UnsupportedOperation", 501)
}

func TestUpdateChannelMaskedRoundTrip(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()
	seedChannel(t, p)

	// Unmasked patch merges present fields (pre-existing behavior).
	resp, err := p.UpdateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"body": map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("UpdateChannel: %v", err)
	}
	if labels, _ := respResource(t, resp)["labels"].(map[string]string); labels["env"] != "prod" {
		t.Fatalf("channel labels not merged: %v", respResource(t, resp))
	}

	// Masked patch applies only cryptoKeyName; the body's provider change is ignored.
	resp, err = p.UpdateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"updateMask": "cryptoKeyName",
		"body": map[string]any{
			"cryptoKeyName": "projects/proj/locations/us-central1/keyRings/r/cryptoKeys/k",
			"provider":      "projects/proj/locations/us-central1/providers/storage.googleapis.com",
		},
	}))
	if err != nil {
		t.Fatalf("masked UpdateChannel: %v", err)
	}
	updated := respResource(t, resp)
	if updated["cryptoKeyName"] != "projects/proj/locations/us-central1/keyRings/r/cryptoKeys/k" {
		t.Fatalf("masked cryptoKeyName not applied: %v", updated)
	}
	if updated["provider"] != "projects/proj/locations/us-central1/providers/pubsub.googleapis.com" {
		t.Fatalf("unmasked provider clobbered: %v", updated)
	}

	// Unsupported mask path fails loud.
	_, err = p.UpdateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"updateMask": "pubsubTopic",
		"body":       map[string]any{"cryptoKeyName": "x"},
	}))
	expectProviderError(t, err, "UnsupportedOperation", 501)
}

// TestTriggerEtagOCC exercises the etag optimistic-concurrency contract:
// rotate on update, reject stale etags on patch and delete with 409 ABORTED.
func TestTriggerEtagOCC(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	created := seedTrigger(t, p, workflows)
	etag1 := created["etag"].(string)
	key := func() map[string]any {
		return map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}
	}

	resp, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"etag": etag1, "serviceAccount": "changed@proj.iam.gserviceaccount.com"},
	}))
	if err != nil {
		t.Fatalf("update with fresh etag: %v", err)
	}
	etag2, _ := respResource(t, resp)["etag"].(string)
	if etag2 == "" || etag2 == etag1 {
		t.Fatalf("etag did not rotate on update: %q -> %q", etag1, etag2)
	}

	// Stale etag on patch → 409 ABORTED.
	_, err = p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"etag": etag1, "serviceAccount": "stale@proj.iam.gserviceaccount.com"},
	}))
	expectProviderError(t, err, "Aborted", 409)
	if perr := err.(*model.ProviderError); perr.Status != "ABORTED" {
		t.Fatalf("expected google.rpc status ABORTED, got %q", perr.Status)
	}

	// The rejected write must not have landed.
	got, err := p.GetTrigger(ctx, newNR(key()))
	if err != nil {
		t.Fatalf("get after rejected patch: %v", err)
	}
	if got.Data["serviceAccount"] != "changed@proj.iam.gserviceaccount.com" {
		t.Fatalf("rejected patch was persisted: %v", got.Data["serviceAccount"])
	}

	// Stale etag on delete → 409; fresh etag succeeds.
	_, err = p.DeleteTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1", "etag": etag1}))
	expectProviderError(t, err, "Aborted", 409)
	if _, err := p.DeleteTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1", "etag": etag2})); err != nil {
		t.Fatalf("delete with fresh etag: %v", err)
	}
	if _, err := p.GetTrigger(ctx, newNR(key())); err == nil {
		t.Fatal("trigger still present after delete")
	}
}

func TestChannelEtagOCC(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()
	created := seedChannel(t, p)
	etag1 := created["etag"].(string)

	resp, err := p.UpdateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"body": map[string]any{"etag": etag1, "labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("update with fresh etag: %v", err)
	}
	etag2, _ := respResource(t, resp)["etag"].(string)
	if etag2 == "" || etag2 == etag1 {
		t.Fatalf("channel etag did not rotate: %q -> %q", etag1, etag2)
	}
	_, err = p.UpdateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/channels/c1",
		"body": map[string]any{"etag": etag1, "labels": map[string]any{"env": "stale"}},
	}))
	expectProviderError(t, err, "Aborted", 409)
	_, err = p.DeleteChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1", "etag": etag1}))
	expectProviderError(t, err, "Aborted", 409)
	if _, err := p.DeleteChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1", "etag": etag2})); err != nil {
		t.Fatalf("delete with fresh etag: %v", err)
	}
}

// TestValidateOnly proves validateOnly=true validates then skips the write.
func TestValidateOnly(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	body := triggerBody("projects/proj/locations/us-central1/workflows/w1")

	// Create validate-only does not persist.
	resp, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1", "validateOnly": "true", "body": body,
	}))
	if err != nil {
		t.Fatalf("validateOnly create: %v", err)
	}
	if respResource(t, resp)["name"] != "projects/proj/locations/us-central1/triggers/t1" {
		t.Fatalf("validateOnly create response: %v", resp.Data)
	}
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("validateOnly create persisted the trigger")
	}

	// Real create, then validateOnly duplicate → AlreadyExists.
	if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t1", "body": body})); err != nil {
		t.Fatalf("real create: %v", err)
	}
	_, err = p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t1", "validateOnly": "true", "body": body}))
	expectProviderError(t, err, "AlreadyExists", 409)

	// An invalid validate-only create still fails validation.
	_, err = p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "bad", "validateOnly": "true", "body": map[string]any{}}))
	expectProviderError(t, err, "InvalidArgument", 400)

	// Update validate-only does not persist.
	if _, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"validateOnly": "true", "body": map[string]any{"serviceAccount": "vo@proj.iam.gserviceaccount.com"},
	})); err != nil {
		t.Fatalf("validateOnly update: %v", err)
	}
	got, _ := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if sa, _ := got.Data["serviceAccount"].(string); sa != "" {
		t.Fatalf("validateOnly update persisted serviceAccount=%q", sa)
	}

	// Delete validate-only leaves the trigger in place.
	if _, err := p.DeleteTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1", "validateOnly": "true",
	})); err != nil {
		t.Fatalf("validateOnly delete: %v", err)
	}
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err != nil {
		t.Fatal("validateOnly delete removed the trigger")
	}

	// Channel create validate-only does not persist.
	cresp, err := p.CreateChannel(ctx, newNR(map[string]any{
		"location": "us-central1", "channelId": "c1", "validateOnly": "true",
		"body": map[string]any{"provider": "projects/proj/locations/us-central1/providers/pubsub.googleapis.com"},
	}))
	if err != nil {
		t.Fatalf("validateOnly channel create: %v", err)
	}
	if respResource(t, cresp)["state"] != "PENDING" {
		t.Fatalf("validateOnly channel response: %v", cresp.Data)
	}
	if _, err := p.GetChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"})); err == nil {
		t.Fatal("validateOnly channel create persisted")
	}

	// Channel update/delete validate-only paths.
	if _, err := p.CreateChannel(ctx, newNR(map[string]any{"location": "us-central1", "channelId": "c1", "body": map[string]any{"provider": "p"}})); err != nil {
		t.Fatalf("real channel create: %v", err)
	}
	ch := map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1"}
	if _, err := p.UpdateChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1", "validateOnly": "true", "body": map[string]any{"labels": map[string]any{"a": "b"}}})); err != nil {
		t.Fatalf("validateOnly channel update: %v", err)
	}
	if _, err := p.DeleteChannel(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/c1", "validateOnly": "true"})); err != nil {
		t.Fatalf("validateOnly channel delete: %v", err)
	}
	if _, err := p.GetChannel(ctx, newNR(ch)); err != nil {
		t.Fatal("validateOnly channel delete removed the channel")
	}
}

func TestPagination(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	body := triggerBody("projects/proj/locations/us-central1/workflows/w1")
	for _, id := range []string{"t1", "t2", "t3"} {
		if _, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": id, "body": body})); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	first, err := p.ListTriggers(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("ListTriggers page 1: %v", err)
	}
	token, _ := first.Data["nextPageToken"].(string)
	if len(first.Data["triggers"].([]any)) != 2 || token == "" {
		t.Fatalf("page 1 = %v, token=%q", first.Data, token)
	}
	second, err := p.ListTriggers(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("ListTriggers page 2: %v", err)
	}
	if len(second.Data["triggers"].([]any)) != 1 || second.Data["nextPageToken"] != nil {
		t.Fatalf("page 2 = %v", second.Data)
	}

	for _, id := range []string{"c1", "c2", "c3"} {
		if _, err := p.CreateChannel(ctx, newNR(map[string]any{"location": "us-central1", "channelId": id, "body": map[string]any{"provider": "p"}})); err != nil {
			t.Fatalf("create channel %s: %v", id, err)
		}
	}
	cfirst, err := p.ListChannels(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("ListChannels page 1: %v", err)
	}
	if len(cfirst.Data["channels"].([]any)) != 2 || cfirst.Data["nextPageToken"] == nil {
		t.Fatalf("channel page 1 = %v", cfirst.Data)
	}
}

func TestUidIsUUID4(t *testing.T) {
	p, _, workflows := newProvider()
	tr := seedTrigger(t, p, workflows)
	for _, tc := range []struct {
		what, uid string
	}{
		{"trigger", tr["uid"].(string)},
		{"channel", seedChannel(t, p)["uid"].(string)},
	} {
		u, err := uuid.Parse(tc.uid)
		if err != nil {
			t.Fatalf("%s uid %q is not a UUID: %v", tc.what, tc.uid, err)
		}
		if u.Version() != 4 {
			t.Fatalf("%s uid %q is UUID v%d, want v4", tc.what, tc.uid, u.Version())
		}
	}
}

func TestNotFoundAndArgumentEdges(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()

	trig := map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/missing"}
	chanName := map[string]any{"location": "us-central1", "name": "locations/us-central1/channels/missing"}

	_, err := p.GetTrigger(ctx, newNR(trig))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.UpdateTrigger(ctx, newNR(trig))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.DeleteTrigger(ctx, newNR(trig))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.GetChannel(ctx, newNR(chanName))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.UpdateChannel(ctx, newNR(chanName))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.DeleteChannel(ctx, newNR(chanName))
	expectProviderError(t, err, "NotFound", 404)

	_, err = p.ListTriggers(ctx, newNR(nil))
	expectProviderError(t, err, "InvalidArgument", 400)
	_, err = p.ListChannels(ctx, newNR(nil))
	expectProviderError(t, err, "InvalidArgument", 400)
	_, err = p.ListProviders(ctx, newNR(nil))
	expectProviderError(t, err, "InvalidArgument", 400)
	_, err = p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/providers/unknown.googleapis.com"}))
	expectProviderError(t, err, "NotFound", 404)
	_, err = p.CreateChannel(ctx, newNR(map[string]any{"location": "us-central1"}))
	expectProviderError(t, err, "InvalidArgument", 400)

	// nil-body CreateTrigger / CreateChannel both fail validation.
	createWorkflow(t, workflows, "us-central1", "w1")
	_, err = p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t1"}))
	expectProviderError(t, err, "InvalidArgument", 400)
	_, err = p.CreateChannel(ctx, newNR(map[string]any{"location": "us-central1", "channelId": "c1"}))
	expectProviderError(t, err, "InvalidArgument", 400)
}

func TestUpdateTriggerInvalidReference(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	seedTrigger(t, p, workflows)
	_, err := p.UpdateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "name": "locations/us-central1/triggers/t1",
		"body": map[string]any{"channel": "projects/proj/locations/us-central1/channels/nope"},
	}))
	expectProviderError(t, err, "NotFound", 404)
}

// delayedGetTriggerStore widens the TOCTOU window for any regression that
// reintroduces a standalone GetTrigger before the atomic update.
type delayedGetTriggerStore struct {
	eventarcstore.Store
	delay time.Duration
}

func (d *delayedGetTriggerStore) GetTrigger(ctx context.Context, projectID, location, id string) (eventarcstore.Trigger, error) {
	t, err := d.Store.GetTrigger(ctx, projectID, location, id)
	time.Sleep(d.delay)
	return t, err
}

// TestConcurrentDisjointMaskedUpdatesNoLostUpdate races 25 labels-only PATCHes
// against 25 serviceAccount-only PATCHes. Because each merge runs inside the
// store's locked mutate closure, both fields must survive.
func TestConcurrentDisjointMaskedUpdatesNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	workflows := workflowsstore.NewMemoryStore()
	p := NewProvider(eventarccore.NewService(&delayedGetTriggerStore{Store: eventarcstore.NewMemoryStore(), delay: 2 * time.Millisecond}, resources, workflows), "proj")
	seedTrigger(t, p, workflows)

	const perField = 25
	var wg sync.WaitGroup
	errs := make([]error, 2*perField)
	for i := 0; i < perField; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = p.UpdateTrigger(ctx, newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/triggers/t1",
				"updateMask": "labels",
				"body":       map[string]any{"labels": map[string]any{"env": fmt.Sprintf("v%d", i)}},
			}))
		}(i)
		go func(i int) {
			defer wg.Done()
			_, errs[perField+i] = p.UpdateTrigger(ctx, newNR(map[string]any{
				"location": "us-central1", "name": "locations/us-central1/triggers/t1",
				"updateMask": "serviceAccount",
				"body":       map[string]any{"serviceAccount": fmt.Sprintf("v%d@proj.iam.gserviceaccount.com", i)},
			}))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}
	got, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	labels, _ := got.Data["labels"].(map[string]string)
	if !strings.HasPrefix(labels["env"], "v") {
		t.Fatalf("labels lost the concurrent update: %v", labels)
	}
	if sa, _ := got.Data["serviceAccount"].(string); !strings.HasPrefix(sa, "v") {
		t.Fatalf("serviceAccount lost the concurrent update: %q", sa)
	}
}

func TestValidationEdges(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	wf := "projects/proj/locations/us-central1/workflows/w1"
	typeFilter := []any{map[string]any{"attribute": "type", "value": "x"}}

	cases := []struct {
		name string
		body map[string]any
	}{
		{"filter entry not object", map[string]any{"destination": map[string]any{"workflow": wf}, "eventFilters": []any{"not-a-map"}}},
		{"filter missing attribute", map[string]any{"destination": map[string]any{"workflow": wf}, "eventFilters": []any{map[string]any{"value": "x"}}}},
		{"filter non-string value", map[string]any{"destination": map[string]any{"workflow": wf}, "eventFilters": []any{map[string]any{"attribute": "type", "value": 123}}}},
		{"two destinations set", map[string]any{
			"destination":  map[string]any{"cloudRun": map[string]any{"service": "s"}, "gke": map[string]any{"cluster": "c"}},
			"eventFilters": typeFilter,
		}},
		{"invalid workflow name", map[string]any{"destination": map[string]any{"workflow": "bogus"}, "eventFilters": typeFilter}},
		{"invalid channel name", map[string]any{"destination": map[string]any{"workflow": wf}, "eventFilters": typeFilter, "channel": "bogus"}},
	}
	for _, tc := range cases {
		_, err := p.CreateTrigger(ctx, newNR(map[string]any{"location": "us-central1", "triggerId": "t", "body": tc.body}))
		expectProviderError(t, err, "InvalidArgument", 400)
	}
}

func TestHandlerMissingParams(t *testing.T) {
	ctx := context.Background()
	p, _, _ := newProvider()
	for name, fn := range map[string]func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error){
		"GetTrigger":    p.GetTrigger,
		"UpdateTrigger": p.UpdateTrigger,
		"DeleteTrigger": p.DeleteTrigger,
		"GetChannel":    p.GetChannel,
		"UpdateChannel": p.UpdateChannel,
		"DeleteChannel": p.DeleteChannel,
	} {
		_, err := fn(ctx, newNR(map[string]any{"location": "us-central1"}))
		if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
			t.Errorf("%s: expected InvalidArgument, got %v", name, err)
		}
	}
	if _, err := p.CreateChannel(ctx, newNR(map[string]any{"channelId": "c1"})); err == nil {
		t.Error("CreateChannel without location: expected InvalidArgument")
	}
	if _, err := p.GetProvider(ctx, newNR(map[string]any{"location": "us-central1"})); err == nil {
		t.Error("GetProvider without name: expected InvalidArgument")
	}
}

// TestIamPolicyStaleEtag exercises the provider-level wrapper's error branch:
// the shared policy package enforces etag OCC on setIamPolicy.
func TestIamPolicyStaleEtag(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	seedTrigger(t, p, workflows)
	set := func(etag, member string) error {
		policyBody := map[string]any{
			"bindings": []any{map[string]any{"role": "roles/eventarc.viewer", "members": []any{member}}},
		}
		if etag != "" {
			policyBody["etag"] = etag
		}
		_, err := p.TriggerSetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1", "body": map[string]any{"policy": policyBody}}))
		return err
	}
	if err := set("", "user:a@example.com"); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	got, err := p.TriggerGetIamPolicy(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	etag1 := got.Data["etag"].(string)
	if err := set(etag1, "user:b@example.com"); err != nil {
		t.Fatalf("matching etag set: %v", err)
	}
	if err := set(etag1, "user:c@example.com"); err == nil {
		t.Fatal("expected stale-etag setIamPolicy to fail")
	}
}

func TestBoolParamNativeBool(t *testing.T) {
	ctx := context.Background()
	p, _, workflows := newProvider()
	createWorkflow(t, workflows, "us-central1", "w1")
	_, err := p.CreateTrigger(ctx, newNR(map[string]any{
		"location": "us-central1", "triggerId": "t1", "validateOnly": true,
		"body": triggerBody("projects/proj/locations/us-central1/workflows/w1"),
	}))
	if err != nil {
		t.Fatalf("validateOnly bool create: %v", err)
	}
	if _, err := p.GetTrigger(ctx, newNR(map[string]any{"location": "us-central1", "name": "locations/us-central1/triggers/t1"})); err == nil {
		t.Fatal("native-bool validateOnly create persisted")
	}
}

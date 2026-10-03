package memorystore

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     params,
		ResourceID: resource.ResourceID("proj"),
	}
}

func newProvider() *Provider { return New(store.NewMemoryResourceStore()) }

func instanceBody(display string) map[string]any {
	return map[string]any{
		"displayName":  display,
		"tier":         "BASIC",
		"memorySizeGb": 1,
		"labels":       map[string]any{"env": "test"},
	}
}

func mustCreate(t *testing.T, p *Provider, location, id string) map[string]any {
	t.Helper()
	resp, err := p.CreateInstance(context.Background(), newNR(map[string]any{
		"location":   location,
		"instanceId": id,
		"body":       instanceBody("primary"),
	}))
	if err != nil {
		t.Fatalf("create instance: %v", err)
	}
	return resp.Data
}

func instanceResponse(t *testing.T, op map[string]any) map[string]any {
	t.Helper()
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("operation is not done: %v", op)
	}
	resp, ok := op["response"].(map[string]any)
	if !ok {
		t.Fatalf("operation has no response object: %v", op)
	}
	return resp
}

func TestInstanceRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	op := mustCreate(t, p, "us-central1", "inst-a")
	inst := instanceResponse(t, op)
	if inst["name"] != "projects/proj/locations/us-central1/instances/inst-a" {
		t.Errorf("name = %v", inst["name"])
	}
	if inst["state"] != "READY" {
		t.Errorf("state = %v, want READY", inst["state"])
	}
	if inst["tier"] != "BASIC" {
		t.Errorf("tier = %v", inst["tier"])
	}
	if inst["host"] == "" || inst["host"] == nil {
		t.Errorf("expected a synthesized host, got %v", inst["host"])
	}
	if inst["port"] != float64(6379) {
		t.Errorf("port = %v, want 6379", inst["port"])
	}
	if inst["locationId"] != "us-central1" || inst["currentLocationId"] != "us-central1" {
		t.Errorf("location ids = %v / %v", inst["locationId"], inst["currentLocationId"])
	}
	if inst["createTime"] == "" || inst["createTime"] == nil {
		t.Errorf("expected createTime, got %v", inst["createTime"])
	}
	if inst["id"] == "" || inst["id"] == nil {
		t.Errorf("expected a synthesized id, got %v", inst["id"])
	}
	if op["name"] == "" || op["name"] == nil {
		t.Errorf("operation name is empty: %v", op)
	}
	if md, ok := op["metadata"].(map[string]any); !ok || md["verb"] != "create" {
		t.Errorf("unexpected operation metadata: %v", op["metadata"])
	}

	got, err := p.GetInstance(ctx, newNR(map[string]any{"location": "us-central1", "instanceId": "inst-a"}))
	if err != nil {
		t.Fatalf("get instance: %v", err)
	}
	if got.Data["id"] != inst["id"] {
		t.Errorf("id not stable across reads: %v vs %v", got.Data["id"], inst["id"])
	}

	list, err := p.ListInstances(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	items, _ := list.Data["instances"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(items))
	}

	delOp, err := p.DeleteInstance(ctx, newNR(map[string]any{"location": "us-central1", "instanceId": "inst-a"}))
	if err != nil {
		t.Fatalf("delete instance: %v", err)
	}
	if done, _ := delOp.Data["done"].(bool); !done {
		t.Errorf("delete operation not done: %v", delOp.Data)
	}
	if _, err := p.GetInstance(ctx, newNR(map[string]any{"location": "us-central1", "instanceId": "inst-a"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestInstanceAlreadyExistsAndNotFound(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-a")

	_, err := p.CreateInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a", "body": instanceBody("dup"),
	}))
	if !isCode(err, "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}

	if _, err := p.GetInstance(ctx, newNR(map[string]any{"location": "us-central1", "instanceId": "nope"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound, got %v", err)
	}
	if _, err := p.DeleteInstance(ctx, newNR(map[string]any{"location": "us-central1", "instanceId": "nope"})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on delete, got %v", err)
	}
	if _, err := p.UpdateInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "nope", "body": map[string]any{"displayName": "x"},
	})); !isCode(err, "NotFound") {
		t.Fatalf("expected NotFound on update, got %v", err)
	}
}

func TestCreateValidatesTier(t *testing.T) {
	p := newProvider()
	_, err := p.CreateInstance(context.Background(), newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a",
		"body": map[string]any{"tier": "PREMIUM"},
	}))
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for bad tier, got %v", err)
	}
}

func TestUpdateAppliesMask(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-a")

	op, err := p.UpdateInstance(ctx, newNR(map[string]any{
		"location":   "us-central1",
		"instanceId": "inst-a",
		"updateMask": "displayName,memorySizeGb",
		"body": map[string]any{
			"displayName":  "renamed",
			"memorySizeGb": 4,
			"tier":         "STANDARD_HA", // not masked: must be retained
		},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	inst := instanceResponse(t, op.Data)
	if inst["displayName"] != "renamed" {
		t.Errorf("displayName = %v", inst["displayName"])
	}
	if inst["memorySizeGb"] != float64(4) {
		t.Errorf("memorySizeGb = %v", inst["memorySizeGb"])
	}
	if inst["tier"] != "BASIC" {
		t.Errorf("unmasked tier must be retained, got %v", inst["tier"])
	}

	// Unsupported mask paths fail loud.
	_, err = p.UpdateInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a",
		"updateMask": "authorizedNetwork",
		"body":       map[string]any{"displayName": "x"},
	}))
	if !isCode(err, "InvalidArgument") {
		t.Fatalf("expected InvalidArgument for unsupported mask, got %v", err)
	}
}

func TestUpdateIsAtomicPreservesConcurrentFields(t *testing.T) {
	// The update reads, merges, and writes under UpsertAtomic; a subsequent
	// masked update must not wipe fields set by the previous one.
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-a")

	if _, err := p.UpdateInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a",
		"updateMask": "labels", "body": map[string]any{"labels": map[string]any{"team": "core"}},
	})); err != nil {
		t.Fatalf("first update: %v", err)
	}
	op, err := p.UpdateInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a",
		"updateMask": "displayName", "body": map[string]any{"displayName": "renamed"},
	}))
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	inst := instanceResponse(t, op.Data)
	labels, _ := inst["labels"].(map[string]any)
	if labels["team"] != "core" {
		t.Errorf("labels must survive an unmasked update, got %v", inst["labels"])
	}
}

func TestUpgradeInstance(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-a")

	op, err := p.UpgradeInstance(ctx, newNR(map[string]any{
		"location": "us-central1", "instanceId": "inst-a",
		"body": map[string]any{"redisVersion": "REDIS_7_2"},
	}))
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	inst := instanceResponse(t, op.Data)
	if inst["redisVersion"] != "REDIS_7_2" {
		t.Errorf("redisVersion = %v", inst["redisVersion"])
	}
	if md, _ := op.Data["metadata"].(map[string]any); md["verb"] != "upgrade" {
		t.Errorf("verb = %v, want upgrade", md["verb"])
	}
}

func TestListPagination(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	for _, id := range []string{"i1", "i2", "i3"} {
		mustCreate(t, p, "us-central1", id)
	}

	first, err := p.ListInstances(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if items, _ := first.Data["instances"].([]any); len(items) != 2 {
		t.Fatalf("page 1 size = %d, want 2", len(items))
	}
	token, _ := first.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected a nextPageToken")
	}
	second, err := p.ListInstances(ctx, newNR(map[string]any{"location": "us-central1", "pageSize": "2", "pageToken": token}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if items, _ := second.Data["instances"].([]any); len(items) != 1 {
		t.Fatalf("page 2 size = %d, want 1", len(items))
	}
	if _, ok := second.Data["nextPageToken"]; ok {
		t.Error("last page must not carry a nextPageToken")
	}
}

func TestListIsLocationScoped(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "a")
	mustCreate(t, p, "europe-west1", "b")

	list, err := p.ListInstances(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := list.Data["instances"].([]any); len(items) != 1 {
		t.Fatalf("expected only the us-central1 instance, got %d", len(items))
	}
}

func TestLocations(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	loc, err := p.GetLocation(ctx, newNR(map[string]any{"location": "us-central1"}))
	if err != nil {
		t.Fatalf("get location: %v", err)
	}
	if loc.Data["name"] != "projects/proj/locations/us-central1" || loc.Data["locationId"] != "us-central1" {
		t.Errorf("unexpected location: %v", loc.Data)
	}

	list, err := p.ListLocations(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list locations: %v", err)
	}
	locs, _ := list.Data["locations"].([]any)
	if len(locs) == 0 {
		t.Fatal("expected at least one synthesized location")
	}
}

func TestInstanceParamsFromName(t *testing.T) {
	// Get/Delete are addressed by the full resource name; the provider must
	// recover location + id from it when the discrete params are absent.
	ctx := context.Background()
	p := newProvider()
	mustCreate(t, p, "us-central1", "inst-a")
	got, err := p.GetInstance(ctx, newNR(map[string]any{"name": "locations/us-central1/instances/inst-a"}))
	if err != nil {
		t.Fatalf("get by name: %v", err)
	}
	if got.Data["name"] != "projects/proj/locations/us-central1/instances/inst-a" {
		t.Errorf("name = %v", got.Data["name"])
	}
}

func TestNumericIDAndHostStable(t *testing.T) {
	if numericID("x") != numericID("x") {
		t.Error("numericID must be stable")
	}
	if numericID("a") == numericID("b") {
		t.Error("numericID should differ for distinct inputs")
	}
	if synthHost("x") != synthHost("x") {
		t.Error("synthHost must be stable")
	}
}

func isCode(err error, code string) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	return perr.Code == code
}

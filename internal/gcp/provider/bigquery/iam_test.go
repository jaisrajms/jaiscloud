package bigquery

import (
	"context"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newIamProvider returns a provider wired to both the BigQuery store and the
// shared policy plane.
func newIamProvider(t *testing.T) (*Provider, *store.MemoryResourceStore) {
	t.Helper()
	resources := store.NewMemoryResourceStore()
	return New(bqstore.NewMemoryStore(), WithResourceStore(resources)), resources
}

func iamNR(params map[string]any) *model.NormalizedRequest {
	return newNR(params)
}

// createIamTable creates a table in an existing dataset.
func createIamTable(t *testing.T, p *Provider, datasetID, tableID string) {
	t.Helper()
	if _, err := p.CreateTable(context.Background(), newNR(map[string]any{
		"datasetId": datasetID,
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": datasetID, "tableId": tableID},
		},
	})); err != nil {
		t.Fatalf("seed table %s: %v", tableID, err)
	}
}

func tableIAMRequest(datasetID, tableID string, body map[string]any) *model.NormalizedRequest {
	params := map[string]any{"datasetId": datasetID, "tableId": tableID}
	if body != nil {
		params["body"] = body
	}
	return iamNR(params)
}

// policyBindings extracts the bindings array from a Policy response map.
func policyBindings(t *testing.T, resp *model.ProviderResponse) []any {
	t.Helper()
	bindings, ok := resp.Data["bindings"].([]any)
	if !ok {
		t.Fatalf("bindings = %v (%T), want []any", resp.Data["bindings"], resp.Data["bindings"])
	}
	return bindings
}

func TestTableIamPolicyLifecycle(t *testing.T) {
	ctx := context.Background()
	p, _ := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "tbl")

	got, err := p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", nil))
	if err != nil {
		t.Fatalf("get empty policy: %v", err)
	}
	if got.HTTPStatus != 200 {
		t.Fatalf("status = %d", got.HTTPStatus)
	}
	if got.Data["version"] != 1 || got.Data["etag"] != "ACAB" {
		t.Fatalf("empty policy = %v", got.Data)
	}
	if b := policyBindings(t, got); len(b) != 0 {
		t.Fatalf("empty policy bindings = %v", b)
	}

	body := map[string]any{"policy": map[string]any{
		"version": float64(1),
		"bindings": []any{map[string]any{
			"role":    "roles/bigquery.dataViewer",
			"members": []any{"user:alice@example.com"},
		}},
	}}
	set, err := p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", body))
	if err != nil {
		t.Fatalf("set policy: %v", err)
	}
	bindings := policyBindings(t, set)
	if len(bindings) != 1 {
		t.Fatalf("set bindings = %v", bindings)
	}
	binding, _ := bindings[0].(map[string]any)
	if binding["role"] != "roles/bigquery.dataViewer" {
		t.Fatalf("binding = %v", binding)
	}
	if set.Data["etag"] == "" || set.Data["etag"] == "ACAB" {
		t.Fatalf("etag = %v, want a bindings-derived etag", set.Data["etag"])
	}

	// The policy survives a read and is scoped to its own table.
	got, err = p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", nil))
	if err != nil {
		t.Fatalf("get stored policy: %v", err)
	}
	if b := policyBindings(t, got); len(b) != 1 {
		t.Fatalf("stored bindings = %v", b)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": "ds",
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": "ds", "tableId": "other"},
		},
	})); err != nil {
		t.Fatalf("seed second table: %v", err)
	}
	other, err := p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "other", nil))
	if err != nil {
		t.Fatalf("get sibling policy: %v", err)
	}
	if b := policyBindings(t, other); len(b) != 0 {
		t.Fatalf("sibling table inherited bindings: %v", b)
	}
}

func TestTableIamStaleEtagIsAborted(t *testing.T) {
	ctx := context.Background()
	p, _ := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "tbl")

	first := map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/bigquery.dataViewer", "members": []any{"user:a@example.com"}}},
	}}
	set, err := p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", first))
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	etag, _ := set.Data["etag"].(string)

	// A write carrying the current etag succeeds.
	current := map[string]any{"policy": map[string]any{
		"etag":     etag,
		"bindings": []any{map[string]any{"role": "roles/bigquery.dataViewer", "members": []any{"user:b@example.com"}}},
	}}
	set, err = p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", current))
	if err != nil {
		t.Fatalf("set with current etag: %v", err)
	}

	// Re-using the now-stale etag is ABORTED (HTTP 409).
	_, err = p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", current))
	wantProviderError(t, err, "Aborted", 409)
	perr := err.(*model.ProviderError)
	if perr.Status != "ABORTED" {
		t.Fatalf("status = %q, want ABORTED", perr.Status)
	}
}

func TestTableIamConditionRequiresVersion3(t *testing.T) {
	ctx := context.Background()
	p, _ := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "tbl")

	conditional := map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{
			"role":      "roles/bigquery.dataViewer",
			"members":   []any{"user:a@example.com"},
			"condition": map[string]any{"title": "t", "expression": "request.time < timestamp(\"2027-01-01T00:00:00Z\")"},
		}},
	}}
	got, err := p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", conditional))
	if err != nil {
		t.Fatalf("set conditional policy: %v", err)
	}
	if got.Data["version"] != 3 {
		t.Fatalf("version = %v, want 3", got.Data["version"])
	}

	// An explicit version 1 cannot carry a condition.
	bad := map[string]any{"policy": map[string]any{
		"version":  float64(1),
		"bindings": conditional["policy"].(map[string]any)["bindings"],
	}}
	_, err = p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", bad))
	wantProviderError(t, err, "InvalidArgument", 400)
}

func TestTableIamMissingTable(t *testing.T) {
	ctx := context.Background()
	p, resources := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "")

	_, err := p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "absent", nil))
	wantProviderError(t, err, "NotFound", 404)
	_, err = p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "absent", map[string]any{
		"policy": map[string]any{"bindings": []any{}},
	}))
	wantProviderError(t, err, "NotFound", 404)

	// A failed set must not create a policy row that a later table of the same
	// name would inherit.
	if _, err := resources.Get(ctx, "proj", store.GlobalRegion, TablePolicyType, tablePolicyKey("ds", "absent")); err == nil {
		t.Fatal("failed set created a policy row")
	}
}

func TestTableIamPermissionsFailOpen(t *testing.T) {
	ctx := context.Background()
	p, _ := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "tbl")

	body := map[string]any{"permissions": []any{"bigquery.tables.get", "bigquery.tables.getData"}}
	got, err := p.TestTableIamPermissions(ctx, tableIAMRequest("ds", "tbl", body))
	if err != nil {
		t.Fatalf("test permissions: %v", err)
	}
	perms, ok := got.Data["permissions"].([]string)
	if !ok || len(perms) != 2 {
		t.Fatalf("permissions = %v (%T), want both requested", got.Data["permissions"], got.Data["permissions"])
	}

	// An absent table answers the documented empty set, not NOT_FOUND.
	got, err = p.TestTableIamPermissions(ctx, tableIAMRequest("ds", "absent", body))
	if err != nil {
		t.Fatalf("test permissions on absent table: %v", err)
	}
	if perms, ok := got.Data["permissions"].([]string); !ok || len(perms) != 0 {
		t.Fatalf("absent table permissions = %v (%T), want []", got.Data["permissions"], got.Data["permissions"])
	}
}

func TestTableIamPolicyCascadesOnDelete(t *testing.T) {
	ctx := context.Background()
	p, _ := newIamProvider(t)
	seedDatasetTable(t, p, "ds", "tbl")
	set := map[string]any{"policy": map[string]any{
		"bindings": []any{map[string]any{"role": "roles/bigquery.dataViewer", "members": []any{"user:a@example.com"}}},
	}}
	if _, err := p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", set)); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := p.DeleteTable(ctx, tableIAMRequest("ds", "tbl", nil)); err != nil {
		t.Fatalf("delete table: %v", err)
	}
	createIamTable(t, p, "ds", "tbl")
	got, err := p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", nil))
	if err != nil {
		t.Fatalf("get after re-create: %v", err)
	}
	if b := policyBindings(t, got); len(b) != 0 {
		t.Fatalf("re-created table inherited a policy: %v", b)
	}

	// Dataset delete drops every table policy it cascades over.
	createIamTable(t, p, "ds", "tbl2")
	if _, err := p.SetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl2", set)); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if _, err := p.DeleteDataset(ctx, iamNR(map[string]any{"datasetId": "ds"})); err != nil {
		t.Fatalf("delete dataset: %v", err)
	}
	seedDatasetTable(t, p, "ds", "tbl2")
	got, err = p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl2", nil))
	if err != nil {
		t.Fatalf("get after dataset re-create: %v", err)
	}
	if b := policyBindings(t, got); len(b) != 0 {
		t.Fatalf("re-created table inherited a dataset-cascaded policy: %v", b)
	}
}

func TestTableIamWithoutPolicyStore(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "tbl")

	_, err := p.GetTableIamPolicy(ctx, tableIAMRequest("ds", "tbl", nil))
	wantProviderError(t, err, "Unimplemented", 501)
}

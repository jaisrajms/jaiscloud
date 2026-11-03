package bigquery

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

// seedDatasetTable creates one dataset (and optionally a table) for the
// routines/rowAccessPolicies tests.
func seedDatasetTable(t *testing.T, p *Provider, datasetID, tableID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": datasetID},
	}})); err != nil {
		t.Fatalf("seed dataset: %v", err)
	}
	if tableID == "" {
		return
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": datasetID,
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": datasetID, "tableId": tableID},
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "region", "type": "STRING"}}},
		},
	})); err != nil {
		t.Fatalf("seed table: %v", err)
	}
}

func wantProviderError(t *testing.T, err error, code string, status int) {
	t.Helper()
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != code || perr.HTTPStatus != status {
		t.Fatalf("expected %s/%d, got %s/%d (%s)", code, status, perr.Code, perr.HTTPStatus, perr.Message)
	}
}

// routineTerraformBody is the shape the hashicorp/google provider
// google_bigquery_routine sends to routines.insert / routines.update: the
// routine reference plus the schema fields it lets the user configure.
func routineTerraformBody(routineID, body string) map[string]any {
	return map[string]any{
		"routineReference": map[string]any{
			"projectId": "proj", "datasetId": "ds", "routineId": routineID,
		},
		"routineType":        "SCALAR_FUNCTION",
		"language":           "SQL",
		"definitionBody":     body,
		"description":        "a terraform routine",
		"determinismLevel":   "DETERMINISTIC",
		"arguments":          []any{map[string]any{"name": "x", "argumentKind": "FIXED_TYPE", "mode": "IN", "dataType": map[string]any{"typeKind": "INT64"}}},
		"returnType":         map[string]any{"typeKind": "INT64"},
		"importedLibraries":  []any{"gs://bucket/lib.js"},
		"strictMode":         true,
		"dataGovernanceType": "DATA_GOVERNANCE_TYPE_UNSPECIFIED",
	}
}

// TestRoutineTerraformLifecycle walks the apply / get / list / update /
// re-read / destroy sequence google_bigquery_routine performs.
func TestRoutineTerraformLifecycle(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "")

	// apply → routines.insert.
	created, err := p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody("tf_routine", "x * 2"),
	}))
	if err != nil {
		t.Fatalf("insert routine: %v", err)
	}
	// Routine has no `kind` member in the Discovery schema, so the response
	// must not invent one.
	if _, ok := created.Data["kind"]; ok {
		t.Fatalf("routine response must not carry an undeclared kind: %v", created.Data["kind"])
	}
	ref, _ := created.Data["routineReference"].(map[string]any)
	if ref["projectId"] != "proj" || ref["datasetId"] != "ds" || ref["routineId"] != "tf_routine" {
		t.Fatalf("routineReference = %v", ref)
	}
	if created.Data["definitionBody"] != "x * 2" || created.Data["routineType"] != "SCALAR_FUNCTION" {
		t.Fatalf("routine body not echoed: %v", created.Data)
	}
	if created.Data["creationTime"] != millis(t0) || created.Data["lastModifiedTime"] != millis(t0) {
		t.Fatalf("routine timestamps = %v/%v", created.Data["creationTime"], created.Data["lastModifiedTime"])
	}
	etag0, _ := created.Data["etag"].(string)
	if etag0 == "" {
		t.Fatalf("routine etag is empty")
	}

	// read → routines.get.
	got, err := p.GetRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "tf_routine"}))
	if err != nil {
		t.Fatalf("get routine: %v", err)
	}
	if got.Data["etag"] != etag0 || got.Data["definitionBody"] != "x * 2" {
		t.Fatalf("get routine diverged: %v", got.Data)
	}

	// list → routines.list (the documented summary subset).
	list, err := p.ListRoutines(ctx, newNR(map[string]any{"datasetId": "ds"}))
	if err != nil {
		t.Fatalf("list routines: %v", err)
	}
	items, _ := list.Data["routines"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 routine, got %d", len(items))
	}
	item, _ := items[0].(map[string]any)
	if item["routineType"] != "SCALAR_FUNCTION" || item["language"] != "SQL" {
		t.Fatalf("routine list summary lost the documented fields: %v", item)
	}
	if _, ok := item["definitionBody"]; ok {
		t.Fatalf("routines.list must emit the documented summary subset, got definitionBody: %v", item)
	}

	// update → routines.update (PUT, full replace).
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Minute)})
	next := routineTerraformBody("tf_routine", "x * 3")
	next["etag"] = etag0
	updated, err := p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "tf_routine", "body": next,
	}))
	if err != nil {
		t.Fatalf("update routine: %v", err)
	}
	if updated.Data["definitionBody"] != "x * 3" {
		t.Fatalf("update not applied: %v", updated.Data)
	}
	if updated.Data["creationTime"] != millis(t0) || updated.Data["lastModifiedTime"] != millis(t0.Add(time.Minute)) {
		t.Fatalf("update timestamps = %v/%v", updated.Data["creationTime"], updated.Data["lastModifiedTime"])
	}
	if updated.Data["etag"] == etag0 {
		t.Fatalf("etag did not change after update")
	}

	// destroy → routines.delete (204), then the read is 404.
	del, err := p.DeleteRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "tf_routine"}))
	if err != nil {
		t.Fatalf("delete routine: %v", err)
	}
	if del.HTTPStatus != 204 {
		t.Fatalf("delete status = %d", del.HTTPStatus)
	}
	if _, err := p.GetRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "tf_routine"})); err != nil {
		wantProviderError(t, err, "NotFound", 404)
	}
	_, err = p.DeleteRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "tf_routine"}))
	wantProviderError(t, err, "NotFound", 404)
}

func TestRoutineUpdateMaskAndEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "")
	created, err := p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody("r", "x * 2"),
	}))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	etag := created.Data["etag"].(string)

	// A masked update changes only the masked field.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Second)})
	masked, err := p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "r", "updateMask": "description",
		"body": map[string]any{"etag": etag, "description": "masked description", "definitionBody": "IGNORED"},
	}))
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	if masked.Data["description"] != "masked description" {
		t.Fatalf("masked field not applied: %v", masked.Data)
	}
	if masked.Data["definitionBody"] != "x * 2" {
		t.Fatalf("unmasked field changed: %v", masked.Data["definitionBody"])
	}

	// A masked path that is absent from the body clears the stored value.
	etag = masked.Data["etag"].(string)
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(2 * time.Second)})
	cleared, err := p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "r", "updateMask": "description",
		"body": map[string]any{"etag": etag},
	}))
	if err != nil {
		t.Fatalf("clearing masked update: %v", err)
	}
	if _, ok := cleared.Data["description"]; ok {
		t.Fatalf("masked absent path did not clear the field: %v", cleared.Data)
	}

	// A stale etag is 412 conditionNotMet.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(3 * time.Second)})
	_, err = p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "r", "body": map[string]any{"etag": etag},
	}))
	wantProviderError(t, err, "PreconditionFailed", 412)

	// A matching etag is accepted (read-modify-write, like Terraform's update).
	current, err := p.GetRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "r"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body := routineTerraformBody("r", "x * 9")
	body["etag"] = current.Data["etag"]
	if _, err := p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "r", "body": body,
	})); err != nil {
		t.Fatalf("matching etag update: %v", err)
	}
}

func TestRoutineValidation(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "")

	// Missing dataset.
	_, err := p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "nope", "body": routineTerraformBody("r", "1"),
	}))
	wantProviderError(t, err, "NotFound", 404)

	// Missing reference.
	_, err = p.InsertRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "body": map[string]any{}}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// Illegal characters in the ID.
	_, err = p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody("bad-id", "1"),
	}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// Over the declared 256-character maximum.
	_, err = p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody(strings.Repeat("a", 257), "1"),
	}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// Duplicate.
	if _, err := p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody("dup", "1"),
	})); err != nil {
		t.Fatalf("insert: %v", err)
	}
	_, err = p.InsertRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "body": routineTerraformBody("dup", "2"),
	}))
	wantProviderError(t, err, "AlreadyExists", 409)

	// Update / get / delete of a missing routine.
	_, err = p.UpdateRoutine(ctx, newNR(map[string]any{
		"datasetId": "ds", "routineId": "missing", "body": routineTerraformBody("missing", "1"),
	}))
	wantProviderError(t, err, "NotFound", 404)
	_, err = p.GetRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "missing"}))
	wantProviderError(t, err, "NotFound", 404)

	// routines.list of a missing dataset is 404 (real GCP validates it).
	_, err = p.ListRoutines(ctx, newNR(map[string]any{"datasetId": "nope"}))
	wantProviderError(t, err, "NotFound", 404)

	// A bad maxResults binding is rejected, naming maxResults.
	_, err = p.ListRoutines(ctx, newNR(map[string]any{"datasetId": "ds", "maxResults": "abc"}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// Routines IAM stays an explicit 501.
	if _, err := p.RoutinesIam(ctx, newNR(nil)); err == nil {
		t.Fatalf("expected Unimplemented for routine IAM")
	} else {
		wantProviderError(t, err, "Unimplemented", 501)
	}
}

// rowAccessPolicyTerraformBody is the shape google_bigquery_row_access_policy
// sends; grantees is input-only and must never be echoed.
func rowAccessPolicyTerraformBody(policyID, predicate string) map[string]any {
	return map[string]any{
		"rowAccessPolicyReference": map[string]any{
			"projectId": "proj", "datasetId": "ds", "tableId": "tbl", "policyId": policyID,
		},
		"filterPredicate": predicate,
		"grantees":        []any{"user:alice@example.com", "group:analysts@example.com"},
	}
}

func TestRowAccessPolicyTerraformLifecycle(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "tbl")

	created, err := p.InsertRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": rowAccessPolicyTerraformBody("eu_only", `region = "EU"`),
	}))
	if err != nil {
		t.Fatalf("insert policy: %v", err)
	}
	ref, _ := created.Data["rowAccessPolicyReference"].(map[string]any)
	if ref["projectId"] != "proj" || ref["datasetId"] != "ds" || ref["tableId"] != "tbl" || ref["policyId"] != "eu_only" {
		t.Fatalf("rowAccessPolicyReference = %v", ref)
	}
	if created.Data["filterPredicate"] != `region = "EU"` {
		t.Fatalf("filterPredicate = %v", created.Data["filterPredicate"])
	}
	if _, ok := created.Data["grantees"]; ok {
		t.Fatalf("grantees is input-only and must not be echoed: %v", created.Data)
	}
	// creationTime/lastModifiedTime are google-datetime strings, not millis.
	if created.Data["creationTime"] != t0.Format(time.RFC3339) {
		t.Fatalf("creationTime = %v, want %s", created.Data["creationTime"], t0.Format(time.RFC3339))
	}
	etag0, _ := created.Data["etag"].(string)

	got, err := p.GetRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "eu_only",
	}))
	if err != nil || got.Data["etag"] != etag0 {
		t.Fatalf("get policy: %v %v", err, got.Data)
	}

	list, err := p.ListRowAccessPolicies(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "tbl"}))
	if err != nil {
		t.Fatalf("list policies: %v", err)
	}
	items, _ := list.Data["rowAccessPolicies"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(items))
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Minute)})
	updated, err := p.UpdateRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "eu_only",
		"body": func() map[string]any {
			b := rowAccessPolicyTerraformBody("eu_only", `region = "US"`)
			b["etag"] = etag0
			return b
		}(),
	}))
	if err != nil {
		t.Fatalf("update policy: %v", err)
	}
	if updated.Data["filterPredicate"] != `region = "US"` {
		t.Fatalf("update not applied: %v", updated.Data)
	}
	if updated.Data["creationTime"] != t0.Format(time.RFC3339) {
		t.Fatalf("creationTime changed on update: %v", updated.Data["creationTime"])
	}
	if updated.Data["lastModifiedTime"] != t0.Add(time.Minute).Format(time.RFC3339) {
		t.Fatalf("lastModifiedTime = %v", updated.Data["lastModifiedTime"])
	}

	del, err := p.DeleteRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "eu_only",
	}))
	if err != nil || del.HTTPStatus != 204 {
		t.Fatalf("delete policy: %v %v", err, del)
	}
	if _, err := p.GetRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "eu_only",
	})); err != nil {
		wantProviderError(t, err, "NotFound", 404)
	}
}

func TestRowAccessPolicyValidationAndBatchDelete(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "tbl")

	// The table must exist.
	_, err := p.InsertRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "nope", "body": rowAccessPolicyTerraformBody("p", "TRUE"),
	}))
	wantProviderError(t, err, "NotFound", 404)

	// Policy IDs follow the declared identifier rule.
	_, err = p.InsertRowAccessPolicy(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": rowAccessPolicyTerraformBody("bad id", "TRUE"),
	}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// A missing route dataset is 404 too.
	_, err = p.ListRowAccessPolicies(ctx, newNR(map[string]any{"datasetId": "nope", "tableId": "tbl"}))
	wantProviderError(t, err, "NotFound", 404)

	for _, id := range []string{"p1", "p2"} {
		if _, err := p.InsertRowAccessPolicy(ctx, newNR(map[string]any{
			"datasetId": "ds", "tableId": "tbl", "body": rowAccessPolicyTerraformBody(id, "TRUE"),
		})); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}

	// A batch naming a missing policy is all-or-nothing.
	_, err = p.BatchDeleteRowAccessPolicies(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl",
		"body": map[string]any{"policyIds": []any{"p1", "missing"}},
	}))
	wantProviderError(t, err, "NotFound", 404)
	list, err := p.ListRowAccessPolicies(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "tbl"}))
	if err != nil {
		t.Fatalf("list after failed batch: %v", err)
	}
	if items, _ := list.Data["rowAccessPolicies"].([]any); len(items) != 2 {
		t.Fatalf("failed batch deleted policies: %d left", len(items))
	}

	del, err := p.BatchDeleteRowAccessPolicies(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": map[string]any{"policyIds": []any{"p1", "p2"}},
	}))
	if err != nil || del.HTTPStatus != 204 {
		t.Fatalf("batch delete: %v %v", err, del)
	}
	list, _ = p.ListRowAccessPolicies(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "tbl"}))
	if items, _ := list.Data["rowAccessPolicies"].([]any); len(items) != 0 {
		t.Fatalf("batch delete left %d policies", len(items))
	}

	// An empty policyIds list is InvalidArgument.
	_, err = p.BatchDeleteRowAccessPolicies(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": map[string]any{"policyIds": []any{}},
	}))
	wantProviderError(t, err, "InvalidArgument", 400)

	// Row access policy IAM stays an explicit 501.
	if _, err := p.RowAccessPoliciesIam(ctx, newNR(nil)); err == nil {
		t.Fatalf("expected Unimplemented for policy IAM")
	} else {
		wantProviderError(t, err, "Unimplemented", 501)
	}
}

func TestModelsUnimplementedDecision(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	_, err := p.Models(ctx, newNR(map[string]any{"datasetId": "ds"}))
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "Unimplemented" || perr.HTTPStatus != 501 {
		t.Fatalf("expected Unimplemented/501, got %v", err)
	}
	// The 501 carries the recorded reason so the decision is discoverable on
	// the wire, not only in the docs.
	if !strings.Contains(perr.Message, "model-training") {
		t.Fatalf("models 501 message lost its reason: %q", perr.Message)
	}
}

// TestRoutineAndPolicyPaging covers the two list cursors: routines uses
// maxResults/pageToken and rowAccessPolicies uses pageSize/pageToken.
func TestRoutineAndPolicyPaging(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedDatasetTable(t, p, "ds", "tbl")
	for i := 0; i < 3; i++ {
		if _, err := p.InsertRoutine(ctx, newNR(map[string]any{
			"datasetId": "ds", "body": routineTerraformBody(fmt.Sprintf("r%d", i), "1"),
		})); err != nil {
			t.Fatalf("insert routine %d: %v", i, err)
		}
	}
	first, err := p.ListRoutines(ctx, newNR(map[string]any{"datasetId": "ds", "maxResults": "2"}))
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if items, _ := first.Data["routines"].([]any); len(items) != 2 || first.Data["nextPageToken"] == "" {
		t.Fatalf("page 1 = %d items, token=%q", len(items), first.Data["nextPageToken"])
	}
	second, err := p.ListRoutines(ctx, newNR(map[string]any{
		"datasetId": "ds", "maxResults": "2", "pageToken": first.Data["nextPageToken"],
	}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if items, _ := second.Data["routines"].([]any); len(items) != 1 {
		t.Fatalf("page 2 = %d items", len(items))
	}

	for i := 0; i < 3; i++ {
		if _, err := p.InsertRowAccessPolicy(ctx, newNR(map[string]any{
			"datasetId": "ds", "tableId": "tbl",
			"body": rowAccessPolicyTerraformBody(fmt.Sprintf("p%d", i), "TRUE"),
		})); err != nil {
			t.Fatalf("insert policy %d: %v", i, err)
		}
	}
	page, err := p.ListRowAccessPolicies(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "pageSize": "2",
	}))
	if err != nil {
		t.Fatalf("policy page 1: %v", err)
	}
	if items, _ := page.Data["rowAccessPolicies"].([]any); len(items) != 2 || page.Data["nextPageToken"] == "" {
		t.Fatalf("policy page 1 = %d items, token=%q", len(items), page.Data["nextPageToken"])
	}
	// A malformed pageSize is the int32 binding INVALID_ARGUMENT.
	_, err = p.ListRowAccessPolicies(ctx, newNR(map[string]any{
		"datasetId": "ds", "tableId": "tbl", "pageSize": "abc",
	}))
	wantProviderError(t, err, "InvalidArgument", 400)
}

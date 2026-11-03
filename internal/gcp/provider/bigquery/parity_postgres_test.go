//go:build gcp_persistence

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	gcpstore "jaiscloud/internal/gcp/store"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// TestSQLParityPostgres runs the identical SQL workload against the Postgres
// backend and asserts the encoded provider responses match the memory store's
// after JSON canonicalization (random job IDs are masked; timestamps are frozen,
// so structure/rows/types/statuses/errors are compared, not bytes). This is
// BQ4's cross-backend parity proof: because SQLite is disposable per-query
// scratch and both stores sit behind the same Catalog, only the store can make
// the results diverge — so a fingerprint diff localizes any regression to a
// store method.
//
// Like every gcp_persistence test, this shares one database and wipes the
// bigquery tables, so the gate must run with -p 1 (see Makefile / CI).
func TestSQLParityPostgres(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres SQL parity test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	want := sqlParityFingerprint(t, bqstore.NewMemoryStore())
	got := sqlParityFingerprint(t, bqstore.NewPostgresStore(pg.Pool()))

	if got == want {
		return
	}
	if diff := firstParityDiff(want, got); diff != "" {
		t.Fatalf("memory and --dsn SQL results diverge: %s", diff)
	}
	t.Fatalf("memory and --dsn SQL fingerprints differ")
}

// firstParityDiff decodes both fingerprints and reports the first op whose
// canonical response or error differs, so a failure names the offending step.
func firstParityDiff(want, got string) string {
	var w, g []parityStep
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		return fmt.Sprintf("decode memory fingerprint: %v", err)
	}
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		return fmt.Sprintf("decode postgres fingerprint: %v", err)
	}
	if len(w) != len(g) {
		return fmt.Sprintf("step count memory=%d postgres=%d", len(w), len(g))
	}
	for i := range w {
		wb, _ := json.Marshal(w[i])
		gb, _ := json.Marshal(g[i])
		if string(wb) != string(gb) {
			return fmt.Sprintf("step %d (%s):\n  memory:   %s\n  postgres: %s", i, w[i].Op, wb, gb)
		}
	}
	return ""
}

// resourcesParityFingerprint runs an identical routines + rowAccessPolicies
// workload against a store and returns a canonical JSON fingerprint of every
// encoded provider response and error. With the clock frozen the fingerprint is
// deterministic, so running it against the memory store and the Postgres store
// proves the two backends produce identical wire responses for the resources
// added by NF31 (create/read/list/update-with-mask/etag-mismatch/delete,
// batch-delete, and the validation errors).
func resourcesParityFingerprint(t *testing.T, s bqstore.Store) string {
	t.Helper()
	ctx := context.Background()

	t0 := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	p := New(s)
	rec := &parityRecorder{}
	call := func(op string, h func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error), params map[string]any) {
		resp, err := h(ctx, newNR(params))
		rec.respond(op, resp, err)
	}

	call("CreateDataset", p.CreateDataset, map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": "ds"},
	}})
	call("CreateTable", p.CreateTable, map[string]any{
		"datasetId": "ds",
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": "ds", "tableId": "tbl"},
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "region", "type": "STRING"}}},
		},
	})

	body := map[string]any{
		"routineReference": map[string]any{"projectId": "proj", "datasetId": "ds", "routineId": "r"},
		"routineType":      "SCALAR_FUNCTION",
		"language":         "SQL",
		"definitionBody":   "x * 2",
		"description":      "parity routine",
	}
	call("InsertRoutine", p.InsertRoutine, map[string]any{"datasetId": "ds", "body": body})
	call("InsertRoutine(missing dataset)", p.InsertRoutine, map[string]any{"datasetId": "nope", "body": body})
	call("InsertRoutine(duplicate)", p.InsertRoutine, map[string]any{"datasetId": "ds", "body": body})
	call("GetRoutine", p.GetRoutine, map[string]any{"datasetId": "ds", "routineId": "r"})
	call("ListRoutines", p.ListRoutines, map[string]any{"datasetId": "ds", "maxResults": "1"})
	call("ListRoutines(missing)", p.ListRoutines, map[string]any{"datasetId": "nope"})
	call("GetRoutine(missing)", p.GetRoutine, map[string]any{"datasetId": "ds", "routineId": "nope"})

	// Read the etag the emulator issued (content hash + update instant); it is
	// deterministic under the frozen clock and identical across backends.
	etagResp, etagErr := p.GetRoutine(ctx, newNR(map[string]any{"datasetId": "ds", "routineId": "r"}))
	if etagErr != nil {
		t.Fatalf("read routine etag: %v", etagErr)
	}
	etag, _ := etagResp.Data["etag"].(string)
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Minute)})
	call("UpdateRoutine(mask)", p.UpdateRoutine, map[string]any{
		"datasetId": "ds", "routineId": "r", "updateMask": "description",
		"body": map[string]any{"etag": etag, "description": "masked"},
	})
	call("UpdateRoutine(stale etag)", p.UpdateRoutine, map[string]any{
		"datasetId": "ds", "routineId": "r", "body": map[string]any{"etag": etag},
	})
	call("DeleteRoutine", p.DeleteRoutine, map[string]any{"datasetId": "ds", "routineId": "r"})
	call("DeleteRoutine(missing)", p.DeleteRoutine, map[string]any{"datasetId": "ds", "routineId": "r"})

	policyBody := func(id, predicate string) map[string]any {
		return map[string]any{
			"rowAccessPolicyReference": map[string]any{
				"projectId": "proj", "datasetId": "ds", "tableId": "tbl", "policyId": id,
			},
			"filterPredicate": predicate,
			"grantees":        []any{"user:alice@example.com"},
		}
	}
	call("InsertRowAccessPolicy", p.InsertRowAccessPolicy, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": policyBody("p1", `region = "EU"`),
	})
	call("InsertRowAccessPolicy(p2)", p.InsertRowAccessPolicy, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": policyBody("p2", `region = "US"`),
	})
	call("InsertRowAccessPolicy(missing table)", p.InsertRowAccessPolicy, map[string]any{
		"datasetId": "ds", "tableId": "nope", "body": policyBody("p3", "TRUE"),
	})
	call("GetRowAccessPolicy", p.GetRowAccessPolicy, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "p1",
	})
	call("ListRowAccessPolicies", p.ListRowAccessPolicies, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "pageSize": "1",
	})
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(2 * time.Minute)})
	call("UpdateRowAccessPolicy", p.UpdateRowAccessPolicy, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "policyId": "p1",
		"body": policyBody("p1", `region = "APAC"`),
	})
	call("BatchDeleteRowAccessPolicies(missing)", p.BatchDeleteRowAccessPolicies, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": map[string]any{"policyIds": []any{"p1", "nope"}},
	})
	call("BatchDeleteRowAccessPolicies", p.BatchDeleteRowAccessPolicies, map[string]any{
		"datasetId": "ds", "tableId": "tbl", "body": map[string]any{"policyIds": []any{"p1", "p2"}},
	})

	out, _ := json.Marshal(rec.steps)
	return string(out)
}

// TestResourcesParityPostgres proves the NF31 routines/rowAccessPolicies wire
// responses are identical across the memory and Postgres backends.
func TestResourcesParityPostgres(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres resources parity test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	mem := bqstore.NewMemoryStore()
	mem.Reset(ctx)
	pgStore := bqstore.NewPostgresStore(pg.Pool())
	pgStore.Reset(ctx)

	want := resourcesParityFingerprint(t, mem)
	got := resourcesParityFingerprint(t, pgStore)
	if got == want {
		return
	}
	if diff := firstParityDiff(want, got); diff != "" {
		t.Fatalf("memory and --dsn routines/rowAccessPolicies responses diverge: %s", diff)
	}
	t.Fatalf("memory and --dsn routines/rowAccessPolicies fingerprints differ")
}

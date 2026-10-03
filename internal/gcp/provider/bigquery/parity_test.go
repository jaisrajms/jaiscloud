package bigquery

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/model"
)

// sqlParityFingerprint runs an identical BigQuery SQL workload against a store
// and returns a canonical JSON fingerprint of every encoded provider response
// and error. Under a frozen clock the fingerprint is deterministic, so running
// it against the memory store and the Postgres store and comparing the two
// proves the query engine yields identical results across backends (BQ4).
//
// Random job IDs are masked; timestamps are fixed by the clock. The workload
// covers SELECT (filter/order/expression inference), aggregation, CTAS,
// INSERT/UPDATE/DELETE/TRUNCATE, an empty result, dry-run, fail-loud, a missing
// table, and tabledata/tables reads.
func sqlParityFingerprint(t *testing.T, s bqstore.Store) string {
	t.Helper()
	ctx := context.Background()

	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})
	s.Reset(ctx)

	p := New(s)

	const ds = "parity_ds"
	const src = "src"
	const dst = "dst"
	const qual = "proj." + ds + "."

	rec := &parityRecorder{}

	nr := func(params map[string]any) *model.NormalizedRequest { return newNR(params) }

	// ── seed ────────────────────────────────────────────────────────────────
	rec.at("CreateDataset")(p.CreateDataset(ctx, nr(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": ds},
		"location":         "US",
	}})))

	rec.at("CreateTable(src)")(p.CreateTable(ctx, nr(map[string]any{
		"datasetId": ds,
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": ds, "tableId": src},
			"schema": map[string]any{"fields": []any{
				map[string]any{"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
				map[string]any{"name": "name", "type": "STRING", "mode": "NULLABLE"},
				map[string]any{"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
				map[string]any{"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
			}},
		},
	})))

	rec.at("InsertAll(src)")(p.InsertAll(ctx, nr(map[string]any{
		"datasetId": ds,
		"tableId":   src,
		"body": map[string]any{"rows": []any{
			map[string]any{"insertId": "1", "json": map[string]any{"id": 1, "name": "alice", "amount": 10.5, "active": true}},
			map[string]any{"insertId": "2", "json": map[string]any{"id": 2, "name": "bob", "amount": 7, "active": false}},
			map[string]any{"insertId": "3", "json": map[string]any{"id": 3, "name": "carol", "amount": nil, "active": true}},
			map[string]any{"insertId": "4", "json": map[string]any{"id": 4, "name": "dave", "amount": 3.25, "active": false}},
		}},
	})))

	// Duplicate insertId: reported as a per-row duplicate, no new row.
	rec.at("InsertAll(src,dup)")(p.InsertAll(ctx, nr(map[string]any{
		"datasetId": ds,
		"tableId":   src,
		"body": map[string]any{"rows": []any{
			map[string]any{"insertId": "1", "json": map[string]any{"id": 99, "name": "dup"}},
		}},
	})))

	getTable := func(op, table string) {
		rec.at(op)(p.GetTable(ctx, nr(map[string]any{"datasetId": ds, "tableId": table})))
	}
	listRows := func(op, table string) {
		rec.at(op)(p.ListRows(ctx, nr(map[string]any{"datasetId": ds, "tableId": table})))
	}

	getTable("GetTable(src)", src)
	listRows("ListRows(src)", src)

	query := func(op, q string, dryRun bool) {
		body := map[string]any{"query": q, "useLegacySql": false}
		if dryRun {
			body["dryRun"] = true
		}
		resp, err := p.Query(ctx, nr(map[string]any{"body": body}))
		rec.call(op, resp, err)
		if err != nil || resp == nil {
			return
		}
		ref, _ := resp.Data["jobReference"].(map[string]any)
		id, _ := ref["jobId"].(string)
		if dryRun || id == "" {
			return
		}
		rec.at(op + "/GetQueryResults")(p.GetQueryResults(ctx, nr(map[string]any{"jobId": id})))
	}

	query("Query(select)", "SELECT name, amount * 2 AS doubled FROM `"+qual+src+"` WHERE amount >= 3 ORDER BY amount", false)
	query("Query(aggregate)", "SELECT active, COUNT(*) AS n, SUM(amount) AS total FROM `"+qual+src+"` GROUP BY active ORDER BY active", false)
	query("Query(ctas)", "CREATE TABLE `"+qual+dst+"` AS SELECT id, name, amount FROM `"+qual+src+"` WHERE amount >= 3", false)
	getTable("GetTable(dst)", dst)
	query("Query(insert)", "INSERT INTO `"+qual+dst+"` (id, name, amount) VALUES (5, 'erin', 1.5)", false)
	query("Query(update)", "UPDATE `"+qual+dst+"` SET amount = amount + 1 WHERE name = 'bob'", false)
	getTable("GetTable(dst,post-dml)", dst)
	listRows("ListRows(dst)", dst)
	query("Query(delete)", "DELETE FROM `"+qual+dst+"` WHERE name = 'alice'", false)
	query("Query(truncate)", "TRUNCATE TABLE `"+qual+dst+"`", false)
	getTable("GetTable(dst,post-truncate)", dst)
	query("Query(empty)", "SELECT id FROM `"+qual+src+"` WHERE id > 1000", false)
	query("Query(fail-loud)", "SELECT * FROM UNNEST([1, 2, 3])", false)
	query("Query(missing-table)", "SELECT * FROM `"+qual+"nope`", false)
	query("Query(dry-run)", "CREATE SCHEMA `proj.parity_ds2`", true)
	query("Query(create-schema)", "CREATE SCHEMA `proj.parity_ds2`", false)
	query("Query(drop-schema)", "DROP SCHEMA `proj.parity_ds2`", false)
	query("Query(drop-table)", "DROP TABLE `"+qual+dst+"`", false)

	data, err := json.Marshal(rec.steps)
	if err != nil {
		t.Fatalf("marshal parity fingerprint: %v", err)
	}
	return string(data)
}

// parityRecorder accumulates canonicalized provider responses in call order.
type parityRecorder struct {
	steps []parityStep
}

type parityStep struct {
	Op     string `json:"op"`
	Status int    `json:"status,omitempty"`
	Data   any    `json:"data,omitempty"`
	Err    string `json:"err,omitempty"`
}

func (r *parityRecorder) call(op string, resp *model.ProviderResponse, err error) {
	if err != nil {
		r.steps = append(r.steps, parityStep{Op: op, Err: err.Error()})
		return
	}
	r.steps = append(r.steps, parityStep{Op: op, Status: resp.HTTPStatus, Data: maskVolatile(resp.Data)})
}

// at returns a recorder bound to an op name so a provider call can be passed
// directly: rec.at("Op")(p.Handler(ctx, nr)).
func (r *parityRecorder) at(op string) func(*model.ProviderResponse, error) {
	return func(resp *model.ProviderResponse, err error) { r.call(op, resp, err) }
}

// maskVolatile replaces randomly generated identifiers so two runs that differ
// only in job id produce the same fingerprint.
func maskVolatile(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			switch k {
			case "jobId", "queryId":
				out[k] = "<id>"
			default:
				out[k] = maskVolatile(val)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = maskVolatile(val)
		}
		return out
	default:
		return v
	}
}

// TestSQLParityMemory exercises the parity workload against the memory store so
// the workload itself is covered by the normal (no-DSN) test run. The
// cross-backend equality assertion lives in TestSQLParityPostgres, which
// compares this fingerprint with the Postgres store's.
func TestSQLParityMemory(t *testing.T) {
	fp := sqlParityFingerprint(t, bqstore.NewMemoryStore())

	var steps []parityStep
	if err := json.Unmarshal([]byte(fp), &steps); err != nil {
		t.Fatalf("decode parity fingerprint: %v", err)
	}
	if len(steps) == 0 {
		t.Fatalf("empty parity fingerprint")
	}

	byOp := make(map[string]parityStep, len(steps))
	for _, s := range steps {
		byOp[s.Op] = s
	}

	mustSucceed := []string{
		"CreateDataset", "CreateTable(src)", "InsertAll(src)", "InsertAll(src,dup)",
		"GetTable(src)", "ListRows(src)",
		"Query(select)", "Query(aggregate)", "Query(ctas)", "Query(insert)",
		"Query(update)", "Query(delete)", "Query(truncate)", "Query(empty)",
		"Query(dry-run)", "Query(create-schema)", "Query(drop-schema)", "Query(drop-table)",
	}
	for _, op := range mustSucceed {
		s, ok := byOp[op]
		if !ok {
			t.Fatalf("parity fingerprint missing op %q", op)
		}
		if s.Err != "" {
			t.Errorf("%s: unexpected error: %s", op, s.Err)
		}
		if s.Data == nil {
			t.Errorf("%s: no response data", op)
		}
	}

	mustFail := []string{"Query(fail-loud)", "Query(missing-table)"}
	for _, op := range mustFail {
		s, ok := byOp[op]
		if !ok {
			t.Fatalf("parity fingerprint missing op %q", op)
		}
		if s.Err == "" {
			t.Errorf("%s: expected an error, got data %v", op, s.Data)
		}
	}
}

// TestTableLastModifiedAdvancesOnInsert proves the provider surfaces a bumped
// table update time on the wire: tables.get.lastModifiedTime moves after
// tabledata.insertAll. Combined with the cross-backend fingerprint this pins
// the BQ12 parity fix at the encoding layer, not just in the store.
func TestTableLastModifiedAdvancesOnInsert(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	p := New(bqstore.NewMemoryStore())
	const ds = "lm_ds"
	const tbl = "t"

	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"projectId": "proj", "datasetId": ds},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{
		"datasetId": ds,
		"body": map[string]any{
			"tableReference": map[string]any{"projectId": "proj", "datasetId": ds, "tableId": tbl},
			"schema":         map[string]any{"fields": []any{map[string]any{"name": "id", "type": "INTEGER"}}},
		},
	})); err != nil {
		t.Fatalf("create table: %v", err)
	}

	lastModified := func() int64 {
		resp, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": ds, "tableId": tbl}))
		if err != nil {
			t.Fatalf("get table: %v", err)
		}
		raw, ok := resp.Data["lastModifiedTime"].(string)
		if !ok {
			t.Fatalf("lastModifiedTime = %T, want string", resp.Data["lastModifiedTime"])
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("parse lastModifiedTime %q: %v", raw, err)
		}
		return n
	}

	before := lastModified()
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(time.Second)})
	if _, err := p.InsertAll(ctx, newNR(map[string]any{
		"datasetId": ds,
		"tableId":   tbl,
		"body": map[string]any{"rows": []any{
			map[string]any{"insertId": "1", "json": map[string]any{"id": 1}},
		}},
	})); err != nil {
		t.Fatalf("insertAll: %v", err)
	}
	if after := lastModified(); after <= before {
		t.Fatalf("lastModifiedTime did not advance after insertAll: before=%d after=%d", before, after)
	}
}

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
)

// fakeSourceReader serves gs:// objects from an in-memory map. It stands in for
// the GCS provider's FetchObjectBytes.
type fakeSourceReader struct {
	objects map[string][]byte
	err     error
}

func (f *fakeSourceReader) FetchObjectBytes(_ context.Context, bucket, object string) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	b, ok := f.objects[bucket+"/"+object]
	if !ok {
		return nil, gcs.ErrNoSuchObject
	}
	return b, nil
}

func (f *fakeSourceReader) ListObjectNames(_ context.Context, bucket, prefix string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	names := make([]string, 0, len(f.objects))
	for k := range f.objects {
		b, obj, ok := strings.Cut(k, "/")
		if ok && b == bucket && strings.HasPrefix(obj, prefix) {
			names = append(names, obj)
		}
	}
	sort.Strings(names)
	return names, nil
}

func newLoadProvider(data []byte) (*Provider, *fakeSourceReader) {
	objects := map[string][]byte{}
	if data != nil {
		objects["bucket/data"] = data
	}
	r := &fakeSourceReader{objects: objects}
	return New(bqstore.NewMemoryStore(), WithSourceReader(r)), r
}

// seedLoadTable creates a dataset and a table with the given schema fields.
func seedLoadTable(t *testing.T, p *Provider, ds, tbl string, fields []any) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": ds},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if _, err := p.CreateTable(ctx, newNR(map[string]any{"datasetId": ds, "body": map[string]any{
		"tableReference": map[string]any{"datasetId": ds, "tableId": tbl},
		"schema":         map[string]any{"fields": fields},
	}})); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func loadNR(ds, tbl string, load map[string]any) *model.NormalizedRequest {
	loadJobSeq++
	return newNR(map[string]any{
		"project": "proj",
		"body": map[string]any{
			"jobReference":  map[string]any{"jobId": fmt.Sprintf("load%d", loadJobSeq)},
			"configuration": map[string]any{"load": load},
		},
	})
}

// loadJobSeq gives every load job in a test a distinct jobId (jobs.insert
// rejects a duplicate jobReference).
var loadJobSeq int

func loadCfg(ds, tbl string, extra map[string]any) map[string]any {
	load := map[string]any{
		"destinationTable": map[string]any{"projectId": "proj", "datasetId": ds, "tableId": tbl},
		"sourceUris":       []any{"gs://bucket/data"},
	}
	for k, v := range extra {
		load[k] = v
	}
	return load
}

func loadStats(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	stats, _ := resp.Data["statistics"].(map[string]any)
	ls, _ := stats["load"].(map[string]any)
	if ls == nil {
		t.Fatalf("missing statistics.load in %v", resp.Data)
	}
	return ls
}

func jobErrorResult(resp *model.ProviderResponse) map[string]any {
	status, _ := resp.Data["status"].(map[string]any)
	er, _ := status["errorResult"].(map[string]any)
	return er
}

func assertProviderErr(t *testing.T, err error, code string, status int) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected ProviderError, got %v", err)
	}
	if perr.Code != code || perr.HTTPStatus != status {
		t.Fatalf("expected %s/%d, got %s/%d (%s)", code, status, perr.Code, perr.HTTPStatus, perr.Message)
	}
}

var loadFields = []any{
	map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
	map[string]any{"name": "name", "type": "STRING"},
	map[string]any{"name": "amount", "type": "FLOAT"},
	map[string]any{"name": "active", "type": "BOOLEAN"},
}

func TestLoadNDJSONAppend(t *testing.T) {
	ctx := context.Background()
	data := []byte("{\"id\":1,\"name\":\"a\",\"amount\":1.5,\"active\":true}\n" +
		"{\"id\":2,\"name\":\"b\",\"amount\":2,\"active\":false}\n")
	p, _ := newLoadProvider(data)
	seedLoadTable(t, p, "ds", "t", loadFields)

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert load job: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("expected 200, got %d", resp.HTTPStatus)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	ls := loadStats(t, resp)
	if ls["outputRows"] != "2" || ls["badRecords"] != "0" || ls["inputFiles"] != "1" {
		t.Fatalf("unexpected load stats: %v", ls)
	}
	if resp.Data["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("expected DONE: %v", resp.Data["status"])
	}

	// Rows are readable and rendered per schema (integers as decimal text).
	rows, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	if rows.Data["totalRows"] != "2" {
		t.Fatalf("expected 2 rows, got %v", rows.Data["totalRows"])
	}
	first := rows.Data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if first[0].(map[string]any)["v"] != "1" {
		t.Fatalf("expected id cell \"1\", got %v", first[0])
	}
	second := rows.Data["rows"].([]any)[1].(map[string]any)["f"].([]any)
	if second[3].(map[string]any)["v"] != "false" {
		t.Fatalf("expected active cell \"false\", got %v", second[3])
	}
}

func TestLoadCSVSkipAndDelimiter(t *testing.T) {
	ctx := context.Background()
	data := []byte("id;name\n1;alice\n2;bob\n")
	p, _ := newLoadProvider(data)
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":     "CSV",
		"skipLeadingRows":  "1",
		"fieldDelimiter":   ";",
		"maxBadRecords":    "0",
		"writeDisposition": "WRITE_APPEND",
	})))
	if err != nil {
		t.Fatalf("insert load job: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" {
		t.Fatalf("expected 2 output rows, got %v", ls)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if rows.Data["totalRows"] != "2" {
		t.Fatalf("expected 2 stored rows, got %v", rows.Data["totalRows"])
	}
	second := rows.Data["rows"].([]any)[1].(map[string]any)["f"].([]any)
	if second[0].(map[string]any)["v"] != "2" || second[1].(map[string]any)["v"] != "bob" {
		t.Fatalf("unexpected second row: %v", second)
	}
}

func TestLoadBadRecords(t *testing.T) {
	ctx := context.Background()
	// Second line is missing the REQUIRED id field.
	data := []byte("{\"id\":1,\"name\":\"a\"}\n{\"name\":\"b\"}\n")

	// maxBadRecords=0 → data-level failure reported in the job result, no write.
	p, _ := newLoadProvider(data)
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert load job: %v", err)
	}
	er := jobErrorResult(resp)
	if er == nil || er["reason"] != "invalid" {
		t.Fatalf("expected invalid job error, got %v", resp.Data["status"])
	}
	if ls := loadStats(t, resp); ls["badRecords"] != "1" {
		t.Fatalf("expected 1 bad record, got %v", ls)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if rows.Data["totalRows"] != "0" {
		t.Fatalf("a failed load must not write rows, got %v", rows.Data["totalRows"])
	}

	// maxBadRecords=1 → tolerate the bad record and write the good one.
	p2, _ := newLoadProvider(data)
	seedLoadTable(t, p2, "ds", "t", loadFields)
	resp2, err := p2.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":  "NEWLINE_DELIMITED_JSON",
		"maxBadRecords": "1",
	})))
	if err != nil {
		t.Fatalf("insert load job: %v", err)
	}
	if er := jobErrorResult(resp2); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp2); ls["badRecords"] != "1" || ls["outputRows"] != "1" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

func TestLoadWriteDispositions(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("{\"id\":9,\"name\":\"z\"}\n"))
	seedLoadTable(t, p, "ds", "t", loadFields)
	// Seed one existing row.
	p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"})); rows.Data["totalRows"] != "1" {
		t.Fatalf("seed failed: %v", rows.Data["totalRows"])
	}

	// WRITE_EMPTY on a non-empty table → 'duplicate' errorResult, no write.
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":     "NEWLINE_DELIMITED_JSON",
		"writeDisposition": "WRITE_EMPTY",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "duplicate" {
		t.Fatalf("expected duplicate job error, got %v", resp.Data["status"])
	}
	if rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"})); rows.Data["totalRows"] != "1" {
		t.Fatalf("WRITE_EMPTY must not write, got %v rows", rows.Data["totalRows"])
	}

	// WRITE_TRUNCATE replaces the existing row.
	resp, err = p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":     "NEWLINE_DELIMITED_JSON",
		"writeDisposition": "WRITE_TRUNCATE",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"})); rows.Data["totalRows"] != "1" {
		t.Fatalf("WRITE_TRUNCATE should leave 1 row, got %v", rows.Data["totalRows"])
	}
}

func TestLoadCreatesTable(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("{\"id\":1}\n"))
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "created", loadCfg("ds", "created", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		}},
	})))
	if err != nil {
		t.Fatalf("insert load job: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if _, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "created"})); err != nil {
		t.Fatalf("expected created table: %v", err)
	}
}

func TestLoadMissingSourceIsJobError(t *testing.T) {
	p, _ := newLoadProvider(nil)
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(context.Background(), loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "notFound" {
		t.Fatalf("expected notFound job error, got %v", resp.Data["status"])
	}
}

func TestLoadFailLoud(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		load     map[string]any
		code     string
		status   int
		noReader bool
	}{
		{"orc", map[string]any{"sourceFormat": "ORC"}, "Unimplemented", 501, false},
		{"datastore-backup", map[string]any{"sourceFormat": "DATASTORE_BACKUP"}, "Unimplemented", 501, false},
		{"quote", map[string]any{"sourceFormat": "CSV", "quote": "'"}, "Unimplemented", 501, false},
		{"quote-empty", map[string]any{"sourceFormat": "CSV", "quote": ""}, "Unimplemented", 501, false},
		{"partitioning", map[string]any{"sourceFormat": "CSV", "timePartitioning": map[string]any{"type": "DAY"}}, "Unimplemented", 501, false},
		{"jagged", map[string]any{"sourceFormat": "CSV", "allowJaggedRows": true}, "Unimplemented", 501, false},
		{"query-wildcard", map[string]any{"sourceFormat": "NEWLINE_DELIMITED_JSON", "sourceUris": []any{"gs://bucket/?.json"}}, "InvalidArgument", 400, false},
		{"multi-wildcard", map[string]any{"sourceFormat": "NEWLINE_DELIMITED_JSON", "sourceUris": []any{"gs://bucket/a*b*c"}}, "InvalidArgument", 400, false},
		{"no-sources", map[string]any{"sourceFormat": "NEWLINE_DELIMITED_JSON", "sourceUris": []any{}}, "InvalidArgument", 400, false},
		{"date-format", map[string]any{"sourceFormat": "CSV", "dateFormat": "%Y"}, "Unimplemented", 501, false},
		{"no-reader", map[string]any{"sourceFormat": "NEWLINE_DELIMITED_JSON"}, "Unimplemented", 501, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			load := loadCfg("ds", "t", tc.load)
			var p *Provider
			if tc.noReader {
				p = New(bqstore.NewMemoryStore())
			} else {
				p, _ = newLoadProvider([]byte("{}\n"))
			}
			seedLoadTable(t, p, "ds", "t", loadFields)
			_, err := p.InsertJob(ctx, loadNR("ds", "t", load))
			assertProviderErr(t, err, tc.code, tc.status)
		})
	}
}

func TestLoadCSVTypeErrorsAreBadRecords(t *testing.T) {
	data := []byte("1,ok,notafloat,true\n2,ok,1.0,true\n")
	p, _ := newLoadProvider(data)
	seedLoadTable(t, p, "ds", "t", loadFields)
	// maxBadRecords=1 tolerates the single malformed numeric record.
	resp, err := p.InsertJob(context.Background(), loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":  "CSV",
		"maxBadRecords": "1",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["badRecords"] != "1" || ls["outputRows"] != "1" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

func TestLoadDefaultFormatIsCSV(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("5,alice\n"))
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", nil))) // no sourceFormat
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "1" {
		t.Fatalf("expected 1 row, got %v", ls)
	}
}

func TestLoadCreateNeverIsJobError(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("1\n"))
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "missing", loadCfg("ds", "missing", map[string]any{
		"sourceFormat":      "CSV",
		"createDisposition": "CREATE_NEVER",
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		}},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "notFound" {
		t.Fatalf("expected notFound job error, got %v", resp.Data["status"])
	}
	if _, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "missing"})); err == nil {
		t.Fatal("CREATE_NEVER must not create the table")
	}
}

func TestLoadFailureLeavesNoTable(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("notanint\n"))
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "newtbl", loadCfg("ds", "newtbl", map[string]any{
		"sourceFormat": "CSV",
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		}},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil {
		t.Fatalf("expected a job error, got %v", resp.Data["status"])
	}
	if _, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "newtbl"})); err == nil {
		t.Fatal("a failed load must not create the destination table")
	}
}

func TestLoadCustomNullMarker(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("N/A,3\n,4\n"))
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "s", "type": "STRING"},
		map[string]any{"name": "n", "type": "INTEGER"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
		"nullMarker":   "N/A",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	list := rows.Data["rows"].([]any)
	first := list[0].(map[string]any)["f"].([]any)
	if first[0].(map[string]any)["v"] != nil {
		t.Fatalf("a custom nullMarker value should be NULL, got %v", first[0])
	}
	second := list[1].(map[string]any)["f"].([]any)
	if second[0].(map[string]any)["v"] != "" {
		t.Fatalf("an empty STRING under a custom marker should be the empty string, got %v", second[0])
	}
}

func TestLoadCustomNullMarkerEmptyIntIsBad(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("x,\n"))
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "s", "type": "STRING"},
		map[string]any{"name": "n", "type": "INTEGER"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
		"nullMarker":   "N/A",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "invalid" {
		t.Fatalf("expected invalid job error, got %v", resp.Data["status"])
	}
}

func TestLoadNonFiniteFloatIsBadRecord(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("1,NaN\n"))
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "amount", "type": "FLOAT"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{"sourceFormat": "CSV"})))
	if err != nil {
		t.Fatalf("a non-finite float must not 500 the request: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "invalid" {
		t.Fatalf("expected invalid job error, got %v", resp.Data["status"])
	}
}

func TestLoadUnknownFieldAndIgnoreUnknown(t *testing.T) {
	ctx := context.Background()
	data := []byte("{\"id\":1,\"bogus\":2}\n")

	p, _ := newLoadProvider(data)
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "invalid" {
		t.Fatalf("an unknown field should fail by default, got %v", resp.Data["status"])
	}

	p2, _ := newLoadProvider(data)
	seedLoadTable(t, p2, "ds", "t", loadFields)
	resp2, err := p2.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":        "NEWLINE_DELIMITED_JSON",
		"ignoreUnknownValues": true,
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp2); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp2); ls["outputRows"] != "1" {
		t.Fatalf("expected 1 row, got %v", ls)
	}
}

func TestLoadCaseInsensitiveFields(t *testing.T) {
	ctx := context.Background()
	p, _ := newLoadProvider([]byte("{\"ID\":1,\"NAME\":\"x\"}\n"))
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("case-insensitive field matching failed: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "1" {
		t.Fatalf("expected 1 row, got %v", ls)
	}
}

func TestParseGSUri(t *testing.T) {
	for _, tc := range []struct {
		uri        string
		wantBucket string
		wantObject string
		wantGlob   bool
		wantErr    bool
	}{
		{"gs://b/o.json", "b", "o.json", false, false},
		{"gs://b/dir/o.csv", "b", "dir/o.csv", false, false},
		{"gs://b/*.json", "b", "*.json", true, false},
		{"gs://b/dir/part-*.parquet", "b", "dir/part-*.parquet", true, false},
		{"http://b/o", "", "", false, true},
		{"gs://b", "", "", false, true},
		{"gs://b/", "", "", false, true},
		{"gs://b/dir/?", "", "", false, true},
		{"gs://b/a*b*c", "", "", false, true},
		{"gs://b*/o", "", "", false, true},
	} {
		b, o, glob, err := parseGSUri(tc.uri)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: expected error", tc.uri)
			}
			continue
		}
		if err != nil || b != tc.wantBucket || o != tc.wantObject || glob != tc.wantGlob {
			t.Errorf("%s: got %q/%q glob=%v err=%v", tc.uri, b, o, glob, err)
		}
	}
}

func TestIntOption(t *testing.T) {
	if n, err := intOption(map[string]any{"n": "3"}, "n"); err != nil || n != 3 {
		t.Fatalf("string int option: %v %v", n, err)
	}
	if n, err := intOption(map[string]any{"n": float64(4)}, "n"); err != nil || n != 4 {
		t.Fatalf("float int option: %v %v", n, err)
	}
	if _, err := intOption(map[string]any{"n": "-1"}, "n"); err == nil {
		t.Fatal("expected error for negative")
	}
	if _, err := intOption(map[string]any{"n": "x"}, "n"); err == nil {
		t.Fatal("expected error for non-numeric")
	}
}

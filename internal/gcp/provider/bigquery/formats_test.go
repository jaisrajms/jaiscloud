package bigquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/parquet-go/parquet-go"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
)

// mustJSON marshals v for parseSchemaFields.
func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// ─── fixtures ────────────────────────────────────────────────────────────────

func encodeParquet[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("parquet write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet close: %v", err)
	}
	return buf.Bytes()
}

func encodeAvro(t *testing.T, schemaJSON string, rows []any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schemaJSON, &buf)
	if err != nil {
		t.Fatalf("avro encoder: %v", err)
	}
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			t.Fatalf("avro encode: %v", err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("avro close: %v", err)
	}
	return buf.Bytes()
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// providerWithObjects builds a provider whose fake GCS reader serves the given
// bucket/object map keys ("bucket/name").
func providerWithObjects(objects map[string][]byte) *Provider {
	return New(bqstore.NewMemoryStore(), WithSourceReader(&fakeSourceReader{objects: objects}))
}

// ─── Parquet ─────────────────────────────────────────────────────────────────

type pqFlatRow struct {
	ID     int64   `parquet:"id"`
	Name   string  `parquet:"name"`
	Score  float64 `parquet:"score"`
	Active bool    `parquet:"active"`
}

func TestLoadParquet(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqFlatRow{
		{1, "a", 1.5, true},
		{2, "b", 2.5, false},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
		map[string]any{"name": "score", "type": "FLOAT"},
		map[string]any{"name": "active", "type": "BOOLEAN"},
	})

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	if err != nil {
		t.Fatalf("insert parquet load: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" || ls["inputFiles"] != "1" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if rows.Data["totalRows"] != "2" {
		t.Fatalf("expected 2 rows, got %v", rows.Data["totalRows"])
	}
}

func TestLoadParquetEmbeddedSchema(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqFlatRow{{1, "a", 1.5, true}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	if err != nil {
		t.Fatalf("insert parquet load: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if len(got) != 4 || got[0].Name != "id" || got[0].Type != "INTEGER" || got[1].Name != "name" || got[1].Type != "STRING" {
		t.Fatalf("unexpected derived schema: %+v", got)
	}
}

type pqLogicalRow struct {
	ID int64 `parquet:"id"`
	D  int32 `parquet:"d,date"`
	TS int64 `parquet:"ts,timestamp(microsecond)"`
	T  int32 `parquet:"t,time(millisecond)"`
}

func TestLoadParquetLogicalTypes(t *testing.T) {
	ctx := context.Background()
	// 19000 days = 2022-01-08; 1_600_000_000_000_000 micros = 2020-09-13 12:26:40 UTC.
	data := encodeParquet(t, []pqLogicalRow{{1, 19000, 1600000000000000, 3600000}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "d", "type": "DATE"},
		map[string]any{"name": "ts", "type": "TIMESTAMP"},
		map[string]any{"name": "t", "type": "TIME"},
	})

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	first := rows.Data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if first[1].(map[string]any)["v"] != "2022-01-08" {
		t.Fatalf("date cell = %v", first[1])
	}
	if first[2].(map[string]any)["v"] != "2020-09-13 12:26:40 UTC" {
		t.Fatalf("timestamp cell = %v", first[2])
	}
	if first[3].(map[string]any)["v"] != "01:00:00" {
		t.Fatalf("time cell = %v", first[3])
	}
}

type pqChild struct {
	City string `parquet:"city"`
	Zip  int64  `parquet:"zip"`
}

type pqNestedRow struct {
	ID   int64    `parquet:"id"`
	Addr pqChild  `parquet:"addr"`
	Tags []string `parquet:"tags,list"`
}

func TestLoadParquetNestedAndRepeated(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqNestedRow{
		{1, pqChild{"paris", 75001}, []string{"a", "b"}},
		{2, pqChild{"rome", 100}, []string{"c"}},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "addr", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
			map[string]any{"name": "zip", "type": "INTEGER"},
		}},
		map[string]any{"name": "tags", "type": "STRING", "mode": "REPEATED"},
	})

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":        "PARQUET",
		"ignoreUnknownValues": true,
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	first := rows.Data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	addr := first[1].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if addr[0].(map[string]any)["v"] != "paris" || addr[1].(map[string]any)["v"] != "75001" {
		t.Fatalf("addr cell = %v", addr)
	}
	tags := first[2].(map[string]any)["v"].([]any)
	if len(tags) != 2 || tags[0].(map[string]any)["v"] != "a" {
		t.Fatalf("tags cell = %v", tags)
	}
}

func TestLoadParquetUnsupportedShape(t *testing.T) {
	ctx := context.Background()
	type withMap struct {
		ID int64             `parquet:"id"`
		M  map[string]string `parquet:"m"`
	}
	data := encodeParquet(t, []withMap{{1, map[string]string{"k": "v"}}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", loadFields)
	_, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	assertProviderErr(t, err, "Unimplemented", 501)
}

// ─── Avro ────────────────────────────────────────────────────────────────────

const avroSimpleSchema = `{"type":"record","name":"row","fields":[
 {"name":"id","type":"long"},
 {"name":"name","type":["null","string"]},
 {"name":"score","type":"double"},
 {"name":"active","type":"boolean"},
 {"name":"tags","type":{"type":"array","items":"string"}}
]}`

type avroSimpleRow struct {
	ID     int64    `avro:"id"`
	Name   *string  `avro:"name"`
	Score  float64  `avro:"score"`
	Active bool     `avro:"active"`
	Tags   []string `avro:"tags"`
}

func TestLoadAvro(t *testing.T) {
	ctx := context.Background()
	a, b := "a", "b"
	data := encodeAvro(t, avroSimpleSchema, []any{
		avroSimpleRow{1, &a, 1.5, true, []string{"x", "y"}},
		avroSimpleRow{2, &b, 2.5, false, []string{}},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
		map[string]any{"name": "score", "type": "FLOAT"},
		map[string]any{"name": "active", "type": "BOOLEAN"},
		map[string]any{"name": "tags", "type": "STRING", "mode": "REPEATED"},
	})

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "AVRO",
	})))
	if err != nil {
		t.Fatalf("insert avro load: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

func TestLoadAvroEmbeddedSchema(t *testing.T) {
	ctx := context.Background()
	a := "a"
	data := encodeAvro(t, avroSimpleSchema, []any{avroSimpleRow{1, &a, 1.5, true, nil}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat": "AVRO",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if len(got) != 5 || got[1].Name != "name" || got[1].Mode != "NULLABLE" || got[4].Mode != "REPEATED" {
		t.Fatalf("unexpected derived schema: %+v", got)
	}
}

func TestLoadAvroLogicalTypes(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"d","type":{"type":"int","logicalType":"date"}},
	 {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
	]}`
	type avroLogicalRow struct {
		ID int64     `avro:"id"`
		D  time.Time `avro:"d"`
		TS time.Time `avro:"ts"`
	}
	d := time.Date(2022, 1, 8, 0, 0, 0, 0, time.UTC)
	ts := time.Date(2020, 9, 13, 12, 26, 40, 0, time.UTC)
	data := encodeAvro(t, schema, []any{avroLogicalRow{1, d, ts}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "d", "type": "DATE"},
		map[string]any{"name": "ts", "type": "TIMESTAMP"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat":        "AVRO",
		"useAvroLogicalTypes": true,
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	first := rows.Data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	if first[1].(map[string]any)["v"] != "2022-01-08" {
		t.Fatalf("date cell = %v", first[1])
	}
	if first[2].(map[string]any)["v"] != "2020-09-13 12:26:40 UTC" {
		t.Fatalf("timestamp cell = %v", first[2])
	}
}

// TestLoadAvroLogicalTypesRaw verifies the Discovery default: with
// useAvroLogicalTypes unset, logical types load as their raw base types.
func TestLoadAvroLogicalTypesRaw(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"d","type":{"type":"int","logicalType":"date"}},
	 {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}}
	]}`
	type avroLogicalRow struct {
		ID int64     `avro:"id"`
		D  time.Time `avro:"d"`
		TS time.Time `avro:"ts"`
	}
	d := time.Date(2022, 1, 8, 0, 0, 0, 0, time.UTC)
	ts := time.Date(2020, 9, 13, 12, 26, 40, 0, time.UTC)
	data := encodeAvro(t, schema, []any{avroLogicalRow{1, d, ts}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat": "AVRO",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, _ := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if len(got) != 3 || got[1].Type != "INTEGER" || got[2].Type != "INTEGER" {
		t.Fatalf("expected raw INTEGER schema, got %+v", got)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	first := rows.Data["rows"].([]any)[0].(map[string]any)["f"].([]any)
	// 19000 days since epoch; 2020-09-13T12:26:40Z in millis.
	if first[1].(map[string]any)["v"] != "19000" {
		t.Fatalf("date cell = %v", first[1])
	}
	if first[2].(map[string]any)["v"] != "1600000000000" {
		t.Fatalf("timestamp cell = %v", first[2])
	}
}

func TestLoadAvroUnsupportedShape(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"m","type":{"type":"map","values":"string"}}
	]}`
	data := encodeAvro(t, schema, []any{map[string]any{"id": int64(1), "m": map[string]any{"k": "v"}}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", loadFields)
	_, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "AVRO",
	})))
	assertProviderErr(t, err, "Unimplemented", 501)
}

// ─── compression ─────────────────────────────────────────────────────────────

func TestLoadGzipCSV(t *testing.T) {
	ctx := context.Background()
	data := gzipBytes(t, []byte("1,a\n2,b\n"))
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})
	// Real BigQuery has no `compression` option on JobConfigurationLoad; gzip is
	// auto-detected from the source bytes.
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

// ─── glob / multi-URI ────────────────────────────────────────────────────────

func TestLoadWildcardGlob(t *testing.T) {
	ctx := context.Background()
	p := providerWithObjects(map[string][]byte{
		"bucket/dir/part-1.csv": []byte("1,a\n"),
		"bucket/dir/part-2.csv": []byte("2,b\n"),
		"bucket/dir/other.txt":  []byte("nope\n"),
	})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
		"sourceUris":   []any{"gs://bucket/dir/part-*.csv"},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	ls := loadStats(t, resp)
	if ls["inputFiles"] != "2" || ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

func TestLoadGlobNoMatchIsJobError(t *testing.T) {
	ctx := context.Background()
	p := providerWithObjects(map[string][]byte{"bucket/dir/part-1.csv": []byte("1,a\n")})
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
		"sourceUris":   []any{"gs://bucket/none-*.csv"},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er == nil || er["reason"] != "notFound" {
		t.Fatalf("expected notFound job error, got %v", resp.Data["status"])
	}
}

func TestLoadMultipleUris(t *testing.T) {
	ctx := context.Background()
	p := providerWithObjects(map[string][]byte{
		"bucket/one.csv": []byte("1,a\n"),
		"bucket/two.csv": []byte("2,b\n"),
	})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
		"sourceUris":   []any{"gs://bucket/one.csv", "gs://bucket/two.csv"},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["inputFiles"] != "2" || ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

// ─── autodetect ──────────────────────────────────────────────────────────────

func TestLoadAutodetectCSV(t *testing.T) {
	ctx := context.Background()
	p := providerWithObjects(map[string][]byte{"bucket/data": []byte("id,name,score\n1,a,1.5\n2,b,2.5\n")})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat":    "CSV",
		"autodetect":      true,
		"skipLeadingRows": "1",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, err := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if len(got) != 3 || got[0].Name != "id" || got[0].Type != "INTEGER" || got[2].Type != "FLOAT" {
		t.Fatalf("unexpected autodetected schema: %+v", got)
	}
}

func TestLoadAutodetectNDJSON(t *testing.T) {
	ctx := context.Background()
	// First row gives the union of keys; the second row omits "extra".
	data := []byte("{\"id\":1,\"name\":\"a\",\"extra\":true}\n{\"id\":2,\"name\":\"b\"}\n")
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
		"autodetect":   true,
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, _ := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if len(got) != 3 || got[0].Type != "INTEGER" || got[1].Type != "STRING" || got[2].Type != "BOOLEAN" {
		t.Fatalf("unexpected autodetected schema: %+v", got)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
}

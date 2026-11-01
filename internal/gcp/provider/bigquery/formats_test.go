package bigquery

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"

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

func TestLoadParquetVariantUnsupported(t *testing.T) {
	ctx := context.Background()
	// A Parquet VARIANT column has no documented BigQuery mapping: the Parquet
	// load type table has no entry for it and BigQuery's JSON type only accepts
	// batch loads from CSV, Avro or JSON.
	type withVariant struct {
		ID int64 `parquet:"id"`
		V  struct {
			Metadata []byte `parquet:"metadata"`
			Value    []byte `parquet:"value"`
		} `parquet:"v,variant"`
	}
	data := encodeParquet(t, []withVariant{{ID: 1}})
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

func TestLoadAvroArrayOfArraysUnsupported(t *testing.T) {
	ctx := context.Background()
	// The Avro load page: "Arrays of arrays are not supported."
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"aa","type":{"type":"array","items":{"type":"array","items":"string"}}}
	]}`
	data := encodeAvro(t, schema, []any{map[string]any{"id": int64(1), "aa": []any{[]any{"x"}}}})
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

// ─── Parquet decimals ────────────────────────────────────────────────────────

// parquetDecimalBytes renders an unscaled integer as the big-endian
// two's-complement fixed-width byte array a DECIMAL column stores.
func parquetDecimalBytes(t *testing.T, unscaled string, size int) []byte {
	t.Helper()
	i, ok := new(big.Int).SetString(unscaled, 10)
	if !ok {
		t.Fatalf("bad unscaled integer %q", unscaled)
	}
	if i.Sign() < 0 {
		i.Add(i, new(big.Int).Lsh(big.NewInt(1), uint(size*8)))
	}
	if i.BitLen() > size*8 {
		t.Fatalf("unscaled integer %s does not fit %d bytes", unscaled, size)
	}
	return i.FillBytes(make([]byte, size))
}

type pqDecimal9x2Row struct {
	ID     int64 `parquet:"id"`
	Amount int32 `parquet:"amount,decimal(2:9)"` // DECIMAL(9,2), INT32-backed
	Neg    int32 `parquet:"neg,decimal(2:9)"`    // DECIMAL(9,2), INT32-backed
}

type pqDecimal38x9Row struct {
	ID  int64    `parquet:"id"`
	Big [16]byte `parquet:"big,decimal(9:38)"` // DECIMAL(38,9), FIXED_LEN_BYTE_ARRAY-backed
}

func TestLoadParquetDecimal(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqDecimal9x2Row{
		{ID: 1, Amount: 123456789, Neg: -123456789}, // 1234567.89 / -1234567.89
		{ID: 2, Amount: 0, Neg: 42},                 // 0.00 / 0.42
	})
	bigData := encodeParquet(t, []pqDecimal38x9Row{
		{ID: 1, Big: [16]byte(parquetDecimalBytes(t, strings.Repeat("9", 38), 16))},
		{ID: 2, Big: [16]byte(parquetDecimalBytes(t, "-1234567890123456789012345678", 16))},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data, "bucket/big": bigData})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "amount", "type": "NUMERIC"},
		map[string]any{"name": "neg", "type": "NUMERIC"},
	})
	if _, err := p.CreateTable(ctx, newNR(map[string]any{"datasetId": "ds", "body": map[string]any{
		"tableReference": map[string]any{"datasetId": "ds", "tableId": "big"},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
			map[string]any{"name": "big", "type": "NUMERIC"},
		}},
	}})); err != nil {
		t.Fatalf("create big table: %v", err)
	}

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	cells := loadRowCells(t, p, "ds", "t")
	want := [][]any{
		{"1", "1234567.89", "-1234567.89"},
		{"2", "0.00", "0.42"},
	}
	for i := range want {
		for j := range want[i] {
			if cells[i][j] != want[i][j] {
				t.Fatalf("row %d cell %d = %v, want %v", i, j, cells[i][j], want[i][j])
			}
		}
	}

	resp, err = p.InsertJob(ctx, loadNR("ds", "big", loadCfg("ds", "big", map[string]any{
		"sourceFormat": "PARQUET",
		"sourceUris":   []any{"gs://bucket/big"},
	})))
	if err != nil {
		t.Fatalf("insert big: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected big job error: %v", er)
	}
	cells = loadRowCells(t, p, "ds", "big")
	if cells[0][1] != "99999999999999999999999999999.999999999" {
		t.Fatalf("DECIMAL(38,9) = %v", cells[0][1])
	}
	if cells[1][1] != "-1234567890123456789.012345678" {
		t.Fatalf("negative DECIMAL(38,9) = %v", cells[1][1])
	}
}

// TestLoadParquetDecimalTargets verifies the documented decimalTargetTypes
// selection: the default is ["NUMERIC"], and an explicit list moves a wider
// decimal to BIGNUMERIC or STRING.
func TestLoadParquetDecimalTargets(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqDecimal38x9Row{{ID: 1, Big: [16]byte(parquetDecimalBytes(t, "123456789", 16))}})
	cases := []struct {
		name    string
		options map[string]any
		want    string
	}{
		{"default", nil, "NUMERIC"},
		{"numeric-and-bignumeric", map[string]any{"decimalTargetTypes": []any{"NUMERIC", "BIGNUMERIC"}}, "NUMERIC"},
		{"string", map[string]any{"decimalTargetTypes": []any{"STRING"}}, "STRING"},
		{"bignumeric-only", map[string]any{"decimalTargetTypes": []any{"BIGNUMERIC"}}, "BIGNUMERIC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := providerWithObjects(map[string][]byte{"bucket/data": data})
			if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
				"datasetReference": map[string]any{"datasetId": "ds"},
			}})); err != nil {
				t.Fatalf("create dataset: %v", err)
			}
			load := map[string]any{"sourceFormat": "PARQUET", "writeDisposition": "WRITE_TRUNCATE"}
			for k, v := range tc.options {
				load[k] = v
			}
			resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", load)))
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
			if len(got) != 2 || got[1].Name != "big" || got[1].Type != tc.want {
				t.Fatalf("derived schema = %+v, want %s", got, tc.want)
			}
			cells := loadRowCells(t, p, "ds", "auto")
			if cells[0][1] != "0.123456789" {
				t.Fatalf("decimal value = %v", cells[0][1])
			}
		})
	}
}

type pqDecimal40x9Row struct {
	ID int64    `parquet:"id"`
	W  [17]byte `parquet:"w,decimal(9:40)"` // DECIMAL(40,9): wider than NUMERIC
}

// TestLoadParquetDecimalWiderThanNumeric covers the Discovery rule's "none of
// the listed types supports the precision and the scale" case: a DECIMAL(40,9)
// selects NUMERIC with the default ["NUMERIC"] list (then a value that does not
// fit it fails), and BIGNUMERIC when the list also offers it.
func TestLoadParquetDecimalWiderThanNumeric(t *testing.T) {
	ctx := context.Background()
	// 10^39 with scale 9: 31 integer digits, within BIGNUMERIC's 38 but beyond
	// NUMERIC's 29.
	unscaled := new(big.Int).Exp(big.NewInt(10), big.NewInt(39), nil).String()
	data := encodeParquet(t, []pqDecimal40x9Row{{ID: 1, W: [17]byte(parquetDecimalBytes(t, unscaled, 17))}})

	t.Run("bignumeric", func(t *testing.T) {
		p := providerWithObjects(map[string][]byte{"bucket/data": data})
		if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
			"datasetReference": map[string]any{"datasetId": "ds"},
		}})); err != nil {
			t.Fatalf("create dataset: %v", err)
		}
		resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
			"sourceFormat":       "PARQUET",
			"decimalTargetTypes": []any{"NUMERIC", "BIGNUMERIC"},
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
		if got[1].Type != "BIGNUMERIC" {
			t.Fatalf("DECIMAL(40,9) with [NUMERIC,BIGNUMERIC] = %s, want BIGNUMERIC", got[1].Type)
		}
		cells := loadRowCells(t, p, "ds", "auto")
		if cells[0][1] != "1000000000000000000000000000000.000000000" {
			t.Fatalf("BIGNUMERIC value = %v", cells[0][1])
		}
	})

	t.Run("default-numeric-overflows", func(t *testing.T) {
		p := providerWithObjects(map[string][]byte{"bucket/data": data})
		if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
			"datasetReference": map[string]any{"datasetId": "ds"},
		}})); err != nil {
			t.Fatalf("create dataset: %v", err)
		}
		_, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
			"sourceFormat": "PARQUET",
		})))
		assertProviderErr(t, err, "InvalidArgument", 400)
	})
}

// TestLoadParquetDecimalOutOfRange verifies that a value the selected target
// cannot hold fails the load with INVALID_ARGUMENT (the Discovery text for
// decimalTargetTypes: "if a value exceeds the supported range when reading the
// data, an error will be thrown").
func TestLoadParquetDecimalOutOfRange(t *testing.T) {
	ctx := context.Background()
	// 10^38 is 39 digits: with scale 9 that is 30 integer digits, one more than
	// NUMERIC (precision 38, scale 9) can hold.
	data := encodeParquet(t, []pqDecimal38x9Row{
		{ID: 1, Big: [16]byte(parquetDecimalBytes(t, "100000000000000000000000000000000000000", 16))},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	_, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	assertProviderErr(t, err, "InvalidArgument", 400)
}

// ─── Parquet maps and geospatial ─────────────────────────────────────────────

type pqMapRow struct {
	ID int64             `parquet:"id"`
	M  map[string]string `parquet:"m"`
}

func TestLoadParquetMap(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqMapRow{{ID: 1, M: map[string]string{"a": "1", "b": "2"}}})
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
	// Discovery MAP_TARGET_TYPE_UNSPECIFIED:
	// struct map_field_name { repeated struct key_value { key value } }
	if len(got) != 2 || got[1].Name != "m" || got[1].Type != "RECORD" || got[1].Mode != "REQUIRED" {
		t.Fatalf("derived map schema = %+v", got)
	}
	if len(got[1].Fields) != 1 || got[1].Fields[0].Name != "key_value" || got[1].Fields[0].Mode != "REPEATED" {
		t.Fatalf("derived map key_value shape = %+v", got[1].Fields)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	entries := cells[0][1].(map[string]any)["f"].([]any)[0].(map[string]any)["v"].([]any)
	got2 := map[string]string{}
	for _, e := range entries {
		pair := e.(map[string]any)["v"].(map[string]any)["f"].([]any)
		got2[pair[0].(map[string]any)["v"].(string)] = pair[1].(map[string]any)["v"].(string)
	}
	if len(got2) != 2 || got2["a"] != "1" || got2["b"] != "2" {
		t.Fatalf("map value = %v", got2)
	}
}

func TestLoadParquetMapArrayOfStruct(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqMapRow{{ID: 1, M: map[string]string{"a": "1"}}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat":   "PARQUET",
		"parquetOptions": map[string]any{"mapTargetType": "ARRAY_OF_STRUCT"},
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
	// Discovery ARRAY_OF_STRUCT: repeated struct map_field_name { key value }
	if len(got) != 2 || got[1].Name != "m" || got[1].Type != "RECORD" || got[1].Mode != "REPEATED" {
		t.Fatalf("derived map schema = %+v", got)
	}
	if len(got[1].Fields) != 2 || got[1].Fields[0].Name != "key" || got[1].Fields[1].Name != "value" {
		t.Fatalf("derived array-of-struct shape = %+v", got[1].Fields)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	entries := cells[0][1].([]any)
	pair := entries[0].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if pair[0].(map[string]any)["v"] != "a" || pair[1].(map[string]any)["v"] != "1" {
		t.Fatalf("array-of-struct entry = %v", pair)
	}
}

type pqMapKeyRow struct {
	ID int64 `parquet:"id"`
	M  map[int32]struct {
		V int32 `parquet:"v,decimal(2:9)"`
	} `parquet:"m"`
}

func TestLoadParquetMapKeyAndDecimalValue(t *testing.T) {
	ctx := context.Background()
	row := pqMapKeyRow{ID: 1, M: map[int32]struct {
		V int32 `parquet:"v,decimal(2:9)"`
	}{7: {V: 42}}}
	data := encodeParquet(t, []pqMapKeyRow{row})
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
	kv := got[1].Fields[0].Fields
	if kv[0].Name != "key" || kv[0].Type != "STRING" {
		t.Fatalf("map key field = %+v", kv[0])
	}
	if kv[1].Name != "value" || kv[1].Type != "RECORD" || kv[1].Fields[0].Type != "NUMERIC" {
		t.Fatalf("map value field = %+v", kv[1])
	}
	cells := loadRowCells(t, p, "ds", "auto")
	entries := cells[0][1].(map[string]any)["f"].([]any)[0].(map[string]any)["v"].([]any)
	pair := entries[0].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if pair[0].(map[string]any)["v"] != "7" {
		t.Fatalf("map key = %v (want the STRING \"7\")", pair[0].(map[string]any)["v"])
	}
	value := pair[1].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if value[0].(map[string]any)["v"] != "0.42" {
		t.Fatalf("decimal map value = %v (want the exact string 0.42)", value[0].(map[string]any)["v"])
	}
}

type pqGeoRow struct {
	ID int64  `parquet:"id"`
	G  []byte `parquet:"g,geometry"`
	H  []byte `parquet:"h,geography"`
}

func TestLoadParquetGeography(t *testing.T) {
	ctx := context.Background()
	// WKB for POINT(1 2).
	wkb := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}
	data := encodeParquet(t, []pqGeoRow{{ID: 1, G: wkb, H: wkb}})
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
	if got[1].Type != "GEOGRAPHY" || got[2].Type != "GEOGRAPHY" {
		t.Fatalf("derived geospatial schema = %+v", got)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	want := "0101000000000000000000f03f0000000000000040"
	if cells[0][1] != want || cells[0][2] != want {
		t.Fatalf("geography cells = %v / %v, want the hex WKB %s", cells[0][1], cells[0][2], want)
	}
}

type pqItem struct {
	Sku string `parquet:"sku"`
	Qty int64  `parquet:"qty"`
}

type pqOrderRow struct {
	ID    int64    `parquet:"id"`
	Items []pqItem `parquet:"items"`
	Addr  *pqChild `parquet:"addr"`
}

// TestLoadParquetRepeatedRecords exercises the multi-column assembly of a
// repeated group (two leaves under one repetition) and an absent optional
// struct, where the repetition/definition levels matter most.
func TestLoadParquetRepeatedRecords(t *testing.T) {
	ctx := context.Background()
	data := encodeParquet(t, []pqOrderRow{
		{ID: 1, Items: []pqItem{{Sku: "a", Qty: 2}, {Sku: "b", Qty: 3}}, Addr: &pqChild{City: "paris", Zip: 75001}},
		{ID: 2, Items: nil, Addr: nil},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "items", "type": "RECORD", "mode": "REPEATED", "fields": []any{
			map[string]any{"name": "sku", "type": "STRING"},
			map[string]any{"name": "qty", "type": "INTEGER"},
		}},
		map[string]any{"name": "addr", "type": "RECORD", "fields": []any{
			map[string]any{"name": "city", "type": "STRING"},
			map[string]any{"name": "zip", "type": "INTEGER"},
		}},
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
	cells := loadRowCells(t, p, "ds", "t")
	items := cells[0][1].([]any)
	if len(items) != 2 {
		t.Fatalf("row 1 items = %v", items)
	}
	first := items[0].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if first[0].(map[string]any)["v"] != "a" || first[1].(map[string]any)["v"] != "2" {
		t.Fatalf("row 1 first item = %v", first)
	}
	second := items[1].(map[string]any)["v"].(map[string]any)["f"].([]any)
	if second[0].(map[string]any)["v"] != "b" || second[1].(map[string]any)["v"] != "3" {
		t.Fatalf("row 1 second item = %v", second)
	}
	addr := cells[0][2].(map[string]any)["f"].([]any)
	if addr[0].(map[string]any)["v"] != "paris" || addr[1].(map[string]any)["v"] != "75001" {
		t.Fatalf("row 1 addr = %v", addr)
	}
	if got := cells[1][1].([]any); len(got) != 0 {
		t.Fatalf("row 2 items = %v, want an empty array", got)
	}
	if cells[1][2] != nil {
		t.Fatalf("row 2 addr = %v, want null", cells[1][2])
	}
}

// ─── Avro advanced shapes ────────────────────────────────────────────────────

type avroDecimalRow struct {
	ID int64    `avro:"id"`
	B  *big.Rat `avro:"b"`
	F  *big.Rat `avro:"f"`
}

const avroDecimalSchema = `{"type":"record","name":"row","fields":[
 {"name":"id","type":"long"},
 {"name":"b","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}},
 {"name":"f","type":{"type":"fixed","name":"dec16","size":16,"logicalType":"decimal","precision":38,"scale":9}}
]}`

func TestLoadAvroDecimal(t *testing.T) {
	ctx := context.Background()
	bigUnscaled, _ := new(big.Int).SetString(strings.Repeat("9", 38), 10)
	data := encodeAvro(t, avroDecimalSchema, []any{
		avroDecimalRow{ID: 1, B: big.NewRat(123456789, 100), F: new(big.Rat).SetFrac(bigUnscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil))},
	})
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
	// Default decimalTargetTypes for Avro is ["NUMERIC"].
	if len(got) != 3 || got[1].Type != "NUMERIC" || got[2].Type != "NUMERIC" {
		t.Fatalf("derived decimal schema = %+v", got)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	if cells[0][1] != "1234567.89" {
		t.Fatalf("bytes decimal = %v", cells[0][1])
	}
	if cells[0][2] != "99999999999999999999999999999.999999999" {
		t.Fatalf("fixed decimal = %v", cells[0][2])
	}
}

func TestLoadAvroDecimalStringTarget(t *testing.T) {
	ctx := context.Background()
	data := encodeAvro(t, avroDecimalSchema, []any{avroDecimalRow{ID: 1, B: big.NewRat(1, 4), F: big.NewRat(1, 4)}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": "ds"},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	resp, err := p.InsertJob(ctx, loadNR("ds", "auto", loadCfg("ds", "auto", map[string]any{
		"sourceFormat":       "AVRO",
		"decimalTargetTypes": []any{"STRING"},
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	tbl, _ := p.GetTable(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "auto"}))
	got := parseSchemaFields(mustJSON(t, tbl.Data["schema"]))
	if got[1].Type != "STRING" || got[2].Type != "STRING" {
		t.Fatalf("derived STRING decimal schema = %+v", got)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	if cells[0][1] != "0.25" {
		t.Fatalf("STRING decimal = %v", cells[0][1])
	}
}

func TestLoadAvroMap(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"m","type":{"type":"map","values":"string"}}
	]}`
	type row struct {
		ID int64             `avro:"id"`
		M  map[string]string `avro:"m"`
	}
	data := encodeAvro(t, schema, []any{row{ID: 1, M: map[string]string{"a": "1", "b": "2"}}})
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
	// map<T> -> "a repeated RECORD that contains two fields: a key and a value".
	if len(got) != 2 || got[1].Type != "RECORD" || got[1].Mode != "REPEATED" {
		t.Fatalf("derived map schema = %+v", got)
	}
	if len(got[1].Fields) != 2 || got[1].Fields[0].Name != "key" || got[1].Fields[0].Type != "STRING" {
		t.Fatalf("derived map fields = %+v", got[1].Fields)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	entries := cells[0][1].([]any)
	got2 := map[string]string{}
	for _, e := range entries {
		pair := e.(map[string]any)["v"].(map[string]any)["f"].([]any)
		got2[pair[0].(map[string]any)["v"].(string)] = pair[1].(map[string]any)["v"].(string)
	}
	if len(got2) != 2 || got2["a"] != "1" || got2["b"] != "2" {
		t.Fatalf("map value = %v", got2)
	}
}

func TestLoadAvroDuration(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"d","type":{"type":"fixed","name":"duration12","size":12,"logicalType":"duration"}}
	]}`
	type row struct {
		ID int64                `avro:"id"`
		D  avro.LogicalDuration `avro:"d"`
	}
	data := encodeAvro(t, schema, []any{row{ID: 1, D: avro.LogicalDuration{Months: 1, Days: 2, Milliseconds: 3}}})
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
	if got[1].Type != "BYTES" {
		t.Fatalf("derived duration schema = %+v", got)
	}
	raw := make([]byte, 12)
	binary.LittleEndian.PutUint32(raw[0:4], 1)
	binary.LittleEndian.PutUint32(raw[4:8], 2)
	binary.LittleEndian.PutUint32(raw[8:12], 3)
	want := base64.StdEncoding.EncodeToString(raw)
	cells := loadRowCells(t, p, "ds", "auto")
	if cells[0][1] != want {
		t.Fatalf("duration = %v, want the 12-byte base64 %s", cells[0][1], want)
	}
}

func TestLoadAvroMultiBranchUnion(t *testing.T) {
	ctx := context.Background()
	schema := `{"type":"record","name":"row","fields":[
	 {"name":"id","type":"long"},
	 {"name":"u","type":["null","int","string"]}
	]}`
	type row struct {
		ID int64 `avro:"id"`
		U  any   `avro:"u"`
	}
	data := encodeAvro(t, schema, []any{
		row{ID: 1, U: map[string]any{"int": int(7)}},
		row{ID: 2, U: map[string]any{"string": "x"}},
		row{ID: 3, U: nil},
	})
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
	// A union with more than one non-null branch -> RECORD with a list of
	// nullable fields.
	if got[1].Type != "RECORD" || got[1].Mode != "NULLABLE" || len(got[1].Fields) != 2 {
		t.Fatalf("derived union schema = %+v", got[1])
	}
	if got[1].Fields[0].Name != "int" || got[1].Fields[1].Name != "string" {
		t.Fatalf("union branch names = %+v", got[1].Fields)
	}
	cells := loadRowCells(t, p, "ds", "auto")
	first := cells[0][1].(map[string]any)["f"].([]any)
	if first[0].(map[string]any)["v"] != "7" || first[1].(map[string]any)["v"] != nil {
		t.Fatalf("int branch row = %v", first)
	}
	second := cells[1][1].(map[string]any)["f"].([]any)
	if second[0].(map[string]any)["v"] != nil || second[1].(map[string]any)["v"] != "x" {
		t.Fatalf("string branch row = %v", second)
	}
	if cells[2][1] != nil {
		t.Fatalf("null union row = %v", cells[2][1])
	}
}

// TestLoadAvroNullableUnionUnwrapped is the regression for a union with one
// non-null branch: hamba decodes it as a one-entry map, which must be unwrapped
// to the plain nullable value.
func TestLoadAvroNullableUnionUnwrapped(t *testing.T) {
	ctx := context.Background()
	a := "a"
	data := encodeAvro(t, avroSimpleSchema, []any{avroSimpleRow{1, &a, 1.5, true, []string{"x"}}})
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
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	cells := loadRowCells(t, p, "ds", "t")
	if cells[0][1] != "a" {
		t.Fatalf("nullable union cell = %v, want the plain value \"a\"", cells[0][1])
	}
}

// loadRowCells renders every row of a table as its cell values, in schema order.
func loadRowCells(t *testing.T, p *Provider, ds, tbl string) [][]any {
	t.Helper()
	resp, err := p.ListRows(context.Background(), newNR(map[string]any{"datasetId": ds, "tableId": tbl}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	rows, _ := resp.Data["rows"].([]any)
	out := make([][]any, 0, len(rows))
	for _, r := range rows {
		cells := r.(map[string]any)["f"].([]any)
		values := make([]any, 0, len(cells))
		for _, c := range cells {
			values = append(values, c.(map[string]any)["v"])
		}
		out = append(out, values)
	}
	return out
}

// TestLoadParquetInt96 covers the type table's "INT96 -> TIMESTAMP" row: the
// legacy 12-byte timestamp is nanoseconds-of-day plus the Julian day number.
func TestLoadParquetInt96(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2020, 9, 13, 12, 26, 40, 0, time.UTC)
	nanos := uint64(12*3600+26*60+40) * uint64(time.Second)
	raw := deprecated.Int96{uint32(nanos), uint32(nanos >> 32), uint32(2440588 + ts.Unix()/86400)}

	schema := parquet.NewSchema("row", parquet.Group{"ts": parquet.Leaf(parquet.Int96Type)})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)
	if _, err := w.WriteRows([]parquet.Row{schema.Deconstruct(nil, map[string]any{"ts": raw})}); err != nil {
		t.Fatalf("write INT96 parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close INT96 parquet: %v", err)
	}

	p := providerWithObjects(map[string][]byte{"bucket/data": buf.Bytes()})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "ts", "type": "TIMESTAMP"},
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
	cells := loadRowCells(t, p, "ds", "t")
	if cells[0][0] != "2020-09-13 12:26:40 UTC" {
		t.Fatalf("INT96 timestamp = %v", cells[0][0])
	}
}

package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
)

// withLoadSpillThreshold lowers the spill threshold for one test and restores
// it afterwards (the spool's threshold is a package variable so a small fixture
// can exercise the file-backed path).
func withLoadSpillThreshold(t *testing.T, n int64) {
	t.Helper()
	old := loadSpillThreshold
	loadSpillThreshold = n
	t.Cleanup(func() { loadSpillThreshold = old })
}

// tempDirForLoads points os.CreateTemp at a private directory and returns it, so
// a test can assert that a spilled source's temp file was removed.
func tempDirForLoads(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	return dir
}

// assertNoTempFiles fails when dir holds anything (a leaked spill file).
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("leaked temp files in %s: %v", dir, names)
	}
}

// seedLoadDataset creates the dataset a load can auto-create its destination
// table in.
func seedLoadDataset(t *testing.T, p *Provider, ds string) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.CreateDataset(ctx, newNR(map[string]any{"body": map[string]any{
		"datasetReference": map[string]any{"datasetId": ds},
	}})); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
}

// ndjsonRows builds count NDJSON records for the loadFields schema.
func ndjsonRows(count int) []byte {
	var b bytes.Buffer
	for i := 0; i < count; i++ {
		fmt.Fprintf(&b, "{\"id\":%d,\"name\":\"row-%d\",\"amount\":%d.5,\"active\":true}\n", i, i, i)
	}
	return b.Bytes()
}

// ─── loadSpool ───────────────────────────────────────────────────────────────

func TestLoadSpoolSpillsReopensAndCleansUp(t *testing.T) {
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 16)

	spool := newLoadSpool()
	first := bytes.Repeat([]byte("a"), 10)
	second := bytes.Repeat([]byte("b"), 10)
	if _, err := spool.Write(first); err != nil {
		t.Fatalf("write: %v", err)
	}
	if spool.Spilled() {
		t.Fatal("spool spilled before the threshold")
	}
	if _, err := spool.Write(second); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !spool.Spilled() {
		t.Fatal("spool did not spill past the threshold")
	}
	if spool.Size() != 20 {
		t.Fatalf("size = %d, want 20", spool.Size())
	}
	// Two Open calls are independent readers over the same bytes, which is what
	// lets a self-describing format probe a schema and then decode the rows.
	for i := 0; i < 2; i++ {
		rc, err := spool.Open()
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, append(append([]byte{}, first...), second...)) {
			t.Fatalf("open %d read %q", i, got)
		}
	}
	// Random access (Parquet) reads from the same file.
	ra, size := spool.ReaderAt()
	if size != 20 {
		t.Fatalf("ReaderAt size = %d", size)
	}
	buf := make([]byte, 4)
	if _, err := ra.ReadAt(buf, 8); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf) != "aabb" {
		t.Fatalf("ReadAt at 8 = %q", buf)
	}
	if err := spool.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertNoTempFiles(t, dir)
	// Close is idempotent.
	if err := spool.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestLoadSpoolAppendReaderCapsSize(t *testing.T) {
	tempDirForLoads(t)
	withLoadSpillThreshold(t, 4)

	spool := newLoadSpool()
	defer spool.Close()
	if err := spool.appendReader(strings.NewReader("0123456789"), 6); err != errLoadSourceTooLarge {
		t.Fatalf("appendReader error = %v, want errLoadSourceTooLarge", err)
	}
}

// ─── BQF3: gs:// sources stream from the seam ────────────────────────────────

// TestLoadStreamsGSObjectInBoundedChunks proves a large gs:// source is consumed
// through the seam in bounded reads rather than as one buffer.
func TestLoadStreamsGSObjectInBoundedChunks(t *testing.T) {
	ctx := context.Background()
	maxRead := 0
	// ~4 MiB of NDJSON.
	rows := ndjsonRows(20000)
	if len(rows) < 1<<20 {
		t.Fatalf("fixture too small: %d bytes", len(rows))
	}
	r := &fakeSourceReader{objects: map[string][]byte{"bucket/data": rows}, maxRead: &maxRead}
	p := New(bqstore.NewMemoryStore(), WithSourceReader(r))
	seedLoadTable(t, p, "ds", "t", loadFields)

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "20000" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	if maxRead == 0 {
		t.Fatal("the source was never read through the seam")
	}
	if maxRead > 1<<20 {
		t.Fatalf("largest single read = %d bytes; the source is not streamed in bounded chunks", maxRead)
	}
}

// TestLoadSpillsGSObjectToTempFile lowers the threshold so the source is
// file-backed, and asserts the load is correct and the temp file is removed.
func TestLoadSpillsGSObjectToTempFile(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 64)

	p := providerWithObjects(map[string][]byte{"bucket/data": ndjsonRows(50)})
	seedLoadTable(t, p, "ds", "t", loadFields)

	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "50" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestLoadMultiFileSpilledSources keeps the multi-object/wildcard contract while
// every source is file-backed.
func TestLoadMultiFileSpilledSources(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 32)

	p := providerWithObjects(map[string][]byte{
		"bucket/dir/part-1.csv": []byte("1,a\n2,b\n3,c\n"),
		"bucket/dir/part-2.csv": []byte("4,d\n5,e\n"),
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
	if ls["outputRows"] != "5" || ls["inputFiles"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestLoadGzipCSVFromSpilledSource keeps the streaming gzip path honest when the
// source itself is file-backed.
func TestLoadGzipCSVFromSpilledSource(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 8)

	p := providerWithObjects(map[string][]byte{"bucket/data": gzipBytes(t, []byte("1,a\n2,b\n"))})
	seedLoadTable(t, p, "ds", "t", []any{
		map[string]any{"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
		map[string]any{"name": "name", "type": "STRING"},
	})
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "CSV",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "2" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestLoadGzipNDJSONAutodetectFromSpilledSource covers autodetect reading a
// file-backed, gzip-wrapped source.
func TestLoadGzipNDJSONAutodetectFromSpilledSource(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 8)

	p := providerWithObjects(map[string][]byte{"bucket/data": gzipBytes(t, ndjsonRows(20))})
	seedLoadDataset(t, p, "ds")
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
		"autodetect":   true,
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "20" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestLoadParquetFromSpilledSource exercises the random-access decode path over a
// file-backed source.
func TestLoadParquetFromSpilledSource(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 16)

	data := encodeParquet(t, []pqFlatRow{{ID: 1, Name: "a", Score: 1.5, Active: true}})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadDataset(t, p, "ds")
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "PARQUET",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "1" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestLoadAvroFromSpilledSource covers the two-pass Avro decode (schema probe,
// then rows) over a file-backed source.
func TestLoadAvroFromSpilledSource(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 16)

	schema := `{"type":"record","name":"r","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}`
	data := encodeAvro(t, schema, []any{
		map[string]any{"id": int64(1), "name": "a"},
		map[string]any{"id": int64(2), "name": "b"},
	})
	p := providerWithObjects(map[string][]byte{"bucket/data": data})
	seedLoadDataset(t, p, "ds")
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "AVRO",
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
	assertNoTempFiles(t, dir)
}

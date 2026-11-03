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
	"jaiscloud/internal/gcp/wire"
)

// ─── BQF5: multipart and resumable bodies ────────────────────────────────────

// TestInsertJobMultipartUploadStreamSpills asserts a streamed multipart media
// part is spooled to a file past the threshold and the file is removed.
func TestInsertJobMultipartUploadStreamSpills(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 64)

	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)
	src := ndjsonRows(40)
	resp, err := p.InsertJob(ctx, newNR(map[string]any{
		"project":      "proj",
		"body":         uploadJobBody("ds", "t", "up-spill"),
		wire.StreamKey: io.NopCloser(bytes.NewReader(src)),
	}))
	if err != nil {
		t.Fatalf("streamed upload: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "40" {
		t.Fatalf("unexpected stats: %v", ls)
	}
	assertNoTempFiles(t, dir)
}

// TestInsertJobResumableUploadSpillsChunks drives a chunked upload whose
// accumulated bytes are file-backed between chunks, then completes it.
func TestInsertJobResumableUploadSpillsChunks(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 48)

	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	start, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project":       "proj",
		"body":          uploadJobBody("ds", "t", "res-spill"),
		wire.BaseURLKey: "http://bq.local",
	}))
	if err != nil {
		t.Fatalf("resumable start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	const prefix = "http://bq.local/upload/bigquery/v2/projects/proj/jobs?uploadType=resumable&upload_id="
	id := strings.TrimPrefix(loc, prefix)
	if id == loc {
		t.Fatalf("unexpected Location %q", loc)
	}

	src := ndjsonRows(30)
	chunk := len(src) / 3
	var sent int
	for sent < len(src) {
		end := sent + chunk
		if end > len(src) {
			end = len(src)
		}
		part := src[sent:end]
		last := end >= len(src)
		resp, err := p.InsertJobResumable(ctx, newNR(map[string]any{
			"project":      "proj",
			"upload_id":    id,
			wire.StreamKey: io.NopCloser(bytes.NewReader(part)),
			"contentRange": fmt.Sprintf("bytes %d-%d/%d", sent, end-1, len(src)),
		}))
		if err != nil {
			t.Fatalf("chunk %d: %v", sent, err)
		}
		sent = end
		if last {
			if er := jobErrorResult(resp); er != nil {
				t.Fatalf("unexpected job error: %v", er)
			}
			if ls := loadStats(t, resp); ls["outputRows"] != "30" {
				t.Fatalf("unexpected stats: %v", ls)
			}
			continue
		}
		if resp.HTTPStatus != 308 {
			t.Fatalf("chunk %d status = %d, want 308", sent, resp.HTTPStatus)
		}
		// The session's accumulated bytes are file-backed between chunks.
		entries, rerr := os.ReadDir(dir)
		if rerr != nil {
			t.Fatalf("read %s: %v", dir, rerr)
		}
		if len(entries) == 0 {
			t.Fatalf("expected a spilled session file after %d bytes", sent)
		}
	}
	assertNoTempFiles(t, dir)
}

// TestInsertJobResumableUploadFailureCleansUp leaves a spilled session file
// behind through a failed load (too many bad records) and asserts it is removed.
func TestInsertJobResumableUploadFailureCleansUp(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 32)

	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	start, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    uploadJobBody("ds", "t", "res-fail"),
	}))
	if err != nil {
		t.Fatalf("resumable start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	id := loc[strings.Index(loc, "upload_id=")+len("upload_id="):]

	// Two records that cannot be coerced to the schema.
	src := []byte("not json\n{\"id\":\"bad\",\"name\":1}\n")
	dst := newNR(map[string]any{
		"project":      "proj",
		"upload_id":    id,
		wire.StreamKey: io.NopCloser(bytes.NewReader(src)),
		"contentRange": fmt.Sprintf("bytes 0-%d/%d", len(src)-1, len(src)),
	})
	resp, err := p.InsertJobResumable(ctx, dst)
	if err != nil {
		t.Fatalf("final chunk: %v", err)
	}
	if er := jobErrorResult(resp); er == nil {
		t.Fatal("expected a job error for records that cannot be coerced")
	}
	assertNoTempFiles(t, dir)
}

// TestResetRemovesSpilledSessionFile covers an in-flight resumable session whose
// accumulated bytes are file-backed when Reset runs.
func TestResetRemovesSpilledSessionFile(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 32)

	p := New(bqstore.NewMemoryStore())
	start, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    uploadJobBody("ds", "t", "res-reset"),
	}))
	if err != nil {
		t.Fatalf("resumable start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	id := loc[strings.Index(loc, "upload_id=")+len("upload_id="):]

	src := ndjsonRows(10)
	if _, err := p.InsertJobResumable(ctx, newNR(map[string]any{
		"project":      "proj",
		"upload_id":    id,
		wire.StreamKey: io.NopCloser(bytes.NewReader(src)),
		"contentRange": fmt.Sprintf("bytes 0-%d/*", len(src)-1),
	})); err != nil {
		t.Fatalf("chunk: %v", err)
	}
	entries, rerr := os.ReadDir(dir)
	if rerr != nil || len(entries) == 0 {
		t.Fatalf("expected a spilled session file (entries=%d err=%v)", len(entries), rerr)
	}

	p.Reset(ctx)
	assertNoTempFiles(t, dir)
	p.mu.Lock()
	n := len(p.uploads)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("Reset left %d upload sessions", n)
	}
}

// TestLoadSpilledSourceStatsMatchBytes keeps statistics.load.inputFileBytes tied
// to the source size on the file-backed path.
func TestLoadSpilledSourceStatsMatchBytes(t *testing.T) {
	ctx := context.Background()
	dir := tempDirForLoads(t)
	withLoadSpillThreshold(t, 32)

	src := ndjsonRows(12)
	p := providerWithObjects(map[string][]byte{"bucket/data": src})
	seedLoadTable(t, p, "ds", "t", loadFields)
	resp, err := p.InsertJob(ctx, loadNR("ds", "t", loadCfg("ds", "t", map[string]any{
		"sourceFormat": "NEWLINE_DELIMITED_JSON",
	})))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if ls := loadStats(t, resp); ls["inputFileBytes"] != fmt.Sprintf("%d", len(src)) {
		t.Fatalf("inputFileBytes = %v, want %d", ls["inputFileBytes"], len(src))
	}
	assertNoTempFiles(t, dir)
}

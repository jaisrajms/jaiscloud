package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	bqstore "jaiscloud/internal/gcp/store/bigquery"
	"jaiscloud/internal/gcp/wire"
)

// uploadJobBody returns a jobs.insert body for a load of ds.t into the upload
// table. The source bytes are supplied out-of-band (media part / resumable
// chunks), so there is no configuration.load.sourceUris.
func uploadJobBody(ds, tbl, jobID string) map[string]any {
	return map[string]any{
		"jobReference": map[string]any{"projectId": "proj", "jobId": jobID},
		"configuration": map[string]any{"load": map[string]any{
			"destinationTable": map[string]any{"projectId": "proj", "datasetId": ds, "tableId": tbl},
			"sourceFormat":     "NEWLINE_DELIMITED_JSON",
		}},
	}
}

func TestInsertJobMultipartUpload(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore()) // no gs:// reader needed for an upload
	seedLoadTable(t, p, "ds", "t", loadFields)

	// Buffered media (wire.MediaKey), as the gateway delivers a multipart body.
	data := []byte("{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n")
	resp, err := p.InsertJob(ctx, newNR(map[string]any{
		"project":     "proj",
		"body":        uploadJobBody("ds", "t", "up1"),
		wire.MediaKey: data,
	}))
	if err != nil {
		t.Fatalf("multipart upload: %v", err)
	}
	if er := jobErrorResult(resp); er != nil {
		t.Fatalf("unexpected job error: %v", er)
	}
	if resp.Data["status"].(map[string]any)["state"] != "DONE" {
		t.Fatalf("expected DONE, got %v", resp.Data["status"])
	}
	ls := loadStats(t, resp)
	if ls["outputRows"] != "2" || ls["inputFiles"] != "0" {
		t.Fatalf("unexpected upload stats: %v", ls)
	}
	rows, err := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if err != nil {
		t.Fatalf("list rows: %v", err)
	}
	if rows.Data["totalRows"] != "2" {
		t.Fatalf("expected 2 loaded rows, got %v", rows.Data["totalRows"])
	}
}

func TestInsertJobMultipartUploadStream(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	// Streamed media (wire.StreamKey), as parseMultipart surfaces it.
	resp, err := p.InsertJob(ctx, newNR(map[string]any{
		"project":      "proj",
		"body":         uploadJobBody("ds", "t", "up2"),
		wire.StreamKey: bytes.NewReader([]byte("{\"id\":7,\"name\":\"z\"}\n")),
	}))
	if err != nil {
		t.Fatalf("streamed upload: %v", err)
	}
	if ls := loadStats(t, resp); ls["outputRows"] != "1" {
		t.Fatalf("expected 1 row, got %v", ls)
	}
}

func TestInsertJobResumableUpload(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	// Initiate: 200 + absolute Location carrying the session id.
	start, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project":       "proj",
		"body":          uploadJobBody("ds", "t", "res1"),
		wire.BaseURLKey: "http://bq.local",
	}))
	if err != nil {
		t.Fatalf("resumable start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	prefix := "http://bq.local/upload/bigquery/v2/projects/proj/jobs?uploadType=resumable&upload_id="
	if !strings.HasPrefix(loc, prefix) {
		t.Fatalf("Location = %q, want prefix %q", loc, prefix)
	}
	id := strings.TrimPrefix(loc, prefix)

	src := []byte("{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n")
	split := 12
	first, second := src[:split], src[split:]

	// First chunk is incomplete → 308 + Range.
	inc, err := p.InsertJobResumable(ctx, newNR(map[string]any{
		"project":      "proj",
		"upload_id":    id,
		wire.MediaKey:  first,
		"contentRange": fmt.Sprintf("bytes 0-%d/%d", len(first)-1, len(src)),
	}))
	if err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if inc.HTTPStatus != 308 {
		t.Fatalf("incomplete chunk status = %d, want 308", inc.HTTPStatus)
	}
	if got, _ := inc.Data[wire.RangeKey].(string); got != fmt.Sprintf("bytes=0-%d", len(first)-1) {
		t.Fatalf("incomplete chunk Range = %q", got)
	}

	// Final chunk completes the source → 200 + Job, rows written.
	done, err := p.InsertJobResumable(ctx, newNR(map[string]any{
		"project":      "proj",
		"upload_id":    id,
		wire.MediaKey:  second,
		"contentRange": fmt.Sprintf("bytes %d-%d/%d", len(first), len(src)-1, len(src)),
	}))
	if err != nil {
		t.Fatalf("final chunk: %v", err)
	}
	if err := jobErrorResult(done); err != nil {
		t.Fatalf("unexpected job error: %v", err)
	}
	if ls := loadStats(t, done); ls["outputRows"] != "2" {
		t.Fatalf("expected 2 rows, got %v", ls)
	}
	rows, _ := p.ListRows(ctx, newNR(map[string]any{"datasetId": "ds", "tableId": "t"}))
	if rows.Data["totalRows"] != "2" {
		t.Fatalf("expected 2 stored rows, got %v", rows.Data["totalRows"])
	}
	// The session is consumed: a further chunk is unknown.
	if _, err := p.InsertJobResumable(ctx, newNR(map[string]any{
		"project": "proj", "upload_id": id, wire.MediaKey: []byte("x"), "contentRange": "bytes */3",
	})); err == nil {
		t.Fatal("expected a 404 for a consumed session")
	}
}

func TestInsertJobResumableStartFailLoud(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	// Unsupported option fails at initiate, before any bytes are uploaded.
	body := uploadJobBody("ds", "t", "bad1")
	body["configuration"].(map[string]any)["load"].(map[string]any)["allowJaggedRows"] = true
	_, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{"project": "proj", "body": body}))
	assertProviderErr(t, err, "Unimplemented", 501)

	// No configuration.load → InvalidArgument.
	_, err = p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project": "proj",
		"body":    map[string]any{"jobReference": map[string]any{"jobId": "bad2"}},
	}))
	assertProviderErr(t, err, "InvalidArgument", 400)
}

func TestInsertJobResumableDuplicateJob(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	seedLoadTable(t, p, "ds", "t", loadFields)

	// A completed jobs.insert makes a later resumable initiate with the same
	// jobId a duplicate.
	if _, err := p.InsertJob(ctx, newNR(map[string]any{
		"project":     "proj",
		"body":        uploadJobBody("ds", "t", "dup"),
		wire.MediaKey: []byte("{\"id\":1,\"name\":\"a\"}\n"),
	})); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	_, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project": "proj", "body": uploadJobBody("ds", "t", "dup"),
	}))
	assertProviderErr(t, err, "AlreadyExists", 409)
}

func TestResetClearsUploadSessions(t *testing.T) {
	ctx := context.Background()
	p := New(bqstore.NewMemoryStore())
	if _, err := p.InsertJobResumableStart(ctx, newNR(map[string]any{
		"project": "proj", "body": uploadJobBody("ds", "t", "s1"),
	})); err != nil {
		t.Fatalf("start: %v", err)
	}
	p.Reset(ctx)
	p.mu.Lock()
	n := len(p.uploads)
	p.mu.Unlock()
	if n != 0 {
		t.Fatalf("Reset left %d upload sessions", n)
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in       string
		start    int64
		total    int64
		hasTotal bool
		wantErr  bool
	}{
		{"bytes 0-99/100", 0, 100, true, false},
		{"bytes 10-19/*", 10, 0, false, false},
		{"bytes */100", -1, 100, true, false},
		{"bytes 0-0/1", 0, 1, true, false},
		{"", 0, 0, false, true},
		{"items 0-1/2", 0, 0, false, true},
		{"bytes 0-1", 0, 0, false, true},
		{"bytes x-1/2", 0, 0, false, true},
	}
	for _, tc := range cases {
		start, total, hasTotal, err := parseContentRange(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: expected error", tc.in)
			}
			continue
		}
		if err != nil || start != tc.start || total != tc.total || hasTotal != tc.hasTotal {
			t.Errorf("%q: got (%d,%d,%v,%v), want (%d,%d,%v,nil)", tc.in, start, total, hasTotal, err, tc.start, tc.total, tc.hasTotal)
		}
	}
}

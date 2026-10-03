package gcp

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// buildBatchBody assembles a multipart/mixed body whose parts each contain a
// raw embedded HTTP request, exactly as the google-api-client emits.
func buildBatchBody(t *testing.T, boundary string, reqs ...string) []byte {
	t.Helper()
	var b bytes.Buffer
	for i, raw := range reqs {
		fmt.Fprintf(&b, "--%s\r\n", boundary)
		b.WriteString("Content-Type: application/http\r\n")
		fmt.Fprintf(&b, "Content-ID: %d\r\n", i+1)
		b.WriteString("\r\n")
		b.WriteString(raw)
		b.WriteString("\r\n")
	}
	fmt.Fprintf(&b, "--%s--\r\n", boundary)
	return b.Bytes()
}

type batchResponsePart struct {
	contentID string
	status    int
	body      string
}

// parseBatchResponse splits a multipart/mixed batch response into its embedded
// status/body parts.
func parseBatchResponse(t *testing.T, resp *http.Response, raw []byte) []batchResponsePart {
	t.Helper()
	ct := resp.Header.Get("Content-Type")
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil || mt != "multipart/mixed" {
		t.Fatalf("response Content-Type = %q (err %v), want multipart/mixed", ct, err)
	}
	mr := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	var out []batchResponsePart
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read response part: %v", err)
		}
		body, _ := io.ReadAll(p)
		// The part body is an embedded HTTP response: status line, headers,
		// blank line, then the payload.
		head, payload, _ := strings.Cut(string(body), "\r\n\r\n")
		lines := strings.Split(head, "\r\n")
		if len(lines) == 0 {
			t.Fatalf("part has no status line: %q", head)
		}
		fields := strings.SplitN(lines[0], " ", 3)
		if len(fields) < 2 {
			t.Fatalf("malformed status line %q", lines[0])
		}
		status, _ := strconv.Atoi(fields[1])
		out = append(out, batchResponsePart{
			contentID: p.Header.Get("Content-ID"),
			status:    status,
			body:      payload,
		})
	}
	return out
}

func TestGCPAdapterIsBatchRequest(t *testing.T) {
	a := New()
	cases := []struct {
		method string
		path   string
		want   bool
	}{
		{http.MethodPost, "/batch/storage/v1", true},
		{http.MethodPost, "//batch/storage/v1", true},
		{http.MethodPost, "/batch/storage/v1/b/bkt/o/x", true},
		{http.MethodGet, "/batch/storage/v1", false},
		{http.MethodPost, "/storage/v1/b/bkt/o", false},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if got := a.IsBatchRequest(r); got != tc.want {
			t.Errorf("IsBatchRequest(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestGCPAdapterServeBatchGetDeleteMissingAndEncodedName(t *testing.T) {
	const boundary = "batch_test_boundary"
	body := buildBatchBody(t, boundary,
		"GET http://localhost:8080/storage/v1/b/bkt/o/folder%2Fobj.txt?alt=json HTTP/1.1\r\nAccept: application/json\r\n\r\n",
		"GET http://localhost:8080/storage/v1/b/bkt/o/missing.txt?alt=json HTTP/1.1\r\n\r\n",
		"DELETE http://localhost:8080/storage/v1/b/bkt/o/gone.txt HTTP/1.1\r\n\r\n",
	)

	var seen []string
	process := func(ctx context.Context, r *http.Request, subBody []byte) (int, http.Header, []byte) {
		seen = append(seen, r.Method+" "+r.URL.EscapedPath())
		h := http.Header{}
		h.Set("Content-Type", "application/json; charset=UTF-8")
		switch {
		case r.Method == http.MethodDelete:
			return http.StatusNoContent, h, []byte("{}")
		case strings.Contains(r.URL.EscapedPath(), "missing"):
			return http.StatusNotFound, h, []byte(`{"error":{"code":404,"message":"object not found"}}`)
		default:
			return http.StatusOK, h, []byte(`{"kind":"storage#object","name":"folder/obj.txt"}`)
		}
	}

	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, body, process)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	want := []string{
		"GET /storage/v1/b/bkt/o/folder%2Fobj.txt",
		"GET /storage/v1/b/bkt/o/missing.txt",
		"DELETE /storage/v1/b/bkt/o/gone.txt",
	}
	if strings.Join(seen, "|") != strings.Join(want, "|") {
		t.Fatalf("dispatched %v, want %v", seen, want)
	}

	parts := parseBatchResponse(t, rec.Result(), rec.Body.Bytes())
	if len(parts) != 3 {
		t.Fatalf("got %d parts, want 3", len(parts))
	}
	if parts[0].status != 200 || !strings.Contains(parts[0].body, "folder/obj.txt") {
		t.Errorf("part0 = %+v, want 200 with object body", parts[0])
	}
	if parts[1].status != 404 || !strings.Contains(parts[1].body, "object not found") {
		t.Errorf("part1 = %+v, want 404", parts[1])
	}
	if parts[2].status != 204 {
		t.Errorf("part2 = %+v, want 204", parts[2])
	}
	// Content-IDs are echoed with the <response-N> convention, in order.
	if parts[0].contentID != "<response-1>" || parts[2].contentID != "<response-3>" {
		t.Errorf("content-ids = %q,%q,%q", parts[0].contentID, parts[1].contentID, parts[2].contentID)
	}
}

// TestGCPAdapterServeBatchForwardsRetryAfter proves a throttled sub-response
// keeps its Retry-After header in the multiplexed batch part.
func TestGCPAdapterServeBatchForwardsRetryAfter(t *testing.T) {
	const boundary = "b"
	body := buildBatchBody(t, boundary,
		"GET http://localhost:8080/storage/v1/b/bkt/o/x?alt=json HTTP/1.1\r\n\r\n",
	)
	process := func(context.Context, *http.Request, []byte) (int, http.Header, []byte) {
		h := http.Header{}
		h.Set("Content-Type", "application/json; charset=UTF-8")
		h.Set("Retry-After", "7")
		return http.StatusTooManyRequests, h, []byte(`{"error":{"code":429,"message":"throttled"}}`)
	}

	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, body, process)

	if !strings.Contains(rec.Body.String(), "Retry-After: 7") {
		t.Fatalf("batch part must carry Retry-After: %s", rec.Body.String())
	}
	parts := parseBatchResponse(t, rec.Result(), rec.Body.Bytes())
	if len(parts) != 1 || parts[0].status != http.StatusTooManyRequests {
		t.Fatalf("parts = %+v, want one 429", parts)
	}
}

func TestGCPAdapterServeBatchForwardsOuterAuthorization(t *testing.T) {
	const boundary = "b"
	body := buildBatchBody(t, boundary, "GET http://localhost:8080/storage/v1/b/bkt/o/x HTTP/1.1\r\n\r\n")

	var gotAuth string
	process := func(ctx context.Context, r *http.Request, subBody []byte) (int, http.Header, []byte) {
		gotAuth = r.Header.Get("Authorization")
		return http.StatusOK, nil, []byte("{}")
	}

	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	req.Header.Set("Authorization", "Bearer outer-token")
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, body, process)

	if gotAuth != "Bearer outer-token" {
		t.Fatalf("sub-request Authorization = %q, want the outer token", gotAuth)
	}
}

func TestGCPAdapterServeBatchGzippedBody(t *testing.T) {
	const boundary = "gz"
	body := buildBatchBody(t, boundary, "GET http://localhost:8080/storage/v1/b/bkt/o/x HTTP/1.1\r\n\r\n")
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	process := func(ctx context.Context, r *http.Request, subBody []byte) (int, http.Header, []byte) {
		return http.StatusOK, nil, []byte(`{"ok":true}`)
	}
	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", bytes.NewReader(gz.Bytes()))
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	req.Header.Set("Content-Encoding", "gzip")
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, gz.Bytes(), process)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	parts := parseBatchResponse(t, rec.Result(), rec.Body.Bytes())
	if len(parts) != 1 || parts[0].status != 200 || !strings.Contains(parts[0].body, `"ok":true`) {
		t.Fatalf("parts = %+v, want one 200 part", parts)
	}
}

func TestGCPAdapterServeBatchMissingBoundary(t *testing.T) {
	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", strings.NewReader("--x--"))
	req.Header.Set("Content-Type", "multipart/mixed")
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, []byte("--x--"), func(context.Context, *http.Request, []byte) (int, http.Header, []byte) {
		t.Fatal("process must not be called without a boundary")
		return 0, nil, nil
	})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestGCPAdapterServeBatchMalformedSubRequestYields400(t *testing.T) {
	const boundary = "bad"
	body := buildBatchBody(t, boundary, "not-a-valid-http-request\r\n\r\n")
	a := New()
	req := httptest.NewRequest(http.MethodPost, "/batch/storage/v1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)
	rec := httptest.NewRecorder()
	a.ServeBatch(context.Background(), rec, req, body, func(context.Context, *http.Request, []byte) (int, http.Header, []byte) {
		t.Fatal("process must not be called for a malformed sub-request")
		return 0, nil, nil
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the batch itself is valid)", rec.Code)
	}
	parts := parseBatchResponse(t, rec.Result(), rec.Body.Bytes())
	if len(parts) != 1 || parts[0].status != http.StatusBadRequest {
		t.Fatalf("parts = %+v, want one 400 part", parts)
	}
}

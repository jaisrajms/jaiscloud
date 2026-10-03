package gcp_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGCSBucketStorageLayout covers buckets.getStorageLayout (the resource
// `gcloud storage cp` reads before uploading).
func TestGCSBucketStorageLayout(t *testing.T) {
	resetState(t)
	createBucket(t, "layout-bucket")

	resp, body := do(t, "GET", "/storage/v1/b/layout-bucket/storageLayout?alt=json", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	m := jsonMap(t, body)
	require.Equal(t, "storage#storageLayout", m["kind"])
	require.Equal(t, "layout-bucket", m["bucket"])
	hn, ok := m["hierarchicalNamespace"].(map[string]any)
	require.True(t, ok, "hierarchicalNamespace missing: %s", body)
	require.Equal(t, false, hn["enabled"])

	resp, _ = do(t, "GET", "/storage/v1/b/no-such-layout-bucket/storageLayout?alt=json", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

type batchPart struct {
	contentID string
	status    int
	body      string
}

// appendBatchSub appends one multipart/mixed part containing a raw embedded
// HTTP request, exactly as the google-api-client emits (the request content
// includes its own terminating blank line; the multipart delimiter CRLF is
// added here before the next boundary).
func appendBatchSub(buf *bytes.Buffer, boundary string, id int, raw string) {
	fmt.Fprintf(buf, "--%s\r\n", boundary)
	buf.WriteString("Content-Type: application/http\r\n")
	fmt.Fprintf(buf, "Content-ID: %d\r\n", id)
	buf.WriteString("\r\n")
	buf.WriteString(raw)
	buf.WriteString("\r\n")
}

// unpackBatch splits a multipart/mixed batch response into its embedded
// responses, in order.
func unpackBatch(t *testing.T, resp *http.Response, raw []byte) []batchPart {
	t.Helper()
	mt, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	require.NoError(t, err)
	require.Equal(t, "multipart/mixed", mt)
	mr := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	var out []batchPart
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		b, err := io.ReadAll(p)
		require.NoError(t, err)
		head, payload, _ := strings.Cut(string(b), "\r\n\r\n")
		statusLine := strings.SplitN(head, "\r\n", 2)[0]
		fields := strings.SplitN(statusLine, " ", 3)
		require.GreaterOrEqual(t, len(fields), 2, "malformed status line %q", statusLine)
		code, err := strconv.Atoi(fields[1])
		require.NoError(t, err)
		out = append(out, batchPart{contentID: p.Header.Get("Content-ID"), status: code, body: payload})
	}
	return out
}

// TestGCSBatchEndpoint exercises the JSON batch endpoint the java
// StorageBatch client uses: GET metadata, GET a missing object (404), and
// DELETE, all multiplexed into one multipart/mixed request.
func TestGCSBatchEndpoint(t *testing.T) {
	resetState(t)
	createBucket(t, "batch-bucket")

	for _, obj := range []string{"a.txt", "b.txt"} {
		resp, _ := do(t, "POST", "/upload/storage/v1/b/batch-bucket/o?uploadType=media&name="+obj,
			[]byte("content of "+obj), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	const boundary = "jaiscloud_batch_boundary"
	var buf bytes.Buffer
	base := gcpBase()
	appendBatchSub(&buf, boundary, 1, "GET "+base+"/storage/v1/b/batch-bucket/o/a.txt?alt=json HTTP/1.1\r\n\r\n")
	appendBatchSub(&buf, boundary, 2, "GET "+base+"/storage/v1/b/batch-bucket/o/missing.txt?alt=json HTTP/1.1\r\n\r\n")
	appendBatchSub(&buf, boundary, 3, "DELETE "+base+"/storage/v1/b/batch-bucket/o/b.txt HTTP/1.1\r\n\r\n")
	fmt.Fprintf(&buf, "--%s--\r\n", boundary)

	resp, body := do(t, "POST", "/batch/storage/v1", buf.Bytes(),
		map[string]string{"Content-Type": "multipart/mixed; boundary=" + boundary})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	parts := unpackBatch(t, resp, body)
	require.Len(t, parts, 3)
	require.Equal(t, 200, parts[0].status)
	var obj map[string]any
	require.NoError(t, json.Unmarshal([]byte(parts[0].body), &obj))
	require.Equal(t, "a.txt", obj["name"])
	require.Equal(t, 404, parts[1].status)
	require.Equal(t, 204, parts[2].status)

	// The batch delete took effect.
	resp, _ = do(t, "GET", "/storage/v1/b/batch-bucket/o/b.txt?alt=json", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestGCSBatchPreservesEncodedObjectName verifies that a %2F-encoded object
// name in a batch sub-request URL round-trips (the batch envelope must not
// decode the name before the codec sees it).
func TestGCSBatchPreservesEncodedObjectName(t *testing.T) {
	resetState(t)
	createBucket(t, "batch-enc-bucket")

	// The literal object name contains a slash; the JSON API path encodes it.
	const object = "dir/literal%2Fname.txt"
	uploadPath := "/upload/storage/v1/b/batch-enc-bucket/o?uploadType=media&name=dir%2Fliteral%252Fname.txt"
	resp, _ := do(t, "POST", uploadPath, []byte("encoded"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	const boundary = "bnd"
	var buf bytes.Buffer
	appendBatchSub(&buf, boundary, 1,
		fmt.Sprintf("GET %s/storage/v1/b/batch-enc-bucket/o/dir%%2Fliteral%%252Fname.txt?alt=json HTTP/1.1\r\n\r\n", gcpBase()))
	fmt.Fprintf(&buf, "--%s--\r\n", boundary)

	resp, body := do(t, "POST", "/batch/storage/v1", buf.Bytes(),
		map[string]string{"Content-Type": "multipart/mixed; boundary=" + boundary})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	parts := unpackBatch(t, resp, body)
	require.Len(t, parts, 1)
	require.Equal(t, 200, parts[0].status)
	var obj map[string]any
	require.NoError(t, json.Unmarshal([]byte(parts[0].body), &obj))
	require.Equal(t, object, obj["name"])
}

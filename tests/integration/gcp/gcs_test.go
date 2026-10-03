// Package gcp_test contains integration tests for the jaiscloud-gcp emulator.
//
// Run with the GCP binary running on the default port:
//
//	./jaiscloud-gcp start &
//	go test -race ./tests/integration/gcp/
//
// Override the endpoint with JAISCLOUD_GCP_ENDPOINT (default http://localhost:8080).
package gcp_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func gcpBase() string {
	if ep := os.Getenv("JAISCLOUD_GCP_ENDPOINT"); ep != "" {
		return ep
	}
	return "http://localhost:8080"
}

func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(gcpBase()+"/_jaiscloud/reset", "", nil)
	require.NoError(t, err)
	resp.Body.Close()
}

// do performs an HTTP request and returns the response plus its body bytes.
func do(t *testing.T, method, path string, body []byte, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, gcpBase()+path, rd)
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, b
}

func createBucket(t *testing.T, name string) {
	t.Helper()
	resp, _ := do(t, "POST", "/storage/v1/b?project=proj", []byte(`{"name":"`+name+`"}`), map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func jsonMap(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

// TestGCSAcceptanceFlow covers the Phase 0 acceptance gate: create bucket →
// upload → list → download → delete, end-to-end over the wire.
func TestGCSAcceptanceFlow(t *testing.T) {
	resetState(t)

	createBucket(t, "accept-bucket")

	// Upload (media).
	resp, body := do(t, "POST", "/upload/storage/v1/b/accept-bucket/o?uploadType=media&name=hello.txt",
		[]byte("hello jaiscloud"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	obj := jsonMap(t, body)
	require.Equal(t, "hello.txt", obj["name"])
	require.Equal(t, "15", obj["size"])

	// List.
	resp, body = do(t, "GET", "/storage/v1/b/accept-bucket/o", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	list := jsonMap(t, body)
	items, _ := list["items"].([]any)
	require.Len(t, items, 1)

	// Download (alt=media).
	resp, body = do(t, "GET", "/storage/v1/b/accept-bucket/o/hello.txt?alt=media", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hello jaiscloud", string(body))

	// Delete.
	resp, _ = do(t, "DELETE", "/storage/v1/b/accept-bucket/o/hello.txt", nil, nil)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Gone after delete.
	resp, _ = do(t, "GET", "/storage/v1/b/accept-bucket/o/hello.txt", nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestGCSResumableUploadMultiChunk(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-bucket")

	// Start a resumable session.
	resp, _ := do(t, "POST", "/upload/storage/v1/b/rs-bucket/o?uploadType=resumable&name=big.bin", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	loc := resp.Header.Get("Location")
	require.NotEmpty(t, loc)
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}
	require.NotEmpty(t, uploadID)

	// Chunk 1 (unknown total) → 308 Resume Incomplete.
	resp, _ = do(t, "PUT", "/upload/storage/v1/b/rs-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte("hello"), map[string]string{"Content-Range": "bytes 0-4/*"})
	require.Equal(t, http.StatusPermanentRedirect, resp.StatusCode)
	require.Equal(t, "bytes=0-4", resp.Header.Get("Range"))

	// Final chunk (known total) → 200 with object resource.
	resp, body := do(t, "PUT", "/upload/storage/v1/b/rs-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte(" world"), map[string]string{"Content-Range": "bytes 5-10/11"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "11", jsonMap(t, body)["size"])

	// Verify assembled object.
	_, body = do(t, "GET", "/storage/v1/b/rs-bucket/o/big.bin?alt=media", nil, nil)
	require.Equal(t, "hello world", string(body))
}

func TestGCSPagination(t *testing.T) {
	resetState(t)
	createBucket(t, "pg-bucket")

	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		resp, _ := do(t, "POST", "/upload/storage/v1/b/pg-bucket/o?uploadType=media&name="+name,
			[]byte(name), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Page 1: maxResults=2 → 2 items + nextPageToken.
	resp, body := do(t, "GET", "/storage/v1/b/pg-bucket/o?maxResults=2", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	page := jsonMap(t, body)
	items, _ := page["items"].([]any)
	require.Len(t, items, 2)
	token, _ := page["nextPageToken"].(string)
	require.NotEmpty(t, token)

	// Page 2: pageToken → 1 item, no further token.
	_, body = do(t, "GET", "/storage/v1/b/pg-bucket/o?pageToken="+token, nil, nil)
	page = jsonMap(t, body)
	items, _ = page["items"].([]any)
	require.Len(t, items, 1)
	_, hasNext := page["nextPageToken"]
	require.False(t, hasNext)
}

func TestGCSBucketIAMAndACL(t *testing.T) {
	resetState(t)
	createBucket(t, "iam-bucket")

	// getIamPolicy returns a default policy with bindings.
	resp, body := do(t, "GET", "/storage/v1/b/iam-bucket/iam", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	pol := jsonMap(t, body)
	bindings, _ := pol["bindings"].([]any)
	require.NotEmpty(t, bindings)

	// setIamPolicy stores a new policy.
	_, _ = do(t, "PUT", "/storage/v1/b/iam-bucket/iam",
		[]byte(`{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`),
		map[string]string{"Content-Type": "application/json"})

	resp, body = do(t, "GET", "/storage/v1/b/iam-bucket/iam", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	pol = jsonMap(t, body)
	bindings, _ = pol["bindings"].([]any)
	require.Len(t, bindings, 1)

	// Bucket ACL list + insert.
	resp, body = do(t, "GET", "/storage/v1/b/iam-bucket/acl", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	acl := jsonMap(t, body)
	require.NotEmpty(t, acl["items"])

	resp, _ = do(t, "POST", "/storage/v1/b/iam-bucket/acl",
		[]byte(`{"entity":"allUsers","role":"READER"}`),
		map[string]string{"Content-Type": "application/json"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

package gcp_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startResumable initiates a resumable session over the given base path and
// returns the upload_id.
func startResumable(t *testing.T, path string) string {
	t.Helper()
	resp, _ := do(t, "POST", path, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "resumable start %s", path)
	loc := resp.Header.Get("Location")
	require.NotEmpty(t, loc, "expected Location header")
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}
	require.NotEmpty(t, uploadID, "no upload_id in Location %q", loc)
	return uploadID
}

func TestGCSResumableUnknownSizeFinalChunk(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-unknown-bucket")

	uploadID := startResumable(t, "/upload/storage/v1/b/rs-unknown-bucket/o?uploadType=resumable&name=x.bin")

	body := []byte("seventeen bytes!!")
	resp, bodyB := do(t, "PUT", "/upload/storage/v1/b/rs-unknown-bucket/o?uploadType=resumable&upload_id="+uploadID,
		body, map[string]string{"Content-Range": "bytes 0-*/*"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", bodyB)
	require.Equal(t, fmt.Sprintf("%d", len(body)), jsonMap(t, bodyB)["size"])
}

func TestGCSResumableOffsetGapPlain503(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-gap-bucket")

	uploadID := startResumable(t, "/upload/storage/v1/b/rs-gap-bucket/o?uploadType=resumable&name=x.bin")

	resp, body := do(t, "PUT", "/upload/storage/v1/b/rs-gap-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte("ok"), map[string]string{"Content-Range": "bytes 4-5/6"})
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
	require.Equal(t, "138", resp.Header.Get("Content-Length"))
	require.Contains(t, strings.ToLower(string(body)), "content-range")
	require.NotContains(t, strings.ToLower(string(body)), "earlier")
}

func TestGCSResumablePostContinuationAndReplay(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-post-bucket")

	uploadID := startResumable(t, "/upload/storage/v1/b/rs-post-bucket/o?uploadType=resumable&name=big.bin")

	// POST chunks (the Go SDK uses POST for resumable chunks).
	resp, _ := do(t, "POST", "/upload/storage/v1/b/rs-post-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte("hello"), map[string]string{"Content-Range": "bytes 0-4/*"})
	require.Equal(t, http.StatusPermanentRedirect, resp.StatusCode)
	require.Equal(t, "bytes=0-4", resp.Header.Get("Range"))

	resp, body := do(t, "POST", "/upload/storage/v1/b/rs-post-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte(" world"), map[string]string{"Content-Range": "bytes 5-10/11"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "11", jsonMap(t, body)["size"])

	// Post-completion status query -> 200 + object.
	resp, body = do(t, "POST", "/upload/storage/v1/b/rs-post-bucket/o?uploadType=resumable&upload_id="+uploadID,
		nil, map[string]string{"Content-Range": "bytes */11"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "post-completion status body: %s", body)
	require.Equal(t, "11", jsonMap(t, body)["size"])
}

func TestGCSResumableRewrittenJSONPath(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-json-bucket")

	// Initiate and chunk entirely on the rewritten /storage/v1/ path.
	uploadID := startResumable(t, "/storage/v1/b/rs-json-bucket/o?uploadType=resumable&name=y.bin")

	resp, _ := do(t, "PUT", "/storage/v1/b/rs-json-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte("data"), map[string]string{"Content-Range": "bytes 0-3/4"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, body := do(t, "GET", "/storage/v1/b/rs-json-bucket/o/y.bin?alt=media", nil, nil)
	require.Equal(t, "data", string(body))
}

func TestGCSResumableDiscoveryPath(t *testing.T) {
	resetState(t)
	createBucket(t, "rs-disc-bucket")

	// The Discovery-documented resumable initiation path used by gcloud.
	uploadID := startResumable(t, "/resumable/upload/storage/v1/b/rs-disc-bucket/o?uploadType=resumable&name=z.bin")

	// Chunk against the session URI path (/upload/...); completes.
	resp, _ := do(t, "PUT", "/upload/storage/v1/b/rs-disc-bucket/o?uploadType=resumable&upload_id="+uploadID,
		[]byte("done"), map[string]string{"Content-Range": "bytes 0-3/4"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	_, body := do(t, "GET", "/storage/v1/b/rs-disc-bucket/o/z.bin?alt=media", nil, nil)
	require.Equal(t, "done", string(body))
}

func TestGCSMultipartSingleQuotedBoundary(t *testing.T) {
	resetState(t)
	createBucket(t, "mp-quote-bucket")

	body := "--BND==\r\nContent-Type: application/json\r\n\r\n{\"name\":\"o.txt\",\"contentType\":\"text/plain\"}\r\n--BND==\r\nContent-Type: text/plain\r\n\r\nhello quoted\r\n--BND==--\r\n"
	resp, bodyB := do(t, "POST", "/upload/storage/v1/b/mp-quote-bucket/o?uploadType=multipart",
		[]byte(body), map[string]string{"Content-Type": "multipart/related; boundary='BND=='"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", bodyB)
	require.Equal(t, "o.txt", jsonMap(t, bodyB)["name"])

	_, bodyB = do(t, "GET", "/storage/v1/b/mp-quote-bucket/o/o.txt?alt=media", nil, nil)
	require.Equal(t, "hello quoted", string(bodyB))
}

// signedGetPath builds a path-style signed GET URL.
func signedPath(bucket, object, expires, date, signature string) string {
	return fmt.Sprintf("/%s/%s?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=test@test.iam.gserviceaccount.com/20260913/auto/storage/goog4_request&X-Goog-Date=%s&X-Goog-Expires=%s&X-Goog-SignedHeaders=host&X-Goog-Signature=%s",
		bucket, object, date, expires, signature)
}

func TestGCSSignedURLExpiredReturnsExpiredToken(t *testing.T) {
	resetState(t)
	createBucket(t, "sgn-exp-bucket")

	resp, body := do(t, "GET", signedPath("sgn-exp-bucket", "o.txt", "60", "20200101T000000Z", "sig"), nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "<Code>ExpiredToken</Code>")
	require.NotContains(t, string(body), "<Details>")
}

func TestGCSSignedURLMalformedDate(t *testing.T) {
	resetState(t)
	createBucket(t, "sgn-date-bucket")

	resp, body := do(t, "GET", signedPath("sgn-date-bucket", "o.txt", "3600", "not-a-date", "sig"), nil, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Contains(t, string(body), "<Code>MalformedSecurityHeader</Code>")
	require.Contains(t, string(body), "<ParameterName>Date</ParameterName>")
}

func TestGCSSignedURLPutStoresObject(t *testing.T) {
	resetState(t)
	createBucket(t, "sgn-put-bucket")

	date := time.Now().UTC().Format("20060102T150405Z")
	path := signedPath("sgn-put-bucket", "o.txt", "3600", date, "sig")
	resp, body := do(t, "PUT", path, []byte("signed bytes"), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	_, body = do(t, "GET", "/storage/v1/b/sgn-put-bucket/o/o.txt?alt=media", nil, nil)
	require.Equal(t, "signed bytes", string(body))
}

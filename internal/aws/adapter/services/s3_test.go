package services

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
		"jaiscloud/internal/model"
)

// ─── extractVirtualHostedBucket ──────────────────────────────────────────────

func TestExtractVirtualHostedBucket_NoBasesConfigured(t *testing.T) {
	c := &S3Codec{}
	if got := c.extractVirtualHostedBucket("mybucket.example.com"); got != "" {
		t.Errorf("want empty string, got %q", got)
	}
}

func TestExtractVirtualHostedBucket_MatchesSingleBase(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.svc.cluster.local"}}
	got := c.extractVirtualHostedBucket("mybucket.jaiscloud.devbox.svc.cluster.local")
	assert.Equal(t, "mybucket", got)
}

func TestExtractVirtualHostedBucket_StripsPortBeforeMatching(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.jaiscloud.devbox.local:4566")
	assert.Equal(t, "mybucket", got, "port suffix must be stripped before base comparison")
}

func TestExtractVirtualHostedBucket_HighPortNumber(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("data-bucket.devbox.local:9000")
	assert.Equal(t, "data-bucket", got)
}

func TestExtractVirtualHostedBucket_NoMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.other.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_MultipleBasesFirstMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"first.local", "second.local"}}
	assert.Equal(t, "bkt", c.extractVirtualHostedBucket("bkt.first.local"))
}

func TestExtractVirtualHostedBucket_MultipleBasesSecondMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"first.local", "second.local"}}
	assert.Equal(t, "bkt", c.extractVirtualHostedBucket("bkt.second.local"))
}

func TestExtractVirtualHostedBucket_HostEqualsBase_NoBucketPrefix(t *testing.T) {
	// A request to the bare base hostname has no bucket prefix — must return ""
	// so Decode falls through to path-style (ListBuckets, etc.).
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.local"}}
	got := c.extractVirtualHostedBucket("jaiscloud.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_BaseAsPartialSubstring(t *testing.T) {
	// base=devbox.local must not match host=mybucket.notdevbox.local
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("mybucket.notdevbox.local")
	assert.Equal(t, "", got)
}

func TestExtractVirtualHostedBucket_HyphenatedBucketName(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	got := c.extractVirtualHostedBucket("my-data-bucket.devbox.local")
	assert.Equal(t, "my-data-bucket", got)
}

// ─── Decode: virtual-hosted routing ──────────────────────────────────────────

// newReq builds a minimal *http.Request suitable for S3Codec.Decode tests.
// host overrides r.Host so tests control exactly what the codec sees.
func newS3Req(method, rawURL, host string) *http.Request {
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		panic(err)
	}
	req.Host = host
	return req
}

func TestS3Decode_CustomVirtualHosted_BucketAndKey(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodGet, "http://mybucket.jaiscloud.devbox.local/prefix/key.txt", "mybucket.jaiscloud.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "prefix/key.txt", nr.Params["_key"])
	assert.Equal(t, "GetObject", nr.Action)
}

func TestS3Decode_CustomVirtualHosted_WithPort(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodPut, "http://mybucket.jaiscloud.devbox.local:4566/mykey",
		"mybucket.jaiscloud.devbox.local:4566")

	nr, err := c.Decode(req, []byte("data"))
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "mykey", nr.Params["_key"])
}

func TestS3Decode_CustomVirtualHosted_ListObjectsV2(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodGet, "http://mybucket.devbox.local/?list-type=2&prefix=logs/",
		"mybucket.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "", nr.Params["_key"])
	assert.Equal(t, "ListObjectsV2", nr.Action)
	assert.Equal(t, "logs/", nr.Params["prefix"])
}

func TestS3Decode_CustomVirtualHosted_HeadObject(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodHead, "http://bkt.devbox.local/some/object", "bkt.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "bkt", nr.Params["_bucket"])
	assert.Equal(t, "some/object", nr.Params["_key"])
	assert.Equal(t, "HeadObject", nr.Action)
}

func TestS3Decode_CustomVirtualHosted_CreateBucket(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"devbox.local"}}
	req := newS3Req(http.MethodPut, "http://newbucket.devbox.local/", "newbucket.devbox.local")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "newbucket", nr.Params["_bucket"])
	assert.Equal(t, "CreateBucket", nr.Action)
}

func TestS3Decode_AWSVirtualHostedTakesPriorityOverCustomBase(t *testing.T) {
	// The existing ".s3." check runs before the custom-base branch; a custom base
	// that overlaps with real S3 hostnames must not interfere.
	c := &S3Codec{VirtualHostBases: []string{"s3.us-east-1.amazonaws.com"}}
	req := newS3Req(http.MethodGet, "http://mybucket.s3.us-east-1.amazonaws.com/k",
		"mybucket.s3.us-east-1.amazonaws.com")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "k", nr.Params["_key"])
}

func TestS3Decode_PathStyleFallsThrough_WhenNoBaseMatch(t *testing.T) {
	c := &S3Codec{VirtualHostBases: []string{"jaiscloud.devbox.local"}}
	req := newS3Req(http.MethodGet, "http://localhost:4566/mybucket/mykey", "localhost:4566")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "mybucket", nr.Params["_bucket"])
	assert.Equal(t, "mykey", nr.Params["_key"])
}

func TestS3Decode_PathStyle_NoBasesConfigured(t *testing.T) {
	c := &S3Codec{}
	req := newS3Req(http.MethodGet, "http://localhost:4566/bucket/key", "localhost:4566")

	nr, err := c.Decode(req, []byte{})
	require.NoError(t, err)
	assert.Equal(t, "bucket", nr.Params["_bucket"])
	assert.Equal(t, "key", nr.Params["_key"])
}

// ─── decodeAWSChunked ─────────────────────────────────────────────────────────

func TestDecodeAWSChunked(t *testing.T) {
	t.Run("single chunk", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("chunk with signature extension", func(t *testing.T) {
		body := []byte("5;chunk-signature=abc123\r\nhello\r\n0;chunk-signature=def456\r\n\r\n")
		assert.Equal(t, []byte("hello"), decodeAWSChunked(body))
	})

	t.Run("multiple chunks", func(t *testing.T) {
		body := []byte("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
		assert.Equal(t, []byte("hello world"), decodeAWSChunked(body))
	})

	t.Run("plain body passthrough", func(t *testing.T) {
		body := []byte("just plain text, not chunked")
		assert.Equal(t, body, decodeAWSChunked(body))
	})

	t.Run("empty body", func(t *testing.T) {
		assert.Equal(t, []byte(nil), decodeAWSChunked([]byte{}))
	})

	t.Run("hex chunk size 0x100 = 256 bytes", func(t *testing.T) {
		data := make([]byte, 256)
		for i := range data {
			data[i] = byte('a' + i%26)
		}
		// Build: "100\r\n<256 bytes>\r\n0\r\n\r\n"
		chunk := append([]byte("100\r\n"), data...)
		chunk = append(chunk, []byte("\r\n0\r\n\r\n")...)
		assert.Equal(t, data, decodeAWSChunked(chunk))
	})
}

// ─── Non-streaming actions with streaming-flagged request ─────────────────────
// The AWS SDK sends Content-Encoding: aws-chunked even for small XML-body
// operations like PutObjectLegalHold. The gateway marks those as "streaming"
// and passes body=nil to the codec. The codec must re-read r.Body for any
// action that is NOT PutObject/UploadPart.

func TestDecode_PutObjectLegalHold_StreamingFlaggedRequest(t *testing.T) {
	xmlBody := `<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>`
	rawURL := "http://localhost:4566/my-bucket/obj.txt?legal-hold"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader(xmlBody))
	req.Header.Set("Content-Encoding", "aws-chunked") // triggers streaming gate in gateway
	req.Header.Set("Host", "localhost:4566")

	c := &S3Codec{}
	// Gateway skips io.ReadAll when Content-Encoding: aws-chunked → passes nil body
	nr, err := c.Decode(req, nil)
	require.NoError(t, err, "PutObjectLegalHold with nil body must not return MalformedXML")
	assert.Equal(t, "PutObjectLegalHold", nr.Action)
	// Codec must have re-read r.Body and stored it in _body
	body, ok := nr.Params["_body"].([]byte)
	require.True(t, ok, "_body must be a []byte")
	assert.Equal(t, xmlBody, string(body))
}

func TestDecode_PutObjectRetention_StreamingFlaggedRequest(t *testing.T) {
	xmlBody := `<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>`
	rawURL := "http://localhost:4566/my-bucket/obj.txt?retention"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader(xmlBody))
	req.Header.Set("Content-Encoding", "aws-chunked")

	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err)
	assert.Equal(t, "PutObjectRetention", nr.Action)
	body, ok := nr.Params["_body"].([]byte)
	require.True(t, ok)
	assert.Equal(t, xmlBody, string(body))
}

func TestDecode_PutObject_StreamingFlaggedRequest_BodyNotRead(t *testing.T) {
	// For PutObject, body=nil must NOT be re-read from r.Body — that is the
	// streaming provider's job.
	rawURL := "http://localhost:4566/my-bucket/obj.txt"
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader("blob data"))
	req.Header.Set("Content-Encoding", "aws-chunked")

	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err)
	assert.Equal(t, "PutObject", nr.Action)
	// _streaming must be true and _body must be nil — provider streams from nr.Raw.Body
	_, streaming := nr.Params["_streaming"]
	assert.True(t, streaming, "_streaming flag must be set for PutObject with nil body")
	body := nr.Params["_body"]
	assert.Nil(t, body, "_body must remain nil for PutObject so the provider can stream it")
}

// ─── P2-8: Presigned URL expiration ──────────────────────────────────────────

func TestPresigned_ExpiredSigV4_403(t *testing.T) {
	// X-Amz-Date in the past, expires in 1 second → already expired.
	past := clock.RealNow().Add(-10 * time.Minute)
	dateStr := past.UTC().Format("20060102T150405Z")
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=1&X-Amz-Signature=fake",
		dateStr,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	_, err := c.Decode(req, nil)
	require.Error(t, err, "expired presigned URL must return an error")
}

func TestPresigned_ValidSigV4_Succeeds(t *testing.T) {
	// X-Amz-Date now, expires in 3600 seconds → still valid.
	now := clock.RealNow()
	dateStr := now.Format("20060102T150405Z")
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=%s&X-Amz-Expires=3600&X-Amz-Signature=fake",
		dateStr,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	nr, err := c.Decode(req, nil)
	require.NoError(t, err, "valid presigned URL must not return an error")
	assert.Equal(t, "GetObject", nr.Action)
}

func TestPresigned_ExpiredSigV2_403(t *testing.T) {
	// Expires is a Unix timestamp in the past.
	expiredUnix := clock.RealNow().Add(-1 * time.Minute).Unix()
	rawURL := fmt.Sprintf(
		"http://localhost:4566/bucket/key?AWSAccessKeyId=AKID&Signature=fake&Expires=%d",
		expiredUnix,
	)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	c := &S3Codec{}
	_, err := c.Decode(req, nil)
	require.Error(t, err, "expired SigV2 presigned URL must return an error")
}

// ─── Encode: x-amz-checksum-crc32 conditional on x-amz-checksum-mode ────────

// newGetObjectNR builds a passthrough NormalizedRequest for Encode tests.
func newGetObjectNR(checksumMode string) *model.NormalizedRequest {
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:4566/bucket/key", nil)
	if checksumMode != "" {
		req.Header.Set("x-amz-checksum-mode", checksumMode)
	}
	return &model.NormalizedRequest{Raw: req, Action: "GetObject"}
}

// newPassthroughResp builds a _passthrough ProviderResponse with a stored CRC32.
func newPassthroughResp(storedCRC32 string, body []byte) *model.ProviderResponse {
	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_passthrough": true,
			"_crc32":       storedCRC32,
			"_raw_body":    body,
		},
	}
}

func TestS3Encode_CRC32_EmitsStoredValue_WhenChecksumModeEnabled(t *testing.T) {
	c := &S3Codec{}
	nr := newGetObjectNR("ENABLED")
	resp := newPassthroughResp("abc123storedCRC==", []byte("hello"))

	_, h, _ := c.Encode(nr, resp)

	assert.Equal(t, "abc123storedCRC==", h.Get("x-amz-checksum-crc32"),
		"stored _crc32 must be emitted verbatim when x-amz-checksum-mode is ENABLED")
}

func TestS3Encode_CRC32_UsesComputedValue_WhenChecksumModeAbsent(t *testing.T) {
	c := &S3Codec{}
	body := []byte("hello")
	nr := newGetObjectNR("") // no checksum-mode header
	resp := newPassthroughResp("should-not-appear", body)

	_, h, _ := c.Encode(nr, resp)

	got := h.Get("x-amz-checksum-crc32")
	assert.NotEqual(t, "should-not-appear", got,
		"stored _crc32 must not be emitted when x-amz-checksum-mode is absent")
	assert.Equal(t, s3ChecksumCRC32(body), got,
		"CRC32 computed from body must be used when checksum mode is not requested")
}

func TestS3Encode_CRC32_CaseInsensitiveChecksumMode(t *testing.T) {
	c := &S3Codec{}
	nr := newGetObjectNR("enabled") // lowercase
	resp := newPassthroughResp("storedValue==", []byte("data"))

	_, h, _ := c.Encode(nr, resp)

	assert.Equal(t, "storedValue==", h.Get("x-amz-checksum-crc32"),
		"x-amz-checksum-mode matching must be case-insensitive")
}

func TestS3Encode_CRC32_NotEmittedFromStore_OnRangeRead(t *testing.T) {
	// Range reads return a partial body whose CRC32 differs from the stored
	// full-object CRC32. The stored value must be suppressed even when the
	// client sends x-amz-checksum-mode: ENABLED.
	c := &S3Codec{}
	body := []byte("partial")
	nr := newGetObjectNR("ENABLED")
	resp := &model.ProviderResponse{
		HTTPStatus: 206,
		Data: map[string]any{
			"_passthrough": true,
			"_crc32":       "storedFullObjectCRC==",
			"_raw_body":    body,
			"_range_start": int64(0),
			"_range_end":   int64(6),
			"_range_total": int64(100),
		},
	}

	_, h, _ := c.Encode(nr, resp)

	assert.NotEqual(t, "storedFullObjectCRC==", h.Get("x-amz-checksum-crc32"),
		"stored full-object CRC32 must not be emitted for a range read")
	assert.Equal(t, s3ChecksumCRC32(body), h.Get("x-amz-checksum-crc32"),
		"CRC32 must be computed from the partial body for range reads")
}

func TestS3Decode_NotificationEventBridgeMarker(t *testing.T) {
	c := &S3Codec{}
	body := []byte(`<NotificationConfiguration><EventBridgeConfiguration/></NotificationConfiguration>`)
	req := newS3Req(http.MethodPut, "http://localhost:4566/b?notification", "localhost:4566")

	nr, err := c.Decode(req, body)
	require.NoError(t, err)
	require.Equal(t, "PutBucketNotificationConfiguration", nr.Action)
	cfg, _ := nr.Params["_notification_config"].(map[string]any)
	_, has := cfg["EventBridgeConfiguration"]
	assert.True(t, has, "an empty <EventBridgeConfiguration/> element must enable EventBridge delivery")

	nr, err = c.Decode(req, []byte(`<NotificationConfiguration></NotificationConfiguration>`))
	require.NoError(t, err)
	cfg, _ = nr.Params["_notification_config"].(map[string]any)
	_, has = cfg["EventBridgeConfiguration"]
	assert.False(t, has, "absent element must not enable it")
}

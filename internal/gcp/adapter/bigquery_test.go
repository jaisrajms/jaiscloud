package gcp

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

func TestBigQueryCodecDecode_DeferredResources(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		// routines: collection + item, read + write.
		{"GET", "/bigquery/v2/projects/p/datasets/d/routines", "Routines"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/routines", "Routines"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/routines/r", "Routines"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/routines/r", "Routines"},
		// models: collection + item, read + write.
		{"GET", "/bigquery/v2/projects/p/datasets/d/models", "Models"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/models", "Models"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/models/m", "Models"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/models/m", "Models"},
		// rowAccessPolicies: table-scoped collection/item + custom method.
		{"GET", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies:batchDelete", "RowAccessPolicies"},
		{"GET", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies/rp", "RowAccessPolicies"},
		{"DELETE", "/bigquery/v2/projects/p/datasets/d/tables/tbl/rowAccessPolicies/rp", "RowAccessPolicies"},
		// WithEndpoint-stripped form (no bigquery/v2 servicePath).
		{"GET", "/projects/p/datasets/d/routines", "Routines"},
		{"GET", "/projects/p/datasets/d/models", "Models"},
		{"GET", "/projects/p/datasets/d/tables/tbl/rowAccessPolicies", "RowAccessPolicies"},
	}
	for _, tc := range cases {
		codec := &BigQueryCodec{Service: "bigquery"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
	}
}

func TestDetectBigQueryDeferredResources(t *testing.T) {
	cases := map[string]string{
		"/bigquery/v2/projects/p/datasets/d/routines":                   "bigquery",
		"/bigquery/v2/projects/p/datasets/d/tables/t/rowAccessPolicies": "bigquery",
		"/projects/p/datasets/d/models":                                 "bigquery",
	}
	for path, want := range cases {
		r := httptest.NewRequest("GET", path, nil)
		if got, _ := DetectService(r); got != want {
			t.Errorf("DetectService(%s) = %q, want %q", path, got, want)
		}
	}
}

// TestBigQueryCodecEncodeErrorReason verifies the BigQuery error envelope
// carries the legacy errors[] array: the client SDKs build their typed error
// from error.errors[0].reason, so a 409 duplicate dataset must surface reason
// "duplicate" (not the HTTP-derived "alreadyExists").
func TestBigQueryCodecEncodeErrorReason(t *testing.T) {
	cases := []struct {
		name, code, message, wantReason, wantStatus string
		http                                        int
	}{
		{"duplicate dataset", "AlreadyExists", "resource already exists", "duplicate", "ALREADY_EXISTS", 409},
		{"conflict alias", "Conflict", "conflict", "duplicate", "ALREADY_EXISTS", 409},
		{"missing resource", "NotFound", "dataset not found", "notFound", "NOT_FOUND", 404},
		{"invalid argument", "InvalidArgument", "bad request", "invalid", "INVALID_ARGUMENT", 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			codec := &BigQueryCodec{Service: "bigquery"}
			status, _, body := codec.EncodeError(nil, model.NewProviderError(tc.code, tc.message, tc.http))
			if status != tc.http {
				t.Fatalf("status = %d, want %d", status, tc.http)
			}
			var env struct {
				Error struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
					Status  string `json:"status"`
					Errors  []struct {
						Domain  string `json:"domain"`
						Reason  string `json:"reason"`
						Message string `json:"message"`
					} `json:"errors"`
				} `json:"error"`
			}
			if err := json.Unmarshal(body, &env); err != nil {
				t.Fatalf("unmarshal %s: %v", body, err)
			}
			if env.Error.Code != tc.http || env.Error.Status != tc.wantStatus {
				t.Errorf("envelope code/status = %d/%q, want %d/%q", env.Error.Code, env.Error.Status, tc.http, tc.wantStatus)
			}
			if len(env.Error.Errors) != 1 {
				t.Fatalf("expected 1 errors[] entry, got %d (%s)", len(env.Error.Errors), body)
			}
			e := env.Error.Errors[0]
			if e.Domain != "global" || e.Reason != tc.wantReason || e.Message != tc.message {
				t.Errorf("errors[0] = %+v, want domain=global reason=%q message=%q", e, tc.wantReason, tc.message)
			}
		})
	}
}

// TestDetectBigQueryUploadPaths verifies the jobs.insert media-upload endpoints
// are claimed by the bigquery service (both the simple and resumable forms)
// rather than falling through to the GCS raw-media fallback.
func TestDetectBigQueryUploadPaths(t *testing.T) {
	paths := []string{
		"/upload/bigquery/v2/projects/p/jobs?uploadType=multipart",
		"/upload/bigquery/v2/projects/p/jobs?uploadType=resumable",
		"/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=x",
		"/resumable/upload/bigquery/v2/projects/p/jobs?uploadType=resumable",
	}
	for _, p := range paths {
		r := httptest.NewRequest("POST", p, nil)
		if got, _ := DetectService(r); got != "bigquery" {
			t.Errorf("DetectService(%s) = %q, want bigquery", p, got)
		}
	}
	// A resumable chunk arrives as a PUT; it must still be claimed.
	r := httptest.NewRequest("PUT", "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=x", nil)
	if got, _ := DetectService(r); got != "bigquery" {
		t.Errorf("DetectService(chunk PUT) = %q, want bigquery", got)
	}
}

// TestBigQueryCodecDecodeUpload covers the media-upload decode paths.
func TestBigQueryCodecDecodeUpload(t *testing.T) {
	codec := &BigQueryCodec{Service: "bigquery"}

	// Multipart: JSON job resource part followed by the file bytes.
	const boundary = "b9142a1c"
	jobJSON := `{"jobReference":{"projectId":"p","jobId":"j1"},"configuration":{"load":{"destinationTable":{"datasetId":"d","tableId":"t"},"sourceFormat":"CSV"}}}`
	body := "--" + boundary + "\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n" + jobJSON +
		"\r\n--" + boundary + "\r\nContent-Type: */*\r\n\r\n1,alice\r\n--" + boundary + "--\r\n"
	r := httptest.NewRequest("POST", "http://bq.local/upload/bigquery/v2/projects/p/jobs?uploadType=multipart", strings.NewReader(body))
	r.Header.Set("Content-Type", "multipart/related; boundary="+boundary)
	nr, err := codec.Decode(r, []byte(body))
	if err != nil {
		t.Fatalf("multipart decode: %v", err)
	}
	if nr.Action != "InsertJob" {
		t.Fatalf("multipart action = %q, want InsertJob", nr.Action)
	}
	if got := nr.Params[wire.BaseURLKey]; got != "http://bq.local" {
		t.Errorf("multipart baseURL = %v, want http://bq.local", got)
	}
	parsed, _ := nr.Params["body"].(map[string]any)
	if parsed == nil {
		t.Fatal("multipart body (job resource) not decoded")
	}
	// The media part must be available to the provider (streamed or buffered).
	if nr.Params[wire.StreamKey] == nil && nr.Params[wire.MediaKey] == nil {
		t.Fatal("multipart media part not surfaced")
	}

	// Resumable start: a JSON job resource, no upload_id.
	r = httptest.NewRequest("POST", "http://bq.local/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", strings.NewReader(jobJSON))
	r.Header.Set("X-Upload-Content-Type", "*/*")
	nr, err = codec.Decode(r, []byte(jobJSON))
	if err != nil {
		t.Fatalf("resumable start decode: %v", err)
	}
	if nr.Action != "InsertJobResumableStart" {
		t.Fatalf("resumable start action = %q, want InsertJobResumableStart", nr.Action)
	}
	if nr.Params["body"] == nil {
		t.Fatal("resumable start did not decode the job resource")
	}

	// Resumable chunk: upload_id + media + Content-Range.
	chunk := []byte("1,alice\n")
	r = httptest.NewRequest("PUT", "http://bq.local/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=abc", bytes.NewReader(chunk))
	r.Header.Set("Content-Range", "bytes 0-7/8")
	nr, err = codec.Decode(r, chunk)
	if err != nil {
		t.Fatalf("resumable chunk decode: %v", err)
	}
	if nr.Action != "InsertJobResumable" {
		t.Fatalf("resumable chunk action = %q, want InsertJobResumable", nr.Action)
	}
	if got, _ := nr.Params[wire.MediaKey].([]byte); string(got) != string(chunk) {
		t.Errorf("resumable chunk media = %q, want %q", got, chunk)
	}
	if got, _ := nr.Params["contentRange"].(string); got != "bytes 0-7/8" {
		t.Errorf("resumable chunk contentRange = %q", got)
	}

	// uploadType=media cannot carry a load configuration — fail loud.
	r = httptest.NewRequest("POST", "http://bq.local/upload/bigquery/v2/projects/p/jobs?uploadType=media", strings.NewReader("x"))
	if _, err := codec.Decode(r, []byte("x")); err == nil {
		t.Fatal("uploadType=media should fail loud")
	}
}

// TestBigQueryCodecEncodeResumable verifies the codec surfaces the resumable
// Location/Range headers the client SDKs require.
func TestBigQueryCodecEncodeResumable(t *testing.T) {
	codec := &BigQueryCodec{Service: "bigquery"}

	status, headers, body := codec.Encode(nil, &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{wire.LocationKey: "http://bq.local/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=abc"},
	})
	if status != 200 || headers.Get("Location") == "" || len(body) != 0 {
		t.Fatalf("session start = %d loc=%q body=%q", status, headers.Get("Location"), body)
	}

	status, headers, _ = codec.Encode(nil, &model.ProviderResponse{
		HTTPStatus: 308,
		Data:       map[string]any{wire.RangeKey: "bytes=0-7"},
	})
	if status != 308 || headers.Get("Range") != "bytes=0-7" {
		t.Fatalf("incomplete chunk = %d range=%q", status, headers.Get("Range"))
	}
}

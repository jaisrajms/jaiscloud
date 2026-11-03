package gateway

import (
	"net/http"
	"testing"
)

// TestIsBigQueryStreamingUpload pins the detection that keeps a BigQuery load
// upload body out of the gateway buffer: only the two bodies that are raw file
// bytes (a multipart/related media part and a resumable chunk) stream. The
// resumable initiate carries the JSON job resource and must stay buffered.
func TestIsBigQueryStreamingUpload(t *testing.T) {
	cases := []struct {
		method, target string
		want           bool
	}{
		{http.MethodPost, "/upload/bigquery/v2/projects/p/jobs?uploadType=multipart", true},
		{http.MethodPut, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=bql_x", true},
		{http.MethodPost, "/resumable/upload/bigquery/v2/projects/p/jobs?uploadType=resumable&upload_id=bql_x", true},
		{http.MethodPost, "/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", false},
		{http.MethodPut, "/resumable/upload/bigquery/v2/projects/p/jobs?uploadType=resumable", false},
		{http.MethodPost, "/upload/bigquery/v2/projects/p/jobs?uploadType=media", false},
		{http.MethodPost, "/upload/bigquery/v2/projects/p/jobs", false},
		{http.MethodPost, "/bigquery/v2/projects/p/jobs", false},
		{http.MethodPost, "/upload/storage/v1/b/bkt/o?uploadType=multipart", false},
	}
	for _, tc := range cases {
		r, err := http.NewRequest(tc.method, tc.target, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.target, err)
		}
		if got := isBigQueryStreamingUpload(r); got != tc.want {
			t.Errorf("%s %s: isBigQueryStreamingUpload = %v, want %v", tc.method, tc.target, got, tc.want)
		}
	}
}

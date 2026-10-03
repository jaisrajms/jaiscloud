package gcp

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

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

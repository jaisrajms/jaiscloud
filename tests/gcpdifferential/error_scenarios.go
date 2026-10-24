//go:build gcp_differential

package gcpdifferential

import (
	"fmt"
	"net/http"
)

// This file is the REST half of the SDK error/retry tour differential
// (demo/sdk-tour errors mode). It records the canonical *error* responses the
// same operations produce on real GCP — ALREADY_EXISTS, NOT_FOUND,
// INVALID_ARGUMENT and a stale-etag IAM conflict — and replays them against the
// emulator, diffing the normalized error envelopes. The happy-path tour set
// proves the clients work; this set proves the failure bodies they branch on do
// not drift.
//
// The golden set lives in its own directory (testdata/golden-tour-errors) so it
// never pollutes the success-path tour goldens. A scenario with no committed
// golden is "pending recording" and skipped, exactly as in the other sets.

// ErrorScenarios returns the REST error-tour scenario list. It is ordered so
// resources exist before the failing operation that targets them.
func ErrorScenarios(project, suffix string) []Scenario {
	n := Names(suffix)
	bucket := n.ErrorBucket
	bqBase := "/bigquery/v2/projects/" + project

	var sc []Scenario

	// ─── Cloud Storage error mapping ────────────────────────────────────────
	sc = append(sc,
		// Baseline creation, then the duplicate: 409 ALREADY_EXISTS.
		Scenario{Op: "error_bucket_create", Service: "storage", Method: http.MethodPost,
			Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q,"location":"US"}`, bucket)},
		Scenario{Op: "error_bucket_already_exists", Service: "storage", Method: http.MethodPost,
			Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q,"location":"US"}`, bucket)},
		// 404 NOT_FOUND for a resource that never existed.
		Scenario{Op: "error_object_not_found", Service: "storage", Method: http.MethodGet,
			Path: "/storage/v1/b/" + bucket + "/o/no%2Fsuch%2Fobject"},
	)

	// ─── Pub/Sub IAM optimistic concurrency (etag OCC) ──────────────────────
	// A stale policy etag must be rejected with a conflict, not applied.
	sc = append(sc,
		Scenario{Op: "error_topic_create", Service: "pubsub", Method: http.MethodPut,
			Path: "/v1/projects/" + project + "/topics/" + n.ErrorTopic, Body: `{}`},
		Scenario{Op: "error_iam_get", Service: "pubsub", Method: http.MethodGet,
			Path: "/v1/projects/" + project + "/topics/" + n.ErrorTopic + ":getIamPolicy",
			Save: map[string]string{"etag": "etag"}},
		Scenario{Op: "error_iam_set_ok", Service: "pubsub", Method: http.MethodPost,
			Path: "/v1/projects/" + project + "/topics/" + n.ErrorTopic + ":setIamPolicy",
			Body: `{"policy":{"etag":"${etag}","bindings":[{"role":"roles/pubsub.publisher","members":["allUsers"]}]}}`},
		Scenario{Op: "error_iam_set_stale", Service: "pubsub", Method: http.MethodPost,
			Path: "/v1/projects/" + project + "/topics/" + n.ErrorTopic + ":setIamPolicy",
			Body: `{"policy":{"etag":"${etag}","bindings":[{"role":"roles/pubsub.viewer","members":["allAuthenticatedUsers"]}]}}`},
	)

	// ─── BigQuery invalid argument + idempotent insert ──────────────────────
	sc = append(sc,
		Scenario{Op: "error_invalid_query", Service: "bigquery", Method: http.MethodPost, Path: bqBase + "/queries",
			Body: `{"query":"SELECT * FROM","useLegacySql":false}`},
		Scenario{Op: "error_bq_dataset_create", Service: "bigquery", Method: http.MethodPost, Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q},"friendlyName":"sdk-tour-errors"}`, project, n.ErrorBQDataset)},
		Scenario{Op: "error_bq_table_create", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.ErrorBQDataset + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]}}`, project, n.ErrorBQDataset, n.ErrorBQTable)},
		// insertId is the API's client-supplied idempotency key: the replay of
		// the same row must return an insertErrors[].duplicate body, not a
		// second row.
		Scenario{Op: "error_insertall_seed", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.ErrorBQDataset + "/tables/" + n.ErrorBQTable + "/insertAll",
			Body: `{"rows":[{"insertId":"idem-1","json":{"id":"1","name":"a"}}]}`},
		Scenario{Op: "error_insertall_duplicate", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.ErrorBQDataset + "/tables/" + n.ErrorBQTable + "/insertAll",
			Body: `{"rows":[{"insertId":"idem-1","json":{"id":"1","name":"a"}}]}`},
		Scenario{Op: "error_insertall_count", Service: "bigquery", Method: http.MethodGet,
			Path: bqBase + "/datasets/" + n.ErrorBQDataset + "/tables/" + n.ErrorBQTable + "/data"},
	)

	return sc
}

// CleanupErrors deletes every resource the error scenario set creates. It is
// best-effort and idempotent (404s are ignored).
func (t *Target) CleanupErrors() []string {
	n := t.Names
	bqBase := "/bigquery/v2/projects/" + t.Project
	type del struct {
		service, desc, method, path, body string
	}
	ops := []del{
		{"bigquery", "dataset", http.MethodDelete, bqBase + "/datasets/" + n.ErrorBQDataset + "?deleteContents=true", ""},
		{"pubsub", "topic", http.MethodDelete, "/v1/projects/" + t.Project + "/topics/" + n.ErrorTopic, ""},
		{"storage", "bucket", http.MethodDelete, "/storage/v1/b/" + n.ErrorBucket, ""},
	}
	var log []string
	for _, op := range ops {
		status, _, err := t.request(op.method, op.service, op.path, op.body, "application/json")
		if err != nil {
			log = append(log, fmt.Sprintf("cleanup errors %s/%s: error: %v", op.service, op.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("cleanup errors %s/%s: HTTP %d", op.service, op.desc, status))
	}
	return log
}

// VerifyErrorsAbsent reads back the principal error-set resources; a fully
// cleaned target reports 404 for both.
func (t *Target) VerifyErrorsAbsent() []string {
	n := t.Names
	bqBase := "/bigquery/v2/projects/" + t.Project
	checks := []struct{ service, desc, path string }{
		{"storage", "bucket", "/storage/v1/b/" + n.ErrorBucket},
		{"bigquery", "dataset", bqBase + "/datasets/" + n.ErrorBQDataset},
		{"pubsub", "topic", "/v1/projects/" + t.Project + "/topics/" + n.ErrorTopic},
	}
	var log []string
	for _, c := range checks {
		status, _, err := t.request(http.MethodGet, c.service, c.path, "", "")
		if err != nil {
			log = append(log, fmt.Sprintf("verify errors %s/%s: error: %v", c.service, c.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("verify errors %s/%s: HTTP %d", c.service, c.desc, status))
	}
	return log
}

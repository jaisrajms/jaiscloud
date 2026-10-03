//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// defaultProject is the emulator's default GCP project (internal/config).
const defaultProject = "jaiscloud-project"

// Scenario is one curated request in the record sequence. Body and Path may
// reference ${name} variables captured from earlier responses via Save.
type Scenario struct {
	Service     string
	Method      string
	Path        string
	Body        string
	ContentType string
	// Save maps a variable name to a dotted path into the response JSON whose
	// scalar value is captured for use by later scenarios (e.g. the ciphertext
	// returned by KMS encrypt feeding KMS decrypt).
	Save map[string]string
}

// runSuffix returns a per-run unique suffix so scenario names stay idempotent
// across repeated record runs against the same emulator.
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}

// Scenarios returns the curated, idempotent request list covering storage,
// pubsub, secretmanager, kms, clouddns, bigquery and iam, including both
// success and error (404) responses.
func Scenarios(suffix string) []Scenario {
	p := defaultProject

	bucket := "conf-bucket-" + suffix
	topic := "conf-topic-" + suffix
	sub := "conf-sub-" + suffix
	secret := "conf-secret-" + suffix
	ring := "conf-ring-" + suffix
	key := "conf-key-" + suffix
	sa := "conf-sa-" + suffix
	zone := "conf-zone-" + suffix
	dnsName := "conf" + suffix + ".example.com."
	ds := "conf_ds_" + suffix
	tbl := "conf_tbl_" + suffix

	saEmail := sa + "@" + p + ".iam.gserviceaccount.com"
	topicName := "projects/" + p + "/topics/" + topic
	subName := "projects/" + p + "/subscriptions/" + sub

	var sc []Scenario

	// ─── Cloud Storage (GCS JSON API) ─────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + p,
			Body: fmt.Sprintf(`{"name":%q}`, bucket)},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b?project=" + p},
		Scenario{Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=hello.txt",
			Body:        "hello jaiscloud",
			ContentType: "text/plain"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o/hello.txt"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o"},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket + "/o/missing-" + suffix},
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + bucket + "/o/hello.txt"},
		Scenario{Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + bucket},
		Scenario{Service: "storage", Method: "GET", Path: "/storage/v1/b/" + bucket},
	)

	// ─── Pub/Sub ──────────────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + p + "/topics/" + topic,
			Body: fmt.Sprintf(`{"name":%q}`, topicName)},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics/" + topic},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics"},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/topics/missing-" + suffix},
		Scenario{Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + p + "/subscriptions/" + sub,
			Body: fmt.Sprintf(`{"name":%q,"topic":%q,"ackDeadlineSeconds":10}`, subName, topicName)},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions/" + sub},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions"},
		Scenario{Service: "pubsub", Method: "POST", Path: "/v1/projects/" + p + "/topics/" + topic + ":publish",
			Body: `{"messages":[{"data":"aGVsbG8="}]}`},
		Scenario{Service: "pubsub", Method: "POST", Path: "/v1/projects/" + p + "/subscriptions/" + sub + ":pull",
			Body: `{"maxMessages":10}`},
		Scenario{Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + p + "/subscriptions/" + sub},
		Scenario{Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + p + "/topics/" + topic},
		Scenario{Service: "pubsub", Method: "GET", Path: "/v1/projects/" + p + "/subscriptions/missing-" + suffix},
	)

	// ─── Secret Manager ───────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets?secretId=" + secret,
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets/" + secret},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets"},
		Scenario{Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + p + "/secrets/" + secret + ":addVersion",
			Body: `{"payload":{"data":"c2VjcmV0"}}`},
		Scenario{Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + p + "/secrets/" + secret + "/versions/1:access"},
		Scenario{Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + p + "/secrets/" + secret + "/versions"},
		Scenario{Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + p + "/secrets/missing-" + suffix},
		Scenario{Service: "secretmanager", Method: "DELETE", Path: "/v1/projects/" + p + "/secrets/" + secret},
	)

	// ─── Cloud KMS ────────────────────────────────────────────────────────────
	base := "/v1/projects/" + p + "/locations/global/keyRings"
	keyPath := base + "/" + ring + "/cryptoKeys/" + key
	sc = append(sc,
		Scenario{Service: "kms", Method: "POST", Path: base + "?keyRingId=" + ring, Body: `{}`},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring},
		Scenario{Service: "kms", Method: "GET", Path: base},
		Scenario{Service: "kms", Method: "POST", Path: base + "/" + ring + "/cryptoKeys?cryptoKeyId=" + key,
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`, Save: map[string]string{"cryptoKey": "name"}},
		Scenario{Service: "kms", Method: "GET", Path: keyPath},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + "/cryptoKeys"},
		Scenario{Service: "kms", Method: "POST", Path: keyPath + ":encrypt",
			Body: `{"plaintext":"aGVsbG8="}`, Save: map[string]string{"ciphertext": "ciphertext"}},
		Scenario{Service: "kms", Method: "POST", Path: keyPath + ":decrypt",
			Body: `{"ciphertext":"${ciphertext}"}`},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + ":getIamPolicy"},
		Scenario{Service: "kms", Method: "GET", Path: base + "/" + ring + "/cryptoKeys/missing-" + suffix},
	)

	// ─── IAM ──────────────────────────────────────────────────────────────────
	saBase := "/v1/projects/" + p + "/serviceAccounts"
	sc = append(sc,
		Scenario{Service: "iam", Method: "POST", Path: saBase + "?accountId=" + sa,
			Body: `{"serviceAccount":{"displayName":"Conformance SA"}}`},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/" + saEmail},
		Scenario{Service: "iam", Method: "GET", Path: saBase},
		Scenario{Service: "iam", Method: "POST", Path: saBase + "/" + saEmail + "/keys", Body: `{}`,
			Save: map[string]string{"keyName": "name"}},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/" + saEmail + "/keys"},
		Scenario{Service: "iam", Method: "POST", Path: saBase + "/" + saEmail + ":signBlob",
			Body: `{"bytesToSign":"aGVsbG8="}`},
		Scenario{Service: "iam", Method: "GET", Path: saBase + "/missing-" + suffix + "@" + p + ".iam.gserviceaccount.com"},
		Scenario{Service: "iam", Method: "DELETE", Path: "/v1/${keyName}"},
		Scenario{Service: "iam", Method: "DELETE", Path: saBase + "/" + saEmail},
	)

	// ─── IAM Service Account Credentials ──────────────────────────────────────
	// The unique iamcredentials verbs. signBlob/signJwt share their path with
	// iam and are exercised by the IAM block above.
	sc = append(sc,
		Scenario{Service: "iamcredentials", Method: "POST",
			Path: "/v1/projects/-/serviceAccounts/" + saEmail + ":generateAccessToken",
			Body: `{"scope":["https://www.googleapis.com/auth/cloud-platform"],"lifetime":"3600s"}`},
		Scenario{Service: "iamcredentials", Method: "POST",
			Path: "/v1/projects/-/serviceAccounts/" + saEmail + ":generateIdToken",
			Body: `{"audience":"https://example.com","includeEmail":true}`},
	)

	// ─── Cloud DNS ────────────────────────────────────────────────────────────
	dnsBase := "/dns/v1/projects/" + p + "/managedZones"
	sc = append(sc,
		Scenario{Service: "clouddns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q,"dnsName":%q,"description":"conformance"}`, zone, dnsName)},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/" + zone},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase},
		Scenario{Service: "clouddns", Method: "GET", Path: "/dns/v1/projects/" + p},
		Scenario{Service: "clouddns", Method: "POST", Path: dnsBase + "/" + zone + "/rrsets",
			Body: fmt.Sprintf(`{"name":%q,"type":"A","ttl":300,"rrdatas":["1.2.3.4"]}`, "www."+dnsName)},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/" + zone + "/rrsets"},
		Scenario{Service: "clouddns", Method: "GET", Path: dnsBase + "/missing-" + suffix},
		Scenario{Service: "clouddns", Method: "DELETE", Path: dnsBase + "/" + zone},
	)

	// ─── BigQuery ─────────────────────────────────────────────────────────────
	// Executed-SQL coverage: the emulator evaluates a bounded Standard SQL
	// subset (BQ1/BQ2), so the wire harness exercises SELECT, DDL (CREATE TABLE
	// AS SELECT), DML (INSERT) and the fail-loud error path alongside the
	// metadata + insertAll surface. The job ids captured from the jobs.query
	// responses feed getQueryResults and jobs.get.
	bqBase := "/bigquery/v2/projects/" + p
	bqTbl2 := "conf_tbl2_" + suffix
	sc = append(sc,
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, p, ds)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"}]}}`, p, ds, tbl)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl + "/insertAll",
			Body: `{"rows":[{"insertId":"1","json":{"id":"1"}}]}`},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl + "/data"},
		// SELECT over the stored row; capture the job id for the read-back calls.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"SELECT id FROM `%s.%s.%s` WHERE id = 1\",\"useLegacySql\":false}", p, ds, tbl),
			Save: map[string]string{"bqSelectJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqSelectJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/jobs/${bqSelectJob}"},
		// DDL: CREATE TABLE AS SELECT.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"CREATE TABLE `%s.%s.%s` AS SELECT id FROM `%s.%s.%s`\",\"useLegacySql\":false}", p, ds, bqTbl2, p, ds, tbl),
			Save: map[string]string{"bqDdlJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqDdlJob}"},
		// DML: INSERT INTO the CTAS table, then read it back.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"INSERT INTO `%s.%s.%s` (id) VALUES (2)\",\"useLegacySql\":false}", p, ds, bqTbl2),
			Save: map[string]string{"bqDmlJob": "jobReference.jobId"}},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/queries/${bqDmlJob}"},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + ds + "/tables/" + bqTbl2 + "/data"},
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT 1"}`},
		// Fail loud: a construct outside the frozen subset is a 400 invalidQuery.
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries",
			Body: fmt.Sprintf("{\"query\":\"MERGE `%s.%s.%s` T USING `%s.%s.%s` S ON T.id = S.id\",\"useLegacySql\":false}", p, ds, tbl, p, ds, tbl)},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/missing_" + suffix},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + bqTbl2},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds},
	)

	// ─── Cloud Datastore (REST data plane) ────────────────────────────────────
	dsA := "conf-ds-a-" + suffix
	dsTxn := "conf-ds-txn-" + suffix
	dsKey := func(name string) string {
		return fmt.Sprintf(`{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask","name":%q}]}`, p, name)
	}
	sc = append(sc,
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":commit",
			Body: fmt.Sprintf(`{"mode":"NON_TRANSACTIONAL","mutations":[{"upsert":{"key":%s,"properties":{"n":{"integerValue":"1"}}}}]}`, dsKey(dsA))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":lookup",
			Body: fmt.Sprintf(`{"keys":[%s]}`, dsKey(dsA))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":lookup",
			Body: fmt.Sprintf(`{"keys":[%s]}`, dsKey("conf-ds-missing-"+suffix))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runQuery",
			Body: `{"query":{"kind":[{"name":"ConfDsTask"}],"filter":{"propertyFilter":{"property":{"name":"n"},"op":"EQUAL","value":{"integerValue":"1"}}}}}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runAggregationQuery",
			Body: `{"aggregationQuery":{"nestedQuery":{"kind":[{"name":"ConfDsTask"}]},"aggregations":[{"alias":"total","count":{}}]}}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":allocateIds",
			Body: fmt.Sprintf(`{"keys":[{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask"}]}]}`, p)},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":reserveIds",
			Body: fmt.Sprintf(`{"keys":[{"partitionId":{"projectId":%q},"path":[{"kind":"ConfDsTask","id":"999999999"}]}]}`, p)},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":beginTransaction", Body: `{}`,
			Save: map[string]string{"txn": "transaction"}},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":commit",
			Body: fmt.Sprintf(`{"mode":"TRANSACTIONAL","transaction":"${txn}","mutations":[{"insert":{"key":%s,"properties":{"n":{"integerValue":"2"}}}}]}`, dsKey(dsTxn))},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":rollback", Body: `{}`},
		Scenario{Service: "datastore", Method: "POST", Path: "/v1/projects/" + p + ":runQuery",
			Body: `{"gqlQuery":{"queryString":"SELECT * FROM ConfDsTask"}}`},
	)

	// ─── Cloud Logging (REST data plane) ──────────────────────────────────────
	logID := "conf-log-" + suffix
	logName := "projects/" + p + "/logs/" + logID
	logFilter := fmt.Sprintf(`logName=\"%s\"`, logName)
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:write",
			Body: fmt.Sprintf(`{"logName":%q,"resource":{"type":"global"},"labels":{"suite":"conformance"},"entries":[{"severity":"INFO","textPayload":"hello"},{"severity":"ERROR","jsonPayload":{"msg":"boom"}}]}`, logName)},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:list",
			Body: fmt.Sprintf(`{"resourceNames":["projects/%s"],"filter":"%s"}`, p, logFilter)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/logs"},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/monitoredResourceDescriptors?pageSize=5"},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + logName},
		Scenario{Service: "logging", Method: "POST", Path: "/v2/entries:list",
			Body: fmt.Sprintf(`{"resourceNames":["projects/%s"],"filter":"%s"}`, p, logFilter)},
	)

	// Cloud Logging config plane (sinks + exclusions).
	sinkID := "conf-sink-" + suffix
	sinkName := "projects/" + p + "/sinks/" + sinkID
	sinkDest := "storage.googleapis.com/conf-bucket-" + suffix
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/sinks",
			Body: fmt.Sprintf(`{"name":%q,"destination":%q,"filter":"severity>=WARNING"}`, sinkID, sinkDest)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + sinkName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/sinks"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + sinkName + "?updateMask=filter",
			Body: `{"filter":"severity>=ERROR"}`},
		Scenario{Service: "logging", Method: "PUT", Path: "/v2/" + sinkName,
			Body: fmt.Sprintf(`{"name":%q,"destination":%q,"filter":"severity>=ERROR","description":"updated"}`, sinkID, sinkDest)},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + sinkName},
	)
	exclID := "conf-exclusion-" + suffix
	exclName := "projects/" + p + "/exclusions/" + exclID
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/exclusions",
			Body: fmt.Sprintf(`{"name":%q,"filter":"severity<DEBUG"}`, exclID)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + exclName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/exclusions"},
		Scenario{Service: "logging", Method: "PATCH", Path: "/v2/" + exclName + "?updateMask=disabled",
			Body: `{"disabled":true}`},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + exclName},
	)

	// Cloud Logging logs-based metrics.
	metricID := "conf-metric-" + suffix
	metricName := "projects/" + p + "/metrics/" + metricID
	sc = append(sc,
		Scenario{Service: "logging", Method: "POST", Path: "/v2/projects/" + p + "/metrics",
			Body: fmt.Sprintf(`{"name":%q,"description":"conformance","filter":"severity>=ERROR","valueExtractor":"EXTRACT(jsonPayload.latency)","labelExtractors":{"code":"EXTRACT(jsonPayload.code)"},"metricDescriptor":{"valueType":"DISTRIBUTION","unit":"ms","labels":[{"key":"code","valueType":"INT64"}]},"bucketOptions":{"linearBuckets":{"numFiniteBuckets":3,"width":1,"offset":0}}}`, metricID)},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + metricName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/projects/" + p + "/metrics"},
		Scenario{Service: "logging", Method: "PUT", Path: "/v2/" + metricName,
			Body: fmt.Sprintf(`{"name":%q,"filter":"severity>=WARNING","valueExtractor":"EXTRACT(jsonPayload.latency)","labelExtractors":{"code":"EXTRACT(jsonPayload.code)"},"metricDescriptor":{"valueType":"DISTRIBUTION","unit":"ms","labels":[{"key":"code","valueType":"INT64"}]}}`, metricID)},
		Scenario{Service: "logging", Method: "DELETE", Path: "/v2/" + metricName},
		Scenario{Service: "logging", Method: "GET", Path: "/v2/" + metricName},
	)

	// ─── Cloud Functions v2 runtime catalog ───────────────────────────────────
	// The runtime catalog is the v2-deploy enabler: gcloud resolves a function's
	// runtime from this list (filtered to environment GEN_2) before uploading
	// source. Both the unfiltered list and an AIP-160 name filter are exercised.
	sc = append(sc,
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/runtimes"},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/runtimes?filter=" + url.QueryEscape(`name="nodejs20"`)},
	)

	// ─── Cloud Functions common APIs + long-running operations ────────────────
	// GetLocation is the shared google.cloud.location.Locations method and
	// CancelOperation/DeleteOperation/WaitOperation are the shared
	// google.longrunning.Operations methods. None are enumerated in the service
	// Discovery document (the classifier upgrades those cells in
	// docs/fidelity-overrides.yaml), so these scenarios pin the served shapes.
	// A create first captures the persisted operation name so get/list/wait/
	// cancel/delete exercise a real stored operation (unknown ids are NotFound).
	fnID := "conf-fn-" + suffix
	sc = append(sc,
		Scenario{Service: "functions", Method: "GET",
			Path: "/v1/projects/" + p + "/locations/us-central1"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/projects/" + p + "/locations/us-central1/functions?functionId=" + url.QueryEscape(fnID),
			Body: `{"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`,
			Save: map[string]string{"fnop": "name"}},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/${fnop}"},
		Scenario{Service: "functions", Method: "GET",
			Path: "/v2/projects/" + p + "/locations/us-central1/operations"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${fnop}:wait"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${fnop}:cancel"},
		Scenario{Service: "functions", Method: "DELETE",
			Path: "/v2/${fnop}"},
	)

	// ─── Cloud Functions v2 upgrade / traffic control plane (FD5) ─────────────
	// The seven 1st→2nd gen upgrade verbs are custom POST methods on a function
	// name (REST-only: documented as FunctionService RPCs, but absent from the
	// public googleapis proto mirror and all generated clients). A fresh function
	// is upgraded through
	// setup → redirect → rollback → redirect → abort, then setup → redirect →
	// commit, then setup → redirect → commitAsGen2 → detach, then deleted.
	upgID := "conf-upg-" + suffix
	sc = append(sc,
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/projects/" + p + "/locations/us-central1/functions?functionId=" + url.QueryEscape(upgID),
			Body: `{"buildConfig":{"runtime":"nodejs20","entryPoint":"handler"}}`,
			Save: map[string]string{"upgFn": "response.name"}},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${upgFn}:setupFunctionUpgradeConfig",
			Body: `{"buildConfigOverrides":{"runtime":"nodejs22"},"serviceConfigOverrides":{"maxInstanceCount":4}}`},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:rollbackFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:abortFunctionUpgrade"},
		Scenario{Service: "functions", Method: "POST",
			Path: "/v2/${upgFn}:setupFunctionUpgradeConfig",
			Body: `{"buildConfigOverrides":{"runtime":"nodejs22"}}`},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:commitFunctionUpgrade"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:setupFunctionUpgradeConfig"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:redirectFunctionUpgradeTraffic"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:commitFunctionUpgradeAsGen2"},
		Scenario{Service: "functions", Method: "POST", Path: "/v2/${upgFn}:detachFunction"},
		Scenario{Service: "functions", Method: "DELETE", Path: "/v2/${upgFn}"},
	)

	// ─── Cloud Monitoring (REST data plane) ───────────────────────────────────
	mType := "conf.metric_" + suffix
	tsType := "conf.ts_" + suffix
	tsFilter := url.QueryEscape(`metric.type="` + tsType + `"`)
	monBase := "/v3/projects/" + p
	sc = append(sc,
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/metricDescriptors",
			Body: fmt.Sprintf(`{"type":%q,"metricKind":"GAUGE","valueType":"INT64","description":"conformance","displayName":"Conf Metric","labels":[{"key":"env","valueType":"STRING","description":"environment"}]}`, mType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/metricDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/metricDescriptors/" + mType},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/timeSeries",
			Body: fmt.Sprintf(`{"timeSeries":[{"metric":{"type":%q,"labels":{"k":"v"}},"resource":{"type":"global"},"metricKind":"GAUGE","valueType":"DOUBLE","points":[{"interval":{"endTime":"2026-01-01T00:00:00Z"},"value":{"doubleValue":1.5}}]}]}`, tsType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/timeSeries?filter=" + tsFilter},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/timeSeries:createService",
			Body: fmt.Sprintf(`{"timeSeries":[{"metric":{"type":%q},"resource":{"type":"global"},"metricKind":"GAUGE","valueType":"DOUBLE","points":[{"interval":{"endTime":"2026-01-01T00:00:00Z"},"value":{"doubleValue":2.5}}]}]}`, tsType)},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/monitoredResourceDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/monitoredResourceDescriptors/gce_instance"},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/alertPolicies",
			Body: fmt.Sprintf(`{"displayName":"Conf Policy %s","combiner":"OR","conditions":[{"displayName":"cond","conditionThreshold":{"filter":%q,"comparison":"COMPARISON_GT","thresholdValue":1,"duration":"60s","trigger":{"count":1}}}]}`, suffix, `metric.type="`+tsType+`"`),
			Save: map[string]string{"policyName": "name"}},
		Scenario{Service: "monitoring", Method: "GET", Path: "/v3/${policyName}"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/alertPolicies"},
		Scenario{Service: "monitoring", Method: "PATCH", Path: "/v3/${policyName}?updateMask=displayName",
			Body: `{"displayName":"Updated Conf Policy"}`},
		Scenario{Service: "monitoring", Method: "POST", Path: monBase + "/notificationChannels",
			Body: fmt.Sprintf(`{"type":"email","displayName":"Conf Channel %s","labels":{"email_address":"conf@example.com"}}`, suffix),
			Save: map[string]string{"channelName": "name"}},
		Scenario{Service: "monitoring", Method: "GET", Path: "/v3/${channelName}"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannels"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:sendVerificationCode"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:getVerificationCode"},
		Scenario{Service: "monitoring", Method: "POST", Path: "/v3/${channelName}:verify"},
		Scenario{Service: "monitoring", Method: "PATCH", Path: "/v3/${channelName}?updateMask=description",
			Body: `{"description":"updated description"}`},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannelDescriptors"},
		Scenario{Service: "monitoring", Method: "GET", Path: monBase + "/notificationChannelDescriptors/email"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: "/v3/${channelName}"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: "/v3/${policyName}"},
		Scenario{Service: "monitoring", Method: "DELETE", Path: monBase + "/metricDescriptors/" + mType},
	)

	// ─── Google Kubernetes Engine (GKE) v1 ────────────────────────────────────
	// GKE shares the canonical /v1/projects/{p}/locations/{l}/clusters path with
	// Managed Kafka on one origin, so the recorder reaches it through the
	// /container path prefix (Terraform/gcloud path-mode routing; the SDK
	// host-mode form is covered by the adapter/router unit test). A cluster is
	// created (returns a CREATE_CLUSTER Operation), read, listed, its operation
	// is fetched and listed, then it is deleted (DELETE_CLUSTER Operation) and a
	// post-delete read proves the NotFound error envelope.
	gkeCluster := "conf-gke-" + suffix
	gkeBase := "/container/v1/projects/" + p + "/locations/us-central1"
	gkeClusterName := gkeBase + "/clusters/" + gkeCluster
	sc = append(sc,
		Scenario{Service: "container", Method: "POST", Path: gkeBase + "/clusters",
			Body: fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":1}}`, gkeCluster),
			Save: map[string]string{"gkeOp": "name"}},
		Scenario{Service: "container", Method: "GET", Path: gkeClusterName},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/clusters"},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/operations/${gkeOp}"},
		Scenario{Service: "container", Method: "GET", Path: gkeBase + "/operations"},
		Scenario{Service: "container", Method: "DELETE", Path: gkeClusterName},
		Scenario{Service: "container", Method: "GET", Path: gkeClusterName},
	)

	// ─── Cloud Run Admin v2 (run.googleapis.com) ──────────────────────────────
	// Cloud Run shares the /v2/projects/{p}/locations/{l} namespace with Cloud
	// Functions/Tasks, so the recorder uses canonical paths that detect as run:
	// the services/revisions family and the run-prefixed operation id. A service
	// is created (returns a done google.longrunning.Operation whose response is
	// the Service), read, listed, its revision listed/fetched, its IAM policy
	// set/read/tested, a description-only PATCH applied, its operation fetched,
	// then it is deleted and a post-delete read proves the NotFound envelope.
	// The bare .../operations list is intentionally NOT recorded: that path is
	// path-ambiguous with Cloud Functions on one origin and stays with functions
	// by design (documented limitation; run operations are reachable by their
	// prefixed id).
	runSvc := "conf-run-" + suffix
	runBase := "/v2/projects/" + p + "/locations/us-central1"
	sc = append(sc,
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services?serviceId=" + runSvc,
			Body: `{"template":{"containers":[{"image":"nginx:latest","ports":[{"containerPort":80}]}]}}`,
			Save: map[string]string{"runOp": "name", "runRev": "response.latestReadyRevision"}},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + "/revisions"},
		Scenario{Service: "run", Method: "GET", Path: "/v2/${runRev}"},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + ":getIamPolicy"},
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services/" + runSvc + ":setIamPolicy",
			Body: `{"policy":{"bindings":[{"role":"roles/run.invoker","members":["allUsers"]}]}}`},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc + ":getIamPolicy"},
		Scenario{Service: "run", Method: "POST", Path: runBase + "/services/" + runSvc + ":testIamPermissions",
			Body: `{"permissions":["run.services.get","run.services.delete"]}`},
		Scenario{Service: "run", Method: "PATCH", Path: runBase + "/services/" + runSvc + "?updateMask=description",
			Body: `{"description":"updated"}`},
		Scenario{Service: "run", Method: "GET", Path: "/v2/${runOp}"},
		Scenario{Service: "run", Method: "DELETE", Path: runBase + "/services/" + runSvc},
		Scenario{Service: "run", Method: "GET", Path: runBase + "/services/" + runSvc},
	)

	return sc
}

// expandVars substitutes ${name} references in s using vars.
func expandVars(s string, vars map[string]string) string {
	for name, val := range vars {
		s = strings.ReplaceAll(s, "${"+name+"}", val)
	}
	return s
}

// captureVar walks a dotted path into a decoded JSON value and returns its
// scalar string form.
func captureVar(v any, path string) (string, bool) {
	cur := v
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = m[seg]
		if !ok {
			return "", false
		}
	}
	switch t := cur.(type) {
	case string:
		return t, true
	case bool:
		return fmt.Sprintf("%t", t), true
	case float64:
		return fmt.Sprintf("%v", t), true
	default:
		b, err := json.Marshal(cur)
		if err != nil {
			return "", false
		}
		return string(b), true
	}
}

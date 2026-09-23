//go:build gcp_conformance

package gcpconformance

import (
	"encoding/json"
	"fmt"
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
	bqBase := "/bigquery/v2/projects/" + p
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
		Scenario{Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT 1"}`},
		Scenario{Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/missing_" + suffix},
		Scenario{Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + ds + "/tables/" + tbl},
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

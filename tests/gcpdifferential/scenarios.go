//go:build gcp_differential

// Package gcpdifferential is an offline-first differential (record/replay)
// conformance harness: it records the responses of a curated, idempotent set of
// requests from REAL GCP into committed goldens, then replays the exact same
// requests against the jaiscloud GCP emulator and diffs the two.
//
// The REST half is stdlib-only: its recorder authenticates with Application
// Default Credentials via the `gcloud` CLI (never printed, never written to disk
// by this package). The AUD6-1 gRPC half (grpc_*.go) instead drives the official
// Google gRPC clients for Datastore, Firestore, Logging and Monitoring, dialing
// real GCP with ADC through the official dialer. In both halves the offline
// replay touches no network beyond the local emulator and requires no
// credentials.
//
// Scope: the REST recorded set covers the services both real GCP and the
// emulator expose — the original twelve (storage, pubsub, secretmanager, kms,
// bigquery, dns, workflows, iam, firestore, compute, sqladmin, redis) plus the
// AUD6 breadth services (container, dataproc, monitoring, logging, datastore,
// workflowexecutions, serviceusage) and the AUD6-2 services (resourcemanager,
// eventarc, tasks, run, functions, metastore, managedkafka). The curated
// Scenarios list may also contain operations that have not been recorded yet:
// a scenario with no committed golden is reported as "pending recording" and
// skipped by TestReplay, so new breadth can land in the tree without breaking
// the offline gate before the next real-GCP capture. Once the user records,
// every scenario gains a golden and starts being diffed.
//
// gRPC-only surfaces are out of scope for the REST differential; the AUD6-1
// gRPC differential (Scenarios in grpc_scenarios.go) covers Datastore,
// Firestore, Logging and Monitoring over gRPC, and google.longrunning.Operations
// is a documented deferral (AUD6-4).
package gcpdifferential

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// EmulatorProjectDefault matches the emulator's default project.
const EmulatorProjectDefault = "jaiscloud-project"

// RealProjectDefault is the verified real-GCP parity project. It is only a
// default; record mode reads GCP_DIFFERENTIAL_PROJECT to override it.
const RealProjectDefault = "parity-diff-jaiscloud"

// RealProjectNumberDefault is the numeric id of RealProjectDefault.
const RealProjectNumberDefault = "978644905877"

// Fixed KMS resource names. KMS keyRings/cryptoKeys cannot be deleted by GCP
// (a cryptoKey can only be scheduled for destruction, ~30 days), so the harness
// uses stable, reusable names instead of per-run names: at most one keyring and
// one key exist, they are re-used across runs, and they are never destroyed.
const (
	FixedKMSKeyRing   = "jaiscloud-differential"
	FixedKMSCryptoKey = "differential-key"
	// FixedKMSKeyRingUS is a second fixed keyring in the `us` multi-region,
	// required because GCS refuses a `global` keyring for a `US` bucket
	// ("Cloud KMS region 'global' not available for use with GCS region 'US'").
	// Like the global pair it is non-deletable and reused.
	FixedKMSKeyRingUS = "jaiscloud-differential-us"
)

// Scenario is one curated request. Path and Body may reference ${name}
// variables captured from earlier responses via Save. The same Scenario is sent
// to real GCP (record mode) and to the emulator (replay mode); only the base
// URL, project and Authorization header differ.
type Scenario struct {
	// Op is a short, stable operation id used in golden filenames and reports.
	Op          string
	Service     string
	Method      string
	Path        string
	Body        string
	ContentType string
	// NoAuth, when true, sends the scenario without an Authorization header.
	// Record mode then captures real GCP's unauthenticated (UNAUTHENTICATED /
	// PERMISSION_DENIED) response while replay captures the emulator's — whose
	// authz is not enforced by design (docs/gcp/wire-contract.md §8). The resulting
	// divergence is accepted by a triage rule, making the authz gap explicit.
	NoAuth bool
	// Save maps a variable name to a dotted path into the response JSON whose
	// scalar value is captured for use by later scenarios (e.g. the ciphertext
	// returned by KMS encrypt feeding KMS decrypt).
	Save map[string]string
	// Headers are extra request headers sent with the scenario (e.g. the
	// Content-Range of a resumable-upload chunk, or X-Upload-Content-Type at
	// session initiation). They are not recorded: the diff compares status and
	// response body, and headers are transport plumbing.
	Headers map[string]string
	// SaveHeader maps a variable name to a response header whose value is
	// captured for use by later scenarios. The storage resumable session URI
	// arrives in the Location header, so it is the canonical use. When the
	// header value is an absolute URL only its path and query are kept, so a
	// later scenario can address the session through the target's normal
	// service/origin mapping (real GCP and the emulator expose different hosts
	// and path prefixes for the same session).
	SaveHeader map[string]string
	// NoRequestCapture suppresses recording the request body. It is used for
	// binary chunk bodies (a resumable upload's 256 KiB chunks) that would
	// otherwise bloat a committed golden by megabytes without adding diff
	// signal: the chunk's status and the finalize response carry the parity.
	NoRequestCapture bool
	// Wait, when non-nil, makes the run poll this scenario's Path (GET) until
	// the JSON response's Wait.Field is truthy, then record the final response.
	// It lets a scenario observe the result of an asynchronous (LRO) mutation,
	// which real GCP completes after the create/delete call returns.
	Wait *WaitSpec
	// Setup, when true, runs the request (honouring Wait and Save) but records
	// no golden and is not diffed. It exists for state a golden depends on but
	// whose own response is best-effort/nondeterministic — e.g. the nack loop
	// that forces a real-GCP dead-letter forward, whose per-attempt
	// deliveryAttempt is not stable across the two backends. A setup scenario
	// runs in both record and replay so the captured scenario that follows sees
	// the same state on both sides.
	Setup bool
}

// WaitSpec configures the polling behaviour for Scenario.Wait.
type WaitSpec struct {
	// Field is a dotted path into the JSON response (e.g. "done"). When set,
	// the poll succeeds once that value is truthy.
	Field string
	// Contains, when non-empty, makes the poll succeed once the canonical JSON
	// of the Field value contains this substring (e.g. a resource name inside a
	// list). It is used for eventually-consistent list reads.
	Contains string
	// MinLen, when > 0, makes the poll succeed once the value at Field is an
	// array or object with at least MinLen elements. It is used to wait for a
	// batch of delivered messages (receivedMessages) to accumulate.
	MinLen int
	// Interval is the delay between polls (default 1s).
	Interval time.Duration
	// Timeout bounds the wait (default 120s); on expiry the last response is
	// recorded so the divergence surfaces rather than hanging.
	Timeout time.Duration
}

// serviceBaseURL maps a service to its real-GCP REST origin. All path prefixes
// (e.g. /storage/v1, /v1/projects/...) are already present in Scenario.Path, so
// the emulator can serve every service from one origin unchanged.
var serviceBaseURL = map[string]string{
	"storage":       "https://storage.googleapis.com",
	"pubsub":        "https://pubsub.googleapis.com",
	"secretmanager": "https://secretmanager.googleapis.com",
	"kms":           "https://cloudkms.googleapis.com",
	"bigquery":      "https://bigquery.googleapis.com",
	// Cloud DNS serves its v1 REST surface under the /dns/v1/ path prefix on
	// the dns.googleapis.com origin.
	"dns": "https://dns.googleapis.com",
	// Cloud Workflows serves /v1/projects/{project}/locations/{location}/...
	// on the workflows.googleapis.com origin.
	"workflows": "https://workflows.googleapis.com",
	// IAM service accounts are served under /v1/projects/{project}/serviceAccounts.
	"iam": "https://iam.googleapis.com",
	// Firestore's REST surface is served under
	// /v1/projects/{project}/databases/{database}/documents on this origin.
	"firestore": "https://firestore.googleapis.com",
	// Synthetic/metadata-only control planes. Their method paths embed the
	// service prefix (compute/v1, sql/v1beta4) or the shared /v1/ shape (redis).
	"compute":  "https://compute.googleapis.com",
	"sqladmin": "https://sqladmin.googleapis.com",
	"redis":    "https://redis.googleapis.com",
	// AUD6 breadth: control-plane / data-plane services added on top of the
	// original twelve. Each already has a REST adapter in
	// internal/gcp/adapter/services.go (or a REST transport package) and its
	// API is enabled on the parity project.
	"serviceusage":       "https://serviceusage.googleapis.com",
	"dataproc":           "https://dataproc.googleapis.com",
	"container":          "https://container.googleapis.com",
	"logging":            "https://logging.googleapis.com",
	"monitoring":         "https://monitoring.googleapis.com",
	"datastore":          "https://datastore.googleapis.com",
	"workflowexecutions": "https://workflowexecutions.googleapis.com",
	// AUD6-2 breadth: services whose APIs are enabled on the parity project but
	// that the differential did not previously cover.
	"resourcemanager": "https://cloudresourcemanager.googleapis.com",
	"eventarc":        "https://eventarc.googleapis.com",
	"tasks":           "https://cloudtasks.googleapis.com",
	"run":             "https://run.googleapis.com",
	"functions":       "https://cloudfunctions.googleapis.com",
	"metastore":       "https://metastore.googleapis.com",
	"managedkafka":    "https://managedkafka.googleapis.com",
	// crypto-medallion demo parity (D1): Cloud Scheduler v1, added on top of the
	// AUD6-2 breadth because the demo's `cm-rollup` job is a REST call against
	// cloudscheduler.googleapis.com.
	"scheduler": "https://cloudscheduler.googleapis.com",
}

// runSuffix returns a per-run unique, resource-name-safe suffix. Record and
// replay each generate their own; the normalizer folds it back to <suffix> so
// the committed goldens stay stable across runs.
func runSuffix() string {
	return fmt.Sprintf("%06x%06x", os.Getpid()&0xffffff, time.Now().UnixNano()&0xffffff)
}

// ResourceNames are the concrete, run-suffixed identifiers created by the
// scenario set. They are normalized to placeholders so goldens contain no
// project- or run-specific strings.
type ResourceNames struct {
	Bucket string
	Topic  string
	Sub    string
	Secret string
	DS     string
	Table  string
	// Cloud DNS: a managed zone, its DNS name, and one record set in it.
	DNSZone  string
	DNSName  string
	DNSRRSet string
	// Cloud Workflows: one workflow definition.
	Workflow string
	// IAM: the accountId of one service account (the email is derived from it
	// and the project).
	ServiceAccount string
	// Firestore: one collection and one document in the (default) database.
	FSCollection string
	FSDoc        string
	// Metadata-only control planes: one probed (always-absent) resource each,
	// used by the read-only smoke to capture the 404/error envelope without
	// creating a real VM / Cloud SQL / Memorystore instance.
	ComputeInstance string
	SQLInstance     string
	RedisInstance   string
	// AUD6: read-only control-plane probes for GKE and Dataproc (never created),
	// a run-suffixed custom logging log name, and a nonexistent custom metric
	// type. All fold to placeholders so goldens stay stable.
	ContainerCluster string
	DataprocCluster  string
	LogName          string
	MetricType       string
	// AUD6-1 gRPC differential: the Cloud Datastore kind and the entity name
	// written into it, both run-suffixed and folded so a golden is stable.
	DSKind   string
	DSEntity string
	// AUD6-6 cursor paging: a separate Datastore kind holding three
	// equal-length entity names, so real-GCP key order and the emulator's
	// canonical-key order agree and the page boundary is deterministic.
	DSPageKind   string
	DSPagePrefix string

	// crypto-medallion demo parity (D1). One run-suffixed resource per mutation
	// the demo performs that the curated set did not already cover: a CMEK GCS
	// bucket, a GCS bucket + NDJSON object used as a BigQuery load source, a
	// Cloud Run v2 service, an Eventarc trigger, a Cloud Scheduler job, a
	// BigQuery load job, a KMS crypto key, a Pub/Sub topic for the Monitoring
	// notification channel, the alert policy display name, and the custom
	// metric type the demo writes.
	CMEKBucket    string
	LoadBucket    string
	EventBucket   string
	RunService    string
	Trigger       string
	SchedJob      string
	BQJob         string
	CryptoKey     string
	NotifyTopic   string
	NotifyDisplay string
	AlertDisplay  string
	DemoMetric    string

	// SDK-tour differential (demo/sdk-tour): one run-suffixed resource per
	// tour scenario. The tour's own fixed objects (its page-object prefix and
	// its small download object) are constants and need no folding.
	TourBucket    string
	TourBQDataset string
	TourBQTable   string
	TourBQJob     string
	TourTopic     string
	TourSub       string
	TourLog       string
	TourSecret    string
	TourFSCounter string
	TourFSPage    string
	TourDataproc  string

	// Error/retry tour differential (demo/sdk-tour errors mode): dedicated
	// resources so the error set never collides with the happy-path tour set
	// when both replay against one emulator run.
	ErrorBucket    string
	ErrorBQDataset string
	ErrorBQTable   string
	ErrorTopic     string

	// Pub/Sub messaging-semantics oracle (PSM1): dedicated topics/subscriptions
	// so the ack/modack, dead-letter and ordering flows never collide with the
	// control-plane CRUD resources above. The gRPC half reuses the REST names
	// where the transport is the same contract; the snapshot and streaming
	// resources are gRPC-only.
	MsgTopic     string
	MsgSub       string
	OrderTopic   string
	OrderSub     string
	AckSub       string
	ModSub       string
	DLQTopic     string
	DLQSub       string
	DLQMainTopic string
	DLQMainSub   string
	SeekSub      string
	SeekSnap     string
	StreamTopic  string
	StreamSub    string
	EODTopic     string
	EODSub       string
	// Ordered redelivery contract (PSM3): two dedicated ordered subscriptions,
	// one for the held-ack + nack fan-out, one for the nack-of-acked case.
	RedelTopic  string
	RedelSub    string
	Redel2Topic string
	Redel2Sub   string
}

// Names derives the run's resource identifiers from suffix.
func Names(suffix string) ResourceNames {
	dnsName := "conf-dns-" + suffix + ".example.com."
	return ResourceNames{
		Bucket:   "conf-bucket-" + suffix,
		Topic:    "conf-topic-" + suffix,
		Sub:      "conf-sub-" + suffix,
		Secret:   "conf-secret-" + suffix,
		DS:       "conf_ds_" + suffix,
		Table:    "conf_tbl_" + suffix,
		DNSZone:  "conf-zone-" + suffix,
		DNSName:  dnsName,
		DNSRRSet: "www." + dnsName,
		Workflow: "conf-workflow-" + suffix,

		ServiceAccount: "conf-sa-" + suffix,

		FSCollection: "conf_docs_" + suffix,
		FSDoc:        "doc_" + suffix,

		ComputeInstance: "conf-vm-" + suffix,
		SQLInstance:     "conf-sql-" + suffix,
		RedisInstance:   "conf-redis-" + suffix,

		ContainerCluster: "conf-gke-" + suffix,
		DataprocCluster:  "conf-dp-" + suffix,
		LogName:          "conf-log-" + suffix,
		MetricType:       "custom.googleapis.com/jaiscloud_nonexistent_" + suffix,
		DSKind:           "conf_ds_kind_" + suffix,
		DSEntity:         "conf_ds_entity_" + suffix,
		DSPageKind:       "conf_ds_pg_kind_" + suffix,
		DSPagePrefix:     "conf_ds_pg_" + suffix + "_",

		// crypto-medallion demo parity (D1).
		CMEKBucket:    "conf-bucket-cmek-" + suffix,
		LoadBucket:    "conf-load-" + suffix,
		EventBucket:   "conf-evt-" + suffix,
		RunService:    "conf-run-" + suffix,
		Trigger:       "conf-trigger-" + suffix,
		SchedJob:      "conf-sched-" + suffix,
		BQJob:         "conf_job_" + suffix,
		CryptoKey:     "conf-key-" + suffix,
		NotifyTopic:   "conf-notify-" + suffix,
		NotifyDisplay: "jaiscloud differential notify",
		AlertDisplay:  "conf-alert-" + suffix,
		DemoMetric:    "custom.googleapis.com/jaiscloud_demo_" + suffix,

		// SDK-tour differential.
		TourBucket:    "tour-bucket-" + suffix,
		TourBQDataset: "tour_ds_" + suffix,
		TourBQTable:   "tour_tbl_" + suffix,
		TourBQJob:     "tour_job_" + suffix,
		TourTopic:     "tour-topic-" + suffix,
		TourSub:       "tour-sub-" + suffix,
		TourLog:       "tour-log-" + suffix,
		TourSecret:    "tour-secret-" + suffix,
		TourFSCounter: "tour_fs_tx_" + suffix,
		TourFSPage:    "tour_fs_page_" + suffix,
		TourDataproc:  "tour-cluster-" + suffix,

		// Error/retry tour differential.
		ErrorBucket:    "errors-bucket-" + suffix,
		ErrorBQDataset: "errors_ds_" + suffix,
		ErrorBQTable:   "errors_tbl_" + suffix,
		ErrorTopic:     "errors-topic-" + suffix,

		// Pub/Sub messaging-semantics oracle (PSM1).
		MsgTopic:     "conf-pubsub-msg-" + suffix,
		MsgSub:       "conf-pubsub-msg-sub-" + suffix,
		OrderTopic:   "conf-pubsub-order-" + suffix,
		OrderSub:     "conf-pubsub-order-sub-" + suffix,
		AckSub:       "conf-pubsub-ack-sub-" + suffix,
		ModSub:       "conf-pubsub-mod-sub-" + suffix,
		DLQTopic:     "conf-pubsub-dlq-" + suffix,
		DLQSub:       "conf-pubsub-dlq-sub-" + suffix,
		DLQMainTopic: "conf-pubsub-dlqmain-" + suffix,
		DLQMainSub:   "conf-pubsub-dlq-main-" + suffix,
		SeekSub:      "conf-pubsub-seek-sub-" + suffix,
		SeekSnap:     "conf-pubsub-seek-snap-" + suffix,
		StreamTopic:  "conf-pubsub-stream-" + suffix,
		StreamSub:    "conf-pubsub-stream-sub-" + suffix,
		EODTopic:     "conf-pubsub-eod-" + suffix,
		EODSub:       "conf-pubsub-eod-sub-" + suffix,
		RedelTopic:   "conf-pubsub-redel-" + suffix,
		RedelSub:     "conf-pubsub-redel-sub-" + suffix,
		Redel2Topic:  "conf-pubsub-redel2-" + suffix,
		Redel2Sub:    "conf-pubsub-redel2-sub-" + suffix,
	}
}

// Scenarios returns the curated request list for the given project and run
// suffix: the original twelve REST services plus the AUD6 breadth services
// (container, dataproc, monitoring, logging, datastore, workflowexecutions,
// serviceusage) and the AUD6-2 services (resourcemanager, eventarc, tasks, run,
// functions, metastore, managedkafka). It also carries negative/error-envelope
// cases (404 / 409 / 400 / 401 authz) alongside the happy paths.
//
// The list is ordered so resources exist before they are read and are deleted
// at the end; error responses are included deliberately. Any scenario without a
// committed golden stays "pending recording" (skipped by TestReplay) until a
// real-GCP capture folds it into a golden.
func Scenarios(project, suffix string) []Scenario {
	n := Names(suffix)

	topicName := "projects/" + project + "/topics/" + n.Topic
	subName := "projects/" + project + "/subscriptions/" + n.Sub

	var sc []Scenario

	// ─── Cloud Storage (GCS JSON API) ─────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "bucket_create", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q}`, n.Bucket)},
		Scenario{Op: "bucket_get", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket},
		Scenario{Op: "buckets_list", Service: "storage", Method: "GET", Path: "/storage/v1/b?project=" + project},
		Scenario{Op: "object_upload", Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + n.Bucket + "/o?uploadType=media&name=hello.txt",
			Body:        "hello jaiscloud",
			ContentType: "text/plain"},
		Scenario{Op: "object_get", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o/hello.txt"},
		Scenario{Op: "objects_list", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o"},
		Scenario{Op: "object_get_missing", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket + "/o/missing-" + suffix},
		// Negative envelopes: creating the bucket again is ALREADY_EXISTS (409),
		// and an uppercase/underscore name is invalid (400). Both run while the
		// bucket exists (before delete).
		Scenario{Op: "bucket_create_dup", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q}`, n.Bucket)},
		Scenario{Op: "bucket_create_invalid", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: `{"name":"INVALID_BUCKET_NAME"}`},
		Scenario{Op: "object_delete", Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + n.Bucket + "/o/hello.txt"},
		Scenario{Op: "bucket_delete", Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + n.Bucket},
		Scenario{Op: "bucket_get_deleted", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.Bucket},
	)

	// ─── Pub/Sub ──────────────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "topic_create", Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + project + "/topics/" + n.Topic,
			Body: fmt.Sprintf(`{"name":%q}`, topicName)},
		Scenario{Op: "topic_get", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics/" + n.Topic},
		Scenario{Op: "topics_list", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics"},
		Scenario{Op: "topic_get_missing", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/topics/missing-" + suffix},
		// Negative envelopes: a duplicate topic is ALREADY_EXISTS (409); an empty
		// publish payload is INVALID_ARGUMENT (400).
		Scenario{Op: "topic_create_dup", Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + project + "/topics/" + n.Topic,
			Body: fmt.Sprintf(`{"name":%q}`, topicName)},
		Scenario{Op: "sub_create", Service: "pubsub", Method: "PUT", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub,
			Body: fmt.Sprintf(`{"name":%q,"topic":%q,"ackDeadlineSeconds":10}`, subName, topicName)},
		Scenario{Op: "sub_get", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub},
		Scenario{Op: "subs_list", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions"},
		Scenario{Op: "topic_publish", Service: "pubsub", Method: "POST", Path: "/v1/projects/" + project + "/topics/" + n.Topic + ":publish",
			Body: `{"messages":[{"data":"aGVsbG8="}]}`},
		Scenario{Op: "topic_publish_empty", Service: "pubsub", Method: "POST", Path: "/v1/projects/" + project + "/topics/" + n.Topic + ":publish",
			Body: `{"messages":[]}`},
		Scenario{Op: "sub_pull", Service: "pubsub", Method: "POST", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub + ":pull",
			Body: `{"maxMessages":10}`},
		Scenario{Op: "sub_delete", Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + project + "/subscriptions/" + n.Sub},
		Scenario{Op: "topic_delete", Service: "pubsub", Method: "DELETE", Path: "/v1/projects/" + project + "/topics/" + n.Topic},
		Scenario{Op: "sub_get_missing", Service: "pubsub", Method: "GET", Path: "/v1/projects/" + project + "/subscriptions/missing-" + suffix},
	)

	// ─── Secret Manager ───────────────────────────────────────────────────────
	sc = append(sc,
		Scenario{Op: "secret_create", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets?secretId=" + n.Secret,
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Op: "secret_get", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets/" + n.Secret},
		Scenario{Op: "secrets_list", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets"},
		// Negative envelopes: a duplicate secret is ALREADY_EXISTS (409); a
		// create with no secretId is INVALID_ARGUMENT (400).
		Scenario{Op: "secret_create_dup", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets?secretId=" + n.Secret,
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Op: "secret_create_missing_id", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets",
			Body: `{"replication":{"automatic":{}}}`},
		Scenario{Op: "secret_add_version", Service: "secretmanager", Method: "POST",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + ":addVersion",
			Body: `{"payload":{"data":"c2VjcmV0"}}`},
		Scenario{Op: "secret_access_version", Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + "/versions/1:access"},
		Scenario{Op: "secret_versions_list", Service: "secretmanager", Method: "GET",
			Path: "/v1/projects/" + project + "/secrets/" + n.Secret + "/versions"},
		Scenario{Op: "secret_get_missing", Service: "secretmanager", Method: "GET", Path: "/v1/projects/" + project + "/secrets/missing-" + suffix},
		Scenario{Op: "secret_delete", Service: "secretmanager", Method: "DELETE", Path: "/v1/projects/" + project + "/secrets/" + n.Secret},
	)

	// ─── Cloud KMS (read + encrypt/decrypt against fixed, reusable resources) ──
	kmsBase := "/v1/projects/" + project + "/locations/global/keyRings"
	ringPath := kmsBase + "/" + FixedKMSKeyRing
	keyPath := ringPath + "/cryptoKeys/" + FixedKMSCryptoKey
	sc = append(sc,
		Scenario{Op: "keyring_get", Service: "kms", Method: "GET", Path: ringPath},
		Scenario{Op: "keyrings_list", Service: "kms", Method: "GET", Path: kmsBase},
		// Negative envelope: creating the fixed keyring again is ALREADY_EXISTS
		// (409). EnsureKMS relies on this same idempotency.
		Scenario{Op: "keyring_create_dup", Service: "kms", Method: "POST",
			Path: kmsBase + "?keyRingId=" + FixedKMSKeyRing, Body: `{}`},
		Scenario{Op: "cryptokey_get", Service: "kms", Method: "GET", Path: keyPath},
		Scenario{Op: "cryptokeys_list", Service: "kms", Method: "GET", Path: ringPath + "/cryptoKeys"},
		Scenario{Op: "cryptokey_encrypt", Service: "kms", Method: "POST", Path: keyPath + ":encrypt",
			Body: `{"plaintext":"aGVsbG8="}`, Save: map[string]string{"ciphertext": "ciphertext"}},
		Scenario{Op: "cryptokey_decrypt", Service: "kms", Method: "POST", Path: keyPath + ":decrypt",
			Body: `{"ciphertext":"${ciphertext}"}`},
		Scenario{Op: "keyring_iam_get", Service: "kms", Method: "GET", Path: ringPath + ":getIamPolicy"},
		Scenario{Op: "cryptokey_get_missing", Service: "kms", Method: "GET", Path: ringPath + "/cryptoKeys/missing-" + suffix},
	)

	// ─── BigQuery ─────────────────────────────────────────────────────────────
	bqBase := "/bigquery/v2/projects/" + project
	sc = append(sc,
		Scenario{Op: "dataset_create", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, project, n.DS)},
		Scenario{Op: "dataset_get", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS},
		Scenario{Op: "datasets_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets"},
		// Negative envelope: a duplicate dataset is ALREADY_EXISTS (409).
		Scenario{Op: "dataset_create_dup", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q}}`, project, n.DS)},
		Scenario{Op: "table_create", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + n.DS + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"}]}}`, project, n.DS, n.Table)},
		Scenario{Op: "table_get", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table},
		Scenario{Op: "tables_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables"},
		Scenario{Op: "tabledata_insert_all", Service: "bigquery", Method: "POST", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table + "/insertAll",
			Body: `{"rows":[{"insertId":"1","json":{"id":"1"}}]}`},
		Scenario{Op: "tabledata_list", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table + "/data"},
		// crypto-medallion demo parity (D1): a jobs.insert load from a gs://
		// NDJSON object (the Spark rollup load path) plus jobs.list. The source
		// bucket/object are created here because the curated GCS bucket above is
		// already deleted by this point; cleanup removes them best-effort.
		Scenario{Op: "bq_load_bucket_create", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q}`, n.LoadBucket)},
		Scenario{Op: "bq_load_object_upload", Service: "storage", Method: "POST",
			Path:        "/upload/storage/v1/b/" + n.LoadBucket + "/o?uploadType=media&name=data.ndjson",
			Body:        "{\"id\":2}\n",
			ContentType: "application/x-ndjson"},
		Scenario{Op: "job_insert_load", Service: "bigquery", Method: "POST", Path: bqBase + "/jobs",
			Body: fmt.Sprintf(`{"jobReference":{"projectId":%q,"jobId":%q,"location":"US"},"configuration":{"load":{"sourceUris":["gs://%s/data.ndjson"],"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}`,
				project, n.BQJob, n.LoadBucket, project, n.DS, n.Table)},
		Scenario{Op: "jobs_list", Service: "bigquery", Method: "GET", Path: bqBase + "/jobs?projection=full&maxResults=10",
			// The job index is eventually consistent on real GCP; poll until this
			// run's job id appears so the golden is stable.
			Wait: &WaitSpec{Field: "jobs", Contains: n.BQJob, Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "query_bad_sql", Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT FROM"}`},
		Scenario{Op: "query", Service: "bigquery", Method: "POST", Path: bqBase + "/queries", Body: `{"query":"SELECT 1"}`},
		Scenario{Op: "dataset_get_missing", Service: "bigquery", Method: "GET", Path: bqBase + "/datasets/missing_" + suffix},
		Scenario{Op: "table_delete", Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + n.DS + "/tables/" + n.Table},
		Scenario{Op: "dataset_delete", Service: "bigquery", Method: "DELETE", Path: bqBase + "/datasets/" + n.DS + "?deleteContents=true"},
	)

	// ─── Cloud DNS (v1 REST, /dns/v1/projects/{project}/managedZones) ─────────
	// A run-suffixed managed zone, its record set (added through a change, the
	// Cloud DNS mutation path), reads, a 404, and no delete scenario: deleting
	// a managed zone answers 204 on real GCP while the emulator answers 200,
	// so the mutation is left to Cleanup() (whose status is not recorded).
	dnsBase := "/dns/v1/projects/" + project + "/managedZones"
	dnsZone := n.DNSZone
	sc = append(sc,
		Scenario{Op: "zone_create", Service: "dns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q,"dnsName":%q,"description":"jaiscloud differential"}`, dnsZone, n.DNSName)},
		// Negative envelopes: a duplicate managed zone is ALREADY_EXISTS (409);
		// a create with no dnsName is INVALID_ARGUMENT (400).
		Scenario{Op: "zone_create_dup", Service: "dns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q,"dnsName":%q,"description":"jaiscloud differential"}`, dnsZone, n.DNSName)},
		Scenario{Op: "zone_create_invalid", Service: "dns", Method: "POST", Path: dnsBase,
			Body: fmt.Sprintf(`{"name":%q}`, "conf-invalid-"+suffix)},
		Scenario{Op: "zone_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone},
		Scenario{Op: "zones_list", Service: "dns", Method: "GET", Path: dnsBase},
		Scenario{Op: "change_create", Service: "dns", Method: "POST", Path: dnsBase + "/" + dnsZone + "/changes",
			Body: fmt.Sprintf(`{"additions":[{"name":%q,"type":"A","ttl":300,"rrdatas":["192.0.2.1"]}]}`, n.DNSRRSet),
			Save: map[string]string{"changeId": "id"}},
		Scenario{Op: "change_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/changes/${changeId}"},
		Scenario{Op: "rrsets_list", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/rrsets"},
		Scenario{Op: "rrset_get", Service: "dns", Method: "GET", Path: dnsBase + "/" + dnsZone + "/rrsets/" + n.DNSRRSet + "/A"},
		Scenario{Op: "zone_get_missing", Service: "dns", Method: "GET", Path: dnsBase + "/missing-" + suffix},
	)

	// ─── Cloud Workflows (/v1/projects/{project}/locations/{location}/workflows) ─
	// Create/update/delete return a done google.longrunning.Operation; the
	// workflow's own get/list reads are plain resources. Delete is the last
	// scenario so the workflow exists for every read above it.
	wfBase := "/v1/projects/" + project + "/locations/us-central1/workflows"
	sc = append(sc,
		Scenario{Op: "workflow_create", Service: "workflows", Method: "POST",
			Path: wfBase + "?workflowId=" + n.Workflow,
			Body: `{"description":"jaiscloud differential","sourceContents":"main:\n  steps:\n    - r:\n        return: 1\n"}`,
			Save: map[string]string{"wfOp": "name"}},
		// create/update/delete are LROs on real GCP: the workflow is not
		// readable until the operation completes. Poll the saved operation name
		// so the reads below observe the post-create state on both sides.
		Scenario{Op: "workflow_wait", Service: "workflows", Method: "GET",
			Path: "/v1/${wfOp}", Wait: &WaitSpec{Field: "done", Interval: time.Second, Timeout: 120 * time.Second}},
		Scenario{Op: "workflow_get", Service: "workflows", Method: "GET", Path: wfBase + "/" + n.Workflow},
		Scenario{Op: "workflows_list", Service: "workflows", Method: "GET", Path: wfBase},
		// Cloud Workflow Executions (AUD6): the executions list is empty for a
		// freshly deployed workflow; the missing-execution probe captures the
		// 404 envelope. Scoped to the workflow created above, before it is
		// deleted.
		Scenario{Op: "wfexec_executions_list", Service: "workflowexecutions", Method: "GET",
			Path: wfBase + "/" + n.Workflow + "/executions"},
		Scenario{Op: "wfexec_execution_get_missing", Service: "workflowexecutions", Method: "GET",
			Path: wfBase + "/" + n.Workflow + "/executions/missing-" + suffix},
		Scenario{Op: "workflow_get_missing", Service: "workflows", Method: "GET", Path: wfBase + "/missing-" + suffix},
		Scenario{Op: "workflow_delete", Service: "workflows", Method: "DELETE", Path: wfBase + "/" + n.Workflow},
	)

	// ─── IAM service accounts (/v1/projects/{project}/serviceAccounts) ───────
	// Create/get/list, the key-ring-style IAM policy read, a 404, then delete.
	// Service-account keys are deliberately excluded: they return private-key
	// material (and signBlob executes a crypto operation), which must not be
	// committed to a golden.
	saBase := "/v1/projects/" + project + "/serviceAccounts"
	saEmail := n.ServiceAccount + "@" + project + ".iam.gserviceaccount.com"
	sc = append(sc,
		Scenario{Op: "sa_create", Service: "iam", Method: "POST",
			Path: saBase + "?accountId=" + n.ServiceAccount,
			Body: `{"serviceAccount":{"displayName":"jaiscloud differential"}}`},
		// Negative envelopes: a duplicate service account is ALREADY_EXISTS
		// (409); an accountId with uppercase/underscores is INVALID_ARGUMENT.
		Scenario{Op: "sa_create_dup", Service: "iam", Method: "POST",
			Path: saBase + "?accountId=" + n.ServiceAccount,
			Body: `{"serviceAccount":{"displayName":"jaiscloud differential"}}`},
		Scenario{Op: "sa_create_invalid", Service: "iam", Method: "POST",
			Path: saBase + "?accountId=Invalid_Account_ID",
			Body: `{"serviceAccount":{"displayName":"jaiscloud differential"}}`},
		Scenario{Op: "sa_get", Service: "iam", Method: "GET", Path: saBase + "/" + saEmail,
			Wait: &WaitSpec{Field: "email", Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "sas_list", Service: "iam", Method: "GET", Path: saBase + "?pageSize=100",
			Wait: &WaitSpec{Field: "accounts", Contains: n.ServiceAccount, Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "sa_iam_get", Service: "iam", Method: "POST", Path: saBase + "/" + saEmail + ":getIamPolicy",
			Wait: &WaitSpec{Field: "etag", Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "sa_get_missing", Service: "iam", Method: "GET",
			Path: saBase + "/missing-" + suffix + "@" + project + ".iam.gserviceaccount.com"},
		Scenario{Op: "sa_delete", Service: "iam", Method: "DELETE", Path: saBase + "/" + saEmail},
	)

	// ─── Firestore documents (REST) ──────────────────────────────────────────
	// The (default) database's document CRUD. The document id is supplied via
	// ?documentId= so the response name is deterministic; delete is last.
	fsBase := "/v1/projects/" + project + "/databases/(default)/documents/" + n.FSCollection
	fsDoc := fsBase + "/" + n.FSDoc
	sc = append(sc,
		Scenario{Op: "fs_doc_create", Service: "firestore", Method: "POST",
			Path: fsBase + "?documentId=" + n.FSDoc,
			Body: `{"fields":{"greeting":{"stringValue":"hello"},"count":{"integerValue":"1"}}}`},
		// Negative envelope: re-creating the document is ALREADY_EXISTS (409).
		Scenario{Op: "fs_doc_create_dup", Service: "firestore", Method: "POST",
			Path: fsBase + "?documentId=" + n.FSDoc,
			Body: `{"fields":{"greeting":{"stringValue":"hello"},"count":{"integerValue":"1"}}}`},
		// crypto-medallion demo parity (D1): the publisher upserts each symbol
		// with an update-less documents.patch (full-document replace), the
		// Firestore read-model write path the demo depends on.
		Scenario{Op: "fs_doc_upsert", Service: "firestore", Method: "PATCH", Path: fsDoc,
			Body: `{"fields":{"greeting":{"stringValue":"hi"},"count":{"integerValue":"2"}}}`},
		Scenario{Op: "fs_doc_get", Service: "firestore", Method: "GET", Path: fsDoc},
		Scenario{Op: "fs_docs_list", Service: "firestore", Method: "GET", Path: fsBase},
		Scenario{Op: "fs_doc_get_missing", Service: "firestore", Method: "GET", Path: fsBase + "/missing-" + suffix},
		Scenario{Op: "fs_doc_delete", Service: "firestore", Method: "DELETE", Path: fsDoc},
	)

	// ─── Metadata-only control planes (G6 / G4) ──────────────────────────────
	// Read-only smoke: the emulator's compute/cloudsql/memorystore surfaces are
	// control-plane/metadata only (G4 = declared no fix), so the differential
	// validates routing, the empty-list shape and the 404 error envelope —
	// never a data plane. No resource is created, so no cleanup is needed.
	// Compute's zone is a stable, always-present geography.
	computeZone := "us-central1-a"
	sc = append(sc,
		Scenario{Op: "compute_instances_list", Service: "compute", Method: "GET",
			Path: "/compute/v1/projects/" + project + "/zones/" + computeZone + "/instances"},
		Scenario{Op: "compute_instance_get_missing", Service: "compute", Method: "GET",
			Path: "/compute/v1/projects/" + project + "/zones/" + computeZone + "/instances/" + n.ComputeInstance},
	)
	sc = append(sc,
		Scenario{Op: "sql_instances_list", Service: "sqladmin", Method: "GET",
			Path: "/sql/v1beta4/projects/" + project + "/instances"},
		Scenario{Op: "sql_instance_get_missing", Service: "sqladmin", Method: "GET",
			Path: "/sql/v1beta4/projects/" + project + "/instances/" + n.SQLInstance},
	)
	sc = append(sc,
		Scenario{Op: "redis_instances_list", Service: "redis", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/instances"},
		Scenario{Op: "redis_instance_get_missing", Service: "redis", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/instances/" + n.RedisInstance},
	)

	// ─── AUD6 breadth: control planes added on top of the original twelve ────
	// Each follows the read-only smoke contract of the metadata-only services
	// above: validate routing, the empty-list shape and the 404 error envelope,
	// never a data plane. The APIs are enabled on the parity project; nothing is
	// created, so no cleanup is needed.

	// Google Kubernetes Engine (container.googleapis.com REST metadata surface).
	sc = append(sc,
		Scenario{Op: "container_clusters_list", Service: "container", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/clusters"},
		Scenario{Op: "container_cluster_get_missing", Service: "container", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/clusters/" + n.ContainerCluster},
	)

	// Cloud Dataproc (dataproc.googleapis.com); region-scoped, never created.
	sc = append(sc,
		Scenario{Op: "dataproc_clusters_list", Service: "dataproc", Method: "GET",
			Path: "/v1/projects/" + project + "/regions/us-central1/clusters"},
		Scenario{Op: "dataproc_cluster_get_missing", Service: "dataproc", Method: "GET",
			Path: "/v1/projects/" + project + "/regions/us-central1/clusters/" + n.DataprocCluster},
	)

	// Cloud Monitoring (monitoring.googleapis.com): a descriptor list filtered
	// to a nonexistent custom metric type, so the golden is not polluted by the
	// real project's built-in descriptors, plus a 404 descriptor get.
	sc = append(sc,
		Scenario{Op: "monitoring_metricdescriptors_list", Service: "monitoring", Method: "GET",
			Path: "/v3/projects/" + project + "/metricDescriptors?filter=metric.type%3D%22" + n.MetricType + "%22"},
		Scenario{Op: "monitoring_metricdescriptor_get_missing", Service: "monitoring", Method: "GET",
			Path: "/v3/projects/" + project + "/metricDescriptors/" + n.MetricType},
	)

	// Cloud Logging (logging.googleapis.com) data plane: write one entry to a
	// run-suffixed custom log, then list the project's log names (scoped to that
	// log). Log entries expire on their own, so no cleanup is needed.
	sc = append(sc,
		Scenario{Op: "log_entry_write", Service: "logging", Method: "POST", Path: "/v2/entries:write",
			Body: fmt.Sprintf(`{"entries":[{"logName":%q,"resource":{"type":"global"},"severity":"INFO","textPayload":"hello jaiscloud"}]}`,
				"projects/"+project+"/logs/"+n.LogName)},
		// crypto-medallion demo parity (D1): the publisher's structured log
		// entry read path (entries.list scoped to the log just written). Real
		// GCP's entry index is eventually consistent, so poll until the entry
		// appears.
		Scenario{Op: "log_entries_list", Service: "logging", Method: "POST", Path: "/v2/entries:list",
			Body: fmt.Sprintf(`{"resourceNames":["projects/%s"],"filter":"logName=\"projects/%s/logs/%s\"","orderBy":"timestamp desc","pageSize":10}`,
				project, project, n.LogName),
			Wait: &WaitSpec{Field: "entries", Contains: n.LogName, Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "log_names_list", Service: "logging", Method: "GET",
			Path: "/v2/projects/" + project + "/logs?resourceNames=projects%2F" + project,
			// The project log-name index is eventually consistent on real GCP
			// (a just-written custom log is not listed immediately); poll until
			// this run's log appears so the golden is stable.
			Wait: &WaitSpec{Field: "logNames", Contains: n.LogName, Interval: time.Second, Timeout: 60 * time.Second}},
	)

	// Cloud Datastore (datastore.googleapis.com) read-only: a lookup of an
	// always-absent key captures routing plus the entity-key shape. The REST
	// Entity/value-union projection a full data-plane flow needs is deferred
	// (AUD3-2).
	sc = append(sc,
		Scenario{Op: "ds_lookup_missing", Service: "datastore", Method: "POST", Path: "/v1/projects/" + project + ":lookup",
			Body: fmt.Sprintf(`{"keys":[{"path":[{"kind":"conf_kind","name":%q}]}]}`, "missing_"+suffix)},
	)

	// ─── AUD6-2 breadth: services the parity project now enables ─────────────
	// Each follows the read-only smoke contract of the metadata-only services
	// above: validate routing, the empty-list shape and the 404 error envelope,
	// never a data plane. Nothing is created, so no cleanup is needed. Real GCP
	// omits an empty collection member (it answers {}) while the emulator
	// returns the empty array; that additive difference is accepted by the
	// extra_field triage rule.

	// Cloud Resource Manager (cloudresourcemanager.googleapis.com): the project
	// resource. Its display name and creation time are accepted differences
	// (see triage.go); projectId/projectNumber/lifecycleState match.
	sc = append(sc,
		Scenario{Op: "project_get", Service: "resourcemanager", Method: "GET",
			Path: "/v1/projects/" + project},
	)

	// Eventarc (eventarc.googleapis.com): triggers under a location.
	sc = append(sc,
		Scenario{Op: "eventarc_triggers_list", Service: "eventarc", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/triggers"},
		Scenario{Op: "eventarc_trigger_get_missing", Service: "eventarc", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/triggers/missing-" + suffix},
	)

	// Cloud Tasks v2 (cloudtasks.googleapis.com): queues under a location.
	sc = append(sc,
		Scenario{Op: "tasks_queues_list", Service: "tasks", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/queues"},
		Scenario{Op: "tasks_queue_get_missing", Service: "tasks", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/queues/missing-" + suffix},
	)

	// Cloud Run v2 (run.googleapis.com): services under a location. The
	// emulator discriminates run from functions on the "services" resource
	// family.
	sc = append(sc,
		Scenario{Op: "run_services_list", Service: "run", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/services"},
		Scenario{Op: "run_service_get_missing", Service: "run", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/services/missing-" + suffix},
	)

	// Cloud Functions v2 (cloudfunctions.googleapis.com): functions under a
	// location.
	sc = append(sc,
		Scenario{Op: "functions_list", Service: "functions", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/functions"},
		Scenario{Op: "function_get_missing", Service: "functions", Method: "GET",
			Path: "/v2/projects/" + project + "/locations/us-central1/functions/missing-" + suffix},
	)

	// Dataproc Metastore (metastore.googleapis.com): services under a location.
	sc = append(sc,
		Scenario{Op: "metastore_services_list", Service: "metastore", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/services"},
		Scenario{Op: "metastore_service_get_missing", Service: "metastore", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/services/missing-" + suffix},
	)

	// Managed Service for Apache Kafka (managedkafka.googleapis.com): clusters
	// under a location. This canonical path is shared with GKE, which the
	// emulator discriminates by host; on the single replay origin it resolves
	// to Managed Kafka (the GKE scenario above records the same path against
	// container.googleapis.com, whose real response is {}).
	sc = append(sc,
		Scenario{Op: "managedkafka_clusters_list", Service: "managedkafka", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/clusters"},
		Scenario{Op: "managedkafka_cluster_get_missing", Service: "managedkafka", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/clusters/missing-" + suffix},
	)

	// ─── crypto-medallion demo parity (D1) ───────────────────────────────────
	// The exact REST mutations the crypto-medallion demo's Terraform + democtl
	// perform that the curated set above did not already cover: Service Usage
	// enable (google_project_service), a KMS crypto key, a CMEK GCS bucket, a
	// Cloud Run v2 service (create/patch/get), an Eventarc trigger, a Cloud
	// Scheduler job, the Monitoring notification channel / alert policy /
	// timeSeries write, and a BigQuery load job (covered in the BigQuery block).
	// Everything created here is run-suffixed and torn down best-effort; the KMS
	// key is the one unavoidable residue (GCP cannot delete crypto keys — see
	// Cleanup).

	// Service Usage: services.enable is what Terraform's google_project_service
	// calls for every API the demo enables.
	sc = append(sc,
		Scenario{Op: "service_enable", Service: "serviceusage", Method: "POST",
			Path: "/v1/projects/" + project + "/services/run.googleapis.com:enable"},
	)

	// Cloud KMS: cryptoKeys.create (Terraform google_kms_crypto_key). The key is
	// created in the fixed, reusable differential keyring so no second keyring
	// is minted; the crypto key itself cannot be deleted by GCP.
	kmsRing := "/v1/projects/" + project + "/locations/global/keyRings/" + FixedKMSKeyRing
	demoKey := kmsRing + "/cryptoKeys/" + n.CryptoKey
	sc = append(sc,
		Scenario{Op: "cryptokey_create", Service: "kms", Method: "POST",
			Path: kmsRing + "/cryptoKeys?cryptoKeyId=" + n.CryptoKey,
			Body: `{"purpose":"ENCRYPT_DECRYPT"}`},
		Scenario{Op: "cryptokey_create_get", Service: "kms", Method: "GET", Path: demoKey},
	)

	// GCS CMEK: the demo's gold/curated bucket carries
	// encryption.default_kms_key_name. A `us` multi-region fixed key is used
	// because a US bucket rejects a `global` keyring; the GCS service account is
	// granted encrypter/decrypter on it by EnsureKMS.
	cmekKey := "projects/" + project + "/locations/us/keyRings/" + FixedKMSKeyRingUS + "/cryptoKeys/" + FixedKMSCryptoKey
	sc = append(sc,
		Scenario{Op: "bucket_create_cmek", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q,"location":"US","encryption":{"defaultKmsKeyName":%q}}`, n.CMEKBucket, cmekKey)},
		Scenario{Op: "bucket_get_cmek", Service: "storage", Method: "GET", Path: "/storage/v1/b/" + n.CMEKBucket},
		Scenario{Op: "bucket_delete_cmek", Service: "storage", Method: "DELETE", Path: "/storage/v1/b/" + n.CMEKBucket},
	)

	// Cloud Scheduler: jobs.create with an httpTarget + retryConfig + oidcToken
	// (the demo's cm-rollup job). The OIDC service account is a project-shaped,
	// run-constant email that the normalizer folds so no golden carries an email.
	schedBase := "/v1/projects/" + project + "/locations/us-central1/jobs"
	// The job resource name in the body is project-relative (no /v1 prefix),
	// while the get path carries the /v1 prefix.
	schedName := "projects/" + project + "/locations/us-central1/jobs/" + n.SchedJob
	schedBody := fmt.Sprintf(`{"name":%q,"description":"jaiscloud differential","schedule":"*/2 * * * *","timeZone":"UTC","httpTarget":{"uri":"https://example.com/rollup","httpMethod":"POST","body":"eyJraW5kIjoicm9sbHVwIn0=","oidcToken":{"serviceAccountEmail":%q,"audience":"https://example.com/rollup"}},"retryConfig":{"retryCount":1,"minBackoffDuration":"15s","maxBackoffDuration":"3600s","maxDoublings":5}}`,
		// The project's App Engine default service account exists in both the
		// parity project and the emulator's synthesized project, and folds
		// cleanly via the <project> substitution (no golden carries an email).
		schedName, project+"@appspot.gserviceaccount.com")
	sc = append(sc,
		Scenario{Op: "scheduler_job_create", Service: "scheduler", Method: "POST", Path: schedBase, Body: schedBody},
		Scenario{Op: "scheduler_job_get", Service: "scheduler", Method: "GET", Path: schedBase + "/" + n.SchedJob},
	)

	// Cloud Monitoring: the notification channel, the custom time-series write
	// (the publisher's trades_per_min metric), and the alert policy that watches
	// it. A dedicated Pub/Sub topic backs the pubsub channel.
	monBase := "/v3/projects/" + project
	notifyTopic := "projects/" + project + "/topics/" + n.NotifyTopic
	alertFilter := fmt.Sprintf(`metric.type = %q AND resource.type = %q`, n.DemoMetric, "global")
	sc = append(sc,
		Scenario{Op: "mon_notify_topic_create", Service: "pubsub", Method: "PUT",
			Path: "/v1/projects/" + project + "/topics/" + n.NotifyTopic,
			Body: fmt.Sprintf(`{"name":%q}`, notifyTopic)},
		Scenario{Op: "mon_notification_channel_create", Service: "monitoring", Method: "POST", Path: monBase + "/notificationChannels",
			Body: fmt.Sprintf(`{"type":"pubsub","displayName":%q,"enabled":true,"labels":{"topic":%q}}`, n.NotifyDisplay, notifyTopic),
			Save: map[string]string{"notifyChannel": "name"}},
		Scenario{Op: "mon_notification_channel_get", Service: "monitoring", Method: "GET", Path: "/v3/${notifyChannel}"},
		// The endTime is folded by the normalizer; a fresh timestamp keeps real
		// GCP from rejecting an out-of-retention point.
		Scenario{Op: "mon_time_series_create", Service: "monitoring", Method: "POST", Path: monBase + "/timeSeries",
			Body: fmt.Sprintf(`{"timeSeries":[{"metric":{"type":%q},"resource":{"type":"global","labels":{"project_id":%q}},"points":[{"interval":{"endTime":%q},"value":{"doubleValue":10}}]}]}`,
				n.DemoMetric, project, time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15:04:05Z07:00"))},
		Scenario{Op: "mon_alert_policy_create", Service: "monitoring", Method: "POST", Path: monBase + "/alertPolicies",
			Body: fmt.Sprintf(`{"displayName":%q,"combiner":"OR","enabled":true,"notificationChannels":["${notifyChannel}"],"conditions":[{"displayName":"trades per minute high","conditionThreshold":{"filter":%q,"comparison":"COMPARISON_GT","thresholdValue":5,"duration":"0s"}}]}`,
				n.AlertDisplay, alertFilter),
			Save: map[string]string{"alertPolicy": "name"}},
		Scenario{Op: "mon_alert_policy_get", Service: "monitoring", Method: "GET", Path: "/v3/${alertPolicy}"},
	)

	// Cloud Run v2: services.create + patch + get (the demo's publisher and
	// leaderboard). A tiny public scale-to-zero image keeps the real-GCP deploy
	// cheap; create/update are LROs, so each is awaited before the read.
	runBase := "/v2/projects/" + project + "/locations/us-central1/services"
	runSvc := runBase + "/" + n.RunService
	runImage := "us-docker.pkg.dev/cloudrun/container/hello"
	sc = append(sc,
		Scenario{Op: "run_service_create", Service: "run", Method: "POST", Path: runBase + "?serviceId=" + n.RunService,
			Body: fmt.Sprintf(`{"template":{"containers":[{"image":%q,"ports":[{"containerPort":8080}]}]}}`, runImage)},
		// Poll for readiness (not just existence): Eventarc's later create
		// requires the destination service to be Ready.
		Scenario{Op: "run_service_get", Service: "run", Method: "GET", Path: runSvc,
			Wait: &WaitSpec{Field: "terminalCondition", Contains: "CONDITION_SUCCEEDED", Interval: 3 * time.Second, Timeout: 240 * time.Second}},
		Scenario{Op: "run_service_patch", Service: "run", Method: "PATCH", Path: runSvc + "?updateMask=template",
			Body: fmt.Sprintf(`{"template":{"containers":[{"image":%q,"ports":[{"containerPort":8080}],"env":[{"name":"DEMO","value":"parity"}]}]}}`, runImage),
			Save: map[string]string{"runPatchOp": "name"}},
		Scenario{Op: "run_service_patch_wait", Service: "run", Method: "GET", Path: "/v2/${runPatchOp}",
			Wait: &WaitSpec{Field: "done", Interval: 3 * time.Second, Timeout: 240 * time.Second}},
		Scenario{Op: "run_service_get_after_patch", Service: "run", Method: "GET", Path: runSvc,
			Wait: &WaitSpec{Field: "terminalCondition", Contains: "CONDITION_SUCCEEDED", Interval: 3 * time.Second, Timeout: 120 * time.Second}},
	)

	// Eventarc: triggers.create (GCS object.finalized filter -> the Cloud Run
	// service above) + get, the demo's cm-leaderboard-live trigger. Real GCP
	// requires a regional filter bucket whose location matches the trigger
	// region, an explicit trigger service account, and the bare Cloud Run
	// service name (verified against real GCP; the demo's full-resource-path
	// destination form is rejected — recorded as a report finding).
	evBase := "/v1/projects/" + project + "/locations/us-central1/triggers"
	sc = append(sc,
		Scenario{Op: "eventarc_bucket_create", Service: "storage", Method: "POST", Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q,"location":"us-central1"}`, n.EventBucket)},
		Scenario{Op: "eventarc_trigger_create", Service: "eventarc", Method: "POST", Path: evBase + "?triggerId=" + n.Trigger,
			Body: fmt.Sprintf(`{"name":%q,"serviceAccount":%q,"destination":{"cloudRun":{"service":%q,"region":"us-central1"}},"eventFilters":[{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},{"attribute":"bucket","value":%q}]}`,
				"projects/"+project+"/locations/us-central1/triggers/"+n.Trigger, project+"@appspot.gserviceaccount.com", n.RunService, n.EventBucket)},
		// The create is an LRO; poll the trigger itself until it exists rather
		// than the (eventarc-specific, not generically served) operation.
		Scenario{Op: "eventarc_trigger_get", Service: "eventarc", Method: "GET", Path: evBase + "/" + n.Trigger,
			Wait: &WaitSpec{Field: "name", Interval: 3 * time.Second, Timeout: 240 * time.Second}},
	)

	// ─── Authz paths (G6 / G1) ────────────────────────────────────────────────
	// The recorder authenticates as the parity-project owner, so a low-privilege
	// principal is unavailable; the only authz differential it can capture is an
	// unauthenticated call. Real GCP rejects it (401 UNAUTHENTICATED) while the
	// emulator serves it — authz is documented as not enforced (G1,
	// docs/gcp/wire-contract.md §8). The divergences are accepted, with that reason, by the
	// triage rules in triage.go, making the accepted risk explicit and gated.
	sc = append(sc,
		Scenario{Op: "storage_buckets_list_noauth", Service: "storage", Method: "GET",
			Path: "/storage/v1/b?project=" + project, NoAuth: true},
		Scenario{Op: "compute_instances_list_noauth", Service: "compute", Method: "GET",
			Path: "/compute/v1/projects/" + project + "/zones/" + computeZone + "/instances", NoAuth: true},
		Scenario{Op: "sql_instances_list_noauth", Service: "sqladmin", Method: "GET",
			Path: "/sql/v1beta4/projects/" + project + "/instances", NoAuth: true},
		Scenario{Op: "redis_instances_list_noauth", Service: "redis", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/instances", NoAuth: true},
		// AUD6: extend the authz-gap record to one of the newly covered
		// services, and capture a real PERMISSION_DENIED (403) envelope — Service
		// Usage rejects an unknown service the parity project cannot see, while
		// the emulator (which does not maintain the Service Usage catalog) serves
		// a synthesized DISABLED service. Both are accepted as documented gaps.
		Scenario{Op: "container_clusters_list_noauth", Service: "container", Method: "GET",
			Path: "/v1/projects/" + project + "/locations/us-central1/clusters", NoAuth: true},
		Scenario{Op: "su_service_get_unknown", Service: "serviceusage", Method: "GET",
			Path: "/v1/projects/" + project + "/services/unknown.googleapis.com"},
	)

	// ─── Pub/Sub messaging semantics (PSM1) ───────────────────────────────────
	// Appended last so the golden indices above (and the control-plane list
	// goldens, which must not observe these extra resources) stay stable.
	sc = append(sc, pubsubMessagingScenarios(project, n)...)

	// ─── Pub/Sub dead-letter republish (PSM2) ─────────────────────────────────
	// Setup-driven: only the final dead-letter pull is recorded; the nack loop
	// that forces the forward is best-effort and not diffed.
	sc = append(sc, pubsubDeadLetterScenarios(project, n)...)

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
		if arr, ok := cur.([]any); ok {
			i, err := strconv.Atoi(seg)
			if err != nil || i < 0 || i >= len(arr) {
				return "", false
			}
			cur = arr[i]
			continue
		}
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
		return "", false
	}
}

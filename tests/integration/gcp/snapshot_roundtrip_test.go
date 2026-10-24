package gcp_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/persistence/version"
)

// This file covers the cross-cutting snapshot invariants for the GCP binary in
// **memory mode** (no --dsn): the exported envelope schema, an
// export → reset → import round-trip across a cross-section of store kinds, a
// project-scoped and a global reset, the envelope validation negatives
// (schema-version and cloud mismatch are rejected without mutating state), a
// real process restart proving memory-mode state (metadata + blob bytes)
// survives a SIGTERM/relaunch cycle, and seeded negatives proving each
// assertion is not vacuous.
//
// The exhaustive per-service round-trip and restart against Postgres (--dsn)
// already lives in tests/persistent_mode/gcp/... (tag gcp_persistence, run by
// the CI test-gcp-persistence job) and is deliberately NOT duplicated here.
// This suite is what `go test ./tests/integration/gcp/` and
// `make test-integration-gcp` run, so no Makefile/CI wiring is needed.

// exportSnapshot fetches GET /_jaiscloud/export and asserts the response is a
// gzip tarball, returning its raw bytes.
func exportSnapshot(t *testing.T) []byte {
	t.Helper()
	resp, body := do(t, "GET", "/_jaiscloud/export", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "export: %s", body)
	require.GreaterOrEqual(t, len(body), 2, "export body too short to be a gzip stream")
	require.True(t, body[0] == 0x1f && body[1] == 0x8b,
		"export must be a gzip stream, got first bytes %x", body[:2])
	return body
}

// importSnapshot posts a snapshot tarball (optionally resetting first) and
// asserts the status code, returning the response body.
func importSnapshot(t *testing.T, tarball []byte, resetFirst bool, want int) []byte {
	t.Helper()
	path := "/_jaiscloud/import"
	if resetFirst {
		path += "?reset_first=true"
	}
	resp, body := do(t, "POST", path, tarball, map[string]string{"Content-Type": "application/gzip"})
	require.Equal(t, want, resp.StatusCode, "import: %s", body)
	return body
}

// readSnapshotEnvelope extracts and decodes envelope.json from a snapshot
// tarball. It also fails if envelope.json is absent.
func readSnapshotEnvelope(t *testing.T, tarball []byte) version.Envelope {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err, "snapshot must be a valid gzip stream")
	defer gz.Close()

	tr := tar.NewReader(gz)
	var env version.Envelope
	found := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if hdr.Name != "envelope.json" {
			continue
		}
		raw, err := io.ReadAll(tr)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &env))
		found = true
		break
	}
	require.True(t, found, "tarball must contain envelope.json")
	return env
}

// mutateSnapshot rebuilds a snapshot tarball, applying mutate to the decoded
// envelope and preserving every other entry (e.g. blobs) verbatim. It lets a
// test drop a store from the envelope and still import a structurally valid
// tarball.
func mutateSnapshot(t *testing.T, tarball []byte, mutate func(*version.Envelope)) []byte {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err)
	tr := tar.NewReader(gz)

	type entry struct {
		hdr  tar.Header
		data []byte
	}
	var others []entry
	var env version.Envelope
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data, err := io.ReadAll(tr)
		require.NoError(t, err)
		if hdr.Name == "envelope.json" {
			require.NoError(t, json.Unmarshal(data, &env))
			continue
		}
		others = append(others, entry{hdr: *hdr, data: data})
	}
	require.NoError(t, gz.Close())

	mutate(&env)
	envJSON, err := json.Marshal(env)
	require.NoError(t, err)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name:     "envelope.json",
		Typeflag: tar.TypeReg,
		Size:     int64(len(envJSON)),
		Mode:     0600,
	}))
	_, err = tw.Write(envJSON)
	require.NoError(t, err)
	for _, e := range others {
		h := e.hdr
		require.NoError(t, tw.WriteHeader(&h))
		_, err := tw.Write(e.data)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// uniqueSuffix returns a per-run suffix so seeded resource names never collide
// with (or leak between) runs. The emulator's global clock lives in the server
// process, so a unique test-side suffix is the deterministic choice here; use
// the repo clock helper (clock.RealNow) rather than time.Now directly.
func uniqueSuffix() string {
	return fmt.Sprintf("%d", clock.RealNow().UnixNano())
}

// mustPost performs a mutating request and asserts HTTP 200.
func mustPost(t *testing.T, method, path, body string) {
	t.Helper()
	var raw []byte
	if body != "" {
		raw = []byte(body)
	}
	headers := map[string]string{}
	if body != "" {
		headers["Content-Type"] = "application/json"
	}
	resp, respBody := do(t, method, path, raw, headers)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, respBody)
}

// getStatus issues a GET and returns the HTTP status code.
func getStatus(t *testing.T, path string) int {
	t.Helper()
	resp, _ := do(t, "GET", path, nil, nil)
	return resp.StatusCode
}

// TestExportImport_SchemaVersion verifies the exported envelope is a gzip tar
// containing envelope.json whose schema_version equals the version this binary
// writes (version.CodeSnapshotVersion — never a hardcoded literal).
func TestExportImport_SchemaVersion(t *testing.T) {
	resetState(t)

	tarball := exportSnapshot(t)
	env := readSnapshotEnvelope(t, tarball)

	require.Equal(t, version.CodeSnapshotVersion, env.SchemaVersion,
		"exported envelope schema_version must match the binary's snapshot schema")
	require.Equal(t, "gcp", env.Cloud, "envelope must be tagged with the gcp cloud")
	require.NotEmpty(t, env.Stores, "envelope must carry at least one store")
}

// TestExportImport_RoundTrip seeds one representative resource across a
// cross-section of GCP store kinds in memory mode, exports the state, resets,
// asserts the seeds are gone, imports the tarball, and asserts each survived.
//
// The cross-section deliberately mixes storage mechanisms rather than echoing
// the 30-service Postgres suite:
//   - blob-backed storage (GCS bucket + object bytes + gcs_objects metadata),
//   - service-owned stores (Pub/Sub, Secret Manager, KMS, Scheduler, Tasks),
//   - the shared ResourceStore (Cloud DNS) that also backs the metadata-only
//     services (clouddns/cloudsql/compute/...).
func TestExportImport_RoundTrip(t *testing.T) {
	resetState(t)

	const project = "rt-project"
	sfx := uniqueSuffix()

	bucket := "rt-bucket-" + sfx
	object := "rt-obj-" + sfx + ".txt"
	const objectBody = "hello-snapshot-round-trip"
	topic := "rt-topic-" + sfx
	sub := "rt-sub-" + sfx
	secret := "rt-secret-" + sfx
	keyring := "rt-kr-" + sfx
	cryptokey := "rt-key-" + sfx
	zone := "rt-zone-" + sfx
	job := "rt-job-" + sfx
	queue := "rt-queue-" + sfx

	// ── Seed ─────────────────────────────────────────────────────────────────
	// GCS: bucket metadata + object metadata + object bytes (blobfs).
	mustPost(t, "POST", "/storage/v1/b?project="+project, `{"name":"`+bucket+`"}`)
	resp, body := do(t, "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object,
		[]byte(objectBody), map[string]string{"Content-Type": "text/plain"})
	require.Equal(t, http.StatusOK, resp.StatusCode, "upload object: %s", body)

	// Pub/Sub: topic + subscription.
	mustPost(t, "PUT", "/v1/projects/"+project+"/topics/"+topic, "{}")
	mustPost(t, "PUT", "/v1/projects/"+project+"/subscriptions/"+sub+"?subscriptionId="+sub,
		`{"topic":"projects/`+project+`/topics/`+topic+`"}`)

	// Secret Manager secret.
	mustPost(t, "POST", "/v1/projects/"+project+"/secrets?secretId="+secret,
		`{"replication":{"automatic":{}},"annotations":{"suite":"snapshot-roundtrip"}}`)

	// KMS key ring + crypto key.
	mustPost(t, "POST", "/v1/projects/"+project+"/locations/global/keyRings?keyRingId="+keyring, "{}")
	mustPost(t, "POST", "/v1/projects/"+project+"/locations/global/keyRings/"+keyring+
		"/cryptoKeys?cryptoKeyId="+cryptokey, `{"purpose":"ENCRYPT_DECRYPT"}`)

	// Cloud Scheduler job (own store).
	mustPost(t, "POST", "/v1/projects/"+project+"/locations/us-central1/jobs",
		`{"name":"projects/`+project+`/locations/us-central1/jobs/`+job+
			`","schedule":"* * * * *","timeZone":"UTC",`+
			`"httpTarget":{"uri":"http://example.test/hook","httpMethod":"GET"}}`)

	// Cloud Tasks queue (own store).
	mustPost(t, "POST", "/v2/projects/"+project+"/locations/us-central1/queues",
		`{"name":"projects/`+project+`/locations/us-central1/queues/`+queue+`"}`)

	// Cloud DNS managed zone (shared ResourceStore).
	mustPost(t, "POST", "/dns/v1/projects/"+project+"/managedZones",
		`{"name":"`+zone+`","dnsName":"`+zone+`.example.com."}`)

	// verifyPresent asserts every seeded resource is readable. The object media
	// read additionally proves the bytes (not just the metadata row) survived.
	verifyPresent := func(phase string) {
		t.Helper()
		// path → optional expected body substring.
		checks := []struct {
			path string
			want string
		}{
			{"/storage/v1/b/" + bucket, ""},
			{"/storage/v1/b/" + bucket + "/o/" + object, ""},
			{"/v1/projects/" + project + "/topics/" + topic, ""},
			{"/v1/projects/" + project + "/subscriptions/" + sub, ""},
			{"/v1/projects/" + project + "/secrets/" + secret, `"suite":"snapshot-roundtrip"`},
			{"/v1/projects/" + project + "/locations/global/keyRings/" + keyring, ""},
			{"/v1/projects/" + project + "/locations/global/keyRings/" + keyring + "/cryptoKeys/" + cryptokey, ""},
			{"/v1/projects/" + project + "/locations/us-central1/jobs/" + job, ""},
			{"/v2/projects/" + project + "/locations/us-central1/queues/" + queue, ""},
			{"/dns/v1/projects/" + project + "/managedZones/" + zone, ""},
		}
		for _, c := range checks {
			resp, b := do(t, "GET", c.path, nil, nil)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: GET %s: %s", phase, c.path, b)
			if c.want != "" {
				require.Containsf(t, string(b), c.want, "%s: GET %s body", phase, c.path)
			}
		}
		// Blob bytes round-tripped through the tar's blobs/ entries.
		resp, b := do(t, "GET", "/storage/v1/b/"+bucket+"/o/"+object+"?alt=media", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: GET object media: %s", phase, b)
		require.Equal(t, objectBody, string(b), "%s: object bytes must survive the round-trip", phase)
	}

	// verifyGone asserts every seeded resource is absent (404).
	verifyGone := func() {
		t.Helper()
		paths := []string{
			"/storage/v1/b/" + bucket,
			"/storage/v1/b/" + bucket + "/o/" + object,
			"/v1/projects/" + project + "/topics/" + topic,
			"/v1/projects/" + project + "/subscriptions/" + sub,
			"/v1/projects/" + project + "/secrets/" + secret,
			"/v1/projects/" + project + "/locations/global/keyRings/" + keyring,
			"/v1/projects/" + project + "/locations/us-central1/jobs/" + job,
			"/v2/projects/" + project + "/locations/us-central1/queues/" + queue,
			"/dns/v1/projects/" + project + "/managedZones/" + zone,
		}
		for _, p := range paths {
			require.Equalf(t, http.StatusNotFound, getStatus(t, p), "after reset: %s must be gone", p)
		}
	}

	verifyPresent("seeded")

	// ── Export → reset → import ──────────────────────────────────────────────
	tarball := exportSnapshot(t)
	env := readSnapshotEnvelope(t, tarball)
	// Every store kind exercised above must be present in the envelope; a
	// missing store there is the failure mode the negative test pins.
	for _, key := range []string{"gcs_objects", "pubsub_messages", "secrets", "keys", "scheduler", "tasks", "resources"} {
		require.Containsf(t, env.Stores, key, "envelope must carry the %q store", key)
	}

	resetState(t)
	verifyGone()

	importSnapshot(t, tarball, true, http.StatusOK)
	verifyPresent("imported")
}

// TestExportImport_DroppedStoreNegative is the seeded negative: it proves the
// round-trip assertion in TestExportImport_RoundTrip is meaningful by dropping
// a store from the envelope before import. The resource owned by the dropped
// store must NOT come back, while resources in other stores must — so if the
// round-trip assertion were vacuous (e.g. import silently no-ops), this test
// fails.
func TestExportImport_DroppedStoreNegative(t *testing.T) {
	resetState(t)

	const project = "rt-neg-project"
	sfx := uniqueSuffix()
	bucket := "rt-neg-bucket-" + sfx
	topic := "rt-neg-topic-" + sfx
	job := "rt-neg-job-" + sfx

	mustPost(t, "POST", "/storage/v1/b?project="+project, `{"name":"`+bucket+`"}`)
	mustPost(t, "PUT", "/v1/projects/"+project+"/topics/"+topic, "{}")
	mustPost(t, "POST", "/v1/projects/"+project+"/locations/us-central1/jobs",
		`{"name":"projects/`+project+`/locations/us-central1/jobs/`+job+
			`","schedule":"* * * * *","timeZone":"UTC",`+
			`"httpTarget":{"uri":"http://example.test/hook","httpMethod":"GET"}}`)

	bucketPath := "/storage/v1/b/" + bucket
	topicPath := "/v1/projects/" + project + "/topics/" + topic
	jobPath := "/v1/projects/" + project + "/locations/us-central1/jobs/" + job

	tarball := exportSnapshot(t)
	env := readSnapshotEnvelope(t, tarball)
	require.Contains(t, env.Stores, "scheduler", "envelope must carry the scheduler store before tampering")

	// Drop the scheduler store: the job's data is now absent from the envelope.
	tampered := mutateSnapshot(t, tarball, func(e *version.Envelope) {
		delete(e.Stores, "scheduler")
	})
	require.NotContains(t, readSnapshotEnvelope(t, tampered).Stores, "scheduler")

	resetState(t)
	require.Equal(t, http.StatusNotFound, getStatus(t, jobPath), "job must be gone after reset")
	require.Equal(t, http.StatusNotFound, getStatus(t, bucketPath), "bucket must be gone after reset")

	// The tampered snapshot is structurally valid and imports successfully —
	// it just carries no scheduler state.
	importSnapshot(t, tampered, true, http.StatusOK)

	// The dropped store's resource must NOT be restored...
	require.Equal(t, http.StatusNotFound, getStatus(t, jobPath),
		"scheduler state was dropped from the envelope, so the job must not be restored")
	// ...while resources in stores that were still present must.
	require.Equal(t, http.StatusOK, getStatus(t, bucketPath),
		"gcs_objects was present in the envelope, so the bucket must be restored")
	require.Equal(t, http.StatusOK, getStatus(t, topicPath),
		"pubsub_messages was present in the envelope, so the topic must be restored")
}

// TestResetScope_Project verifies the project-scoped reset path GCP actually
// supports: POST /_jaiscloud/reset?account=<project> wipes the named project's
// entries in the shared ResourceStore and leaves another project's entries
// intact, mirroring AWS's TestResetScope_AccountAndRegion for the scoped store.
// GCP maps a project to the store account key.
//
// Scope: this exercises the shared ResourceStore (stores.resources), which is
// the GCP store that implements admin.ScopedResetter (ResetAccount/ResetScope);
// Cloud DNS managed zones live there, as do the other metadata-only services
// (cloudsql/compute/...). Service-owned stores (gcs/pubsub/kms/scheduler/...) do
// NOT implement admin.ScopedResetter, so a scoped reset falls back to their
// whole-store Reset — the admin handler's documented "over-wipe (acceptable)"
// path (see internal/admin/admin.go). Per-service project-scoped reset for
// those stores is tracked by the GTC3 project-isolation session, not here.
func TestResetScope_Project(t *testing.T) {
	resetState(t)

	const (
		projectA = "scope-project-a"
		projectB = "scope-project-b"
		zone     = "scope-shared-zone" // same-named resource in both projects
	)
	zonePathA := "/dns/v1/projects/" + projectA + "/managedZones/" + zone
	zonePathB := "/dns/v1/projects/" + projectB + "/managedZones/" + zone

	mustPost(t, "POST", "/dns/v1/projects/"+projectA+"/managedZones",
		`{"name":"`+zone+`","dnsName":"`+zone+`.example.com."}`)
	mustPost(t, "POST", "/dns/v1/projects/"+projectB+"/managedZones",
		`{"name":"`+zone+`","dnsName":"`+zone+`.example.com."}`)
	require.Equal(t, http.StatusOK, getStatus(t, zonePathA))
	require.Equal(t, http.StatusOK, getStatus(t, zonePathB))

	// Scoped reset of project A only.
	resp, body := do(t, "POST", "/_jaiscloud/reset?account="+projectA, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "scoped reset: %s", body)

	require.Equal(t, http.StatusNotFound, getStatus(t, zonePathA),
		"project A's zone must be wiped by a reset scoped to project A")
	require.Equal(t, http.StatusOK, getStatus(t, zonePathB),
		"project B's same-named zone must survive a reset scoped to project A")
}

// TestResetScope_Global verifies the unscoped reset (no ?account=) wipes every
// project's state, mirroring AWS's TestResetScope_GlobalReset. It is the
// complement of TestResetScope_Project: that test pins the scoped path, this
// one pins that the default reset really is global (a regression that made the
// default scoped to one account would leave the other project's resource).
func TestResetScope_Global(t *testing.T) {
	resetState(t)

	const (
		projectA = "global-scope-project-a"
		projectB = "global-scope-project-b"
		topic    = "global-scope-topic"
	)
	topicPathA := "/v1/projects/" + projectA + "/topics/" + topic
	topicPathB := "/v1/projects/" + projectB + "/topics/" + topic

	mustPost(t, "PUT", topicPathA, "{}")
	mustPost(t, "PUT", topicPathB, "{}")
	require.Equal(t, http.StatusOK, getStatus(t, topicPathA))
	require.Equal(t, http.StatusOK, getStatus(t, topicPathB))

	// Unscoped reset.
	resp, body := do(t, "POST", "/_jaiscloud/reset", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "global reset: %s", body)

	require.Equal(t, http.StatusNotFound, getStatus(t, topicPathA),
		"a global reset must wipe project A's state")
	require.Equal(t, http.StatusNotFound, getStatus(t, topicPathB),
		"a global reset must wipe project B's state")
}

// TestExportImport_RejectSchemaVersion verifies the import path honours the
// envelope's schema version: a snapshot written by an older or newer binary is
// rejected with 409 schema_version_mismatch (internal/admin/admin.go Step 1)
// and — because the version check runs before reset_first — the rejection does
// not mutate the running instance's state.
func TestExportImport_RejectSchemaVersion(t *testing.T) {
	resetState(t)

	// Seed one resource so a rejected import has state it must leave untouched.
	const project = "reject-schema-project"
	sfx := uniqueSuffix()
	bucket := "reject-schema-bucket-" + sfx
	mustPost(t, "POST", "/storage/v1/b?project="+project, `{"name":"`+bucket+`"}`)

	tarball := exportSnapshot(t)

	for _, tc := range []struct {
		name    string
		version int
	}{
		{"too-old", version.CodeSnapshotVersion - 1},
		{"too-new", version.CodeSnapshotVersion + 1},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tampered := mutateSnapshot(t, tarball, func(e *version.Envelope) {
				e.SchemaVersion = tc.version
			})
			require.Equal(t, tc.version, readSnapshotEnvelope(t, tampered).SchemaVersion)

			body := importSnapshot(t, tampered, true, http.StatusConflict)
			require.Contains(t, string(body), "schema_version_mismatch",
				"a schema-v%d snapshot must be rejected with the documented error", tc.version)

			// The rejected import must not have wiped or altered state.
			require.Equal(t, http.StatusOK, getStatus(t, "/storage/v1/b/"+bucket),
				"a rejected import must leave existing state intact")
		})
	}
}

// TestExportImport_RejectCloudMismatch verifies the envelope's cloud tag is
// honoured: a snapshot tagged for another cloud is rejected with 409
// cloud_mismatch and does not mutate the running instance's state.
func TestExportImport_RejectCloudMismatch(t *testing.T) {
	resetState(t)

	const project = "reject-cloud-project"
	sfx := uniqueSuffix()
	topic := "reject-cloud-topic-" + sfx
	topicPath := "/v1/projects/" + project + "/topics/" + topic
	mustPost(t, "PUT", topicPath, "{}")

	tarball := exportSnapshot(t)
	tampered := mutateSnapshot(t, tarball, func(e *version.Envelope) {
		e.Cloud = "aws"
	})
	require.Equal(t, "aws", readSnapshotEnvelope(t, tampered).Cloud)

	body := importSnapshot(t, tampered, true, http.StatusConflict)
	require.Contains(t, string(body), "cloud_mismatch",
		"a snapshot tagged for another cloud must be rejected with the documented error")
	require.Equal(t, http.StatusOK, getStatus(t, topicPath),
		"a rejected import must leave existing state intact")
}

// TestMemoryRestart_StateSurvives is the GCP analogue of the AWS
// tests/integration/memory_restart_test.go. It starts jaiscloud-gcp in memory
// mode (no --dsn) against a temp --data-dir, seeds a cross-section of stores,
// SIGTERMs the process (which runs SnapshotLoop.Stop → a final state.json +
// blob save), relaunches from the same data-dir and instance id, and asserts
// every seed survived — metadata through state.json, object bytes through the
// session blob dir / snapshot blobs/ entries.
//
// A SIGKILL would not exercise this path: the final save happens on graceful
// shutdown. The binary is located via JAISCLOUD_GCP_BINARY or
// ../../../jaiscloud-gcp (CI builds it at the repo root); the test skips when
// it is absent so a plain `go test ./tests/integration/gcp/` stays green.
func TestMemoryRestart_StateSurvives(t *testing.T) {
	binary := restartBinary(t)

	dataDir := t.TempDir()
	// A fixed instance id pins the session blob dir (/tmp/jaiscloud-<id>/blobs)
	// across both launches — the memory backend's byte store.
	instanceID := fmt.Sprintf("gcp-restart-%d", freePort(t))
	port, grpcPort, hmsPort := freePort(t), freePort(t), freePort(t)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	t.Cleanup(func() { os.RemoveAll(filepath.Join(os.TempDir(), "jaiscloud-"+instanceID)) })

	const project = "restart-project"
	sfx := uniqueSuffix()

	bucket := "restart-bucket-" + sfx
	object := "restart-obj-" + sfx + ".txt"
	const objectBody = "hello-memory-restart"
	topic := "restart-topic-" + sfx
	sub := "restart-sub-" + sfx
	secret := "restart-secret-" + sfx
	keyring := "restart-kr-" + sfx
	cryptokey := "restart-key-" + sfx
	zone := "restart-zone-" + sfx
	job := "restart-job-" + sfx
	queue := "restart-queue-" + sfx
	goneTopic := "restart-gone-" + sfx
	goneTopicPath := "/v1/projects/" + project + "/topics/" + goneTopic

	// mustCallAt performs a mutating request against the restart server and
	// asserts the expected status.
	mustCallAt := func(method, path, body string, want int) {
		t.Helper()
		var payload []byte
		headers := map[string]string{}
		if body != "" {
			payload = []byte(body)
			headers["Content-Type"] = "application/json"
		}
		resp, b := doAt(t, base, method, path, payload, headers)
		require.Equalf(t, want, resp.StatusCode, "%s %s: %s", method, path, b)
	}

	// verifyPresent reads every seed back, requiring 200 and — for GCS — the
	// exact object bytes, proving the blob store (not just metadata) survived.
	verifyPresent := func(phase string) {
		t.Helper()
		checks := []struct {
			path string
			want string
		}{
			{"/storage/v1/b/" + bucket, ""},
			{"/storage/v1/b/" + bucket + "/o/" + object, ""},
			{"/v1/projects/" + project + "/topics/" + topic, ""},
			{"/v1/projects/" + project + "/subscriptions/" + sub, ""},
			{"/v1/projects/" + project + "/secrets/" + secret, `"suite":"memory-restart"`},
			{"/v1/projects/" + project + "/locations/global/keyRings/" + keyring, ""},
			{"/v1/projects/" + project + "/locations/global/keyRings/" + keyring + "/cryptoKeys/" + cryptokey, ""},
			{"/v1/projects/" + project + "/locations/us-central1/jobs/" + job, ""},
			{"/v2/projects/" + project + "/locations/us-central1/queues/" + queue, ""},
			{"/dns/v1/projects/" + project + "/managedZones/" + zone, ""},
		}
		for _, c := range checks {
			resp, b := doAt(t, base, "GET", c.path, nil, nil)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "%s: GET %s: %s", phase, c.path, b)
			if c.want != "" {
				require.Containsf(t, string(b), c.want, "%s: GET %s body", phase, c.path)
			}
		}
		resp, b := doAt(t, base, "GET", "/storage/v1/b/"+bucket+"/o/"+object+"?alt=media", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: GET object media: %s", phase, b)
		require.Equal(t, objectBody, string(b), "%s: object bytes must survive the restart", phase)
	}

	// ── Launch 1: seed ───────────────────────────────────────────────────────
	cmd1 := startRestartServer(t, binary, dataDir, instanceID, port, grpcPort, hmsPort)
	waitRestartReady(t, base)

	mustCallAt("POST", "/storage/v1/b?project="+project, `{"name":"`+bucket+`"}`, http.StatusOK)
	{
		resp, b := doAt(t, base, "POST", "/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object,
			[]byte(objectBody), map[string]string{"Content-Type": "text/plain"})
		require.Equal(t, http.StatusOK, resp.StatusCode, "upload object: %s", b)
	}
	mustCallAt("PUT", "/v1/projects/"+project+"/topics/"+topic, "{}", http.StatusOK)
	mustCallAt("PUT", "/v1/projects/"+project+"/subscriptions/"+sub+"?subscriptionId="+sub,
		`{"topic":"projects/`+project+`/topics/`+topic+`"}`, http.StatusOK)
	mustCallAt("POST", "/v1/projects/"+project+"/secrets?secretId="+secret,
		`{"replication":{"automatic":{}},"annotations":{"suite":"memory-restart"}}`, http.StatusOK)
	mustCallAt("POST", "/v1/projects/"+project+"/locations/global/keyRings?keyRingId="+keyring, "{}", http.StatusOK)
	mustCallAt("POST", "/v1/projects/"+project+"/locations/global/keyRings/"+keyring+
		"/cryptoKeys?cryptoKeyId="+cryptokey, `{"purpose":"ENCRYPT_DECRYPT"}`, http.StatusOK)
	mustCallAt("POST", "/v1/projects/"+project+"/locations/us-central1/jobs",
		`{"name":"projects/`+project+`/locations/us-central1/jobs/`+job+
			`","schedule":"* * * * *","timeZone":"UTC",`+
			`"httpTarget":{"uri":"http://example.test/hook","httpMethod":"GET"}}`, http.StatusOK)
	mustCallAt("POST", "/v2/projects/"+project+"/locations/us-central1/queues",
		`{"name":"projects/`+project+`/locations/us-central1/queues/`+queue+`"}`, http.StatusOK)
	mustCallAt("POST", "/dns/v1/projects/"+project+"/managedZones",
		`{"name":"`+zone+`","dnsName":"`+zone+`.example.com."}`, http.StatusOK)
	// A resource deleted before the restart must stay deleted after it.
	mustCallAt("PUT", goneTopicPath, "{}", http.StatusOK)

	verifyPresent("seeded")
	mustCallAt("DELETE", goneTopicPath, "", http.StatusOK)

	// ── Graceful stop: SIGTERM runs the final snapshot save ──────────────────
	stopRestartServer(t, cmd1)

	// ── Launch 2: restart from the same data-dir ─────────────────────────────
	cmd2 := startRestartServer(t, binary, dataDir, instanceID, port, grpcPort, hmsPort)
	t.Cleanup(func() { stopRestartServer(t, cmd2) })
	waitRestartReady(t, base)

	verifyPresent("after restart")
	require.Equal(t, http.StatusNotFound, getStatusAt(t, base, goneTopicPath),
		"a topic deleted before the restart must not be resurrected by the restore")
}

// ── process-restart helpers ──────────────────────────────────────────────────

// restartBinary resolves the jaiscloud-gcp binary for the restart test, or
// skips the test when it has not been built.
func restartBinary(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("JAISCLOUD_GCP_BINARY"); b != "" {
		if _, err := os.Stat(b); err == nil {
			return b
		}
		t.Skipf("JAISCLOUD_GCP_BINARY=%q not found", b)
	}
	candidates := []string{
		"../../../jaiscloud-gcp", // CI builds it at the repo root
		filepath.Join(os.Getenv("GOPATH"), "bin", "jaiscloud-gcp"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Skip("jaiscloud-gcp binary not found; run: go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/")
	return ""
}

// freePort returns an available localhost TCP port. The restart server must not
// reuse the ports of the emulator the rest of the package talks to (:8080 REST,
// :8081 gRPC, :9083 HMS).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "find free port")
	p := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return p
}

// restartWriter forwards the child process's output to the test log.
type restartWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *restartWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		i := strings.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:i])
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

// startRestartServer starts jaiscloud-gcp in memory mode (no --dsn) against
// dataDir, with the given instance id so the session blob dir is stable across
// launches. All three listeners use caller-supplied ports.
func startRestartServer(t *testing.T, binary, dataDir, instanceID string, port, grpcPort, hmsPort int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binary, "start",
		"--port", strconv.Itoa(port),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--hms-port", strconv.Itoa(hmsPort),
		"--data-dir", dataDir,
		"--log-level", "warn",
	)
	cmd.Env = append(os.Environ(),
		"JAISCLOUD_EPHEMERAL=false",
		"JAISCLOUD_STATE_DIR="+dataDir,
		"JAISCLOUD_INSTANCE_ID="+instanceID,
	)
	cmd.Stdout = &restartWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", port)}
	cmd.Stderr = &restartWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", port)}
	require.NoError(t, cmd.Start(), "start jaiscloud-gcp on port %d", port)
	return cmd
}

// waitRestartReady polls the health endpoint until the process is ready.
func waitRestartReady(t *testing.T, base string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := clock.RealNow().Add(30 * time.Second)
	for clock.RealNow().Before(deadline) {
		if resp, err := client.Get(base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", base)
}

// stopRestartServer SIGTERMs the process and waits for a clean exit (which runs
// SnapshotLoop.Stop → a final state.json + blob save). It kills the process if
// it does not exit within 20s.
func stopRestartServer(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Log("warning: jaiscloud-gcp did not stop within 20s after SIGTERM; killed")
	}
}

// getStatusAt issues a GET against base and returns the HTTP status code.
func getStatusAt(t *testing.T, base, path string) int {
	t.Helper()
	resp, _ := doAt(t, base, "GET", path, nil, nil)
	return resp.StatusCode
}

//go:build gcp_differential

package gcpdifferential

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// This file is the REST half of the SDK-tour differential (demo/sdk-tour). It
// mirrors the exact REST operations the official Go SDK tour issues — a
// resumable (chunked) GCS upload, a media download, a paginated object list,
// and BigQuery insertAll/query/load-job — against the same target plumbing the
// curated REST differential uses: record against real GCP, replay against the
// emulator, then diff the normalized exchanges.
//
// The golden set lives in its own directory (testdata/golden-tour) so it never
// pollutes the curated REST goldens. A scenario with no committed golden is
// "pending recording" and skipped, exactly as in the curated set.

const (
	// tourChunkSize is the resumable chunk size the tour's Go writer uses
	// (256 KiB, GCS's required non-final-chunk multiple).
	tourChunkSize = 256 * 1024
	// tourResumableObject is uploaded through the resumable protocol; it is two
	// chunks (512 KiB) so the golden covers session start, a non-final chunk and
	// the finalizing chunk.
	tourResumableObject = "resumable/payload.bin"
	// tourMediaObject is a small object downloaded with alt=media so the
	// checksum/download path is captured without a multi-megabyte body.
	tourMediaObject = "checksum.txt"
	// tourMediaBody is that object's deterministic content.
	tourMediaBody = "jaiscloud-sdk-tour-checksum"
	// tourPagePrefix and tourPageCount describe the paginated list fixture.
	tourPagePrefix = "page/"
	tourPageCount  = 7
	// tourLoadObject is the NDJSON object a BigQuery load job reads.
	tourLoadObject = "load/rows.ndjson"
	tourLoadBody   = "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n"
)

// FixedKMSCryptoKeyAsym is the fixed, reusable asymmetric (sign) KMS key the
// tour's kms.asymmetric_sign scenario uses. Like the symmetric differential key
// it cannot be deleted (only scheduled for destruction), so it is created once
// and reused; EnsureTourKMS tolerates ALREADY_EXISTS.
const FixedKMSCryptoKeyAsym = "differential-asym"

// tourPayload returns the deterministic resumable-upload body. It is
// deliberately small (two 256 KiB chunks) — the differential proves the
// resumable wire protocol, not a multi-megabyte transfer, and a compact body
// keeps the committed golden small.
func tourPayload() []byte {
	const unit = "jaiscloud-sdk-tour-resumable-payload-"
	total := 2 * tourChunkSize
	return []byte(strings.Repeat(unit, total/len(unit)+1)[:total])
}

// bqQuote wraps an identifier in backticks for a Standard SQL query.
func bqQuote(s string) string { return "`" + s + "`" }

// TourScenarios returns the REST SDK-tour scenario list. It is ordered so
// resources exist before they are read and are deleted at the end by
// Target.CleanupTour.
func TourScenarios(project, suffix string) []Scenario {
	n := Names(suffix)
	bucket := n.TourBucket
	bqBase := "/bigquery/v2/projects/" + project

	var sc []Scenario

	// ─── Cloud Storage: bucket, resumable upload, download, pagination ───────
	sc = append(sc,
		Scenario{Op: "tour_bucket_create", Service: "storage", Method: http.MethodPost,
			Path: "/storage/v1/b?project=" + project,
			Body: fmt.Sprintf(`{"name":%q,"location":"US"}`, bucket)},
		// Session start: the response carries the session URI in Location.
		Scenario{Op: "tour_resumable_start", Service: "storage", Method: http.MethodPost,
			Path:       "/upload/storage/v1/b/" + bucket + "/o?uploadType=resumable&name=resumable%2Fpayload.bin",
			Headers:    map[string]string{"X-Upload-Content-Type": "application/octet-stream"},
			Body:       `{}`,
			SaveHeader: map[string]string{"session": "Location"}},
		// Non-final chunk (256 KiB) -> 308 Resume Incomplete. The chunk body is
		// binary and intentionally not captured; status carries the parity.
		Scenario{Op: "tour_resumable_chunk", Service: "storage", Method: http.MethodPut,
			Path:             "${session}",
			Headers:          map[string]string{"Content-Range": fmt.Sprintf("bytes 0-%d/*", tourChunkSize-1)},
			Body:             string(tourPayload()[:tourChunkSize]),
			ContentType:      "application/octet-stream",
			NoRequestCapture: true},
		// Final chunk (last 256 KiB) -> 200 + the created object resource.
		Scenario{Op: "tour_resumable_finalize", Service: "storage", Method: http.MethodPut,
			Path:             "${session}",
			Headers:          map[string]string{"Content-Range": fmt.Sprintf("bytes %d-%d/%d", tourChunkSize, 2*tourChunkSize-1, 2*tourChunkSize)},
			Body:             string(tourPayload()[tourChunkSize:]),
			ContentType:      "application/octet-stream",
			NoRequestCapture: true},
		// Object metadata read: the checksum-bearing (crc32c/md5/size) observable.
		Scenario{Op: "tour_object_metadata", Service: "storage", Method: http.MethodGet,
			Path: "/storage/v1/b/" + bucket + "/o/resumable%2Fpayload.bin"},
		// Small media upload + chunked media download (alt=media).
		Scenario{Op: "tour_media_upload", Service: "storage", Method: http.MethodPost,
			Path:        "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + tourMediaObject,
			Body:        tourMediaBody,
			ContentType: "text/plain"},
		Scenario{Op: "tour_media_download", Service: "storage", Method: http.MethodGet,
			Path: "/storage/v1/b/" + bucket + "/o/" + tourMediaObject + "?alt=media"},
	)

	// The list-pagination fixture: 7 tiny objects, then four page reads
	// (pageSize=2 -> 2+2+2+1). Each page scenario saves its nextPageToken for
	// the next one; the token folds in the normalized path so the golden is
	// stable.
	for i := 0; i < tourPageCount; i++ {
		name := fmt.Sprintf("page%%2F%02d.txt", i)
		sc = append(sc, Scenario{
			Op: fmt.Sprintf("tour_page_upload_%02d", i), Service: "storage", Method: http.MethodPost,
			Path:        "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + name,
			Body:        fmt.Sprintf("page-%d", i),
			ContentType: "text/plain",
		})
	}
	for p := 1; p <= 4; p++ {
		// The GCS JSON API names the page-size parameter maxResults (pageSize
		// is silently ignored and returns the whole listing in one page).
		q := "/storage/v1/b/" + bucket + "/o?prefix=page%2F&maxResults=2"
		if p > 1 {
			q += "&pageToken=${pageToken}"
		}
		sc = append(sc, Scenario{
			Op: fmt.Sprintf("tour_list_page_%d", p), Service: "storage", Method: http.MethodGet,
			Path: q, Save: map[string]string{"pageToken": "nextPageToken"},
		})
	}

	// ─── BigQuery: dataset/table, insertAll + query, and a gs:// load job ────
	sc = append(sc,
		Scenario{Op: "tour_bq_dataset_create", Service: "bigquery", Method: http.MethodPost, Path: bqBase + "/datasets",
			Body: fmt.Sprintf(`{"datasetReference":{"projectId":%q,"datasetId":%q},"friendlyName":"sdk-tour"}`, project, n.TourBQDataset)},
		Scenario{Op: "tour_bq_table_create", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.TourBQDataset + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]}}`, project, n.TourBQDataset, n.TourBQTable)},
		Scenario{Op: "tour_bq_insertall", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.TourBQDataset + "/tables/" + n.TourBQTable + "/insertAll",
			Body: `{"rows":[{"insertId":"1","json":{"id":"1","name":"alice"}},{"insertId":"2","json":{"id":"2","name":"bob"}}]}`},
		Scenario{Op: "tour_bq_query", Service: "bigquery", Method: http.MethodPost, Path: bqBase + "/queries",
			Body: fmt.Sprintf(`{"query":"SELECT id, name FROM %s ORDER BY id","useLegacySql":false}`, bqQuote(project+"."+n.TourBQDataset+"."+n.TourBQTable))},
		// A load job reads the NDJSON object uploaded above.
		Scenario{Op: "tour_bq_load_object_upload", Service: "storage", Method: http.MethodPost,
			Path:        "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=load%2Frows.ndjson",
			Body:        tourLoadBody,
			ContentType: "application/x-ndjson"},
		Scenario{Op: "tour_bq_load_table_create", Service: "bigquery", Method: http.MethodPost,
			Path: bqBase + "/datasets/" + n.TourBQDataset + "/tables",
			Body: fmt.Sprintf(`{"tableReference":{"projectId":%q,"datasetId":%q,"tableId":%q},"schema":{"fields":[{"name":"id","type":"INTEGER"},{"name":"name","type":"STRING"}]}}`, project, n.TourBQDataset, n.TourBQTable+"_load")},
		Scenario{Op: "tour_bq_load_insert", Service: "bigquery", Method: http.MethodPost, Path: bqBase + "/jobs",
			Body: fmt.Sprintf(`{"jobReference":{"projectId":%q,"jobId":%q,"location":"US"},"configuration":{"load":{"sourceUris":["gs://%s/load/rows.ndjson"],"destinationTable":{"projectId":%q,"datasetId":%q,"tableId":%q},"sourceFormat":"NEWLINE_DELIMITED_JSON","writeDisposition":"WRITE_APPEND"}}}`,
				project, n.TourBQJob, bucket, project, n.TourBQDataset, n.TourBQTable+"_load")},
		// Poll the job until DONE; the final job resource is the golden.
		Scenario{Op: "tour_bq_load_poll", Service: "bigquery", Method: http.MethodGet,
			Path: bqBase + "/jobs/" + n.TourBQJob + "?location=US",
			Wait: &WaitSpec{Field: "status.state", Contains: "DONE", Interval: time.Second, Timeout: 60 * time.Second}},
		Scenario{Op: "tour_bq_load_tabledata", Service: "bigquery", Method: http.MethodGet,
			Path: bqBase + "/datasets/" + n.TourBQDataset + "/tables/" + n.TourBQTable + "_load/data"},
	)

	// ─── Cloud Dataproc: cluster-create LRO, its operation poll, and read ───
	// Dataproc's cluster create is a google.longrunning.Operation: real GCP
	// returns done=false at submit and the cluster settles asynchronously, so
	// the create response's operation name is saved and polled with
	// operations.get until done; a clusters.get then reads the settled cluster
	// back. Mirrors tests/integration/gcp/sdk-dataproc (create -> operation poll
	// -> get). The machine type and disks are pinned (pd-standard, 100 GiB) to
	// stay inside the parity project's Dataproc disk quota and to let the
	// emulator echo them verbatim; the zone is given as a full resource URL so
	// real GCP's canonicalized zoneUri matches.
	dpBase := "/v1/projects/" + project + "/regions/us-central1"
	dpMachine := "https://www.googleapis.com/compute/v1/projects/" + project +
		"/zones/us-central1-a/machineTypes/n1-standard-4"
	dpDisk := `{"bootDiskSizeGb":100,"bootDiskType":"pd-standard"}`
	dpConfig := fmt.Sprintf(
		`{"gceClusterConfig":{"zoneUri":"https://www.googleapis.com/compute/v1/projects/%s/zones/us-central1-a"},"softwareConfig":{"imageVersion":"2.2"},"masterConfig":{"machineTypeUri":%q,"diskConfig":%s},"workerConfig":{"numInstances":2,"machineTypeUri":%q,"diskConfig":%s}}`,
		project, dpMachine, dpDisk, dpMachine, dpDisk)
	sc = append(sc,
		Scenario{Op: "tour_dataproc_cluster_create", Service: "dataproc", Method: http.MethodPost,
			Path: dpBase + "/clusters",
			Body: fmt.Sprintf(`{"projectId":%q,"clusterName":%q,"config":%s}`, project, n.TourDataproc, dpConfig),
			Save: map[string]string{"dpOp": "name"}},
		Scenario{Op: "tour_dataproc_cluster_poll", Service: "dataproc", Method: http.MethodGet,
			Path: "/v1/${dpOp}",
			Wait: &WaitSpec{Field: "done", Interval: 2 * time.Second, Timeout: 15 * time.Minute}},
		Scenario{Op: "tour_dataproc_cluster_get", Service: "dataproc", Method: http.MethodGet,
			Path: dpBase + "/clusters/" + n.TourDataproc},
	)

	// The tour's IAM read-modify-write drives the Pub/Sub topic's IAM policy
	// over gRPC (google.iam.v1.IAMPolicy); the emulator exposes that surface
	// only on gRPC, so it lives in the gRPC tour set (tour_scenarios_grpc.go).

	return sc
}

// EnsureTourKMS creates the fixed, reusable resources the tour's KMS scenarios
// need (the global keyring and the asymmetric sign key). It is uncaptured setup:
// GCP cannot delete either, so fixed names are reused and ALREADY_EXISTS is
// success, exactly as EnsureKMS does for the symmetric key.
func (t *Target) EnsureTourKMS() error {
	base := "/v1/projects/" + t.Project + "/locations/global/keyRings"
	ring := base + "/" + FixedKMSKeyRing
	steps := []struct {
		desc, method, path, body string
	}{
		{"keyring", http.MethodPost, base + "?keyRingId=" + FixedKMSKeyRing, "{}"},
		{"asym key", http.MethodPost, ring + "/cryptoKeys?cryptoKeyId=" + FixedKMSCryptoKeyAsym,
			`{"purpose":"ASYMMETRIC_SIGN","versionTemplate":{"algorithm":"RSA_SIGN_PKCS1_2048_SHA256"}}`},
	}
	for _, s := range steps {
		status, body, err := t.request(s.method, "kms", s.path, s.body, "application/json")
		if err != nil {
			return fmt.Errorf("ensure tour kms %s: %w", s.desc, err)
		}
		switch status {
		case http.StatusOK, http.StatusConflict:
		default:
			if strings.Contains(string(body), "ALREADY_EXISTS") || strings.Contains(string(body), "already exists") {
				break
			}
			return fmt.Errorf("ensure tour kms %s: status %d: %s", s.desc, status, trimBody(body))
		}
	}
	return nil
}

// CleanupTour deletes every resource the REST tour scenario set creates. It is
// best-effort and idempotent (404s are ignored); KMS resources are excluded
// because GCP cannot delete them.
func (t *Target) CleanupTour() []string {
	n := t.Names
	bucket := n.TourBucket
	objects := []string{
		"resumable%2Fpayload.bin",
		tourMediaObject,
		"load%2Frows.ndjson",
	}
	for i := 0; i < tourPageCount; i++ {
		objects = append(objects, fmt.Sprintf("page%%2F%02d.txt", i))
	}
	bqBase := "/bigquery/v2/projects/" + t.Project
	type del struct {
		service, desc, method, path, body string
	}
	ops := []del{
		{"bigquery", "load table", http.MethodDelete, bqBase + "/datasets/" + n.TourBQDataset + "/tables/" + n.TourBQTable + "_load", ""},
		{"bigquery", "dataset", http.MethodDelete, bqBase + "/datasets/" + n.TourBQDataset + "?deleteContents=true", ""},
		// Dataproc cluster (the create scenario's LRO subject). Deleting also
		// tears down the cluster's VMs; the returned operation is not waited on
		// (best-effort cleanup, like the other deletes).
		{"dataproc", "cluster", http.MethodDelete, "/v1/projects/" + t.Project + "/regions/us-central1/clusters/" + n.TourDataproc, ""},
	}
	for _, o := range objects {
		ops = append(ops, del{"storage", "object " + o, http.MethodDelete, "/storage/v1/b/" + bucket + "/o/" + o, ""})
	}
	ops = append(ops, del{"storage", "bucket", http.MethodDelete, "/storage/v1/b/" + bucket, ""})

	var log []string
	for _, op := range ops {
		status, _, err := t.request(op.method, op.service, op.path, op.body, "application/json")
		if err != nil {
			log = append(log, fmt.Sprintf("cleanup tour %s/%s: error: %v", op.service, op.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("cleanup tour %s/%s: HTTP %d", op.service, op.desc, status))
	}
	return log
}

// VerifyTourAbsent reads back the principal tour resources and reports the
// observed status; a fully cleaned target reports 404 for all of them.
func (t *Target) VerifyTourAbsent() []string {
	n := t.Names
	bqBase := "/bigquery/v2/projects/" + t.Project
	checks := []struct{ service, desc, path string }{
		{"storage", "bucket", "/storage/v1/b/" + n.TourBucket},
		{"bigquery", "dataset", bqBase + "/datasets/" + n.TourBQDataset},
	}
	var log []string
	for _, c := range checks {
		status, _, err := t.request(http.MethodGet, c.service, c.path, "", "")
		if err != nil {
			log = append(log, fmt.Sprintf("verify tour %s/%s: error: %v", c.service, c.desc, err))
			continue
		}
		log = append(log, fmt.Sprintf("verify tour %s/%s: HTTP %d", c.service, c.desc, status))
	}
	return log
}

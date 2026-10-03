//go:build lakehouse_e2e

package lakehouse_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestLakehousePipelineK3d runs the vendor-neutral Medallion (multi-hop) ELT
// pipeline entirely inside the k3d cluster and verifies the published data
// product.
//
// Ingest seeds raw events into the emulator's GCS; Transform is a real PySpark
// job and Derive a real Spark SQL (sparkSqlJob) job, both submitted through the
// emulator's Dataproc API and executed as client-mode pods in the cluster (so
// the SQL hop exercises the real `spark-sql` engine); Publish copies the curated
// output to the serving bucket and writes a manifest.
//
// The test asserts both halves of the contract: the in-cluster Job's own
// success summary, and the actual bytes in the emulator's GCS (read back
// through a port-forward), so a pipeline that "succeeds" without producing
// data still fails.
func TestLakehousePipelineK3d(t *testing.T) {
	// The seed size, matching the RECORDS env the pipeline Job is rendered with.
	wantRecords := records()

	requireK3d(t)

	deleteJob(t)
	applyPipeline(t)
	t.Cleanup(func() {
		_, _ = kubectl("-n", namespace(), "delete", "job", jobName, "--ignore-not-found")
	})

	// Both Spark hops run as Dataproc jobs; the emulator waits for each before
	// the driver proceeds, so Job completion implies both reached DONE.
	waitJobComplete(t, 15*time.Minute)

	logs := jobLogs(t)
	if strings.Contains(logs, "PIPELINE_FAILED") {
		t.Fatalf("pipeline reported failure:\n%s", logs)
	}
	sum := parseSummary(t, logs)

	if !sum.OK {
		t.Fatalf("pipeline summary not OK: %+v", sum)
	}
	if sum.Pipeline != "medallion-elt" {
		t.Errorf("pipeline = %q, want medallion-elt", sum.Pipeline)
	}
	if got, want := strings.Join(sum.Stages, ","), "ingest,transform,derive,publish"; got != want {
		t.Errorf("stages = %q, want %q", got, want)
	}
	// The Derive hop must have run as a sparkSqlJob (this e2e's purpose: the real
	// spark-sql engine, not just the job API), with Transform still a pysparkJob.
	if got := sum.JobTypes["derive"]; got != "sparkSqlJob" {
		t.Errorf("derive job type = %q, want sparkSqlJob (job_types=%v)", got, sum.JobTypes)
	}
	if got := sum.JobTypes["transform"]; got != "pysparkJob" {
		t.Errorf("transform job type = %q, want pysparkJob (job_types=%v)", got, sum.JobTypes)
	}
	if sum.InputRows != wantRecords {
		t.Errorf("input_rows = %d, want %d", sum.InputRows, wantRecords)
	}
	if len(sum.IngestRegionCounts) == 0 {
		t.Fatalf("no ingest_region_counts in summary: %+v", sum)
	}
	if len(sum.CuratedObjects) == 0 {
		t.Fatalf("no curated objects in summary: %+v", sum)
	}
	if len(sum.PublishedObjects) != len(sum.CuratedObjects) {
		t.Errorf("published %d object(s), want %d (one per curated part)",
			len(sum.PublishedObjects), len(sum.CuratedObjects))
	}
	// The cluster is GKE-backed (virtualClusterConfig) and metastore-attached.
	if sum.Cluster == "" {
		t.Fatalf("summary has no cluster: %+v", sum)
	}
	if !strings.Contains(sum.MetastoreService, "/services/") {
		t.Errorf("metastore_service = %q, want a projects/.../services/... name", sum.MetastoreService)
	}
	// Both Spark jobs must have staged a real, non-empty driver-output object.
	if len(sum.DriverOutputs) != 2 {
		t.Errorf("driver_outputs = %v, want one per Spark job", sum.DriverOutputs)
	}
	for id, out := range sum.DriverOutputs {
		if !strings.HasPrefix(out.URI, "gs://") || out.Bytes == 0 {
			t.Errorf("driver output %s = %+v, want a non-empty gs:// object", id, out)
		}
	}

	base, stop := startPortForward(t)
	defer stop()

	// Independently confirm against the live emulator that the cluster the
	// pipeline used is GKE-backed and RUNNING (the summary is pipeline-asserted;
	// this reads the resource directly).
	cl := getJSON(t, base, "/v1/projects/"+testProject+"/regions/"+testRegion+"/clusters/"+sum.Cluster)
	if _, ok := cl["virtualClusterConfig"].(map[string]any); !ok {
		t.Errorf("cluster %s has no virtualClusterConfig: %v", sum.Cluster, cl)
	}
	if st, _ := cl["status"].(map[string]any); st == nil || st["state"] != "RUNNING" {
		t.Errorf("cluster %s status = %v, want RUNNING", sum.Cluster, cl["status"])
	}

	// The serving manifest is the pipeline's published contract.
	manifestRaw := getObject(t, base, publishedBucket, "manifest.json")
	var manifest struct {
		Pipeline         string   `json:"pipeline"`
		InputRows        int      `json:"input_rows"`
		PublishedObjects []string `json:"published_objects"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("parse published manifest: %v\n%s", err, manifestRaw)
	}
	if manifest.Pipeline != "medallion-elt" || manifest.InputRows != wantRecords {
		t.Errorf("published manifest mismatch: %+v", manifest)
	}

	// Read every published part and aggregate orders per region.
	orders := map[string]int{}
	for _, name := range sum.PublishedObjects {
		for _, line := range strings.Split(string(getObject(t, base, publishedBucket, name)), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var row struct {
				Region string `json:"region"`
				Orders int    `json:"orders"`
			}
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				t.Fatalf("parse published row %q: %v", line, err)
			}
			orders[row.Region] += row.Orders
		}
	}

	// The published aggregation must reproduce the ingested distribution: that
	// is what proves the two Spark hops preserved every record and grouped it
	// correctly (no silent row loss or miscount). The expected counts come from
	// the pipeline's own ingest-side tally, so the assertion stays correct as
	// the seed size changes.
	if len(orders) != len(sum.IngestRegionCounts) {
		t.Fatalf("published regions = %v, want %v", orders, sum.IngestRegionCounts)
	}
	total := 0
	for region, want := range sum.IngestRegionCounts {
		if got := orders[region]; got != want {
			t.Errorf("orders[%s] = %d, want %d", region, got, want)
		}
		total += want
	}
	if total != wantRecords {
		t.Fatalf("total orders = %d, want %d", total, wantRecords)
	}

	t.Logf("pipeline OK: %d input rows -> %d published object(s), orders=%v",
		sum.InputRows, len(sum.PublishedObjects), orders)
}

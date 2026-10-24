//go:build iceberg_k3d_e2e

package icebergk3d_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	k8shelpers "jaiscloud/internal/k8shelpers"
)

// TestDataprocIcebergStreamingK3d proves the demo's untested link: a
// Dataproc-submitted Spark Structured Streaming job that consumes the emulator's
// live Managed Kafka broker and sinks into an **Iceberg** table through the Hive
// Metastore catalog (`writeStream.toTable`, SPK2).
//
// The existing streaming gates write a `gs://` JSON file sink; the SPK1 batch
// gate proved the Iceberg-on-HMS recipe but only for a batch write. This gate
// joins them: it extends the SPK1 recipe with the Kafka connector and an Iceberg
// Structured Streaming append sink with a `gs://` checkpoint, then asserts the
// committed snapshot log grows while the job runs and stays readable.
//
// Flow:
//
//   - stage the Iceberg runtime + the Kafka connector closure on the emulator's
//     GCS and pass them in the job's jarFileUris
//   - create a Metastore service and a GKE-backed cluster attached to it; create
//     a live Managed Kafka cluster + topic (k8s broker mode) and seed CSV records
//   - submit a pysparkJob whose driver CREATEs the Iceberg table and runs
//     `readStream.format("kafka") ... writeStream.format("iceberg")
//     .toTable(...)` with a `gs://` checkpoint (kept RUNNING in awaitTermination)
//   - seed a second wave and assert Iceberg snapshots/data files grow under GCS
//   - cancel (assert CANCELLED + the driver k8s Job reaped), then read the table
//     back from a batch job: row count and snapshot count prove the committed
//     snapshots are readable
//   - delete the Kafka cluster and assert the broker Pod/Service are reaped
//
// Run with:
//
//	make test-e2e-iceberg-streaming-k3d
//
// It is inert without a k3d cluster (requireK3d skips), and skips when the
// deployed emulator is not running the k8s Kafka broker (bootstrapAddress is the
// synthesized cloud.goog name).
func TestDataprocIcebergStreamingK3d(t *testing.T) {
	requireK3d(t)

	base, stop := startPortForward(t)
	// Register the port-forward teardown first so it runs last (t.Cleanup is
	// LIFO): the resource cleanups below still need the forward.
	t.Cleanup(stop)

	// Unique per run so re-runs never collide with a prior run's bucket, HMS
	// database/table, cluster, Kafka cluster or job id in the shared
	// Postgres-backed store.
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	bucket := "iceberg-stream-" + run
	db := "iceberg_stream_" + run
	table := "trades"
	cluster := "iceberg-stream-cluster-" + run
	streamJob := "iceberg-stream-" + run
	readJob := "iceberg-stream-read-" + run
	mkCluster := "iceberg-stream-src-" + run
	group := "iceberg-stream-group-" + run
	const topic = "trades"
	const partitions = 3
	location := fmt.Sprintf("gs://%s/%s/%s", bucket, run, table)

	ensureBucket(t, base, bucket)
	t.Cleanup(func() { deleteBucketObjects(t, base, bucket) })

	icebergJarURI := stageIcebergJar(t, base, bucket)
	jarURIs := append([]string{icebergJarURI}, stageKafkaConnectorJars(t, base, bucket)...)

	msRef := createMetastoreService(t, base)
	createGkeCluster(t, base, cluster, msRef)
	t.Cleanup(func() { deleteCluster(t, base, cluster) })

	// Live Managed Kafka broker + topic. Skips when the broker is not k8s-mode.
	addr := createMKCluster(t, base, mkCluster)
	createMKTopic(t, base, mkCluster, topic, partitions)
	brokerPod := strings.SplitN(addr, ".", 2)[0]
	clusterNS := mkClusterNamespace(mkCluster)

	// Stage the streaming driver. It creates the Iceberg table (the streaming
	// writeStream requires the table to exist) and then streams Kafka into it.
	uploadText(t, base, bucket, "stream_iceberg.py",
		icebergStreamingScript(bucket, db, table, location, addr, topic, group))
	uploadText(t, base, bucket, "read_iceberg.py", readScript(db, table))

	submitIcebergStreamingJob(t, base, bucket, cluster, streamJob, jarURIs, icebergStreamingProps(bucket, run))
	waitForState(t, base, streamJob, "RUNNING", 5*time.Minute)

	// First wave: the stream commits an Iceberg snapshot with Parquet data files
	// under the table LOCATION, while the job stays RUNNING.
	firstTotal := produceWave(t, clusterNS, brokerPod, topic, partitions, 2)
	snaps1, data1 := waitForIcebergCommit(t, base, streamJob, bucket, run, table, cluster, 1, 1, 8*time.Minute)
	t.Logf("after wave 1: iceberg snapshots=%d data=%d (produced %d records)", snaps1, data1, firstTotal)

	// Second wave: the append sink commits again, so the committed snapshot log
	// and data files grow while the job is still RUNNING. This is the append
	// semantics check: each micro-batch is a new snapshot, not an overwrite.
	secondTotal := produceWave(t, clusterNS, brokerPod, topic, partitions, 1)
	snaps2, data2 := waitForIcebergCommit(t, base, streamJob, bucket, run, table, cluster,
		snaps1+1, data1+1, 8*time.Minute)
	if snaps2 <= snaps1 || data2 <= data1 {
		t.Fatalf("iceberg snapshot/data files did not grow after wave 2 (snaps %d->%d, data %d->%d)",
			snaps1, snaps2, data1, data2)
	}
	t.Logf("after wave 2: iceberg snapshots=%d data=%d (produced %d more records)", snaps2, data2, secondTotal)

	// The streaming job must still be RUNNING (a batch job would be DONE).
	if got := jobState(t, base, streamJob); got != "RUNNING" {
		t.Fatalf("streaming job state = %q after two waves, want RUNNING", got)
	}

	// Cancel: the store settles CANCELLED and the driver k8s Job is reaped.
	code, resp := api(t, http.MethodPost, base+jobPath(streamJob)+":cancel", nil)
	mustOK(t, "cancel iceberg streaming job "+streamJob, code, resp)
	waitForState(t, base, streamJob, "CANCELLED", 2*time.Minute)
	waitDriverReaped(t, streamJob, 90*time.Second)

	// Read the table back from a batch job to prove the committed snapshots are
	// readable: the row count is every record from both waves and the snapshot
	// count reflects the appended micro-batches. (The read runs after the cancel
	// because the k3d node is memory-tight — a streaming driver + executor plus a
	// second read job's driver + executor does not fit alongside the Kafka broker
	// — the growth *while running* is asserted above on the committed GCS log.)
	submitPySparkJob(t, base, cluster, readJob, fmt.Sprintf("gs://%s/read_iceberg.py", bucket),
		icebergJarURI, icebergCatalogProps(bucket, run))
	waitForState(t, base, readJob, "DONE", 10*time.Minute)
	out := driverOutput(t, base, readJob)
	rows := parseMetric(t, out, "ICEBERG_ROWS")
	snapsRead := parseMetric(t, out, "ICEBERG_SNAPSHOTS")
	if rows < firstTotal+secondTotal {
		t.Fatalf("read-back rows = %d, want >= %d (both waves)\n%s", rows, firstTotal+secondTotal, out)
	}
	if snapsRead < 2 {
		t.Fatalf("read-back snapshots = %d, want >= 2 (append micro-batches)\n%s", snapsRead, out)
	}

	// Deleting the Kafka cluster reaps the broker Pod and Service.
	deleteMKCluster(t, base, mkCluster)
	waitForResourceGone(t, clusterNS, "svc", brokerPod, 60*time.Second)
	waitForResourceGone(t, clusterNS, "pod", brokerPod, 90*time.Second)

	t.Logf("iceberg streaming sink OK: rows=%d snapshots=%d (iceberg) data=%d (gs://%s/%s/%s)",
		rows, snapsRead, data2, bucket, run, table)
}

// icebergStreamingScript is the PySpark Structured Streaming driver staged to
// GCS. It creates the HiveCatalog/Iceberg table (a streaming `toTable` requires
// an existing table), then reads the Kafka source (loaded from the staged
// connector jars) with a `gs://` checkpoint and appends into the Iceberg table
// through the emulator's HadoopFileIO `gs://` data path, blocking forever in
// awaitTermination so the Dataproc job stays RUNNING until cancelled.
func icebergStreamingScript(bucket, db, table, location, bootstrap, topic, group string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS hms.%s")
spark.sql(
    "CREATE TABLE IF NOT EXISTS hms.%s.%s "
    "(symbol string, price double, size int) "
    "USING iceberg LOCATION '%s'"
)
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", %q)
    .option("subscribe", %q)
    .option("startingOffsets", "earliest")
    .option("kafka.group.id", %q)
    .load()
)
parsed = (
    raw.select(
        F.from_csv(F.col("value").cast("string"),
                   "symbol string, price double, size int").alias("r")
    )
    .select("r.symbol", "r.price", "r.size")
)
(
    parsed.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "gs://%s/iceberg-checkpoint")
    .trigger(processingTime="5 seconds")
    .toTable("hms.%s.%s")
    .awaitTermination()
)
`, db, db, table, location, bootstrap, topic, group, bucket, db, table)
}

// icebergStreamingProps extends the SPK1 catalog props with the streaming
// checkpoint marker (which also marks the job long-running for the emulator's
// lifecycle model) and a JVM memory overhead. The overhead sizes the executor
// container so the JVM's off-heap use (Kafka + Netty + GCS + Hadoop clients) is
// covered — at Spark 3.5's 450m heap the default overhead is too tight. (The
// client-mode driver pod has no memory limit, so on the memory-tight k3d node
// the driver can still be node-OOM-killed; see the README caveat.)
func icebergStreamingProps(bucket, run string) map[string]any {
	props := icebergCatalogProps(bucket, run)
	props["spark.sql.streaming.checkpointLocation"] = fmt.Sprintf("gs://%s/iceberg-checkpoint", bucket)
	props["spark.driver.memoryOverhead"] = "1g"
	props["spark.executor.memoryOverhead"] = "1g"
	return props
}

// submitIcebergStreamingJob submits the streaming pysparkJob with the Iceberg
// runtime + Kafka connector jars in jarFileUris.
func submitIcebergStreamingJob(t *testing.T, base, bucket, cluster, jobID string, jarURIs []string, props map[string]any) {
	t.Helper()
	body := map[string]any{
		"job": map[string]any{
			"reference": map[string]any{"jobId": jobID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": fmt.Sprintf("gs://%s/stream_iceberg.py", bucket),
				"jarFileUris":       jarURIs,
				"properties":        props,
			},
		},
	}
	code, resp := api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/jobs:submit", testProject, testRegion), body)
	mustOK(t, "submit iceberg streaming job "+jobID, code, resp)
	if _, ok := resp["pysparkJob"]; !ok {
		t.Fatalf("submit response has no pysparkJob: %v", resp)
	}
}

// waitForIcebergCommit polls the table's GCS LOCATION until at least minSnaps
// Iceberg snapshots (manifest-list `snap-*.avro` files) and minData Parquet data
// files exist, plus at least one Spark checkpoint commit under the gs://
// checkpoint (the checkpoint commit file can land just after the Iceberg
// metadata commit), asserting the streaming job never left RUNNING. It returns
// the observed (snapshots, data) counts.
func waitForIcebergCommit(t *testing.T, base, jobID, bucket, run, table string, cluster string, minSnaps, minData int, timeout time.Duration) (int, int) {
	t.Helper()
	prefix := run + "/" + table + "/"
	deadline := time.Now().Add(timeout)
	var snaps, data int
	for time.Now().Before(deadline) {
		if got := jobState(t, base, jobID); got != "RUNNING" {
			dumpSparkDiagnostics(t, cluster)
			t.Fatalf("iceberg streaming job left RUNNING for %q before snapshots>=%d data>=%d\n%s",
				got, minSnaps, minData, driverOutput(t, base, jobID))
		}
		snaps = countObjects(t, base, bucket, prefix+"metadata/", "snap-")
		data = countObjects(t, base, bucket, prefix+"data/", ".parquet")
		commits := batchIDs(t, base, bucket, "iceberg-checkpoint/commits/")
		if snaps >= minSnaps && data >= minData && len(commits) > 0 {
			return snaps, data
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("iceberg table did not reach snapshots>=%d data>=%d within %s (got snapshots=%d data=%d) under gs://%s/%s",
		minSnaps, minData, timeout, snaps, data, bucket, prefix)
	return snaps, data
}

// sparkNamespace is the per-cluster namespace Dataproc provisions for a
// cluster's driver/executor pods.
func sparkNamespace(cluster string) string {
	return k8shelpers.NamespaceName("dataproc", testProject, cluster)
}

// dumpSparkDiagnostics logs the Dataproc namespace's pods/events and the node
// memory when a streaming job dies, to classify OOMKilled (node vs cgroup).
func dumpSparkDiagnostics(t *testing.T, cluster string) {
	t.Helper()
	ns := sparkNamespace(cluster)
	if out, err := kubectl("-n", ns, "get", "pods", "-o", "wide"); err == nil {
		t.Logf("dataproc pods (%s):\n%s", ns, out)
	}
	if out, err := kubectl("top", "node", "--no-headers"); err == nil {
		t.Logf("node top: %s", strings.TrimSpace(out))
	}
	if out, err := kubectl("-n", ns, "get", "events", "--sort-by=.lastTimestamp"); err == nil {
		t.Logf("dataproc events (%s):\n%s", ns, out)
	}
}

// waitForResourceGone asserts a namespaced resource is reaped.
func waitForResourceGone(t *testing.T, ns, kind, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := kubectl("-n", ns, "get", kind, name); err != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s/%s was not reaped within %s", kind, name, timeout)
}

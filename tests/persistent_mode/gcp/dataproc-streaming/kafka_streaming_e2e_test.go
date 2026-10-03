//go:build dataproc_streaming_e2e

// This file is the real-Kafka counterpart to the `rate`-source smoke in
// streaming_e2e_test.go (STR7): it proves a genuine Spark Structured Streaming
// Kafka source can consume from the live Managed Kafka (Redpanda) broker the
// emulator provisions.
//
// Unlike the Pub/Sub path, Spark's Kafka source is a first-party connector, but
// it is NOT bundled in apache/spark:3.5.0 (the deployed spark-gcs image ships
// only the GCS connector). The gate therefore stages the connector and its
// transitives on the emulator's own GCS and passes them through the Dataproc
// PySparkJob `jarFileUris` field — exactly how a real Dataproc Kafka job loads
// the connector (STR1 maps `jarFileUris` -> `spark-submit --jars`), so this also
// exercises that feature end-to-end.
//
// Flow:
//
//   - create a Managed Kafka cluster (k8s broker mode) + 3-partition topic via
//     the REST API; the broker advertises <svc>.<ns>.svc.cluster.local:9092
//   - seed records with `rpk topic produce` inside the broker Pod
//   - submit a pysparkJob running readStream.format("kafka") against the live
//     bootstrapAddress with a gs:// checkpoint and a gs:// JSON sink
//   - assert the job stays RUNNING, checkpoint offsets/commits and sink part
//     files appear, seed a second wave and assert the micro-batch and the
//     checkpoint-log consumer offsets advance
//   - cancel, assert CANCELLED, and assert the driver k8s Job is reaped; delete
//     the Kafka cluster and assert the broker Pod/Service are reaped
//
// Run with:
//
//	make test-dataproc-streaming-kafka
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent, and the test skips when the deployed
// emulator is not running the k8s broker (bootstrapAddress is the synthesized
// cloud.goog name).
package dataprocstreaming_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Managed Kafka control-plane client. The broker startup (image pull + ready
// wait) is synchronous with cluster create, so it needs a longer timeout than
// the Dataproc-facing httpClient.
var mkCreateClient = &http.Client{Timeout: 5 * time.Minute}

// downloadClient streams connector jars from Maven Central; the default
// httpClient timeout is too tight for a cold multi-MB fetch.
var downloadClient = &http.Client{Timeout: 3 * time.Minute}

// mkAPI performs a JSON request against the Managed Kafka REST API with the
// given client (cluster create must use mkCreateClient).
func mkAPI(t *testing.T, client *http.Client, method, rawURL string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, rawURL, rd)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, rawURL, err)
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out
}

// mkClusterPath is the Managed Kafka cluster resource path (locations, not the
// Dataproc regions path).
func mkClusterPath(cluster string) string {
	return fmt.Sprintf("/v1/projects/%s/locations/%s/clusters/%s", testProject, testRegion, cluster)
}

// createMKCluster creates a Managed Kafka cluster and returns the live broker's
// bootstrapAddress. It skips (rather than fails) when the deployed emulator is
// not running the k8s broker, so the gate stays inert on a metadata-only
// deployment.
func createMKCluster(t *testing.T, base, cluster string) string {
	t.Helper()
	deleteMKCluster(t, base, cluster)
	t.Cleanup(func() { deleteMKCluster(t, base, cluster) })

	code, body := mkAPI(t, mkCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{})
	if code < 200 || code >= 300 {
		t.Fatalf("create managed kafka cluster %s: HTTP %d: %v", cluster, code, body)
	}
	code, cl := mkAPI(t, httpClient, http.MethodGet, base+mkClusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get managed kafka cluster %s: HTTP %d: %v", cluster, code, cl)
	}
	addr := strField(cl, "bootstrapAddress")
	if addr == "" {
		t.Fatalf("managed kafka cluster %s has no bootstrapAddress: %v", cluster, cl)
	}
	if strings.HasSuffix(addr, ".cloud.goog") {
		t.Skipf("managed kafka broker is not in k8s mode (bootstrapAddress=%q) — run the emulator with JAISCLOUD_KAFKA_BROKER_MODE=k8s", addr)
	}
	return addr
}

func deleteMKCluster(t *testing.T, base, cluster string) {
	t.Helper()
	code, body := mkAPI(t, mkCreateClient, http.MethodDelete, base+mkClusterPath(cluster), nil)
	if code >= 300 && code != http.StatusNotFound {
		t.Logf("cleanup: delete managed kafka cluster %s: HTTP %d: %v", cluster, code, body)
	}
}

func createMKTopic(t *testing.T, base, cluster, topic string, partitions int) {
	t.Helper()
	code, body := mkAPI(t, httpClient, http.MethodPost,
		base+mkClusterPath(cluster)+"/topics?topicId="+topic,
		map[string]any{"partitionCount": partitions, "replicationFactor": 1})
	if code < 200 || code >= 300 {
		t.Fatalf("create topic %s: HTTP %d: %v", topic, code, body)
	}
}

// produceKafkaRecords produces values to one topic partition by running rpk
// inside the broker Pod (the Redpanda image ships it; the advertised
// <svc>.<ns>.svc.cluster.local listener is not reachable host-side).
func produceKafkaRecords(t *testing.T, brokerPod, topic string, partition int, values []string) {
	t.Helper()
	args := []string{"-n", namespace(), "exec", "-i", brokerPod, "-c", "redpanda", "--",
		"rpk", "topic", "produce", topic,
		"-p", strconv.Itoa(partition),
		"--brokers", "127.0.0.1:9092",
	}
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(strings.Join(values, "\n") + "\n")
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("rpk produce to %s partition %d: %v\n--- stdout ---\n%s\n--- stderr ---\n%s",
			topic, partition, err, out.String(), errb.String())
	}
}

// produceWave seeds rounds×partitions records, spread one round per partition.
func produceWave(t *testing.T, brokerPod, topic string, partitions, perPartition int) int {
	t.Helper()
	for p := 0; p < partitions; p++ {
		values := make([]string, perPartition)
		for i := range values {
			values[i] = fmt.Sprintf("rec-%d-%d", p, i)
		}
		produceKafkaRecords(t, brokerPod, topic, p, values)
	}
	return partitions * perPartition
}

// kafkaConnectorJars is the minimal connector closure. Everything else
// spark-sql-kafka-0-10 and kafka-clients need (jsr305, lz4-java, snappy-java,
// zstd-jni, slf4j, spark-tags) is already in apache/spark:3.5.0; commons-pool2
// is the one dependency the image carries only as the incompatible 1.x line.
var kafkaConnectorJars = []struct {
	name string
	url  string
}{
	{
		"spark-sql-kafka-0-10_2.12-3.5.0.jar",
		"https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar",
	},
	{
		"spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
		"https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
	},
	{
		"kafka-clients-3.4.1.jar",
		"https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
	},
	{
		"commons-pool2-2.11.1.jar",
		"https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
	},
}

// stageConnectorJars downloads the connector closure (host-side, cached through
// KAFKA_CONNECTOR_JARS_DIR for offline runs) and uploads each jar into the
// run's bucket, returning the gs:// URIs for pysparkJob.jarFileUris.
func stageConnectorJars(t *testing.T, base, bucket string) []string {
	t.Helper()
	dir := os.Getenv("KAFKA_CONNECTOR_JARS_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	uris := make([]string, 0, len(kafkaConnectorJars))
	for _, jar := range kafkaConnectorJars {
		path := filepath.Join(dir, jar.name)
		if _, err := os.Stat(path); err != nil {
			downloadFile(t, jar.url, path)
		}
		uploadFileObject(t, base, bucket, "jars/"+jar.name, path)
		uris = append(uris, fmt.Sprintf("gs://%s/jars/%s", bucket, jar.name))
	}
	return uris
}

func downloadFile(t *testing.T, url, dest string) {
	t.Helper()
	resp, err := downloadClient.Get(url)
	if err != nil {
		t.Fatalf("download %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download %s: HTTP %d", url, resp.StatusCode)
	}
	f, err := os.Create(dest)
	if err != nil {
		t.Fatalf("create %s: %v", dest, err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		t.Fatalf("download %s: write: %v", url, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", dest, err)
	}
}

// uploadFileObject uploads a local file as a GCS object (media upload, JSON
// API).
func uploadFileObject(t *testing.T, base, bucket, name, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	q := url.Values{"uploadType": {"media"}, "name": {name}}.Encode()
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", base, bucket, q), f)
	if err != nil {
		t.Fatalf("build upload %s: %v", name, err)
	}
	req.Header.Set("Content-Type", "application/java-archive")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("upload %s: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload %s/%s: HTTP %d: %s", bucket, name, resp.StatusCode, raw)
	}
}

// kafkaStreamingScript is the PySpark Structured Streaming driver staged to
// GCS. It reads the Kafka source (loaded from the staged connector jars),
// writes JSON parts to a gs:// sink with a gs:// checkpoint, tracks consumer
// offsets in the checkpoint's offset log (Spark's Kafka source rejects
// broker-side auto-commit — see the source-provider option validation), and
// blocks forever in awaitTermination so the Dataproc job stays RUNNING until
// cancelled.
func kafkaStreamingScript(bucket, bootstrap, topic, group string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", %q)
    .option("subscribe", %q)
    .option("startingOffsets", "earliest")
    .option("kafka.group.id", %q)
    .load()
)
out = df.selectExpr("CAST(value AS STRING) AS v")
(
    out.writeStream.format("json")
    .option("path", "gs://%s/kafka-sink")
    .option("checkpointLocation", "gs://%s/kafka-checkpoint")
    .trigger(processingTime="2 seconds")
    .start()
    .awaitTermination()
)
`, bootstrap, topic, group, bucket, bucket)
}

// submitKafkaStreamingJob submits the pysparkJob with the connector jars in
// jarFileUris and returns the job id.
func submitKafkaStreamingJob(t *testing.T, base, bucket, cluster, jobID string, jarURIs []string) {
	t.Helper()
	body := map[string]any{
		"job": map[string]any{
			"reference": map[string]any{"jobId": jobID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": fmt.Sprintf("gs://%s/kafka_stream.py", bucket),
				"jarFileUris":       jarURIs,
				"properties": map[string]any{
					"spark.sql.streaming.checkpointLocation": fmt.Sprintf("gs://%s/kafka-checkpoint", bucket),
					// The e2e host is memory-tight (a co-located Redpanda uses
					// 1G). Use Spark's 450m minimum heap but keep the k8s
					// executor container limit (heap + overhead) generous enough
					// for the JVM's off-heap usage, or it is cgroup-OOMKilled.
					"spark.driver.memory":           "450m",
					"spark.driver.memoryOverhead":   "512m",
					"spark.executor.memory":         "450m",
					"spark.executor.memoryOverhead": "512m",
					"spark.executor.instances":      "1",
					"spark.sql.shuffle.partitions":  "1",
				},
			},
		},
	}
	code, resp := api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/jobs:submit", testProject, testRegion), body)
	mustOK(t, "submit kafka streaming job "+jobID, code, resp)
	if _, ok := resp["pysparkJob"]; !ok {
		t.Fatalf("submit response has no pysparkJob: %v", resp)
	}
}

// batchIDs returns the numeric micro-batch ids under a checkpoint subprefix
// (e.g. kafka-checkpoint/commits/), sorted ascending.
func batchIDs(t *testing.T, base, bucket, prefix string) []int64 {
	t.Helper()
	names := listObjects(t, base, bucket, prefix)
	ids := make([]int64, 0, len(names))
	for _, n := range names {
		short := n[strings.LastIndex(n, "/")+1:]
		if id, err := strconv.ParseInt(short, 10, 64); err == nil {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// waitForBatch waits until a micro-batch newer than after has committed (both
// checkpoint offsets and commits, plus a sink part file), asserting the job
// never left RUNNING. It returns the newest committed batch id.
func waitForBatch(t *testing.T, base, jobID, bucket string, after int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if got := jobState(t, base, jobID); got != "RUNNING" {
			t.Fatalf("kafka streaming job left RUNNING for %q before a new micro-batch committed", got)
		}
		offsets := batchIDs(t, base, bucket, "kafka-checkpoint/offsets/")
		commits := batchIDs(t, base, bucket, "kafka-checkpoint/commits/")
		sink := listObjects(t, base, bucket, "kafka-sink/")
		if len(offsets) > 0 && len(commits) > 0 &&
			offsets[len(offsets)-1] > after && commits[len(commits)-1] > after &&
			hasCommittedPartFile(sink) {
			return commits[len(commits)-1]
		}
		last = fmt.Sprintf("offsets=%v commits=%v sink=%v", offsets, commits, sink)
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("no kafka micro-batch after id %d within %s (%s)", after, timeout, last)
	return after
}

// readObjectText fetches a GCS object's bytes through the JSON API (alt=media).
func readObjectText(t *testing.T, base, bucket, name string) string {
	t.Helper()
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", base, bucket, url.PathEscape(name))
	resp, err := httpClient.Get(u)
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, name, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("read %s/%s: HTTP %d: %s", bucket, name, resp.StatusCode, raw)
	}
	return string(raw)
}

// kafkaOffsetTotal reads one checkpoint offset-log entry and sums the Kafka
// consumer offsets it records. The OffsetSeqLog file is line-based:
//
//	v1
//	<metadata json>
//	{"orders":{"0":4,"1":4,"2":4}}   // KafkaSourceOffset.json, one line
//
// Spark's Kafka source keeps offsets in this log rather than committing to the
// broker (it rejects `kafka.enable.auto.commit`), so this is the authoritative
// consumer-offset position.
func kafkaOffsetTotal(t *testing.T, base, bucket string, batchID int64) int64 {
	t.Helper()
	name := fmt.Sprintf("kafka-checkpoint/offsets/%d", batchID)
	text := readObjectText(t, base, bucket, name)
	var total int64
	for i, line := range strings.Split(text, "\n") {
		if i < 2 { // "v1" version line + metadata line
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" || line == "-" {
			continue
		}
		var byTopic map[string]map[string]int64
		if err := json.Unmarshal([]byte(line), &byTopic); err != nil {
			continue
		}
		for _, parts := range byTopic {
			for _, off := range parts {
				total += off
			}
		}
	}
	if total == 0 {
		t.Fatalf("no Kafka offsets parsed from gs://%s/%s: %q", bucket, name, text)
	}
	return total
}

// waitForKafkaOffsetTotal polls the newest committed batch's checkpoint offsets
// until they reach want, asserting the job stays RUNNING.
func waitForKafkaOffsetTotal(t *testing.T, base, jobID, bucket string, want int, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		if got := jobState(t, base, jobID); got != "RUNNING" {
			t.Fatalf("kafka streaming job left RUNNING for %q before checkpoint offsets reached %d", got, want)
		}
		commits := batchIDs(t, base, bucket, "kafka-checkpoint/commits/")
		if len(commits) > 0 {
			last = kafkaOffsetTotal(t, base, bucket, commits[len(commits)-1])
			if last >= int64(want) {
				return last
			}
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("kafka checkpoint consumer offsets = %d, want >= %d within %s", last, want, timeout)
	return last
}

// waitForResourceGone asserts a namespaced resource is reaped.
func waitForResourceGone(t *testing.T, kind, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := kubectl("-n", namespace(), "get", kind, name); err != nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s/%s was not reaped within %s", kind, name, timeout)
}

func TestDataprocKafkaStreamingK3d(t *testing.T) {
	requireK3d(t)

	base, stop := startPortForward(t)
	t.Cleanup(stop)

	// Unique per run: the emulator store is shared Postgres, so re-runs must not
	// collide on bucket, cluster, topic, group or job id.
	run := fmt.Sprintf("%d", time.Now().Unix())
	bucket := "kafka-stream-" + run
	dataprocCluster := "kafka-stream-cluster-" + run
	mkCluster := "kafka-src-" + run
	group := "spark-kafka-group-" + run
	jobID := "kafka-job-" + run
	const (
		topic      = "orders"
		partitions = 3
	)

	ensureBucket(t, base, bucket)
	t.Cleanup(func() { deleteBucketObjects(t, base, bucket) })

	// Live Managed Kafka broker + topic.
	addr := createMKCluster(t, base, mkCluster)
	createMKTopic(t, base, mkCluster, topic, partitions)
	brokerPod := strings.SplitN(addr, ".", 2)[0]

	// Stage the connector closure and the driver on the emulator's GCS.
	jarURIs := stageConnectorJars(t, base, bucket)
	uploadText(t, base, bucket, "kafka_stream.py", kafkaStreamingScript(bucket, addr, topic, group))

	createCluster(t, base, dataprocCluster)
	t.Cleanup(func() { deleteCluster(t, base, dataprocCluster) })

	// Seed the first wave before submitting; Spark reads from earliest in its
	// own consumer group, so the records are waiting for the first micro-batch.
	firstTotal := produceWave(t, brokerPod, topic, partitions, 4)

	submitKafkaStreamingJob(t, base, bucket, dataprocCluster, jobID, jarURIs)
	waitForState(t, base, jobID, "RUNNING", 5*time.Minute)

	// The first non-empty micro-batch commits under the gs:// checkpoint and
	// lands output in the gs:// sink, with the checkpoint offset log recording
	// the full first wave.
	firstBatch := waitForBatch(t, base, jobID, bucket, -1, 6*time.Minute)
	if got := kafkaOffsetTotal(t, base, bucket, firstBatch); got < int64(firstTotal) {
		t.Fatalf("first batch %d checkpoint offsets = %d, want >= %d (first wave)", firstBatch, got, firstTotal)
	}
	t.Logf("kafka stream first micro-batch %d committed (sink objects=%v)",
		firstBatch, listObjects(t, base, bucket, "kafka-sink/"))

	// A second wave must advance the stream: a new micro-batch commits and the
	// checkpoint consumer offsets catch up to everything produced.
	secondTotal := produceWave(t, brokerPod, topic, partitions, 2)
	waitForBatch(t, base, jobID, bucket, firstBatch, 6*time.Minute)
	waitForKafkaOffsetTotal(t, base, jobID, bucket, firstTotal+secondTotal, 3*time.Minute)

	if got := jobState(t, base, jobID); got != "RUNNING" {
		t.Fatalf("job state = %q after micro-batches, want RUNNING", got)
	}

	// Cancel: the store settles CANCELLED and the driver k8s Job/pod is reaped.
	code, resp := api(t, http.MethodPost, base+jobPath(jobID)+":cancel", nil)
	mustOK(t, "cancel kafka streaming job "+jobID, code, resp)
	waitForState(t, base, jobID, "CANCELLED", 2*time.Minute)
	waitDriverReaped(t, jobID, 90*time.Second)

	// Deleting the Kafka cluster reaps the broker Pod and Service.
	deleteMKCluster(t, base, mkCluster)
	waitForResourceGone(t, "svc", brokerPod, 60*time.Second)
	waitForResourceGone(t, "pod", brokerPod, 90*time.Second)
}

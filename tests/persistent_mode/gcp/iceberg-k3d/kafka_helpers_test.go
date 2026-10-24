//go:build iceberg_k3d_e2e

package icebergk3d_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkabroker "jaiscloud/internal/gcp/broker/kafka"
)

// This file carries the Managed Kafka + connector-staging helpers the SPK2
// streaming gate (streaming_e2e_test.go) needs on top of the SPK1 Iceberg/HMS
// machinery in helpers_test.go. They mirror the ones in
// tests/persistent_mode/gcp/dataproc-streaming/ (the Kafka-source streaming
// gate), which lives in its own package and cannot be imported.

// mkCreateClient is the Managed Kafka control-plane client. Cluster startup
// (image pull + ready wait) is synchronous with cluster create, so it needs a
// longer timeout than the Dataproc-facing httpClient.
var mkCreateClient = &http.Client{Timeout: 5 * time.Minute}

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

// mkClusterNamespace is the per-cluster namespace the broker manager provisions
// for a Managed Kafka cluster (the KNS1 seam); the broker Pod/Service live there,
// not in the emulator's namespace.
func mkClusterNamespace(cluster string) string {
	return kafkabroker.ClusterNamespace(testProject, testRegion, cluster)
}

// produceKafkaRecords produces values to one topic partition by running rpk
// inside the broker Pod (the Redpanda image ships it; the advertised
// <svc>.<ns>.svc.cluster.local listener is not reachable host-side).
func produceKafkaRecords(t *testing.T, ns, brokerPod, topic string, partition int, values []string) {
	t.Helper()
	args := []string{"-n", ns, "exec", "-i", brokerPod, "-c", "redpanda", "--",
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

// produceWave seeds rounds×partitions records, spread one round per partition,
// using the "symbol,price,size" CSV shape the streaming driver parses.
func produceWave(t *testing.T, ns, brokerPod, topic string, partitions, perPartition int) int {
	t.Helper()
	for p := 0; p < partitions; p++ {
		values := make([]string, perPartition)
		for i := range values {
			values[i] = fmt.Sprintf("SYM%d,%.2f,%d", p, 100.0+float64(i), i+1)
		}
		produceKafkaRecords(t, ns, brokerPod, topic, p, values)
	}
	return partitions * perPartition
}

// kafkaConnectorJars is the minimal connector closure for a Spark Structured
// Streaming Kafka source. Everything else spark-sql-kafka-0-10 and kafka-clients
// need (jsr305, lz4-java, snappy-java, zstd-jni, slf4j, spark-tags) is already
// in apache/spark:3.5.0; commons-pool2 is the one dependency the image carries
// only as the incompatible 1.x line.
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

// stageKafkaConnectorJars downloads the connector closure (host-side, cached
// through KAFKA_CONNECTOR_JARS_DIR for offline runs) and uploads each jar into
// the run's bucket, returning the gs:// URIs for pysparkJob.jarFileUris.
func stageKafkaConnectorJars(t *testing.T, base, bucket string) []string {
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

// ─── object-prefix helpers ─────────────────────────────────────────────────

// countObjects returns how many object names under bucket/prefix contain sub.
func countObjects(t *testing.T, base, bucket, prefix, sub string) int {
	t.Helper()
	n := 0
	for _, name := range listObjects(t, base, bucket, prefix) {
		if strings.Contains(name, sub) {
			n++
		}
	}
	return n
}

// batchIDs returns the numeric micro-batch ids under a checkpoint subprefix
// (e.g. iceberg-checkpoint/commits/), sorted ascending.
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

// ─── client-mode driver reap helpers ───────────────────────────────────────

// driverJobNames returns the names of the client-mode k8s Jobs for a job id. A
// listing error (kubectl/API unavailable) is returned, never folded into an
// empty result, so the reap assertion cannot pass spuriously.
func driverJobNames(t *testing.T, jobID string) ([]string, error) {
	t.Helper()
	out, err := kubectl("-n", namespace(), "get", "jobs",
		"-l", "jaiscloud.io/job-id="+jobID,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			names = append(names, l)
		}
	}
	return names, nil
}

func driverPodNames(t *testing.T, jobID string) []string {
	t.Helper()
	out, err := kubectl("-n", namespace(), "get", "pods",
		"-l", "jaiscloud.io/job-id="+jobID,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil
	}
	var names []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			names = append(names, l)
		}
	}
	return names
}

// waitDriverReaped asserts the client-mode k8s Job for jobID is gone after
// cancel. A listing error is fatal: "could not list" must never be mistaken for
// "reaped". k8shelpers.Cancel uses foreground deletion, so the Job (and its pod)
// is only removed once the driver has actually stopped; any remaining pod is
// logged.
func waitDriverReaped(t *testing.T, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		names, err := driverJobNames(t, jobID)
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		lastErr = nil
		if len(names) == 0 {
			if pods := driverPodNames(t, jobID); len(pods) > 0 {
				t.Logf("driver Job reaped but pod(s) still present (may be terminating): %v", pods)
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("could not confirm the driver k8s Job reap for job %s: %v", jobID, lastErr)
	}
	t.Fatalf("cancel did not reap the driver k8s Job for job %s", jobID)
}

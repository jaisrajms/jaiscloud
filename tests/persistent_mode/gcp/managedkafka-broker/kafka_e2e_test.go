//go:build managedkafka_broker_e2e

// This file is the real-Kafka data-plane gate (W2.3/MK6): it proves a genuine
// Kafka-wire client can use the endpoint the Managed Kafka API advertises.
//
// It reuses the MK1 k3d harness in broker_e2e_test.go (the same
// `managedkafka_broker_e2e` build tag and `make test-managedkafka-broker-k8s`
// target) and adds a one-shot k8s Job that runs the emulator image's hidden
// `kafka-probe` command. The probe is a real franz-go client: it produces
// records across partitions, joins a consumer group, consumes and commits, and
// reports committed offsets and lag. The Job runs in the emulator's namespace so
// it shares the in-cluster DNS view the broker advertises
// (<svc>.<ns>.svc.cluster.local:9092) — a host-side client cannot follow that
// listener, which is why the probe must run in-cluster rather than through a
// port-forward.
//
// After the probe commits, the test reads the group back through the Managed
// Kafka REST API to prove the control plane serves the live broker's committed
// offsets, then deletes the cluster and asserts the broker is reaped.
//
// Run with:
//
//	make test-managedkafka-broker-k8s
package managedkafkabroker_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"
)

// kafkaProbeResult mirrors the JSON line the emulator's `kafka-probe` command
// prints. The probe's integer-keyed maps render as strings on the wire.
type kafkaProbeResult struct {
	OK         bool              `json:"probe_ok"`
	Topic      string            `json:"topic"`
	Group      string            `json:"group"`
	Produced   int               `json:"produced"`
	Consumed   int               `json:"consumed"`
	Partitions map[string]int    `json:"partitions"`
	Committed  map[string]int64  `json:"committed"`
	Lag        map[string]int64  `json:"lag"`
	Configs    map[string]string `json:"configs"`
}

func TestManagedKafkaKafkaE2eK3d(t *testing.T) {
	requireK3d(t)

	base, _, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	run := fmt.Sprintf("%d", time.Now().Unix())
	cluster := "mk-e2e-" + run
	group := "mk-e2e-group-" + run
	const (
		topic      = "orders"
		partitions = 3
		records    = 12
		// topicConfig is a property override the API must apply to the real
		// broker (MK7), proven below by the probe's DescribeTopicConfigs.
		topicConfig = "retention.ms"
		// topicConfigValue must differ from the broker default so the override
		// is unambiguous.
		topicConfigValue = "86400000"
	)

	// Delete any cluster left by a prior run, then clean this one up.
	deleteCluster(t, base, cluster)
	t.Cleanup(func() { deleteCluster(t, base, cluster) })

	// Create the cluster through the GCP API. The request blocks until the
	// broker is ready (sync LRO), so it uses the long-timeout client.
	code, body := api(t, clusterCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{})
	if code < 200 || code >= 300 {
		t.Fatalf("create cluster: HTTP %d: %v", code, body)
	}

	// Create a multi-partition topic with a property override through the GCP
	// API; the core provisions it on the live broker so the probe can produce to
	// it and read the override back.
	code, topicBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/topics?topicId="+topic,
		map[string]any{
			"partitionCount":    partitions,
			"replicationFactor": 1,
			"configs":           map[string]any{topicConfig: topicConfigValue},
		})
	if code < 200 || code >= 300 {
		t.Fatalf("create topic: HTTP %d: %v", code, topicBody)
	}

	// The advertised address must be the in-cluster Service the broker owns.
	code, cl := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get cluster: HTTP %d: %v", code, cl)
	}
	addr := strField(cl, "bootstrapAddress")
	if addr == "" || strings.HasSuffix(addr, ".cloud.goog") {
		t.Fatalf("bootstrapAddress = %q, want a live broker endpoint (is the emulator running JAISCLOUD_KAFKA_BROKER_MODE=k8s?)", addr)
	}
	if want := fmt.Sprintf(".%s.svc.cluster.local:9092", namespace()); !strings.HasSuffix(addr, want) {
		t.Fatalf("bootstrapAddress %q does not end with %q", addr, want)
	}
	svcName := strings.SplitN(addr, ".", 2)[0]

	// Run the real franz-go client in-cluster as a one-shot Job using the
	// emulator image the deployment is running (so the hidden kafka-probe
	// command matches the code under test).
	image := emulatorImage(t)
	job := "mk-probe-" + run
	deleteProbeJob(t, job)
	t.Cleanup(func() { deleteProbeJob(t, job) })
	applyProbeJob(t, job, image, addr, topic, group, partitions, records)

	probe := waitProbeJob(t, job)
	if probe.Topic != topic || probe.Group != group {
		t.Fatalf("probe summary names topic=%q group=%q, want %q/%q", probe.Topic, probe.Group, topic, group)
	}
	if probe.Produced != records || probe.Consumed != records {
		t.Fatalf("probe produced=%d consumed=%d, want %d each", probe.Produced, probe.Consumed, records)
	}
	perPartition := records / partitions
	for p := 0; p < partitions; p++ {
		key := strconv.Itoa(p)
		if probe.Partitions[key] != perPartition {
			t.Errorf("partition %s produced=%d, want %d", key, probe.Partitions[key], perPartition)
		}
		if probe.Committed[key] != int64(perPartition) {
			t.Errorf("partition %s committed offset=%d, want %d", key, probe.Committed[key], perPartition)
		}
		if probe.Lag[key] != 0 {
			t.Errorf("partition %s lag=%d, want 0", key, probe.Lag[key])
		}
	}

	// MK7: the configs the API accepted must be honored by the real broker, not
	// merely echoed. The probe reads them back over the Kafka wire.
	if got := probe.Configs[topicConfig]; got != topicConfigValue {
		t.Fatalf("broker %s = %q, want the API-applied %q (broker configs: %v)", topicConfig, got, topicConfigValue, probe.Configs)
	}

	// The control plane must surface the group and offsets the broker holds.
	code, cgBody := api(t, httpClient, http.MethodGet,
		base+clusterPath(cluster)+"/consumerGroups/"+group, nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get consumer group: HTTP %d: %v", code, cgBody)
	}
	wantTopic := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/topics/%s", testProject, testRegion, cluster, topic)
	topics, _ := cgBody["topics"].(map[string]any)
	entry, _ := topics[wantTopic].(map[string]any)
	parts, _ := entry["partitions"].(map[string]any)
	if len(parts) != partitions {
		t.Fatalf("consumer group topics[%s].partitions = %v, want %d partitions", wantTopic, parts, partitions)
	}
	committed := 0
	for p, raw := range parts {
		pm, _ := raw.(map[string]any)
		offStr, _ := pm["offset"].(string)
		off, err := strconv.Atoi(offStr)
		if err != nil {
			t.Fatalf("partition %s offset %q is not an integer: %v", p, offStr, err)
		}
		committed += off
	}
	if committed != records {
		t.Fatalf("control plane committed offsets sum to %d, want %d (from the broker)", committed, records)
	}

	// Delete the cluster and assert the broker is reaped.
	deleteCluster(t, base, cluster)
	waitForResourceGone(t, "svc", svcName, 60*time.Second)
	waitForResourceGone(t, "pod", svcName, 90*time.Second)
}

// emulatorImage returns the image the running emulator Deployment uses, so the
// probe Job runs exactly the code under test. Overridable via JAISCLOUD_GCP_IMAGE.
func emulatorImage(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("JAISCLOUD_GCP_IMAGE"); v != "" {
		return v
	}
	out, err := kubectl("-n", namespace(), "get", "deploy", "jaiscloud-gcp",
		"-o", "jsonpath={.spec.template.spec.containers[0].image}")
	if err != nil {
		t.Fatalf("read emulator image: %v", err)
	}
	image := strings.TrimSpace(out)
	if image == "" {
		t.Fatal("emulator deployment has no container image")
	}
	return image
}

// probeJobData renders the probe Job manifest.
type probeJobData struct {
	Name       string
	Namespace  string
	Image      string
	Address    string
	Topic      string
	Group      string
	Partitions int
	Records    int
}

var probeJobManifest = template.Must(template.New("probe-job").Parse(`apiVersion: batch/v1
kind: Job
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
  labels:
    app: {{.Name}}
    jaiscloud.io/managedkafka-probe: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: {{.Name}}
    spec:
      restartPolicy: Never
      containers:
      - name: probe
        image: {{.Image}}
        imagePullPolicy: Always
        command: ["/jaiscloud"]
        args:
        - kafka-probe
        - --brokers
        - "{{.Address}}"
        - --topic
        - "{{.Topic}}"
        - --group
        - "{{.Group}}"
        - --partitions
        - "{{.Partitions}}"
        - --records
        - "{{.Records}}"
`))

// applyProbeJob applies the probe Job manifest through `kubectl apply -f -`.
func applyProbeJob(t *testing.T, job, image, addr, topic, group string, partitions, records int) {
	t.Helper()
	var rendered bytes.Buffer
	err := probeJobManifest.Execute(&rendered, probeJobData{
		Name:       job,
		Namespace:  namespace(),
		Image:      image,
		Address:    addr,
		Topic:      topic,
		Group:      group,
		Partitions: partitions,
		Records:    records,
	})
	if err != nil {
		t.Fatalf("render probe job manifest: %v", err)
	}
	cmd := exec.Command("kubectl", "-n", namespace(), "apply", "-f", "-")
	cmd.Stdin = &rendered
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("apply probe job: %v\n%s%s", err, out.String(), errb.String())
	}
}

// deleteProbeJob removes a prior/leftover probe Job (and, through the Job's
// default pod ownership, its pod). Best-effort: cleanup must not fail the test.
func deleteProbeJob(t *testing.T, job string) {
	t.Helper()
	_, _ = kubectl("-n", namespace(), "delete", "job", job, "--ignore-not-found", "--wait=false")
}

// waitProbeJob polls until the probe Job succeeds, failing fast (with logs) once
// it enters a terminal failure, and parses the probe's JSON summary from its
// logs.
func waitProbeJob(t *testing.T, job string) kafkaProbeResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var last string
	for time.Now().Before(deadline) {
		out, err := kubectl("-n", namespace(), "get", "job", job,
			"-o", "jsonpath={.status.succeeded}|{.status.failed}")
		if err == nil {
			last = strings.TrimSpace(out)
			fields := strings.Split(last, "|")
			succeeded := fields[0]
			failed := ""
			if len(fields) > 1 {
				failed = fields[1]
			}
			if succeeded == "1" {
				return parseProbeLogs(t, job)
			}
			if failed != "" && failed != "0" {
				logs, _ := kubectl("-n", namespace(), "logs", "job/"+job, "--tail=120")
				t.Fatalf("probe job failed (failed=%s) — is the deployed image built from this tree?\n--- probe logs ---\n%s", failed, logs)
			}
		}
		time.Sleep(3 * time.Second)
	}
	logs, _ := kubectl("-n", namespace(), "logs", "job/"+job, "--tail=120")
	t.Fatalf("probe job did not complete within 3m (last status: %s)\n--- probe logs ---\n%s", last, logs)
	return kafkaProbeResult{}
}

// parseProbeLogs extracts the probe's JSON summary (the last line that decodes
// as a JSON object) from the Job's pod logs.
func parseProbeLogs(t *testing.T, job string) kafkaProbeResult {
	t.Helper()
	logs, err := kubectl("-n", namespace(), "logs", "job/"+job)
	if err != nil {
		t.Fatalf("read probe job logs: %v", err)
	}
	lines := strings.Split(logs, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var result kafkaProbeResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			continue
		}
		if !result.OK {
			t.Fatalf("probe reported failure: %s\n--- probe logs ---\n%s", line, logs)
		}
		return result
	}
	t.Fatalf("probe logs contain no JSON summary\n--- probe logs ---\n%s", logs)
	return kafkaProbeResult{}
}

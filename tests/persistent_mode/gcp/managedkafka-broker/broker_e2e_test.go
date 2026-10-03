//go:build managedkafka_broker_e2e

// Package managedkafkabroker_test is the real-Kubernetes smoke for the Managed
// Kafka broker lifecycle (W1.1/MK1). It proves the k8s broker topology end to
// end against the deployed emulator:
//
//   - create a Managed Kafka cluster via the REST API
//   - assert the returned bootstrapAddress is the in-cluster Redpanda Service
//     (<svc>.<ns>.svc.cluster.local:9092), not the synthesized cloud.goog name
//   - assert the Service has ready endpoints and a TCP connection to it (via a
//     host port-forward) succeeds
//   - delete the cluster and assert the broker Pod and Service are reaped
//
// Run with:
//
//	make test-managedkafka-broker-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The deployed emulator must run with
// JAISCLOUD_KAFKA_BROKER_MODE=k8s (deploy/k8s/jaiscloud-gcp.yaml).
//
// The full produce/consume + consumer-group gate against a real Kafka client is
// the planned MK6 session; this smoke stops at reachability. MK5 adds a reset +
// orphan-sweep case below (TestManagedKafkaBrokerResetAndSweepK3d).
package managedkafkabroker_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	testProject = "jaiscloud-project"
	testRegion  = "us-central1"
)

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
}

func kubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("kubectl %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d Managed Kafka broker smoke")
	}
	if _, err := kubectl("-n", namespace(), "get", "svc", "jaiscloud-gcp"); err != nil {
		t.Skipf("svc/jaiscloud-gcp not reachable in namespace %q (apply deploy/k8s/jaiscloud-gcp.yaml): %v", namespace(), err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startPortForward forwards a Service to a free local port and returns the
// base URL, the chosen local port, and a stop function.
func startPortForward(t *testing.T, svc string, remotePort int) (string, int, func()) {
	t.Helper()
	port := freePort(t)
	cmd := exec.Command("kubectl", "-n", namespace(), "port-forward",
		"svc/"+svc, fmt.Sprintf("%d:%d", port, remotePort))
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward %s: %v", svc, err)
	}
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return base, port, stop
		}
		// The broker Service has no /_jaiscloud/health; only require that the
		// local listener accepts a connection.
		if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond); err == nil {
			c.Close()
			return base, port, stop
		}
		time.Sleep(300 * time.Millisecond)
	}
	stop()
	t.Fatalf("port-forward to svc/%s never became ready: %s", svc, strings.TrimSpace(errb.String()))
	return "", 0, func() {}
}

var httpClient = &http.Client{Timeout: 60 * time.Second}

// clusterCreateClient allows the synchronous broker startup (image pull + ready
// wait) to complete within one request.
var clusterCreateClient = &http.Client{Timeout: 4 * time.Minute}

func api(t *testing.T, client *http.Client, method, rawURL string, body any) (int, map[string]any) {
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

func clusterPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/locations/%s/clusters/%s", testProject, testRegion, name)
}

func strField(m map[string]any, path ...string) string {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = mm[k]
	}
	s, _ := cur.(string)
	return s
}

func TestManagedKafkaBrokerK3d(t *testing.T) {
	requireK3d(t)

	base, _, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	run := fmt.Sprintf("%d", time.Now().Unix())
	cluster := "mk-smoke-" + run
	t.Cleanup(func() { deleteCluster(t, base, cluster) })
	// Delete any broker left by a prior run before asserting reaping.
	deleteCluster(t, base, cluster)

	// Create the cluster. The request blocks until the broker is ready (sync
	// LRO), so give it a long timeout.
	code, body := api(t, clusterCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{"labels": map[string]string{"smoke": "managedkafka-broker"}})
	if code < 200 || code >= 300 {
		t.Fatalf("create cluster: HTTP %d: %v", code, body)
	}

	code, cl := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get cluster: HTTP %d: %v", code, cl)
	}
	addr := strField(cl, "bootstrapAddress")
	if addr == "" {
		t.Fatalf("cluster has no bootstrapAddress: %v", cl)
	}
	if strings.HasSuffix(addr, ".cloud.goog") {
		t.Fatalf("bootstrapAddress %q is the synthesized mock address — the deployed emulator is not running JAISCLOUD_KAFKA_BROKER_MODE=k8s", addr)
	}

	// The address must be the in-cluster Service DNS the broker manager owns.
	wantSuffix := fmt.Sprintf(".%s.svc.cluster.local:9092", namespace())
	if !strings.HasSuffix(addr, wantSuffix) {
		t.Fatalf("bootstrapAddress %q does not end with %q", addr, wantSuffix)
	}
	svcName := strings.SplitN(addr, ".", 2)[0]
	if !strings.HasPrefix(svcName, "mkbroker-") {
		t.Fatalf("broker Service name %q is not a managedkafka broker resource", svcName)
	}

	// The Service must exist and have ready endpoints (the Pod's readiness probe
	// passed), which is what proves something listens at the advertised address.
	if _, err := kubectl("-n", namespace(), "get", "svc", svcName); err != nil {
		t.Fatalf("broker Service %s not found: %v", svcName, err)
	}
	waitForServiceEndpoints(t, svcName, 30*time.Second)

	// A TCP connection through the Service reaches the broker.
	_, brokerPort, stopBroker := startPortForward(t, svcName, 9092)
	defer stopBroker()

	t.Run("tcp reachable", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", brokerPort), 10*time.Second)
		if err != nil {
			t.Fatalf("dial broker Service: %v", err)
		}
		conn.Close()
	})

	// MK4: an ACL created through the API is applied to the broker's Kafka ACL
	// table, and deleting it removes the binding. rpk runs inside the broker
	// Pod (the Redpanda image ships it), so this reads the broker's own view.
	code, topicBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/topics?topicId=orders",
		map[string]any{"partitionCount": 1, "replicationFactor": 1})
	if code < 200 || code >= 300 {
		t.Fatalf("create topic: HTTP %d: %v", code, topicBody)
	}

	entry := map[string]any{"principal": "User:acl-smoke", "permissionType": "ALLOW", "operation": "READ", "host": "*"}
	code, aclBody := api(t, httpClient, http.MethodPost,
		base+clusterPath(cluster)+"/acls?aclId=topic%2Forders",
		map[string]any{"aclEntries": []any{entry}})
	if code < 200 || code >= 300 {
		t.Fatalf("create acl: HTTP %d: %v", code, aclBody)
	}
	assertBrokerAcl(t, svcName, "User:acl-smoke", true)

	code, delBody := api(t, httpClient, http.MethodDelete,
		base+clusterPath(cluster)+"/acls/topic/orders", nil)
	if code >= 300 {
		t.Fatalf("delete acl: HTTP %d: %v", code, delBody)
	}
	assertBrokerAcl(t, svcName, "User:acl-smoke", false)

	// Delete the cluster and assert the broker is reaped.
	deleteCluster(t, base, cluster)
	waitForResourceGone(t, "svc", svcName, 60*time.Second)
	waitForResourceGone(t, "pod", svcName, 90*time.Second)
}

// TestManagedKafkaBrokerResetAndSweepK3d proves the MK5 lifecycle hardening
// against a live deployment:
//
//   - /_jaiscloud/reset reaps the running broker's Pod and Service and wipes
//     the cluster from the store;
//   - a broker-labeled Pod/Service the emulator never tracked is swept too;
//   - the same cluster id is reusable after reset and gets a fresh broker.
//
// The reuse caveat mirrors AWS EMR/Dataproc reset: reset does not drain
// in-flight producer/consumer work, and broker bytes are ephemeral — a reused
// id starts from a clean broker.
func TestManagedKafkaBrokerResetAndSweepK3d(t *testing.T) {
	requireK3d(t)

	base, _, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	run := fmt.Sprintf("%d", time.Now().Unix())
	cluster := "mk-reset-" + run
	deleteCluster(t, base, cluster)

	code, body := api(t, clusterCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{})
	if code < 200 || code >= 300 {
		t.Fatalf("create cluster: HTTP %d: %v", code, body)
	}
	code, cl := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get cluster: HTTP %d: %v", code, cl)
	}
	svcName := strings.SplitN(strField(cl, "bootstrapAddress"), ".", 2)[0]
	if !strings.HasPrefix(svcName, "mkbroker-") {
		t.Fatalf("unexpected broker service %q", svcName)
	}

	// Seed an orphan broker-labeled Pod/Service the emulator never tracked; the
	// reset sweep must reap it (the once-per-process startup sweep is covered by
	// the package unit tests).
	orphan := "mkbroker-orphan-" + run
	if _, err := kubectl("-n", namespace(), "run", orphan, "--image=busybox:1.36", "--restart=Never",
		"--labels", "jaiscloud.io/broker=managedkafka", "--", "sleep", "3600"); err != nil {
		t.Fatalf("seed orphan pod: %v", err)
	}
	if _, err := kubectl("-n", namespace(), "create", "service", "clusterip", orphan, "--tcp=9092:9092"); err != nil {
		t.Fatalf("seed orphan service: %v", err)
	}
	if _, err := kubectl("-n", namespace(), "label", "svc", orphan, "jaiscloud.io/broker=managedkafka"); err != nil {
		t.Fatalf("label orphan service: %v", err)
	}
	t.Cleanup(func() {
		_, _ = kubectl("-n", namespace(), "delete", "pod", orphan, "--ignore-not-found")
		_, _ = kubectl("-n", namespace(), "delete", "svc", orphan, "--ignore-not-found")
	})

	code, resetBody := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil)
	if code < 200 || code >= 300 {
		t.Fatalf("reset: HTTP %d: %v", code, resetBody)
	}

	// Reset reaps the tracked broker and the untracked orphan, and wipes state.
	waitForResourceGone(t, "svc", svcName, 60*time.Second)
	waitForResourceGone(t, "pod", svcName, 90*time.Second)
	waitForResourceGone(t, "svc", orphan, 60*time.Second)
	waitForResourceGone(t, "pod", orphan, 90*time.Second)
	if code, _ := api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil); code != http.StatusNotFound {
		t.Fatalf("cluster still present after reset: HTTP %d", code)
	}

	// The same cluster id is reusable after reset and gets a fresh broker.
	code, body = api(t, clusterCreateClient, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", testProject, testRegion, cluster),
		map[string]any{})
	if code < 200 || code >= 300 {
		t.Fatalf("recreate cluster after reset: HTTP %d: %v", code, body)
	}
	code, cl = api(t, httpClient, http.MethodGet, base+clusterPath(cluster), nil)
	if code < 200 || code >= 300 {
		t.Fatalf("get recreated cluster: HTTP %d: %v", code, cl)
	}
	if addr := strField(cl, "bootstrapAddress"); !strings.HasPrefix(addr, "mkbroker-") {
		t.Fatalf("recreated cluster bootstrapAddress = %q, want a live broker", addr)
	}
	deleteCluster(t, base, cluster)
}

// assertBrokerAcl reads the broker's Kafka ACL table from inside the broker Pod
// and asserts whether the principal is present.
func assertBrokerAcl(t *testing.T, pod, principal string, want bool) {
	t.Helper()
	out, err := kubectl("-n", namespace(), "exec", pod, "-c", "redpanda", "--",
		"rpk", "acl", "list", "--brokers", "127.0.0.1:9092")
	if err != nil {
		t.Fatalf("rpk acl list in %s: %v", pod, err)
	}
	got := strings.Contains(out, principal)
	if got != want {
		t.Fatalf("broker ACL table principal %q present=%v, want %v:\n%s", principal, got, want, out)
	}
}

func deleteCluster(t *testing.T, base, cluster string) {
	t.Helper()
	code, body := api(t, clusterCreateClient, http.MethodDelete, base+clusterPath(cluster), nil)
	if code >= 300 && code != http.StatusNotFound {
		t.Logf("cleanup: delete cluster %s: HTTP %d: %v", cluster, code, body)
	}
}

func waitForServiceEndpoints(t *testing.T, svc string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := kubectl("-n", namespace(), "get", "endpoints", svc,
			"-o", "jsonpath={range .subsets[*].addresses[*]}{.ip}{\"\\n\"}{end}")
		if err == nil && strings.TrimSpace(out) != "" {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("Service %s never got ready endpoints within %s", svc, timeout)
}

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

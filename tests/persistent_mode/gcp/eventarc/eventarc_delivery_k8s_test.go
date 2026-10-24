//go:build eventarc_e2e

// Package eventarc_test is the real-Kubernetes delivery gate for the Eventarc
// event-delivery engine (EV4). The engine (EV1–EV3) is unit-tested against an
// httptest sink; this proves a published Pub/Sub or Cloud Storage event actually
// reaches a real HTTP receiver deployed in the cluster, and that
// destination.cloudRun delivery works through the run runtime.
//
// It deploys a small recording sink (a Pod + ClusterIP Service running an
// inline Python HTTP handler) into the emulator's namespace, then:
//
//   - create a Pub/Sub topic + an Eventarc trigger with an httpEndpoint
//     destination; publish a message; assert the sink received a binary-mode
//     CloudEvents POST with the ce-* headers and the push-delivery body
//   - upload a GCS object against a Cloud Storage trigger and assert the
//     finalized CloudEvent (object-metadata body) arrived
//   - create a Cloud Run service that runs the same recording sink, point a
//     trigger's cloudRun destination at it, publish, and read the delivered
//     CloudEvent back through the run service's data plane
//
// Run with:
//
//	make test-e2e-eventarc-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The deployed emulator must have Eventarc and
// Cloud Run enabled (the manifest enables both; Cloud Run execution must be
// k8s).
//
// Required env:
//
//	EVENTARC_E2E_K8S — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	K8S_NAMESPACE — default jaiscloud
package eventarc_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	testProject  = "jaiscloud-project"
	testLocation = "us-central1"
)

// sinkScript is an inline Python HTTP recorder: it appends every POST (path,
// lowercased headers, body) to an in-memory list and serves that list as JSON
// on any GET. CloudEvents binary-mode headers are lowercased so the Go side can
// look them up case-insensitively regardless of Go's header canonicalization.
const sinkScript = `
import http.server, json, os
records = []
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0) or 0)
        body = self.rfile.read(n).decode('utf-8', 'replace')
        records.append({'path': self.path,
                        'headers': {k.lower(): v for k, v in self.headers.items()},
                        'body': body})
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'ok')
    def do_GET(self):
        out = json.dumps(records).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    def log_message(self, *a):
        pass
http.server.HTTPServer(('0.0.0.0', int(os.environ.get('PORT', '8080'))), H).serve_forever()
`

// sinkRecord is one request the recording sink received.
type sinkRecord struct {
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
}

// kubectl runs kubectl in the test namespace, optionally feeding stdin.
func kubectl(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("kubectl", append([]string{"-n", namespace()}, args...)...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String()
}

func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d Eventarc delivery smoke")
	}
	// Non-fatal probe: a failed lookup just means the cluster is not deployed.
	cmd := exec.Command("kubectl", "-n", namespace(), "get", "svc", "jaiscloud-gcp")
	if err := cmd.Run(); err != nil {
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

// startPortForward forwards a Service to a free local port and returns the base
// URL plus a stop function. Readiness is a successful TCP connect, so it works
// for the emulator and the plain sink alike.
func startPortForward(t *testing.T, svc string, remotePort int) (string, func()) {
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
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return "http://" + addr, stop
		}
		time.Sleep(300 * time.Millisecond)
	}
	stop()
	t.Fatalf("port-forward to svc/%s never became ready: %s", svc, strings.TrimSpace(errb.String()))
	return "", func() {}
}

var (
	// httpClient bounds API calls; createClient allows the synchronous k8s
	// startup (image pull + readiness wait) to complete within one request.
	httpClient   = &http.Client{Timeout: 60 * time.Second}
	createClient = &http.Client{Timeout: 5 * time.Minute}
)

// api sends a JSON request and returns the status, the decoded body object and
// the raw body (for endpoints that return an array).
func api(t *testing.T, client *http.Client, method, rawURL string, body any) (int, map[string]any, []byte) {
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
	if len(raw) > 0 && raw[0] == '{' {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out, raw
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

// pollRunOperation settles a Cloud Run mutation operation returned by the create
// surface. Eventarc's own trigger operations are always synchronous, but its
// Cloud Run destination service is created through the Run LRO: inline (done) in
// the default mode, in flight under the demo's async-LRO pacing cue, where this
// polls the location-scoped operations.get surface (GET /v2/{operation}).
func pollRunOperation(t *testing.T, base string, op map[string]any, timeout time.Duration) map[string]any {
	t.Helper()
	if done, _ := op["done"].(bool); done {
		return op
	}
	name, _ := op["name"].(string)
	if name == "" {
		t.Fatalf("in-flight run operation has no name to poll: %v", op)
	}
	pollURL := base + "/v2/" + name
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, settled, _ := api(t, httpClient, http.MethodGet, pollURL, nil)
		if code != http.StatusOK {
			t.Fatalf("poll run operation %s: HTTP %d: %v", name, code, settled)
		}
		if done, _ := settled["done"].(bool); done {
			return settled
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("run operation %s did not settle within %s", name, timeout)
	return nil
}

// applySink deploys the recording sink as a Pod + ClusterIP Service. The
// manifest is applied as JSON through stdin so the inline script needs no YAML
// quoting.
func applySink(t *testing.T, name string) {
	t.Helper()
	pod := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"name": name, "namespace": namespace(), "labels": map[string]any{"app": name},
		},
		"spec": map[string]any{
			"restartPolicy": "Never",
			"containers": []any{map[string]any{
				"name":    "sink",
				"image":   "python:3-alpine",
				"command": []any{"python3"},
				"args":    []any{"-u", "-c", sinkScript},
				"env":     []any{map[string]any{"name": "PORT", "value": "8080"}},
				"ports":   []any{map[string]any{"containerPort": 8080}},
				"readinessProbe": map[string]any{
					"httpGet":             map[string]any{"path": "/", "port": 8080},
					"initialDelaySeconds": 1,
					"periodSeconds":       1,
					"failureThreshold":    30,
				},
			}},
		},
	}
	svc := map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"name": name, "namespace": namespace()},
		"spec": map[string]any{
			"selector": map[string]any{"app": name},
			"ports":    []any{map[string]any{"port": 8080, "targetPort": 8080}},
		},
	}
	for _, m := range []map[string]any{pod, svc} {
		raw, _ := json.Marshal(m)
		kubectl(t, string(raw), "apply", "-f", "-")
	}
	kubectl(t, "", "wait", "--for=condition=Ready", "pod/"+name, "--timeout=180s")
	waitSinkReachable(t, name)
}

// waitSinkReachable proves the emulator's own view of the sink works before a
// trigger points at it: it resolves the Service DNS and opens an HTTP
// connection from inside the sink pod. A Ready pod is not enough — the
// Service's Endpoints are programmed asynchronously, and the delivery engine is
// fire-and-forget with no retry, so a POST during that window would be lost.
func waitSinkReachable(t *testing.T, name string) {
	t.Helper()
	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/", name, namespace())
	script := "import urllib.request; urllib.request.urlopen('" + url + "', timeout=2).read()"
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cmd := exec.Command("kubectl", "-n", namespace(), "exec", name, "--", "python3", "-c", script)
		if err := cmd.Run(); err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("sink service %s never became reachable from inside the cluster", url)
}

// sinkClusterURL is the address the in-cluster emulator POSTs to.
func sinkClusterURL(name string) string {
	return fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/", name, namespace())
}

// readSink fetches the records a forwarded sink has collected.
func readSink(t *testing.T, base string) []sinkRecord {
	t.Helper()
	resp, err := httpClient.Get(base + "/")
	if err != nil {
		t.Fatalf("read sink: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var records []sinkRecord
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &records); err != nil {
			t.Fatalf("decode sink records: %v: %s", err, raw)
		}
	}
	return records
}

// pollSink waits until a record matches pred, returning it.
func pollSink(t *testing.T, base string, pred func(sinkRecord) bool, timeout time.Duration) sinkRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range readSink(t, base) {
			if pred(r) {
				return r
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("sink %s received no matching request within %s (records: %+v)", base, timeout, readSink(t, base))
	return sinkRecord{}
}

// createTopic creates a Pub/Sub topic (PUT is the create verb).
func createTopic(t *testing.T, base, topic string) {
	t.Helper()
	code, body, _ := api(t, httpClient, http.MethodPut,
		base+"/v1/projects/"+testProject+"/topics/"+topic, map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("create topic %s: HTTP %d: %v", topic, code, body)
	}
}

// publish publishes one message with the given plaintext.
func publish(t *testing.T, base, topic, payload string) {
	t.Helper()
	code, body, _ := api(t, httpClient, http.MethodPost,
		base+"/v1/projects/"+testProject+"/topics/"+topic+":publish",
		map[string]any{"messages": []any{map[string]any{
			"data": base64.StdEncoding.EncodeToString([]byte(payload)),
		}}})
	if code != http.StatusOK {
		t.Fatalf("publish to %s: HTTP %d: %v", topic, code, body)
	}
}

// createTrigger creates an Eventarc trigger. topic may be empty for a storage
// trigger (no Pub/Sub transport).
func createTrigger(t *testing.T, base, id, topic string, filters []any, destination map[string]any) {
	t.Helper()
	body := map[string]any{
		"destination":  destination,
		"eventFilters": filters,
	}
	if topic != "" {
		body["transport"] = map[string]any{"pubsub": map[string]any{
			"topic": "projects/" + testProject + "/topics/" + topic,
		}}
	}
	code, out, _ := api(t, httpClient, http.MethodPost,
		base+"/v1/projects/"+testProject+"/locations/"+testLocation+"/triggers?triggerId="+id, body)
	if code != http.StatusOK {
		t.Fatalf("create trigger %s: HTTP %d: %v", id, code, out)
	}
}

func deleteTrigger(t *testing.T, base, id string) {
	t.Helper()
	code, _, _ := api(t, httpClient, http.MethodDelete,
		base+"/v1/projects/"+testProject+"/locations/"+testLocation+"/triggers/"+id, nil)
	if code != http.StatusOK && code != http.StatusNotFound {
		t.Logf("cleanup trigger %s: HTTP %d", id, code)
	}
}

func createBucket(t *testing.T, base, bucket string) {
	t.Helper()
	code, body, _ := api(t, httpClient, http.MethodPost,
		base+"/storage/v1/b?project="+testProject, map[string]any{"name": bucket})
	if code != http.StatusOK {
		t.Fatalf("create bucket %s: HTTP %d: %v", bucket, code, body)
	}
}

func uploadObject(t *testing.T, base, bucket, object string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		base+"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+url.QueryEscape(object),
		strings.NewReader("eventarc object body"))
	if err != nil {
		t.Fatalf("build upload: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload object: HTTP %d: %s", resp.StatusCode, b)
	}
}

func TestEventarcDeliveryK8s(t *testing.T) {
	if os.Getenv("EVENTARC_E2E_K8S") == "" {
		t.Skip("EVENTARC_E2E_K8S not set — skipping Eventarc k3d delivery e2e test")
	}
	requireK3d(t)

	base, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	if code, body, _ := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	sinkName := "eventarc-sink-" + run
	topic := "eventarc-topic-" + run
	bucket := "eventarc-bucket-" + run

	// ── HTTP endpoint sink ────────────────────────────────────────────────────
	applySink(t, sinkName)
	t.Cleanup(func() {
		_, _ = exec.Command("kubectl", "-n", namespace(), "delete", "pod", sinkName,
			"--ignore-not-found", "--wait=false").Output()
		_, _ = exec.Command("kubectl", "-n", namespace(), "delete", "svc", sinkName,
			"--ignore-not-found", "--wait=false").Output()
	})
	sinkBase, stopSink := startPortForward(t, sinkName, 8080)
	t.Cleanup(stopSink)

	createTopic(t, base, topic)
	createBucket(t, base, bucket)

	// Pub/Sub → httpEndpoint. A `type` filter plus the trigger's transport is
	// what real Eventarc requires to select the event.
	pubsubTrigger := "eventarc-pubsub-" + run
	t.Cleanup(func() { deleteTrigger(t, base, pubsubTrigger) })
	createTrigger(t, base, pubsubTrigger, topic,
		[]any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		map[string]any{"httpEndpoint": map[string]any{"uri": sinkClusterURL(sinkName)}})

	publish(t, base, topic, "hello eventarc")
	rec := pollSink(t, sinkBase, func(r sinkRecord) bool {
		return r.Headers["ce-type"] == "google.cloud.pubsub.topic.v1.messagePublished"
	}, 30*time.Second)
	assertCloudEvent(t, rec, map[string]string{
		"ce-source":      "//pubsub.googleapis.com/projects/" + testProject + "/topics/" + topic,
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.pubsub.topic.v1.messagePublished",
		"Content-Type":   "application/json",
	})
	if rec.Headers["ce-id"] == "" || rec.Headers["ce-time"] == "" {
		t.Errorf("ce-id/ce-time must be set: %+v", rec.Headers)
	}
	var push struct {
		Message struct {
			Data string `json:"data"`
		} `json:"message"`
		Subscription string `json:"subscription"`
	}
	if err := json.Unmarshal([]byte(rec.Body), &push); err != nil {
		t.Fatalf("decode push body %q: %v", rec.Body, err)
	}
	if push.Message.Data != base64.StdEncoding.EncodeToString([]byte("hello eventarc")) {
		t.Errorf("message.data = %q, want the base64 payload", push.Message.Data)
	}
	if !strings.Contains(push.Subscription, "/subscriptions/eventarc-") {
		t.Errorf("subscription = %q, want the provisioned eventarc subscription", push.Subscription)
	}

	// ── Cloud Storage → httpEndpoint ─────────────────────────────────────────
	storageTrigger := "eventarc-storage-" + run
	t.Cleanup(func() { deleteTrigger(t, base, storageTrigger) })
	createTrigger(t, base, storageTrigger, "",
		[]any{
			map[string]any{"attribute": "type", "value": "google.cloud.storage.object.v1.finalized"},
			map[string]any{"attribute": "bucket", "value": bucket},
		},
		map[string]any{"httpEndpoint": map[string]any{"uri": sinkClusterURL(sinkName)}})

	uploadObject(t, base, bucket, "dir/object.txt")
	rec = pollSink(t, sinkBase, func(r sinkRecord) bool {
		return r.Headers["ce-type"] == "google.cloud.storage.object.v1.finalized"
	}, 30*time.Second)
	assertCloudEvent(t, rec, map[string]string{
		"ce-source":      "//storage.googleapis.com/projects/_/buckets/" + bucket,
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.storage.object.v1.finalized",
		"Content-Type":   "application/json",
	})
	if !strings.Contains(rec.Body, "dir/object.txt") {
		t.Errorf("storage body does not carry the object metadata: %s", rec.Body)
	}

	// ── Cloud Run destination ────────────────────────────────────────────────
	// A Cloud Run service running the same recording sink. The Eventarc engine
	// invokes it through the run runtime, so reading the revision back through
	// the emulator's data plane proves the delivered POST landed.
	runSvc := "eventarc-run-" + run
	t.Cleanup(func() { deleteRunService(t, base, runSvc) })
	deleteRunService(t, base, runSvc)
	uri := createRunSinkService(t, base, runSvc)

	runTopic := "eventarc-runtopic-" + run
	createTopic(t, base, runTopic)
	runTrigger := "eventarc-run-" + run + "-trigger"
	t.Cleanup(func() { deleteTrigger(t, base, runTrigger) })
	createTrigger(t, base, runTrigger, runTopic,
		[]any{map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"}},
		map[string]any{"cloudRun": map[string]any{"service": runSvc, "region": testLocation}})

	publish(t, base, runTopic, "hello cloud run")
	runRec := pollRunSink(t, base, uri, 60*time.Second)
	assertCloudEvent(t, runRec, map[string]string{
		"ce-source":      "//pubsub.googleapis.com/projects/" + testProject + "/topics/" + runTopic,
		"ce-specversion": "1.0",
		"ce-type":        "google.cloud.pubsub.topic.v1.messagePublished",
		"Content-Type":   "application/json",
	})
}

// assertCloudEvent checks a received request carries want headers. The sink
// lowercases header names, so the lookup is case-folded.
func assertCloudEvent(t *testing.T, rec sinkRecord, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if got := rec.Headers[strings.ToLower(k)]; got != v {
			t.Errorf("header %s = %q, want %q (record: %+v)", k, got, v, rec.Headers)
		}
	}
}

// createRunSinkService creates a Cloud Run service that runs the recording sink
// and returns its invocation uri. The create LRO is inline in the default mode
// and settled through operations.get under async-LRO mode.
func createRunSinkService(t *testing.T, base, id string) string {
	t.Helper()
	collection := "/v2/projects/" + testProject + "/locations/" + testLocation + "/services"
	create := map[string]any{
		"template": map[string]any{
			"containers": []any{map[string]any{
				"image":   "python:3-alpine",
				"command": []any{"python3"},
				"args":    []any{"-u", "-c", sinkScript},
				"ports":   []any{map[string]any{"containerPort": 8080}},
			}},
		},
	}
	code, op, _ := api(t, createClient, http.MethodPost, base+collection+"?serviceId="+id, create)
	if code != http.StatusOK {
		t.Fatalf("create run service %s: HTTP %d: %v", id, code, op)
	}
	op = pollRunOperation(t, base, op, 2*time.Minute)
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("create run service %s: operation never settled: %v", id, op)
	}
	created, _ := op["response"].(map[string]any)
	if created == nil {
		t.Fatalf("create run service %s: no response Service: %v", id, op)
	}
	uri := strField(created, "uri")
	if uri == "" {
		t.Fatalf("create run service %s: empty uri: %v", id, created)
	}
	return uri
}

func deleteRunService(t *testing.T, base, id string) {
	t.Helper()
	path := "/v2/projects/" + testProject + "/locations/" + testLocation + "/services/" + id
	code, _, _ := api(t, createClient, http.MethodDelete, base+path, nil)
	if code != http.StatusOK && code != http.StatusNotFound {
		t.Logf("cleanup run service %s: HTTP %d", id, code)
	}
}

// pollRunSink invokes the Cloud Run sink's data plane (GET /) through the
// emulator with the service authority as a Host header, until it reports a
// matching CloudEvent POST.
func pollRunSink(t *testing.T, base, uri string, timeout time.Duration) sinkRecord {
	t.Helper()
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse run uri %q: %v", uri, err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, base+"/", nil)
		if err != nil {
			t.Fatalf("build invocation: %v", err)
		}
		req.Host = u.Host
		resp, err := httpClient.Do(req)
		if err == nil {
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var records []sinkRecord
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &records)
			}
			for _, r := range records {
				if r.Headers["ce-type"] == "google.cloud.pubsub.topic.v1.messagePublished" {
					return r
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("cloudRun sink %s received no matching CloudEvent within %s", uri, timeout)
	return sinkRecord{}
}

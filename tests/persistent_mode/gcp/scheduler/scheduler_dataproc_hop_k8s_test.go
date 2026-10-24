//go:build scheduler_e2e

// Package scheduler_test is the real-Kubernetes gate for the demo's batch
// trigger chain (SPK3): a Cloud Scheduler httpTarget tick reaches an in-cluster
// submitter, which calls dataproc.jobs.submit. It proves the emulator's cron
// engine delivers with the real Cloud Scheduler headers — including
// X-CloudScheduler-ScheduleTime, which is constant across a run's retries — and
// that the hop lands a Dataproc job.
//
// It deploys a small recording submitter (a Pod + ClusterIP Service running an
// inline Python HTTP handler) into the emulator's namespace, then:
//
//   - creates a Dataproc cluster and a Scheduler httpTarget job whose uri points
//     at the submitter; forces a tick against a fixed clock; asserts the
//     submitter received the expected Cloud Scheduler headers and called
//     dataproc.jobs.submit, and that the job is recorded DONE
//   - points a second job with a retryConfig at the submitter's /flaky path
//     (which answers 500 once, then 200); asserts the failed attempt records its
//     status, the retry fires after the backoff, and both attempts carry the
//     same X-CloudScheduler-ScheduleTime
//
// Run with:
//
//	make test-e2e-scheduler-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The target sets JAISCLOUD_SPARK_EXECUTOR_MODE=mock
// so the submitted job settles lazily in the store — the gate is about the hop,
// not a Spark run (which would need the memory-tight k3d node's headroom).
//
// Required env:
//
//	SCHEDULER_E2E_K8S — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	K8S_NAMESPACE — default jaiscloud
package scheduler_test

import (
	"bytes"
	"encoding/base64"
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
	testProject  = "jaiscloud-project"
	testLocation = "us-central1"
	testRegion   = "us-central1"
)

// submitterScript is an inline Python HTTP submitter. On a POST it records the
// request (path, lowercased headers, body) and, unless the path is /flaky on its
// first FLAKY_FAILS calls, calls dataproc.jobs.submit against the emulator and
// records the response status. A GET returns the collected records as JSON.
const submitterScript = `
import http.server, json, os, threading, urllib.request, urllib.error

records = []
lock = threading.Lock()
next_id = [0]
flaky_remaining = [int(os.environ.get('FLAKY_FAILS', '0'))]

EMULATOR = os.environ['EMULATOR']
PROJECT = os.environ['PROJECT']
REGION = os.environ['REGION']
CLUSTER = os.environ['CLUSTER']

def submit(job_id):
    body = json.dumps({'job': {
        'reference': {'jobId': job_id},
        'placement': {'clusterName': CLUSTER},
        'sparkSqlJob': {'queryList': {'queries': ['SELECT 1']}},
    }}).encode()
    url = EMULATOR + '/v1/projects/' + PROJECT + '/regions/' + REGION + '/jobs:submit'
    req = urllib.request.Request(url, data=body,
                                 headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code

class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0) or 0)
        body = self.rfile.read(n).decode('utf-8', 'replace')
        rec = {'path': self.path,
               'headers': {k.lower(): v for k, v in self.headers.items()},
               'body': body}
        flaky = self.path.startswith('/flaky')
        with lock:
            if flaky and flaky_remaining[0] > 0:
                flaky_remaining[0] -= 1
                rec['failed'] = True
                records.append(rec)
                self.send_response(500)
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            next_id[0] += 1
            job_id = 'sched-job-%d' % next_id[0]
        rec['jobId'] = job_id
        rec['submitStatus'] = submit(job_id)
        with lock:
            records.append(rec)
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'ok')
    def do_GET(self):
        with lock:
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

// sinkRecord is one request the submitter received.
type sinkRecord struct {
	Path         string            `json:"path"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	JobID        string            `json:"jobId"`
	SubmitStatus int               `json:"submitStatus"`
	Failed       bool              `json:"failed"`
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
		t.Skip("kubectl not found — skipping k3d Scheduler→Dataproc hop smoke")
	}
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
// URL plus a stop function.
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

var httpClient = &http.Client{Timeout: 60 * time.Second}

// api sends a JSON request and returns the status, the decoded body object and
// the raw body.
func api(t *testing.T, method, rawURL string, body any) (int, map[string]any, []byte) {
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
	resp, err := httpClient.Do(req)
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

func mustOK(t *testing.T, verb string, code int, body map[string]any) map[string]any {
	t.Helper()
	if code < 200 || code >= 300 {
		t.Fatalf("%s: HTTP %d: %v", verb, code, body)
	}
	return body
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

// intField navigates a nested map path returning the number as an int, or 0.
func intField(m map[string]any, path ...string) int {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return 0
		}
		cur = mm[k]
	}
	f, _ := cur.(float64)
	return int(f)
}

// ─── submitter workload ────────────────────────────────────────────────────

// applySubmitter deploys the recording submitter as a Pod + ClusterIP Service.
func applySubmitter(t *testing.T, name, cluster string) {
	t.Helper()
	emulator := fmt.Sprintf("http://jaiscloud-gcp.%s.svc.cluster.local:8080", namespace())
	pod := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"name": name, "namespace": namespace(), "labels": map[string]any{"app": name},
		},
		"spec": map[string]any{
			"restartPolicy": "Never",
			"containers": []any{map[string]any{
				"name":    "submitter",
				"image":   "python:3-alpine",
				"command": []any{"python3"},
				"args":    []any{"-u", "-c", submitterScript},
				"env": []any{
					map[string]any{"name": "PORT", "value": "8080"},
					map[string]any{"name": "EMULATOR", "value": emulator},
					map[string]any{"name": "PROJECT", "value": testProject},
					map[string]any{"name": "REGION", "value": testRegion},
					map[string]any{"name": "CLUSTER", "value": cluster},
					map[string]any{"name": "FLAKY_FAILS", "value": "1"},
				},
				"ports": []any{map[string]any{"containerPort": 8080}},
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
	waitServiceEndpoint(t, name)
}

// waitServiceEndpoint blocks until the Service has a published ready endpoint and
// then settles briefly. A Ready pod does not mean kube-proxy has programmed the
// Service's ClusterIP yet; a tick that races that programming gets "connection
// refused", and a job with no retryConfig delivers exactly once, so the hop would
// be lost. Waiting for the endpoint (plus a short settle) removes the race.
func waitServiceEndpoint(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		out, _ := kubectlQuiet("get", "endpoints", name, "-o", "jsonpath={.subsets[0].addresses[0].ip}")
		if strings.TrimSpace(out) != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service %s has no ready endpoint after 60s", name)
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
}

// kubectlQuiet runs kubectl and returns stdout plus a folded error without
// failing the test, for polling reads.
func kubectlQuiet(args ...string) (string, error) {
	cmd := exec.Command("kubectl", append([]string{"-n", namespace()}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%w: %s", err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func deleteSubmitter(t *testing.T, name string) {
	t.Helper()
	_, _ = exec.Command("kubectl", "-n", namespace(), "delete", "pod", name,
		"--ignore-not-found", "--wait=false").Output()
	_, _ = exec.Command("kubectl", "-n", namespace(), "delete", "svc", name,
		"--ignore-not-found", "--wait=false").Output()
}

// readSink fetches the records the forwarded submitter has collected.
func readSink(t *testing.T, base string) []sinkRecord {
	t.Helper()
	resp, err := httpClient.Get(base + "/")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var records []sinkRecord
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &records)
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
	t.Fatalf("submitter %s received no matching request within %s", base, timeout)
	return sinkRecord{}
}

// ─── Dataproc wiring (REST, mock Spark) ─────────────────────────────────────

func clusterPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/clusters/%s", testProject, testRegion, name)
}

func jobPath(id string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/jobs/%s", testProject, testRegion, id)
}

func gkeVirtualBody(name string) map[string]any {
	return map[string]any{
		"clusterName": name,
		"virtualClusterConfig": map[string]any{
			"kubernetesClusterConfig": map[string]any{
				"gkeClusterConfig": map[string]any{
					"gkeClusterTarget": fmt.Sprintf("projects/%s/locations/%s/clusters/gke-target", testProject, testRegion),
				},
			},
		},
	}
}

// createCluster submits a cluster create and waits until it reports RUNNING.
func createCluster(t *testing.T, base, name string) {
	t.Helper()
	code, prior, _ := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code == http.StatusOK {
		pollOperation(t, base, prior)
	}
	code, op, _ := api(t, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/regions/%s/clusters", testProject, testRegion), gkeVirtualBody(name))
	mustOK(t, "create cluster "+name, code, op)
	if done, _ := op["done"].(bool); !done {
		pollOperation(t, base, op)
	}
	waitClusterRunning(t, base, name, 2*time.Minute)
}

func waitClusterRunning(t *testing.T, base, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, cl, _ := api(t, http.MethodGet, base+clusterPath(name), nil)
		if code == http.StatusOK {
			switch strField(cl, "status", "state") {
			case "RUNNING":
				return
			case "ERROR":
				t.Fatalf("cluster %s reached ERROR: %v", name, cl)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster %s did not reach RUNNING within %s", name, timeout)
}

func deleteCluster(t *testing.T, base, name string) {
	t.Helper()
	code, body, _ := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code == http.StatusOK {
		if done, _ := body["done"].(bool); !done {
			pollOperation(t, base, body)
		}
	}
}

func pollOperation(t *testing.T, base string, op map[string]any) map[string]any {
	t.Helper()
	name, _ := op["name"].(string)
	if name == "" {
		return op
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		code, cur, _ := api(t, http.MethodGet, base+"/v1/"+name, nil)
		if code >= 300 {
			t.Fatalf("poll operation %s: HTTP %d", name, code)
		}
		if done, _ := cur["done"].(bool); done {
			resp, _ := cur["response"].(map[string]any)
			if resp == nil {
				return cur
			}
			return resp
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("operation %s did not complete within 2m", name)
	return nil
}

// waitJobDone polls the job until mock mode settles it to DONE.
func waitJobDone(t *testing.T, base, id string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(deadline) {
		code, job, _ := api(t, http.MethodGet, base+jobPath(id), nil)
		if code == http.StatusOK {
			last = job
			switch strField(job, "status", "state") {
			case "DONE":
				return
			case "ERROR":
				t.Fatalf("submitted job %s reached ERROR: %v", id, job)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("submitted job %s never reached DONE: %v", id, last)
}

// ─── Cloud Scheduler wiring (REST) ──────────────────────────────────────────

func schedulerJobPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/locations/%s/jobs/%s", testProject, testLocation, name)
}

// createSchedulerJob creates an httpTarget cron job that POSTs a small JSON body
// to uri. retry may be nil.
func createSchedulerJob(t *testing.T, base, name, uri string, retry map[string]any) {
	t.Helper()
	body := map[string]any{
		"name":     fmt.Sprintf("projects/%s/locations/%s/jobs/%s", testProject, testLocation, name),
		"schedule": "* * * * *",
		"timeZone": "UTC",
		"httpTarget": map[string]any{
			"uri":        uri,
			"httpMethod": "POST",
			"body":       base64.StdEncoding.EncodeToString([]byte(hopBody)),
			"oidcToken": map[string]any{
				"serviceAccountEmail": "demo-runner@" + testProject + ".iam.gserviceaccount.com",
				"audience":            uri,
			},
		},
	}
	if retry != nil {
		body["retryConfig"] = retry
	}
	code, out, _ := api(t, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/jobs", testProject, testLocation), body)
	mustOK(t, "create scheduler job "+name, code, out)
}

const hopBody = `{"kind":"rollup"}`

func schedulerJob(t *testing.T, base, name string) map[string]any {
	t.Helper()
	code, job, _ := api(t, http.MethodGet, base+schedulerJobPath(name), nil)
	mustOK(t, "get scheduler job "+name, code, job)
	return job
}

func schedulerScheduleTime(t *testing.T, base, name string) time.Time {
	t.Helper()
	raw := strField(schedulerJob(t, base, name), "scheduleTime")
	slot, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse scheduleTime %q: %v", raw, err)
	}
	return slot
}

// setClock fixes the emulator clock at t (RFC3339 accepted by the admin API).
func setClock(t *testing.T, base string, mode string, at time.Time) {
	t.Helper()
	body := map[string]any{"mode": mode}
	if !at.IsZero() {
		body["time"] = at.UTC().Format(time.RFC3339)
	}
	code, out, _ := api(t, http.MethodPost, base+"/_jaiscloud/clock", body)
	if code != http.StatusNoContent {
		t.Fatalf("set clock %s: HTTP %d: %v", mode, code, out)
	}
}

func tick(t *testing.T, base string) {
	t.Helper()
	code, out, _ := api(t, http.MethodPost, base+"/_jaiscloud/scheduler-tick", nil)
	if code != http.StatusNoContent {
		t.Fatalf("scheduler-tick: HTTP %d: %v", code, out)
	}
}

// assertDeliveryHeaders checks a delivered request carries the headers real
// Cloud Scheduler attaches, with wantSlot as the run's X-CloudScheduler-ScheduleTime.
func assertDeliveryHeaders(t *testing.T, rec sinkRecord, wantSlot, wantJob string) {
	t.Helper()
	want := map[string]string{
		"user-agent":                    "Google-Cloud-Scheduler",
		"x-cloudscheduler":              "true",
		"x-cloudscheduler-jobname":      wantJob,
		"x-cloudscheduler-scheduletime": wantSlot,
	}
	for k, v := range want {
		if got := rec.Headers[k]; got != v {
			t.Errorf("header %s = %q, want %q (record: %+v)", k, got, v, rec.Headers)
		}
	}
	if rec.Headers["authorization"] == "" {
		t.Errorf("oidcToken set but no Authorization header: %+v", rec.Headers)
	}
	if rec.Body != hopBody {
		t.Errorf("delivered body = %q, want %q", rec.Body, hopBody)
	}
}

func TestSchedulerDataprocHopK3d(t *testing.T) {
	if os.Getenv("SCHEDULER_E2E_K8S") == "" {
		t.Skip("SCHEDULER_E2E_K8S not set — skipping Scheduler→Dataproc hop k3d smoke")
	}
	requireK3d(t)

	base, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	if code, out, _ := api(t, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, out)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	cluster := "sched-cluster-" + run
	submitter := "sched-submitter-" + run

	// The cluster settles with the real clock; freeze the clock only once the
	// hop is being exercised. Restore it before the cluster is deleted (cleanups
	// run LIFO, so this cleanup registered after deleteCluster runs first).
	createCluster(t, base, cluster)
	t.Cleanup(func() { deleteCluster(t, base, cluster) })
	t.Cleanup(func() { setClock(t, base, "real", time.Time{}) })

	applySubmitter(t, submitter, cluster)
	t.Cleanup(func() { deleteSubmitter(t, submitter) })
	submitterBase, stopSub := startPortForward(t, submitter, 8080)
	t.Cleanup(stopSub)

	// ── the hop: Scheduler httpTarget → submitter → dataproc.jobs.submit ──
	slot0 := time.Now().UTC().Truncate(time.Second)
	setClock(t, base, "fixed", slot0)
	jobName := "sched-hop-" + run
	hopURI := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/hop", submitter, namespace())
	createSchedulerJob(t, base, jobName, hopURI, nil)

	slot := schedulerScheduleTime(t, base, jobName)
	setClock(t, base, "fixed", slot)
	tick(t, base)

	rec := pollSink(t, submitterBase, func(r sinkRecord) bool { return r.Path == "/hop" }, 60*time.Second)
	assertDeliveryHeaders(t, rec, slot.UTC().Format(time.RFC3339),
		"projects/"+testProject+"/locations/"+testLocation+"/jobs/"+jobName)
	if rec.JobID == "" || rec.SubmitStatus != http.StatusOK {
		t.Fatalf("submitter did not submit a Dataproc job: %+v", rec)
	}
	waitJobDone(t, base, rec.JobID, 30*time.Second)
	if got := schedulerJob(t, base, jobName); strField(got, "lastAttemptTime") == "" {
		t.Fatalf("scheduler job has no lastAttemptTime after the tick: %v", got)
	}

	// ── retry: a 500 target is retried after the backoff, same schedule time ──
	flakyName := "sched-flaky-" + run
	flakyURI := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/flaky", submitter, namespace())
	createSchedulerJob(t, base, flakyName, flakyURI,
		map[string]any{"retryCount": 1, "minBackoffDuration": "15s"})

	flakySlot := schedulerScheduleTime(t, base, flakyName)
	setClock(t, base, "fixed", flakySlot)
	tick(t, base)

	// First attempt fails: the recorded status is the 500 and the next fire is
	// the backoff.
	first := pollSink(t, submitterBase, func(r sinkRecord) bool {
		return r.Path == "/flaky" && r.Failed
	}, 60*time.Second)
	got := schedulerJob(t, base, flakyName)
	if code := intField(got, "status", "code"); code != http.StatusInternalServerError {
		t.Fatalf("after first attempt status.code = %d, want 500 (%v)", code, got)
	}
	afterFirst := schedulerScheduleTime(t, base, flakyName)
	if want := flakySlot.Add(15 * time.Second); !afterFirst.Equal(want) {
		t.Fatalf("retry scheduleTime = %v, want %v", afterFirst, want)
	}

	// The retry succeeds.
	setClock(t, base, "fixed", afterFirst)
	tick(t, base)
	pollSink(t, submitterBase, func(r sinkRecord) bool {
		return r.Path == "/flaky" && r.JobID != "" && r.SubmitStatus == http.StatusOK
	}, 60*time.Second)

	// Both attempts carried the run's schedule time, so a target can dedupe them.
	wantFlaky := flakySlot.UTC().Format(time.RFC3339)
	var flaky []sinkRecord
	for _, r := range readSink(t, submitterBase) {
		if r.Path == "/flaky" {
			flaky = append(flaky, r)
		}
	}
	if len(flaky) != 2 {
		t.Fatalf("/flaky deliveries = %d, want 2: %+v", len(flaky), flaky)
	}
	for i, r := range flaky {
		if got := r.Headers["x-cloudscheduler-scheduletime"]; got != wantFlaky {
			t.Errorf("/flaky attempt %d X-CloudScheduler-ScheduleTime = %q, want %q", i, got, wantFlaky)
		}
	}
	if first.Headers["x-cloudscheduler-scheduletime"] != wantFlaky {
		t.Errorf("first failed attempt X-CloudScheduler-ScheduleTime = %q, want %q",
			first.Headers["x-cloudscheduler-scheduletime"], wantFlaky)
	}
}

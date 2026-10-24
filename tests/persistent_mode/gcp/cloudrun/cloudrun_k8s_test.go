//go:build cloudrun_e2e

// Package cloudrun_test is the real-Kubernetes smoke for the Cloud Run Admin v2
// k8s execution path (CR3). It proves behavioural execution end to end against a
// deployed emulator running with JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s:
//
//   - create a service whose template names a public image (nginx:latest) on a
//     declared container port, and settle the create LRO (inline in the default
//     synchronous mode, polled through operations.get under async-LRO mode)
//   - assert the returned Service shape (uri, latestReadyRevision, Ready
//     terminal condition, trafficStatuses[0] at 100%)
//   - assert the revision's Pod and Service exist in the cluster
//   - invoke the generated uri through the emulator with the authority as a Host
//     header and assert the container's response (the data-plane proxy)
//   - list/get the revision, set/get/test service IAM, delete the service, and
//     assert both the record is gone and the revision workloads are reaped
//
// Run with:
//
//	make test-e2e-cloudrun-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The deployed emulator must run with
// JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s (the Makefile target and
// deploy/k8s/jaiscloud-gcp.yaml both configure it).
//
// Required env:
//
//	CLOUDRUN_E2E_K8S — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	K8S_NAMESPACE — default jaiscloud
package cloudrun_test

import (
	"bytes"
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
	testProject   = "jaiscloud-project"
	testLocation  = "us-central1"
	image         = "nginx:latest"
	containerPort = 80
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
		t.Skip("kubectl not found — skipping k3d Cloud Run execution smoke")
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

// startPortForward forwards a Service to a free local port and returns the base
// URL and a stop function.
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
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return base, stop
		}
		time.Sleep(300 * time.Millisecond)
	}
	stop()
	t.Fatalf("port-forward to svc/%s never became ready: %s", svc, strings.TrimSpace(errb.String()))
	return "", func() {}
}

var httpClient = &http.Client{Timeout: 60 * time.Second}

// createClient allows the synchronous k8s startup (image pull + readiness wait)
// to complete within one create request.
var createClient = &http.Client{Timeout: 5 * time.Minute}

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

// pollRunOperation settles a Cloud Run mutation operation returned by the
// create/delete surface. In the default synchronous LRO mode the operation is
// already done (the resource is in response); under the demo's async-LRO pacing
// cue (JAISCLOUD_LRO_MODE=async) it comes back done:false and the harness must
// poll the location-scoped operations.get surface (GET /v2/{operation}) until it
// settles, exactly as the official client does. It returns the settled operation.
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
		code, settled := api(t, httpClient, http.MethodGet, pollURL, nil)
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

// workloadSelector matches a service's revision Pod and Service by label.
func workloadSelector(svcID string) string {
	return "app=jaiscloud-cloudrun,jaiscloud.io/run-service=" + svcID
}

// waitForWorkloads polls until both a Pod and a Service carrying the service's
// labels exist. Their existence proves the k8s runtime manager created the
// revision workload (EnsureRevision runs inline before the create operation is
// recorded, so the workload exists once the operation settles).
func waitForWorkloads(t *testing.T, svcID string, timeout time.Duration) (pod, svc string) {
	t.Helper()
	sel := workloadSelector(svcID)
	deadline := time.Now().Add(timeout)
	for {
		pods, _ := kubectl("-n", namespace(), "get", "pods", "-l", sel, "-o", "jsonpath={.items[*].metadata.name}")
		svcs, _ := kubectl("-n", namespace(), "get", "svc", "-l", sel, "-o", "jsonpath={.items[*].metadata.name}")
		pods, svcs = strings.TrimSpace(pods), strings.TrimSpace(svcs)
		if pods != "" && svcs != "" {
			return pods, svcs
		}
		if time.Now().After(deadline) {
			t.Fatalf("revision workload for %q not created within %s (pods=%q svc=%q)", svcID, timeout, pods, svcs)
		}
		time.Sleep(2 * time.Second)
	}
}

// waitForWorkloadsGone polls until no Pod or Service carries the service's
// labels, proving teardown on delete.
func waitForWorkloadsGone(t *testing.T, svcID string, timeout time.Duration) {
	t.Helper()
	sel := workloadSelector(svcID)
	deadline := time.Now().Add(timeout)
	for {
		pods, _ := kubectl("-n", namespace(), "get", "pods", "-l", sel, "-o", "jsonpath={.items[*].metadata.name}")
		svcs, _ := kubectl("-n", namespace(), "get", "svc", "-l", sel, "-o", "jsonpath={.items[*].metadata.name}")
		pods, svcs = strings.TrimSpace(pods), strings.TrimSpace(svcs)
		if pods == "" && svcs == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("revision workload for %q not reaped within %s (pods=%q svc=%q)", svcID, timeout, pods, svcs)
		}
		time.Sleep(2 * time.Second)
	}
}

func TestCloudRunK8sExecution(t *testing.T) {
	if os.Getenv("CLOUDRUN_E2E_K8S") == "" {
		t.Skip("CLOUDRUN_E2E_K8S not set — skipping Cloud Run k8s e2e test")
	}
	requireK3d(t)

	base, stop := startPortForward(t, "jaiscloud-gcp", 8080)
	t.Cleanup(stop)

	if code, body := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	svcID := "cr-e2e-" + run
	runBase := "/v2/projects/" + testProject + "/locations/" + testLocation
	collection := runBase + "/services"
	svcPath := collection + "/" + svcID
	// Cloud Run renders resource names without the /v2 prefix.
	svcName := "projects/" + testProject + "/locations/" + testLocation + "/services/" + svcID
	t.Cleanup(func() { deleteService(t, base, svcPath) })
	// Delete any service left by a prior run before asserting creation semantics.
	deleteService(t, base, svcPath)

	// Create a service whose template declares nginx and a container port. In k8s
	// mode the revision runtime is created inline (EnsureRevision runs before the
	// operation is recorded), so the workload is ready once the operation settles.
	// The operation itself is done inline in the default synchronous LRO mode and
	// in flight under async-LRO mode, where pollRunOperation settles it.
	create := map[string]any{
		"template": map[string]any{
			"containers": []any{
				map[string]any{
					"image": image,
					"ports": []any{map[string]any{"containerPort": containerPort}},
				},
			},
		},
	}
	code, op := api(t, createClient, http.MethodPost, base+collection+"?serviceId="+svcID, create)
	if code != http.StatusOK {
		t.Fatalf("create service: HTTP %d: %v", code, op)
	}
	op = pollRunOperation(t, base, op, 2*time.Minute)
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("create service operation never settled: %v", op)
	}
	created, _ := op["response"].(map[string]any)
	if created == nil {
		t.Fatalf("create service operation has no response Service: %v", op)
	}
	if got := strField(created, "name"); got != svcName {
		t.Fatalf("created service name = %q, want %q", got, svcName)
	}
	uri := strField(created, "uri")
	if !strings.HasPrefix(uri, "http://"+svcID+"-") {
		t.Fatalf("created service uri = %q, want http://%s- prefix (k8s mode synthesizes http)", uri, svcID)
	}
	revName := strField(created, "latestReadyRevision")
	if !strings.Contains(revName, "/revisions/") {
		t.Fatalf("latestReadyRevision = %q, want a /revisions/ name", revName)
	}
	if ct := strField(created, "terminalCondition", "type"); ct != "Ready" {
		t.Fatalf("terminalCondition.type = %q, want Ready", ct)
	}
	if pct, _ := created["trafficStatuses"].([]any); len(pct) == 0 {
		t.Fatalf("trafficStatuses empty: %v", created)
	} else if ts, _ := pct[0].(map[string]any); ts == nil {
		t.Fatalf("trafficStatuses[0] malformed: %v", created)
	} else if p, _ := ts["percent"].(float64); p != 100 {
		t.Fatalf("trafficStatuses[0].percent = %v, want 100", ts["percent"])
	}

	// The k8s runtime manager must have created the revision Pod + Service.
	pod, svc := waitForWorkloads(t, svcID, 30*time.Second)
	t.Logf("revision workload created: pod=%s svc=%s", pod, svc)

	// Invoke the generated host through the emulator: the port-forward reaches the
	// emulator, the Host header routes to the run invocation codec, and the
	// emulator proxies in-cluster to the revision runtime.
	t.Run("invoke", func(t *testing.T) {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatalf("parse uri %q: %v", uri, err)
		}
		req, err := http.NewRequest(http.MethodGet, base+"/?compat=go", nil)
		if err != nil {
			t.Fatalf("build invocation: %v", err)
		}
		req.Host = u.Host
		req.Header.Set("X-Compat-Test", "cloud-run")
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("invoke %s: %v", uri, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("invoke: HTTP %d: %s", resp.StatusCode, b)
		}
		if !strings.Contains(string(b), "Welcome to nginx") {
			t.Fatalf("invoke did not reach the nginx container; body=%q", b)
		}
	})

	t.Run("get and revision", func(t *testing.T) {
		code, svcBody := api(t, httpClient, http.MethodGet, base+svcPath, nil)
		if code != http.StatusOK {
			t.Fatalf("get service: HTTP %d: %v", code, svcBody)
		}
		if got := strField(svcBody, "latestReadyRevision"); got != revName {
			t.Fatalf("get latestReadyRevision = %q, want %q", got, revName)
		}

		code, list := api(t, httpClient, http.MethodGet, base+svcPath+"/revisions", nil)
		if code != http.StatusOK {
			t.Fatalf("list revisions: HTTP %d: %v", code, list)
		}
		if !containsRevision(list, revName) {
			t.Fatalf("list revisions does not contain %q: %v", revName, list)
		}

		code, rev := api(t, httpClient, http.MethodGet, base+"/v2/"+revName, nil)
		if code != http.StatusOK {
			t.Fatalf("get revision: HTTP %d: %v", code, rev)
		}
		if got := strField(rev, "service"); got != svcName {
			t.Fatalf("revision service = %q, want %q", got, svcName)
		}
		if got := containerImage(rev); got != image {
			t.Fatalf("revision containers[0].image = %q, want %q", got, image)
		}
	})

	t.Run("iam", func(t *testing.T) {
		code, saved := api(t, httpClient, http.MethodPost, base+svcPath+":setIamPolicy", map[string]any{
			"policy": map[string]any{
				"bindings": []any{map[string]any{"role": "roles/run.invoker", "members": []any{"allUsers"}}},
			},
		})
		if code != http.StatusOK {
			t.Fatalf("setIamPolicy: HTTP %d: %v", code, saved)
		}
		if !policyHasBinding(saved, "roles/run.invoker", "allUsers") {
			t.Fatalf("setIamPolicy response lost the binding: %v", saved)
		}

		code, pol := api(t, httpClient, http.MethodGet, base+svcPath+":getIamPolicy", nil)
		if code != http.StatusOK {
			t.Fatalf("getIamPolicy: HTTP %d: %v", code, pol)
		}
		if !policyHasBinding(pol, "roles/run.invoker", "allUsers") {
			t.Fatalf("getIamPolicy lost the binding: %v", pol)
		}

		code, perms := api(t, httpClient, http.MethodPost, base+svcPath+":testIamPermissions", map[string]any{
			"permissions": []any{"run.services.get", "run.services.delete"},
		})
		if code != http.StatusOK {
			t.Fatalf("testIamPermissions: HTTP %d: %v", code, perms)
		}
		if got := stringSlice(perms["permissions"]); strings.Join(got, ",") != "run.services.get,run.services.delete" {
			t.Fatalf("testIamPermissions = %v, want both requested permissions", perms["permissions"])
		}
	})

	if code, body := api(t, httpClient, http.MethodDelete, base+svcPath, nil); code != http.StatusOK {
		t.Fatalf("delete service: HTTP %d: %v", code, body)
	}
	if code, _ := api(t, httpClient, http.MethodGet, base+svcPath, nil); code != http.StatusNotFound {
		t.Fatalf("get after delete: HTTP %d, want 404", code)
	}
	waitForWorkloadsGone(t, svcID, 60*time.Second)
}

// deleteService removes a service if present, tolerating a missing one.
func deleteService(t *testing.T, base, svcPath string) {
	t.Helper()
	code, _ := api(t, httpClient, http.MethodDelete, base+svcPath, nil)
	if code != http.StatusOK && code != http.StatusNotFound {
		t.Logf("cleanup delete %s: HTTP %d", svcPath, code)
	}
}

// containerImage returns the first container's image from a rendered Revision.
func containerImage(rev map[string]any) string {
	containers, _ := rev["containers"].([]any)
	if len(containers) == 0 {
		return ""
	}
	m, _ := containers[0].(map[string]any)
	return strField(m, "image")
}

func containsRevision(list map[string]any, want string) bool {
	revs, _ := list["revisions"].([]any)
	for _, r := range revs {
		if m, ok := r.(map[string]any); ok && strField(m, "name") == want {
			return true
		}
	}
	return false
}

func policyHasBinding(pol map[string]any, role, member string) bool {
	bindings, _ := pol["bindings"].([]any)
	for _, b := range bindings {
		m, _ := b.(map[string]any)
		if m == nil || strField(m, "role") != role {
			continue
		}
		for _, got := range stringSlice(m["members"]) {
			if got == member {
				return true
			}
		}
	}
	return false
}

func stringSlice(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

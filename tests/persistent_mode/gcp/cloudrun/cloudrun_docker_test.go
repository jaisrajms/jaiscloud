//go:build cloudrun_e2e

// This file is the real-Docker smoke for the Cloud Run docker execution path
// (D1). It proves behavioural execution end to end against an emulator started
// with JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=docker on the host:
//
//   - create a service whose template names a public image (nginx:latest) on a
//     declared container port, and settle the create LRO (inline in the default
//     synchronous mode, polled through operations.get under async-LRO mode)
//   - assert the returned Service shape (uri is http, latestReadyRevision,
//     terminal condition, trafficStatuses[0] at 100%)
//   - assert a labelled revision container exists on the local Docker daemon
//   - invoke the generated uri through the emulator with the authority as a Host
//     header and assert the container's response (the data-plane proxy)
//   - delete the service and assert the container is reaped
//
// Run with:
//
//	make test-e2e-cloudrun-docker
//
// It is inert without a reachable local Docker daemon: requireDocker skips when
// the docker CLI or the default-context daemon is absent. The emulator must run
// with JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=docker (the Makefile target does this).
//
// Required env:
//
//	CLOUDRUN_E2E_DOCKER — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	JAISCLOUD_HOST — default http://localhost:8080
package cloudrun_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func dockerBaseURL() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return strings.TrimRight(h, "/")
	}
	return "http://localhost:8080"
}

// dockerCLI runs docker against the local default-context daemon (the daemon the
// emulator's /var/run/docker.sock belongs to), independent of a remote context.
func dockerCLI(args ...string) (string, error) {
	full := append([]string{"--context", "default"}, args...)
	cmd := exec.Command("docker", full...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

func requireDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("CLOUDRUN_E2E_DOCKER") == "" {
		t.Skip("CLOUDRUN_E2E_DOCKER not set — skipping Cloud Run docker e2e test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found — skipping Cloud Run docker e2e test")
	}
	if _, err := dockerCLI("info"); err != nil {
		t.Skipf("local Docker daemon not reachable: %v", err)
	}
}

// dockerContainers returns the ids of a service's Cloud Run revision containers.
func dockerContainers(t *testing.T, svcID string) []string {
	t.Helper()
	out, err := dockerCLI("ps", "-a",
		"--filter", "label=jaiscloud.io/service=cloudrun",
		"--filter", "label=jaiscloud.io/run-service="+svcID,
		"--format", "{{.ID}}")
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return strings.Fields(strings.TrimSpace(out))
}

func waitForDockerContainers(t *testing.T, svcID string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if n := len(dockerContainers(t, svcID)); n >= want {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("docker containers for %q = %d, want >= %d", svcID, n, want)
		}
		time.Sleep(time.Second)
	}
}

func TestCloudRunDockerExecution(t *testing.T) {
	requireDocker(t)
	base := dockerBaseURL()

	if code, body := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	svcID := "cr-docker-" + run
	runBase := "/v2/projects/" + testProject + "/locations/" + testLocation
	collection := runBase + "/services"
	svcPath := collection + "/" + svcID
	// Cloud Run renders resource names without the /v2 prefix.
	svcName := "projects/" + testProject + "/locations/" + testLocation + "/services/" + svcID
	t.Cleanup(func() { deleteService(t, base, svcPath) })
	deleteService(t, base, svcPath)

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
	// In docker mode the revision container is created inline (the runtime is
	// ensured before the operation is recorded); the create operation is done
	// inline in the default synchronous LRO mode and in flight under async-LRO
	// mode, where pollRunOperation settles it.
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
		t.Fatalf("created service uri = %q, want http://%s- prefix (docker mode synthesizes http)", uri, svcID)
	}
	if revName := strField(created, "latestReadyRevision"); !strings.Contains(revName, "/revisions/") {
		t.Fatalf("latestReadyRevision = %q, want a /revisions/ name", revName)
	}
	if ct := strField(created, "terminalCondition", "type"); ct != "Ready" {
		t.Fatalf("terminalCondition.type = %q, want Ready", ct)
	}
	if ts, _ := created["trafficStatuses"].([]any); len(ts) == 0 {
		t.Fatalf("trafficStatuses empty: %v", created)
	} else if m, _ := ts[0].(map[string]any); m == nil {
		t.Fatalf("trafficStatuses[0] malformed: %v", created)
	} else if p, _ := m["percent"].(float64); p != 100 {
		t.Fatalf("trafficStatuses[0].percent = %v, want 100", m["percent"])
	}

	// The docker runtime manager must have started a labelled container.
	waitForDockerContainers(t, svcID, 1, 60*time.Second)

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

	if code, body := api(t, httpClient, http.MethodDelete, base+svcPath, nil); code != http.StatusOK {
		t.Fatalf("delete service: HTTP %d: %v", code, body)
	}
	if code, _ := api(t, httpClient, http.MethodGet, base+svcPath, nil); code != http.StatusNotFound {
		t.Fatalf("get after delete: HTTP %d, want 404", code)
	}

	// The revision container must be reaped on service delete.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if len(dockerContainers(t, svcID)) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("container for %q survived service delete", svcID)
		}
		time.Sleep(time.Second)
	}
}

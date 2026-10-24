package ui

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"jaiscloud/internal/docker"
)

func TestKubernetesConfigSource(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("JAISCLOUD_K8S_APISERVER", "")
	t.Setenv("JAISCLOUD_K8S_TOKEN", "")
	t.Setenv("JAISCLOUD_K8S_TOKEN_FILE", "")

	if got := kubernetesConfigSource(); got != "no k8s config (using default host)" {
		t.Errorf("no config: source = %q", got)
	}

	t.Setenv("JAISCLOUD_K8S_APISERVER", "https://example:6443")
	if got := kubernetesConfigSource(); got != "JAISCLOUD_K8S_* env" {
		t.Errorf("env config: source = %q", got)
	}

	t.Setenv("JAISCLOUD_K8S_APISERVER", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	if got := kubernetesConfigSource(); got != "in-cluster service account" {
		t.Errorf("in-cluster: source = %q", got)
	}
}

// A DOCKER_HOST override must be surfaced, never silently followed: the
// emulator's executors only use the local socket. Probing a nonexistent socket
// keeps the assertion independent of the host's daemon.
func TestDockerHealthNotesIgnoredDockerHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "ssh://user@remote")
	health := dockerHealthAt(context.Background(), "/nonexistent/docker.sock")
	if health.Available {
		t.Fatalf("docker health = available, want unavailable without a socket")
	}
	if !strings.Contains(health.Detail, "DOCKER_HOST=ssh://user@remote is ignored") {
		t.Errorf("detail = %q, want it to mention the ignored override", health.Detail)
	}
}

// fakeDockerSocket serves the Engine API's /_ping over a unix socket, so the
// reachable path of dockerHealthAt (and the deployment checks that follow it)
// can be exercised without a real daemon.
func fakeDockerSocket(t *testing.T, status int) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_ping" {
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})}
	go func() { _ = srv.Serve(ln) }() //nolint:errcheck
	t.Cleanup(func() { _ = srv.Close() })
	return socket
}

// A reachable daemon in the default (host) deployment reports available with the
// plain detail and no container-deployment qualifier.
func TestDockerHealthAtReachableHostDeployment(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv(docker.EnvDeployMode, "host")
	socket := fakeDockerSocket(t, http.StatusOK)

	health := dockerHealthAt(context.Background(), socket)
	if !health.Available {
		t.Fatalf("reachable host-mode daemon = unavailable: %q", health.Detail)
	}
	if !strings.Contains(health.Detail, "docker daemon reachable on "+socket) {
		t.Errorf("detail = %q, want the socket named", health.Detail)
	}
	if strings.Contains(health.Detail, "container deployment") {
		t.Errorf("host mode must not claim a container deployment: %q", health.Detail)
	}
}

// Health must refuse to call a containerized deployment usable when the shared
// network is unset: the socket answers, but every workload would be unreachable.
func TestDockerHealthAtContainerDeploymentWithoutNetwork(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv(docker.EnvDeployMode, "container")
	t.Setenv(docker.EnvNetwork, "")
	socket := fakeDockerSocket(t, http.StatusOK)

	health := dockerHealthAt(context.Background(), socket)
	if health.Available {
		t.Fatal("a containerized emulator without a shared network must not report the backend available")
	}
	if !strings.Contains(health.Detail, docker.EnvNetwork) {
		t.Errorf("detail = %q, want it to name %s as the fix", health.Detail, docker.EnvNetwork)
	}
}

// A containerized deployment with the shared network reports available and says
// how workloads are addressed.
func TestDockerHealthAtContainerDeploymentWithNetwork(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv(docker.EnvDeployMode, "container")
	t.Setenv(docker.EnvNetwork, "jaiscloud_jaiscloud")
	socket := fakeDockerSocket(t, http.StatusOK)

	health := dockerHealthAt(context.Background(), socket)
	if !health.Available {
		t.Fatalf("containerized daemon with a network = unavailable: %q", health.Detail)
	}
	if !strings.Contains(health.Detail, "container deployment") || !strings.Contains(health.Detail, "jaiscloud_jaiscloud") {
		t.Errorf("detail = %q, want the container deployment and its network", health.Detail)
	}
}

// A non-200 /_ping is a failure, not a reachable daemon.
func TestDockerHealthAtNon200Ping(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	t.Setenv(docker.EnvDeployMode, "host")
	socket := fakeDockerSocket(t, http.StatusServiceUnavailable)

	health := dockerHealthAt(context.Background(), socket)
	if health.Available {
		t.Fatal("a non-200 /_ping must not report the daemon available")
	}
	if !strings.Contains(health.Detail, "/_ping") {
		t.Errorf("detail = %q, want the failing ping", health.Detail)
	}
}

// The handler must always return a well-formed document with both probes, so
// the admin Runtime view can render even when an engine is unreachable.
func TestBuildRuntimeHealthHandler_Shape(t *testing.T) {
	rec := httptest.NewRecorder()
	buildRuntimeHealthHandler()(rec, httptest.NewRequest(http.MethodGet, "/api/ui/v1/gcp/runtime", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var health RuntimeHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if health.Docker.Detail == "" {
		t.Errorf("docker probe returned no detail: %+v", health.Docker)
	}
	if health.Kubernetes.Detail == "" {
		t.Errorf("kubernetes probe returned no detail: %+v", health.Kubernetes)
	}
}

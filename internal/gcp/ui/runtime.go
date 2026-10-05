package ui

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/k8shelpers"
)

// dockerSocket is the socket the emulator's Docker executors use. They hardcode
// the local socket (internal/executor/lambda/docker.go, internal/executor/ecs/
// docker.go, internal/gcp/runexec/docker.go, internal/sparkhelpers/docker.go)
// and ignore DOCKER_HOST, so the health probe deliberately matches them rather
// than the developer's `docker` CLI context (which may be a remote SSH/tcp
// context the emulator cannot reach).
const dockerSocket = "/var/run/docker.sock"

// k8sSATokenPath is where an in-cluster service account token is mounted.
const k8sSATokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// RuntimeEngineHealth is the liveness of one host engine the emulator can use.
// It is deliberately a probe, not a configured value: Available=true means the
// engine answered just now.
type RuntimeEngineHealth struct {
	Available bool   `json:"available"`
	Detail    string `json:"detail,omitempty"`
}

// RuntimeHealth is the payload for GET /api/ui/v1/gcp/runtime, driving the
// read-only Docker/Kubernetes rows on the admin Runtime view.
type RuntimeHealth struct {
	Docker     RuntimeEngineHealth `json:"docker"`
	Kubernetes RuntimeEngineHealth `json:"kubernetes"`
}

// buildRuntimeHealthHandler probes the host engines on each request (the admin
// view polls, so results stay live). The probe budget is shared so a hung
// daemon cannot stall the response.
func buildRuntimeHealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		uihelper.WriteJSON(w, RuntimeHealth{
			Docker:     dockerHealth(ctx),
			Kubernetes: kubernetesHealth(ctx),
		})
	}
}

// dockerHealth pings the Docker daemon over the same local socket the
// executors use. It reports (rather than follows) a DOCKER_HOST override, so
// the health can never claim the emulator will use a daemon it cannot.
func dockerHealth(ctx context.Context) RuntimeEngineHealth {
	return dockerHealthAt(ctx, dockerSocket)
}

// dockerHealthAt probes a specific socket, so the behaviour is testable.
func dockerHealthAt(ctx context.Context, socket string) RuntimeEngineHealth {
	ignored := ""
	if h := os.Getenv("DOCKER_HOST"); h != "" {
		ignored = fmt.Sprintf(" (emulator uses a local unix socket; DOCKER_HOST=%s is ignored)", h)
	}
	if _, err := os.Stat(socket); err != nil {
		return RuntimeEngineHealth{Available: false, Detail: "socket not found: " + socket + ignored}
	}
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: err.Error() + ignored}
	}
	resp, err := client.Do(req)
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: err.Error() + ignored}
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return RuntimeEngineHealth{Available: false, Detail: fmt.Sprintf("docker /_ping = %d%s", resp.StatusCode, ignored)}
	}
	return RuntimeEngineHealth{Available: true, Detail: "docker daemon reachable on " + socket + ignored}
}

// kubernetesHealth reports whether the same client the executors use can reach
// the API server, and names how that client resolved its config so the operator
// knows where to point a fix. It reflects the emulator's configuration
// (in-cluster or the JAISCLOUD_K8S_* env), never a developer's kubeconfig.
func kubernetesHealth(ctx context.Context) RuntimeEngineHealth {
	source := kubernetesConfigSource()
	client, err := k8shelpers.NewClient()
	if err != nil {
		return RuntimeEngineHealth{Available: false, Detail: source + ": client: " + err.Error()}
	}
	version, err := client.Discovery().ServerVersion()
	if err != nil {
		return RuntimeEngineHealth{
			Available: false,
			Detail:    source + ": " + err.Error() + "; set JAISCLOUD_K8S_APISERVER / JAISCLOUD_K8S_TOKEN to point the emulator at a cluster",
		}
	}
	return RuntimeEngineHealth{Available: true, Detail: source + ", server " + version.GitVersion}
}

// kubernetesConfigSource names the config k8shelpers.NewClient resolves, in the
// same precedence order: in-cluster service account, then JAISCLOUD_K8S_* env,
// then the bare in-cluster default host (nothing configured).
func kubernetesConfigSource() string {
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return "in-cluster service account"
	}
	if _, err := os.Stat(k8sSATokenPath); err == nil {
		return "in-cluster service account"
	}
	if os.Getenv("JAISCLOUD_K8S_APISERVER") != "" ||
		os.Getenv("JAISCLOUD_K8S_TOKEN") != "" ||
		os.Getenv("JAISCLOUD_K8S_TOKEN_FILE") != "" {
		return "JAISCLOUD_K8S_* env"
	}
	return "no k8s config (using default host)"
}

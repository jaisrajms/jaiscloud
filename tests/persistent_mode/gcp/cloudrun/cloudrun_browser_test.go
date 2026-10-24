//go:build cloudrun_e2e

// This file is the SPK4 gate: it proves a Cloud Run data-plane invocation is
// reachable from something that cannot set a Host header on its own (a browser).
//
// The k8s/docker executors synthesize the invocation authority
// http://{service}-{token}.{location}.{suffix}[:port] and route by Host
// (run.IsInvocationHost -> internal/gcp/adapter/router.go). The execution smokes
// above set the Host header by hand; a browser only sends the host it navigated
// to, and the default suffix (*.run.app) does not resolve. The recipe is to
// configure the suffix as a *.localhost name — the OS resolver and the Go
// resolver map any *.localhost name to loopback — and the authority port as the
// host-forwarded emulator port, then browse the synthesized uri directly.
//
// This test dials the synthesized uri with no Host override (the
// browser-equivalent path), so it covers DNS resolution + Host-based routing +
// the data-plane proxy end to end, and logs the URL to open in a real browser.
//
// Run with:
//
//	make test-e2e-cloudrun-browser
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The Makefile target configures the deployed
// emulator's JAISCLOUD_CLOUDRUN_URL_SUFFIX / JAISCLOUD_CLOUDRUN_URL_PORT before
// running.
//
// Required env:
//
//	CLOUDRUN_E2E_BROWSER — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	CLOUDRUN_BROWSER_SUFFIX — expected authority suffix (default run.localhost)
//	CLOUDRUN_BROWSER_PORT   — host-forwarded authority port (default 18080)
//	K8S_NAMESPACE           — default jaiscloud
package cloudrun_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(t *testing.T, key string, def int) int {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q is not an integer: %v", key, v, err)
	}
	return n
}

// startFixedPortForward forwards a Service port onto a fixed local port and
// returns the base URL and a stop function. The port must be fixed (not a free
// port) so it matches the authority the emulator synthesizes: the browser dials
// the synthesized hostname at that port, which resolves to loopback and lands on
// this forward.
func startFixedPortForward(t *testing.T, svc string, localPort, remotePort int) (string, func()) {
	t.Helper()
	cmd := exec.Command("kubectl", "-n", namespace(), "port-forward",
		"svc/"+svc, fmt.Sprintf("%d:%d", localPort, remotePort))
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward %s %d:%d: %v", svc, localPort, remotePort, err)
	}
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", localPort)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return base, stop
		}
		time.Sleep(300 * time.Millisecond)
	}
	stop()
	t.Fatalf("port-forward to svc/%s on :%d never became ready: %s", svc, localPort, strings.TrimSpace(errb.String()))
	return "", func() {}
}

// noProxyClient dials the synthesized authority directly. A proxy env var would
// otherwise divert the *.localhost request away from the port-forward.
var noProxyClient = &http.Client{
	Timeout:   60 * time.Second,
	Transport: &http.Transport{Proxy: nil},
}

func TestCloudRunBrowserReachability(t *testing.T) {
	if os.Getenv("CLOUDRUN_E2E_BROWSER") == "" {
		t.Skip("CLOUDRUN_E2E_BROWSER not set — skipping Cloud Run browser-reachability e2e test")
	}
	requireK3d(t)

	suffix := envOr("CLOUDRUN_BROWSER_SUFFIX", "run.localhost")
	port := envInt(t, "CLOUDRUN_BROWSER_PORT", 18080)

	base, stop := startFixedPortForward(t, "jaiscloud-gcp", port, 8080)
	t.Cleanup(stop)

	if code, body := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	svcID := "cr-browser-" + run
	runBase := "/v2/projects/" + testProject + "/locations/" + testLocation
	collection := runBase + "/services"
	svcPath := collection + "/" + svcID
	t.Cleanup(func() { deleteService(t, base, svcPath) })

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
	// The create is done inline in the default synchronous LRO mode; settle it
	// through operations.get when the demo's async-LRO pacing cue is applied.
	op = pollRunOperation(t, base, op, 2*time.Minute)
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("create service operation never settled: %v", op)
	}
	created, _ := op["response"].(map[string]any)
	if created == nil {
		t.Fatalf("create service operation has no response Service: %v", op)
	}
	uri := strField(created, "uri")

	// The authority must be the *.localhost name a browser can resolve at the
	// host-forwarded port, or the recipe does not hold.
	u, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse synthesized uri %q: %v", uri, err)
	}
	wantHostSuffix := "." + testLocation + "." + suffix
	if !strings.HasSuffix(u.Hostname(), wantHostSuffix) {
		t.Fatalf("synthesized uri host %q does not end in %q — is JAISCLOUD_CLOUDRUN_URL_SUFFIX=%s set?", u.Hostname(), wantHostSuffix, suffix)
	}
	if got := u.Port(); got != strconv.Itoa(port) {
		t.Fatalf("synthesized uri port = %q, want %d — is JAISCLOUD_CLOUDRUN_URL_PORT=%d set?", got, port, port)
	}
	if !strings.HasSuffix(u.Hostname(), ".localhost") {
		t.Fatalf("synthesized uri host %q is not a *.localhost name; a browser cannot resolve it without a hosts entry", u.Hostname())
	}

	// Dial the synthesized uri with no Host override — exactly what a browser
	// does. Any *.localhost name resolves to loopback, so this exercises DNS +
	// Host-based routing + the data-plane proxy end to end.
	resp, err := noProxyClient.Get(uri + "/?compat=browser")
	if err != nil {
		t.Fatalf("GET %s (browser-equivalent, no Host override): %v", uri, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: HTTP %d: %s", uri, resp.StatusCode, b)
	}
	if !strings.Contains(string(b), "Welcome to nginx") {
		t.Fatalf("GET %s did not reach the service; body=%q", uri, b)
	}
	t.Logf("browser-reachable: open %s/?compat=browser in the demo browser", uri)

	if code, body := api(t, httpClient, http.MethodDelete, base+svcPath, nil); code != http.StatusOK {
		t.Fatalf("delete service: HTTP %d: %v", code, body)
	}
	waitForWorkloadsGone(t, svcID, 60*time.Second)
}

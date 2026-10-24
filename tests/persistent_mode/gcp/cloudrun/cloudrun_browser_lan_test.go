//go:build cloudrun_e2e

// This file is the SPK7 gate: it proves a Cloud Run data-plane invocation is
// reachable from a browser that is NOT on the emulator host, so the demo's
// recording take can run locally against a remote emulator.
//
// SPK4 (cloudrun_browser_test.go) covers the *.localhost authority, which only
// resolves to loopback on the emulator host itself. A recording browser on
// another machine resolves *.localhost to its own loopback and never reaches the
// data plane. SPK7 serves the demo URL on the emulator host's LAN address: the
// synthesized suffix is a nip.io wildcard name (`run.<lan-ip>.nip.io`), so the
// public resolver maps any authority under it to the emulator host's LAN IP and
// the browser sends the Host the router routes on — no /etc/hosts edit and no
// reverse proxy.
//
// The test dials the synthesized nip.io authority with no Host override (the
// browser-equivalent path) through a port-forward bound to 0.0.0.0, so the
// request genuinely leaves loopback and lands on the LAN address a remote
// browser would use. It covers wildcard-DNS resolution + Host-based routing +
// the data-plane proxy end to end, and logs the URL to open in the recording
// browser.
//
// Run with:
//
//	make test-e2e-cloudrun-browser-lan
//
// It is inert without a k3d cluster (requireK3d skips) and requires nip.io DNS
// resolution. The Makefile target configures the deployed emulator's
// JAISCLOUD_CLOUDRUN_URL_SUFFIX / JAISCLOUD_CLOUDRUN_URL_PORT first.
//
// Required env:
//
//	CLOUDRUN_E2E_BROWSER_LAN — set to a non-empty value to run (else skipped)
//	CLOUDRUN_BROWSER_LAN_IP  — the emulator host's LAN IP (the nip.io target)
//
// Optional env:
//
//	CLOUDRUN_BROWSER_LAN_PORT — LAN-forwarded authority port (default 8080)
//	K8S_NAMESPACE             — default jaiscloud
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

// startLANPortForward forwards a Service onto a fixed local port bound to every
// interface, so the LAN-IP nip.io authority a remote browser dials lands here.
// The port must be fixed (not a free port) so it matches the port the emulator
// synthesizes into the authority.
func startLANPortForward(t *testing.T, svc string, localPort, remotePort int) (string, func()) {
	t.Helper()
	cmd := exec.Command("kubectl", "-n", namespace(), "port-forward",
		"--address", "0.0.0.0", "svc/"+svc, fmt.Sprintf("%d:%d", localPort, remotePort))
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start LAN port-forward %s %d:%d: %v", svc, localPort, remotePort, err)
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
	t.Fatalf("LAN port-forward to svc/%s on :%d never became ready: %s", svc, localPort, strings.TrimSpace(errb.String()))
	return "", func() {}
}

func TestCloudRunBrowserLANReachability(t *testing.T) {
	if os.Getenv("CLOUDRUN_E2E_BROWSER_LAN") == "" {
		t.Skip("CLOUDRUN_E2E_BROWSER_LAN not set — skipping Cloud Run LAN browser-reachability e2e test")
	}
	requireK3d(t)

	lanIP := strings.TrimSpace(os.Getenv("CLOUDRUN_BROWSER_LAN_IP"))
	if lanIP == "" {
		t.Skip("CLOUDRUN_BROWSER_LAN_IP not set — cannot build the nip.io authority")
	}
	port := envInt(t, "CLOUDRUN_BROWSER_LAN_PORT", 8080)
	suffix := fmt.Sprintf("run.%s.nip.io", lanIP)

	base, stop := startLANPortForward(t, "jaiscloud-gcp", port, 8080)
	t.Cleanup(stop)

	if code, body := api(t, httpClient, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	svcID := "cr-lan-" + run
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
	op = pollRunOperation(t, base, op, 2*time.Minute)
	if done, _ := op["done"].(bool); !done {
		t.Fatalf("create service operation never settled: %v", op)
	}
	created, _ := op["response"].(map[string]any)
	if created == nil {
		t.Fatalf("create service operation has no response Service: %v", op)
	}
	uri := strField(created, "uri")

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
	// The whole point of SPK7: the authority must NOT be a *.localhost name,
	// which would resolve to loopback on the recording machine.
	if strings.HasSuffix(u.Hostname(), ".localhost") {
		t.Fatalf("synthesized uri host %q is a *.localhost name; a remote recording browser cannot resolve it", u.Hostname())
	}

	// Dial the nip.io authority with no Host override. Wildcard DNS maps it to
	// the emulator host's LAN IP, where the 0.0.0.0 port-forward is listening,
	// so this exercises DNS + Host-based routing + the data-plane proxy.
	resp, err := noProxyClient.Get(uri + "/?compat=lan")
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
	t.Logf("LAN browser-reachable: open %s/?compat=lan in the recording browser", uri)

	if code, body := api(t, httpClient, http.MethodDelete, base+svcPath, nil); code != http.StatusOK {
		t.Fatalf("delete service: HTTP %d: %v", code, body)
	}
	waitForWorkloadsGone(t, svcID, 60*time.Second)
}

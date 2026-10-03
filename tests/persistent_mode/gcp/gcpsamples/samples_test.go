//go:build gcpsamples_e2e

// Package gcpsamples_test is an application-level end-to-end suite for
// jaiscloud-gcp: it deploys the official Spring Cloud GCP sample applications
// (Pub/Sub, Firestore, Datastore) into a k3d cluster, points them at the
// in-cluster emulator, and drives them over HTTP through the real Java client
// libraries.
//
// Run with:
//
//	make test-e2e-gcp-samples-k3d
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent.
package gcpsamples_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const (
	pubsubDeploy    = "gcp-sample-pubsub"
	firestoreDeploy = "gcp-sample-firestore"
	datastoreDeploy = "gcp-sample-datastore"

	projectID = "jaiscloud-project"
)

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
}

// repoRoot resolves the checkout root from this source file so the suite can
// apply the sample manifest regardless of the working directory.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func manifestPath() string {
	if v := os.Getenv("GCP_SAMPLES_MANIFEST"); v != "" {
		return v
	}
	return filepath.Join(repoRoot(), "deploy", "k8s", "gcp-samples", "samples.yaml")
}

// kubectl runs kubectl and returns stdout (stderr folded into the error).
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

// requireK3d skips the test unless kubectl is present and the emulator Service
// is reachable in the target namespace.
func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d GCP samples e2e")
	}
	if _, err := kubectl("-n", namespace(), "get", "svc", "jaiscloud-gcp"); err != nil {
		t.Skipf("svc/jaiscloud-gcp not reachable in namespace %q (apply deploy/k8s/jaiscloud-gcp.yaml): %v", namespace(), err)
	}
}

func applySamples(t *testing.T) {
	t.Helper()
	path := manifestPath()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("samples manifest not found at %s: %v", path, err)
	}
	if out, err := kubectl("apply", "-f", path); err != nil {
		t.Fatalf("apply samples: %v\n%s", err, out)
	}
}

func waitRollout(t *testing.T, deploy string) {
	t.Helper()
	if out, err := kubectl("-n", namespace(), "rollout", "status", "deployment/"+deploy, "--timeout=240s"); err != nil {
		t.Fatalf("rollout %s: %v\n%s", deploy, err, out)
	}
}

// portForward starts `kubectl port-forward svc/<svc> <local>:<remote>` and
// returns the base URL. The process is torn down when the test ends.
func portForward(t *testing.T, svc string, local, remote int) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "-n", namespace(), "port-forward",
		fmt.Sprintf("svc/%s", svc), fmt.Sprintf("%d:%d", local, remote))
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start port-forward %s: %v", svc, err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	addr := fmt.Sprintf("127.0.0.1:%d", local)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return "http://" + addr
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("port-forward svc/%s did not become ready on %s: %s", svc, addr, errb.String())
	return ""
}

func resetEmulator(t *testing.T, emuBase string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, emuBase+"/_jaiscloud/reset", nil)
	if err != nil {
		t.Fatalf("build reset request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("reset emulator: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("reset emulator: HTTP %d: %s", resp.StatusCode, body)
	}
}

// httpClient does not follow redirects so callers can observe the sample's
// RedirectView status codes directly.
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func do(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	return resp
}

// ok asserts a 2xx or 3xx (the samples redirect after a mutating call).
func ok(t *testing.T, resp *http.Response, what string) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: HTTP %d: %s", what, resp.StatusCode, body)
	}
}

func get(t *testing.T, rawURL string) string {
	t.Helper()
	resp := do(t, mustReq(t, http.MethodGet, rawURL, nil))
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: HTTP %d: %s", rawURL, resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func postForm(t *testing.T, rawURL string, form url.Values) {
	t.Helper()
	req := mustReq(t, http.MethodPost, rawURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp := do(t, req)
	ok(t, resp, "POST "+rawURL)
}

func postJSON(t *testing.T, rawURL, body string) {
	t.Helper()
	req := mustReq(t, http.MethodPost, rawURL, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := do(t, req)
	ok(t, resp, "POST "+rawURL)
}

func mustReq(t *testing.T, method, rawURL string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, rawURL, err)
	}
	return req
}

// waitLogContains polls the Deployment's logs for a marker.
func waitLogContains(t *testing.T, deploy, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if out, err := kubectl("-n", namespace(), "logs", "deploy/"+deploy, "--tail=400"); err == nil && strings.Contains(out, needle) {
			return
		}
		time.Sleep(time.Second)
	}
	out, _ := kubectl("-n", namespace(), "logs", "deploy/"+deploy, "--tail=400")
	t.Fatalf("logs of %s did not contain %q within %s; tail:\n%s", deploy, needle, timeout, out)
}

func TestSpringCloudGCPSamples(t *testing.T) {
	requireK3d(t)

	applySamples(t)
	for _, d := range []string{pubsubDeploy, firestoreDeploy, datastoreDeploy} {
		waitRollout(t, d)
	}

	emu := portForward(t, "jaiscloud-gcp", 18080, 8080)
	resetEmulator(t, emu)

	t.Run("pubsub", func(t *testing.T) { testPubSub(t, emu) })
	t.Run("firestore", func(t *testing.T) { testFirestore(t, emu) })
	t.Run("datastore", func(t *testing.T) { testDatastore(t) })
}

func testPubSub(t *testing.T, emu string) {
	base := portForward(t, pubsubDeploy, 18081, 8080)

	id := time.Now().UnixNano()
	topic := fmt.Sprintf("e2e-topic-%d", id)
	sub := fmt.Sprintf("e2e-sub-%d", id)
	msg := fmt.Sprintf("hello-%d", id)

	postForm(t, base+"/createTopic", url.Values{"topicName": {topic}})
	postForm(t, base+"/createSubscription", url.Values{"topicName": {topic}, "subscriptionName": {sub}})

	// A streaming subscriber started here (real Pub/Sub client) must receive
	// the messages published below.
	ok(t, do(t, mustReq(t, http.MethodGet, base+"/subscribe?subscription="+url.QueryEscape(sub), nil)), "subscribe")

	for i := 0; i < 3; i++ {
		ok(t, do(t, mustReq(t, http.MethodGet,
			fmt.Sprintf("%s/postMessage?topicName=%s&message=%s&count=1", base, url.QueryEscape(topic), url.QueryEscape(msg)), nil)), "postMessage")
	}

	waitLogContains(t, pubsubDeploy,
		fmt.Sprintf("Message received from %s subscription: %s", sub, msg), 30*time.Second)

	// Independent cross-check: the topic exists in the emulator via its REST
	// API, proving the gRPC admin call reached the shared store.
	resp := do(t, mustReq(t, http.MethodGet, emu+"/v1/projects/"+projectID+"/topics/"+topic, nil))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("emulator topic %s: HTTP %d: %s", topic, resp.StatusCode, body)
	}
}

func testFirestore(t *testing.T, emu string) {
	base := portForward(t, firestoreDeploy, 18082, 8080)

	name := fmt.Sprintf("e2e-user-%d", time.Now().UnixNano())
	postForm(t, base+"/users/saveUser", url.Values{"name": {name}, "age": {"42"}})

	// Read back through the sample's repository (Firestore gRPC client).
	out := get(t, base+"/users/age?age=42")
	if !strings.Contains(out, name) {
		t.Fatalf("Firestore readback missing %q: %s", name, out)
	}

	// Independent cross-check over the emulator's Firestore REST API: the
	// gRPC-written document must be visible there too (same project, so the
	// two transports share state).
	docURL := fmt.Sprintf("%s/v1/projects/%s/databases/%%28default%%29/documents/users/%s", emu, projectID, url.PathEscape(name))
	resp := do(t, mustReq(t, http.MethodGet, docURL, nil))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("emulator REST read of %s: HTTP %d: %s", docURL, resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), name) {
		t.Fatalf("emulator REST read of %s missing %q: %s", docURL, name, body)
	}
}

func testDatastore(t *testing.T) {
	base := portForward(t, datastoreDeploy, 18083, 8080)

	title := fmt.Sprintf("SRE Book %d", time.Now().UnixNano())
	postJSON(t, base+"/saveBook", fmt.Sprintf(`{"title":%q,"author":"Google","year":2024}`, title))

	out := get(t, base+"/findAllBooks")
	if !strings.Contains(out, title) {
		t.Fatalf("Datastore readback missing %q: %s", title, out)
	}

	byAuthor := get(t, base+"/findByAuthor?author=Google")
	if !strings.Contains(byAuthor, title) {
		t.Fatalf("Datastore findByAuthor missing %q: %s", title, byAuthor)
	}
}

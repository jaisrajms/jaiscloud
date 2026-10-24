//go:build functions_e2e

// Package functions_test verifies the Cloud Functions native-executor contract
// end to end: a source archive uploaded to the emulated GCS, a function created
// with a v2 buildConfig.source.storageSource, and an invocation that runs the
// archive under a Functions-Framework image via the shared container executor.
//
// Unlike the Lambda-contract source-execution tests (which use a Lambda handler
// signature and an AWS RIE image), this exercises the GCP-native path: the
// Functions Framework serves POST / on $PORT, the source is mounted at
// /workspace, and FUNCTION_TARGET selects the entry point.
//
// Requires a jaiscloud-gcp started with JAISCLOUD_EXECUTOR_MODE=docker (the
// Makefile target test-e2e-functions-framework-docker does this) or =k8s, a
// running Docker daemon / k3d cluster, and the Functions-Framework image
// available.
//
// Required env:
//
//	FUNCTIONS_E2E_FF_IMAGE — set to the Functions-Framework image to run (else skipped)
//
// Optional env:
//
//	JAISCLOUD_HOST — default http://localhost:8080
package functions_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// ffSource is the Python Functions-Framework source mounted into the container.
// The framework loads hello() because the function's entryPoint is "hello".
const ffSource = `import functions_framework

@functions_framework.http
def hello(request):
    return {"hello": "world", "method": request.method, "body": request.get_data(as_text=True)}
`

// ffEventSource is the CloudEvent-signature source mounted for the event e2e.
// The Functions Framework discards a cloud_event handler's return value, so the
// handler itself is the observable marker: it fails (HTTP 500, → a non-"delivered"
// delivery record) unless the delivered event carries exactly the marker the
// test published. A "delivered" record therefore proves the framework invoked
// this handler with this event, not merely that an invocation was attempted.
//
// The handler reads the marker the way a real gen2 function does — from the
// Pub/Sub MessagePublishedData envelope (cloud_event.data["message"]["data"],
// base64) — so the e2e locks the real wire envelope (FNX5), not the raw payload
// the emulator used to send.
func ffEventSource(marker string) string {
	return `import base64
import json

import functions_framework


@functions_framework.cloud_event
def hello_event(cloud_event):
    message = (cloud_event.data or {}).get("message") or {}
    raw = message.get("data")
    if not isinstance(raw, str):
        raise RuntimeError(f"missing MessagePublishedData envelope: {cloud_event.data!r}")
    got = (json.loads(base64.b64decode(raw)) or {}).get("marker")
    if got != "` + marker + `":
        raise RuntimeError(f"unexpected event marker: {got!r}")
    return {"marker": got}
`
}

func requireFFEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("FUNCTIONS_E2E_FF_IMAGE") == "" {
		t.Skip("FUNCTIONS_E2E_FF_IMAGE not set — skipping Cloud Functions Functions-Framework e2e test")
	}
}

// uploadFFSource uploads a source archive through the v2 generateUploadUrl flow
// and returns the provisioned storageSource bucket/object it landed in.
func uploadFFSource(t *testing.T, zipBytes []byte) (bucket, object string) {
	t.Helper()

	code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions:generateUploadUrl",
		[]byte(`{}`), "application/json")
	if code != http.StatusOK {
		t.Fatalf("generateUploadUrl: HTTP %d: %s", code, body)
	}
	var up struct {
		UploadURL     string `json:"uploadUrl"`
		StorageSource struct {
			Bucket string `json:"bucket"`
			Object string `json:"object"`
		} `json:"storageSource"`
	}
	if err := json.Unmarshal(body, &up); err != nil {
		t.Fatalf("generateUploadUrl response: %v (%s)", err, body)
	}
	if up.UploadURL == "" || up.StorageSource.Bucket == "" || up.StorageSource.Object == "" {
		t.Fatalf("generateUploadUrl incomplete: %s", body)
	}
	if code, body := putAbsolute(t, up.UploadURL, zipBytes, "application/zip"); code != http.StatusOK {
		t.Fatalf("upload source: HTTP %d: %s", code, body)
	}
	return up.StorageSource.Bucket, up.StorageSource.Object
}

// deployFFFunction uploads a Functions-Framework source archive through the v2
// generateUploadUrl flow and creates a v2 function pointing at it.
func deployFFFunction(t *testing.T, id, entryPoint string) {
	t.Helper()

	bucket, object := uploadFFSource(t, buildZip(t, map[string]string{"main.py": ffSource}))
	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"` + entryPoint + `",` +
		`"source":{"storageSource":{"bucket":"` + bucket + `","object":"` + object + `"}}}}`)
	if code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions?functionId="+id,
		create, "application/json"); code != http.StatusOK {
		t.Fatalf("create function: HTTP %d: %s", code, body)
	}
}

// deployFFEventFunction creates a Pub/Sub topic and a v2 function whose
// eventTrigger subscribes to it, deploying the given source archive.
func deployFFEventFunction(t *testing.T, id, topic, entryPoint, source string) {
	t.Helper()

	if code, body := do(t, "PUT", "/v1/projects/proj/topics/"+topic, []byte(`{}`), "application/json"); code != http.StatusOK {
		t.Fatalf("create topic: HTTP %d: %s", code, body)
	}
	bucket, object := uploadFFSource(t, buildZip(t, map[string]string{"main.py": source}))
	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"` + entryPoint + `",` +
		`"source":{"storageSource":{"bucket":"` + bucket + `","object":"` + object + `"}}},` +
		`"eventTrigger":{"eventType":"google.cloud.pubsub.topic.v1.messagePublished",` +
		`"pubsubTopic":"projects/proj/topics/` + topic + `","retryPolicy":"RETRY_POLICY_RETRY"}}`)
	if code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions?functionId="+id,
		create, "application/json"); code != http.StatusOK {
		t.Fatalf("create event function: HTTP %d: %s", code, body)
	}
}

// deliveryRecord is the subset of a persisted event-delivery record the event
// e2e asserts on.
type deliveryRecord struct {
	FunctionID string `json:"functionId"`
	EventType  string `json:"eventType"`
	Data       string `json:"data"`
	Status     string `json:"status"`
	Result     string `json:"result"`
	Attempts   int    `json:"attempts"`
}

// functionDeliveries reads the emulator export and returns the persisted Cloud
// Functions event-delivery records. Real Cloud Functions exposes no delivery
// API, so the export (the registered "functions" snapshotter) is the observation
// surface for retry/dead-letter outcomes, exactly as in
// tests/integration/gcp/functions_test.go.
func functionDeliveries(t *testing.T) []deliveryRecord {
	t.Helper()

	code, body := do(t, "GET", "/_jaiscloud/export", nil, "")
	if code != http.StatusOK {
		t.Fatalf("export: HTTP %d: %s", code, body)
	}
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("export gzip: %v", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	var env struct {
		Stores map[string]json.RawMessage `json:"stores"`
	}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("export tar: %v", err)
		}
		if hdr.Name != "envelope.json" {
			continue
		}
		raw, rerr := io.ReadAll(tr)
		if rerr != nil {
			t.Fatalf("read envelope: %v", rerr)
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("envelope: %v", err)
		}
	}
	var snap struct {
		Deliveries json.RawMessage `json:"deliveries"`
	}
	if err := json.Unmarshal(env.Stores["functions"], &snap); err != nil {
		t.Fatalf("functions snapshot: %v", err)
	}
	if len(snap.Deliveries) == 0 {
		return nil
	}
	// The memory store snapshots deliveries as map[scope]map[id]Delivery; the
	// Postgres store snapshots them as a []{projectId, delivery} array.
	var out []deliveryRecord
	var arr []struct {
		Delivery deliveryRecord `json:"delivery"`
	}
	if err := json.Unmarshal(snap.Deliveries, &arr); err == nil {
		for _, r := range arr {
			out = append(out, r.Delivery)
		}
		return out
	}
	var byScope map[string]map[string]deliveryRecord
	if err := json.Unmarshal(snap.Deliveries, &byScope); err != nil {
		t.Fatalf("functions deliveries snapshot: %v", err)
	}
	for _, byID := range byScope {
		for _, d := range byID {
			out = append(out, d)
		}
	}
	return out
}

// waitForFFDelivery polls the export until the function's delivery settles
// (leaves the pending state), returning the terminal record. The first event
// pays the container cold start, so the deadline is generous.
func waitForFFDelivery(t *testing.T, id string) deliveryRecord {
	t.Helper()

	deadline := time.Now().Add(3 * time.Minute)
	var last deliveryRecord
	found := false
	for {
		for _, d := range functionDeliveries(t) {
			if d.FunctionID == id {
				last, found = d, true
			}
		}
		if found && last.Status != "pending" {
			return last
		}
		if time.Now().Before(deadline) {
			time.Sleep(3 * time.Second)
			continue
		}
		if found {
			t.Fatalf("delivery for %s never settled: %+v", id, last)
		}
		t.Fatalf("no delivery recorded for function %s", id)
		return deliveryRecord{}
	}
}

// invokeTrigger invokes a function through its synthesized HTTPS-trigger URL by
// sending the trigger Host header, and returns the raw HTTP status and body.
func invokeTrigger(t *testing.T, function, id, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, host()+"/"+id, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build trigger request: %v", err)
	}
	req.Host = function
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("trigger %s/%s: %v", function, id, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// invokeTriggerUntil polls the HTTPS trigger until the function's cold start
// completes (the first attempts may 500 while the container/pod starts) and
// returns the first successful response body.
func invokeTriggerUntil(t *testing.T, id, body string) []byte {
	t.Helper()
	triggerHost := "us-central1-proj.cloudfunctions.net"
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, b := invokeTrigger(t, triggerHost, id, body)
		if code == http.StatusOK {
			return b
		}
		if time.Now().Before(deadline) {
			time.Sleep(3 * time.Second)
			continue
		}
		t.Fatalf("trigger never succeeded: HTTP %d: %s", code, b)
	}
}

// TestFunctionsFrameworkHTTP invokes a Functions-Framework function through its
// HTTPS trigger and asserts the framework actually served the request (the
// handler's JSON response, not mock echo).
func TestFunctionsFrameworkHTTP(t *testing.T) {
	requireFFEnv(t)
	reset(t)

	id := fmt.Sprintf("ff-%d", time.Now().UnixNano())
	deployFFFunction(t, id, "hello")

	payload := `{"name":"jaiscloud"}`
	got := string(invokeTriggerUntil(t, id, payload))

	if !strings.Contains(got, "hello") || !strings.Contains(got, "world") {
		t.Fatalf("functions framework did not serve the request; result=%q", got)
	}
	if !strings.Contains(got, "jaiscloud") {
		t.Fatalf("functions framework did not receive the request body; result=%q", got)
	}
}

// TestFunctionsFrameworkEvent publishes a Pub/Sub message and asserts it is
// delivered as a CloudEvent to a Functions-Framework @functions_framework.cloud_event
// handler running under the native executor. The framework discards the
// handler's return value, so the handler validates the event payload itself: it
// raises unless the CloudEvent carries the nonce the test published, which makes
// a "delivered" record proof the handler actually ran with this event.
func TestFunctionsFrameworkEvent(t *testing.T) {
	requireFFEnv(t)
	reset(t)

	const topic = "ff-events"
	const id = "ff-event-fn"
	marker := fmt.Sprintf("marker-%d", time.Now().UnixNano())

	deployFFEventFunction(t, id, topic, "hello_event", ffEventSource(marker))

	payload := `{"marker":"` + marker + `"}`
	publish := []byte(`{"messages":[{"data":"` + base64.StdEncoding.EncodeToString([]byte(payload)) + `"}]}`)
	if code, body := do(t, "POST", "/v1/projects/proj/topics/"+topic+":publish",
		publish, "application/json"); code != http.StatusOK {
		t.Fatalf("publish: HTTP %d: %s", code, body)
	}

	d := waitForFFDelivery(t, id)
	if d.Status != "delivered" {
		t.Fatalf("event delivery not delivered (handler marker check failed?): %+v", d)
	}
	if !strings.Contains(d.Data, marker) {
		t.Fatalf("delivery data %q does not carry the published marker %q", d.Data, marker)
	}
	if !strings.Contains(d.EventType, "pubsub") {
		t.Fatalf("delivery event type = %q, want a pubsub event", d.EventType)
	}
	// The framework's CloudEvent response body ("OK") is the only wire signal
	// that the handler returned without raising.
	if !strings.Contains(d.Result, "OK") {
		t.Fatalf("delivery result = %q, want the framework's OK", d.Result)
	}
}

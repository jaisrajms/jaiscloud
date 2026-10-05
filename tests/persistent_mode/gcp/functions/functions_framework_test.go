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

func requireFFEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("FUNCTIONS_E2E_FF_IMAGE") == "" {
		t.Skip("FUNCTIONS_E2E_FF_IMAGE not set — skipping Cloud Functions Functions-Framework e2e test")
	}
}

// deployFFFunction uploads a Functions-Framework source archive through the v2
// generateUploadUrl flow and creates a v2 function pointing at it.
func deployFFFunction(t *testing.T, id, entryPoint string) {
	t.Helper()

	zipBytes := buildZip(t, map[string]string{"main.py": ffSource})

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

	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"` + entryPoint + `",` +
		`"source":{"storageSource":{"bucket":"` + up.StorageSource.Bucket + `","object":"` + up.StorageSource.Object + `"}}}}`)
	if code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions?functionId="+id,
		create, "application/json"); code != http.StatusOK {
		t.Fatalf("create function: HTTP %d: %s", code, body)
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

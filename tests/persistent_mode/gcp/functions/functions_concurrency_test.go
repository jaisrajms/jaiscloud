//go:build functions_e2e

// This file verifies the Cloud Functions invocation admission plane (FP1) end
// to end under the Docker executor: a function capped at one instance admits a
// single in-flight invocation and throttles a concurrent one with HTTP 429
// RESOURCE_EXHAUSTED. It shares the deployment/invocation helpers in
// functions_docker_test.go.
//
// Requires a jaiscloud-gcp started with JAISCLOUD_EXECUTOR_MODE=docker (the
// Makefile target test-e2e-functions-docker does this), a running Docker daemon,
// and the runtime image available locally.
package functions_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// callResult is the outcome of a concurrent :call issued off the test goroutine.
type callResult struct {
	code int
	body []byte
	err  error
}

// callFunctionConcurrent issues the synchronous :call from a goroutine without
// touching testing.T (whose FailNow must run on the test goroutine). The caller
// reports err on the test goroutine.
func callFunctionConcurrent(path string) callResult {
	req, err := http.NewRequest(http.MethodPost, host()+path, bytes.NewReader([]byte(`{"data":"{}"}`)))
	if err != nil {
		return callResult{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return callResult{err: err}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return callResult{code: resp.StatusCode, body: b, err: err}
}

// deploySleepFunction uploads a Python archive whose handler sleeps for
// sleepSecs and creates a v2 function capped at a single instance, so that two
// concurrent synchronous invocations cannot both be admitted.
func deploySleepFunction(t *testing.T, id string, sleepSecs int) {
	t.Helper()

	handler := "import time\n\ndef handler(event, context):\n    time.sleep(" +
		strconv.Itoa(sleepSecs) + ")\n    return {\"slept\": " + strconv.Itoa(sleepSecs) + "}\n"
	zipBytes := buildZip(t, map[string]string{"lambda_function.py": handler})

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

	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"lambda_function.handler",` +
		`"source":{"storageSource":{"bucket":"` + up.StorageSource.Bucket + `","object":"` + up.StorageSource.Object + `"}}},` +
		`"serviceConfig":{"maxInstanceCount":1,"maxInstanceRequestConcurrency":1}}`)
	if code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions?functionId="+id,
		create, "application/json"); code != http.StatusOK {
		t.Fatalf("create function: HTTP %d: %s", code, body)
	}
}

// warmSleepFunction invokes the sleeping function until the handler actually
// runs, so the subsequent concurrency assertion is not confused by a cold-start
// error that would release the admission slot early.
func warmSleepFunction(t *testing.T, id string) {
	t.Helper()
	callBody := []byte(`{"data":"{}"}`)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, body := do(t, "POST", "/v1/projects/proj/locations/us-central1/functions/"+id+":call",
			callBody, "application/json")
		if code != http.StatusOK {
			t.Fatalf("warm call: HTTP %d: %s", code, body)
		}
		var call map[string]any
		if err := json.Unmarshal(body, &call); err != nil {
			t.Fatalf("warm call response: %v (%s)", err, body)
		}
		if errStr, _ := call["error"].(string); errStr != "" {
			if time.Now().Before(deadline) {
				time.Sleep(3 * time.Second)
				continue
			}
			t.Fatalf("function never became invokable: %s", errStr)
		}
		if result, _ := call["result"].(string); strings.Contains(result, `"slept"`) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("handler did not run: %s", body)
		}
	}
}

// TestFunctionConcurrencyThrottlesDocker deploys a function capped at one
// instance and confirms that of two concurrent synchronous invocations exactly
// one is admitted and the other is throttled with HTTP 429 RESOURCE_EXHAUSTED.
func TestFunctionConcurrencyThrottlesDocker(t *testing.T) {
	requireDockerEnv(t)
	reset(t)

	const id = "slow"
	deploySleepFunction(t, id, 3)
	warmSleepFunction(t, id)

	// Both requests are launched concurrently. The first holds the single
	// admission slot for the full 3s handler; the second must be rejected
	// before it reaches the executor.
	path := "/v1/projects/proj/locations/us-central1/functions/" + id + ":call"
	results := make(chan callResult, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- callFunctionConcurrent(path) }()
	}

	var ok, throttled int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent :call: %v", r.err)
		}
		switch r.code {
		case http.StatusOK:
			ok++
		case http.StatusTooManyRequests:
			throttled++
			if !strings.Contains(string(r.body), "RESOURCE_EXHAUSTED") {
				t.Fatalf("429 body missing RESOURCE_EXHAUSTED: %s", r.body)
			}
		default:
			t.Fatalf("unexpected status %d: %s", r.code, r.body)
		}
	}
	if ok != 1 || throttled != 1 {
		t.Fatalf("got %d admitted / %d throttled, want 1/1", ok, throttled)
	}
}

//go:build functions_e2e

// Package functions_test verifies the Cloud Functions source-execution path end
// to end: a source archive uploaded to the emulated GCS, a function created
// with a v2 buildConfig.source.storageSource, and a synchronous :call that runs
// the archive under the shared Docker (Lambda RIE) executor.
//
// Requires a jaiscloud-gcp started with JAISCLOUD_EXECUTOR_MODE=docker (the
// Makefile target test-e2e-functions-docker does this), a running Docker daemon,
// and the runtime image available locally.
//
// Required env:
//
//	FUNCTIONS_E2E_DOCKER_IMAGE — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	JAISCLOUD_HOST — default http://localhost:8080
package functions_test

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func host() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:8080"
}

func requireDockerEnv(t *testing.T) {
	t.Helper()
	if os.Getenv("FUNCTIONS_E2E_DOCKER_IMAGE") == "" {
		t.Skip("FUNCTIONS_E2E_DOCKER_IMAGE not set — skipping Cloud Functions Docker e2e test")
	}
}

func do(t *testing.T, method, path string, body []byte, contentType string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, host()+path, r)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func reset(t *testing.T) {
	t.Helper()
	code, body := do(t, "POST", "/_jaiscloud/reset", nil, "")
	if code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %s", code, body)
	}
}

// buildZip builds an in-memory zip from name→content entries.
func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, content := range files {
		f, err := w.Create(name)
		if err != nil {
			t.Fatalf("zip create %s: %v", name, err)
		}
		if _, err := f.Write([]byte(content)); err != nil {
			t.Fatalf("zip write %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// putAbsolute PUTs body to an absolute URL (the source-upload URL returned by
// generateUploadUrl points at the emulator origin, not the fixed host()).
func putAbsolute(t *testing.T, url string, body []byte, contentType string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT %s: %v", url, err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestFunctionSourceExecutionDocker deploys a Python function whose zip is
// uploaded through the v2 generateUploadUrl flow (a GCS-backed upload target),
// then invokes it and asserts the handler actually ran (not mock echo).
func TestFunctionSourceExecutionDocker(t *testing.T) {
	requireDockerEnv(t)
	testFunctionSourceExecution(t, "hello")
}

// deploySourceFunction uploads a Python source archive through the v2
// generateUploadUrl flow (a GCS-backed upload target) and creates a v2 function
// with the given id referencing it. Shared by the Docker and K8s
// source-execution tests.
func deploySourceFunction(t *testing.T, id string) {
	t.Helper()

	zipBytes := buildZip(t, map[string]string{
		"lambda_function.py": "def handler(event, context):\n    return {\"hello\": \"world\", \"input\": event}\n",
	})

	// 1. generateUploadUrl returns a GCS-backed upload target; PUT the archive to it.
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

	// 2. Create a v2 function referencing the uploaded storageSource.
	create := []byte(`{"buildConfig":{"runtime":"python312","entryPoint":"lambda_function.handler",` +
		`"source":{"storageSource":{"bucket":"` + up.StorageSource.Bucket + `","object":"` + up.StorageSource.Object + `"}}}}`)
	if code, body := do(t, "POST", "/v2/projects/proj/locations/us-central1/functions?functionId="+id,
		create, "application/json"); code != http.StatusOK {
		t.Fatalf("create function: HTTP %d: %s", code, body)
	}
}

// testFunctionSourceExecution is the shared Cloud Functions source-execution
// flow, run against a Docker- or K8s-backed emulator. It deploys a function
// from a GCS-uploaded archive, invokes it, and asserts the handler actually ran
// (not mock echo).
func testFunctionSourceExecution(t *testing.T, id string) {
	t.Helper()
	reset(t)
	deploySourceFunction(t, id)
	invokeUntilHandlerResult(t, id)

	// Deleting the function removes it (and its archive) cleanly.
	if code, body := do(t, "DELETE", "/v1/projects/proj/locations/us-central1/functions/"+id, nil, ""); code != http.StatusOK {
		t.Fatalf("delete function: HTTP %d: %s", code, body)
	}
	if code, _ := do(t, "GET", "/v1/projects/proj/locations/us-central1/functions/"+id, nil, ""); code != http.StatusNotFound {
		t.Fatalf("get after delete: HTTP %d, want 404", code)
	}
}

// invokeUntilHandlerResult invokes the deployed function synchronously, retrying
// until the cold start completes or the deadline passes, and asserts the
// uploaded handler actually ran (not mock echo). Returns the result payload.
func invokeUntilHandlerResult(t *testing.T, id string) string {
	t.Helper()
	payload := `{"name":"jaiscloud"}`
	callBody := []byte(`{"data":` + strconv.Quote(payload) + `}`)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		code, body := do(t, "POST", "/v1/projects/proj/locations/us-central1/functions/"+id+":call",
			callBody, "application/json")
		if code != http.StatusOK {
			t.Fatalf("call: HTTP %d: %s", code, body)
		}
		var call map[string]any
		if err := json.Unmarshal(body, &call); err != nil {
			t.Fatalf("call response: %v (%s)", err, body)
		}
		if errStr, _ := call["error"].(string); errStr != "" {
			if time.Now().Before(deadline) {
				time.Sleep(3 * time.Second)
				continue
			}
			t.Fatalf("invoke never succeeded: %s", errStr)
		}
		result, _ := call["result"].(string)
		if !strings.Contains(result, `"hello"`) || !strings.Contains(result, "jaiscloud") {
			t.Fatalf("handler did not run as expected; result=%q", result)
		}
		return result
	}
}

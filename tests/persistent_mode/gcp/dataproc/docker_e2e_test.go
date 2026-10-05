//go:build dataproc_docker_e2e

// This file is the real-Docker smoke for the Dataproc docker Spark executor
// (D2). It proves behavioural execution end to end against an emulator started
// with JAISCLOUD_SPARK_EXECUTOR_MODE=docker on the host:
//
//   - create a GCS bucket and stage a PySpark script + a text input into it
//     (through the emulated GCS JSON API)
//   - create a Dataproc cluster and submit the script as a pysparkJob
//   - assert the job reaches DONE, that the script actually read the gs:// input
//     and wrote a gs:// output (the wired GCS connector), and that the driver's
//     stdout was staged to the job's driverOutputResourceUri
//   - assert a labelled Dataproc driver container ran on the local Docker daemon
//   - submit a long-running driver, cancel it, and assert the job reaches
//     CANCELLED and its container is reaped
//
// Run with:
//
//	make test-e2e-dataproc-docker
//
// It is inert without a reachable local Docker daemon: requireDocker skips when
// the docker CLI or the default-context daemon is absent. The emulator must run
// with JAISCLOUD_SPARK_EXECUTOR_MODE=docker (the Makefile target does this), a
// Spark image with the GCS connector, and STORAGE_EMULATOR_HOST pointing at the
// host so main.go can rewrite it to host.docker.internal for the container.
//
// Required env:
//
//	DATAPROC_DOCKER_E2E — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	JAISCLOUD_HOST — default http://localhost:8080
package dataproc_test

import (
	"bytes"
	"encoding/json"
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

const (
	dpProject = "jaiscloud-project"
	dpRegion  = "us-central1"
)

func dpHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return strings.TrimRight(h, "/")
	}
	return "http://localhost:8080"
}

func requireDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("DATAPROC_DOCKER_E2E") == "" {
		t.Skip("DATAPROC_DOCKER_E2E not set — skipping Dataproc docker e2e test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found — skipping Dataproc docker e2e test")
	}
	if _, err := dockerCLI("info"); err != nil {
		t.Skipf("local Docker daemon not reachable: %v", err)
	}
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

func dpRequest(t *testing.T, method, path string, body []byte, contentType string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, dpHost()+path, rd)
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
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func dpJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode JSON %q: %v", body, err)
	}
	return m
}

func dpStr(m map[string]any, path ...string) string {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = mm[k]
	}
	s, _ := cur.(string)
	return s
}

func dpReset(t *testing.T) {
	t.Helper()
	if code, body := dpRequest(t, http.MethodPost, "/_jaiscloud/reset", nil, ""); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %s", code, body)
	}
}

// dpCreateBucket creates a GCS bucket through the emulated JSON API.
func dpCreateBucket(t *testing.T, bucket string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": bucket})
	code, resp := dpRequest(t, http.MethodPost, "/storage/v1/b?project="+dpProject, body, "application/json")
	if code != http.StatusOK {
		t.Fatalf("create bucket %s: HTTP %d: %s", bucket, code, resp)
	}
}

// dpUpload writes bytes to a GCS object via a simple media upload.
func dpUpload(t *testing.T, bucket, object, contentType string, data []byte) {
	t.Helper()
	path := "/upload/storage/v1/b/" + bucket + "/o?uploadType=media&name=" + url.QueryEscape(object)
	code, resp := dpRequest(t, http.MethodPost, path, data, contentType)
	if code != http.StatusOK {
		t.Fatalf("upload %s: HTTP %d: %s", object, code, resp)
	}
}

// dpListObjects returns the object names under a prefix.
func dpListObjects(t *testing.T, bucket, prefix string) []string {
	t.Helper()
	code, resp := dpRequest(t, http.MethodGet, "/storage/v1/b/"+bucket+"/o?prefix="+url.QueryEscape(prefix), nil, "")
	if code != http.StatusOK {
		t.Fatalf("list objects: HTTP %d: %s", code, resp)
	}
	var names []string
	if items, _ := dpJSON(t, resp)["items"].([]any); items != nil {
		for _, it := range items {
			if m, ok := it.(map[string]any); ok {
				names = append(names, dpStr(m, "name"))
			}
		}
	}
	return names
}

// dpGetObject fetches an object's bytes.
func dpGetObject(t *testing.T, bucket, object string) []byte {
	t.Helper()
	code, resp := dpRequest(t, http.MethodGet, "/storage/v1/b/"+bucket+"/o/"+url.PathEscape(object)+"?alt=media", nil, "")
	if code != http.StatusOK {
		t.Fatalf("get object %s: HTTP %d: %s", object, code, resp)
	}
	return resp
}

// dpCreateCluster creates a cluster and drives its create LRO to completion.
func dpCreateCluster(t *testing.T, base, name string) {
	t.Helper()
	body := []byte(`{"projectId":"` + dpProject + `","clusterName":"` + name + `","config":{"gceClusterConfig":{"zoneUri":"` + dpRegion + `-a"}}}`)
	code, resp := dpRequest(t, http.MethodPost, base+"/clusters", body, "application/json")
	if code != http.StatusOK {
		t.Fatalf("create cluster: HTTP %d: %s", code, resp)
	}
	dpPollOperation(t, dpJSON(t, resp))
}

func dpPollOperation(t *testing.T, op map[string]any) map[string]any {
	t.Helper()
	name := dpStr(op, "name")
	if name == "" {
		t.Fatalf("operation has no name: %v", op)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		code, resp := dpRequest(t, http.MethodGet, "/v1/"+name, nil, "")
		if code != http.StatusOK {
			t.Fatalf("get operation %s: HTTP %d: %s", name, code, resp)
		}
		cur := dpJSON(t, resp)
		if done, _ := cur["done"].(bool); done {
			resp, _ := cur["response"].(map[string]any)
			return resp
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("operation %s did not complete", name)
	return nil
}

// dpSubmit submits a job and returns the initial (PENDING/RUNNING) job body.
func dpSubmit(t *testing.T, base string, job map[string]any) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"job": job})
	code, resp := dpRequest(t, http.MethodPost, base+"/jobs:submit", body, "application/json")
	if code != http.StatusOK {
		t.Fatalf("submit job: HTTP %d: %s", code, resp)
	}
	return dpJSON(t, resp)
}

// dpPollJob polls a job until terminal and returns its final body.
func dpPollJob(t *testing.T, base, jobID string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, resp := dpRequest(t, http.MethodGet, base+"/jobs/"+jobID, nil, "")
		if code != http.StatusOK {
			t.Fatalf("get job %s: HTTP %d: %s", jobID, code, resp)
		}
		obj := dpJSON(t, resp)
		switch dpStr(obj, "status", "state") {
		case "DONE", "ERROR", "CANCELLED":
			return obj
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal state", jobID)
	return nil
}

func dpDataprocContainers(t *testing.T) []string {
	t.Helper()
	out, err := dockerCLI("ps", "-a", "--filter", "label=jaiscloud.io/service=dataproc", "--format", "{{.ID}}")
	if err != nil {
		t.Fatalf("docker ps: %v", err)
	}
	return strings.Fields(strings.TrimSpace(out))
}

func TestDataprocDockerExecution(t *testing.T) {
	requireDocker(t)
	base := "/v1/projects/" + dpProject + "/regions/" + dpRegion

	dpReset(t)
	t.Cleanup(func() { dpReset(t) })

	run := fmt.Sprintf("%d", time.Now().UnixNano())
	bucket := "dp-e2e-" + run
	cluster := "dp-docker-" + run
	bucketURI := "gs://" + bucket

	dpCreateBucket(t, bucket)
	script := `
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("jaiscloud-dp-docker-e2e").getOrCreate()
df = spark.read.text("` + bucketURI + `/input.txt")
n = df.count()
print("LINES=%d" % n)
df.write.mode("overwrite").text("` + bucketURI + `/out")
spark.stop()
`
	dpUpload(t, bucket, "input.txt", "text/plain", []byte("hello\nworld\n"))
	dpUpload(t, bucket, "main.py", "text/x-python", []byte(script))
	sleepScript := `
import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("jaiscloud-dp-docker-sleep").getOrCreate()
print("SLEEPING")
time.sleep(600)
spark.stop()
`
	dpUpload(t, bucket, "sleep.py", "text/x-python", []byte(sleepScript))

	dpCreateCluster(t, base, cluster)

	jobID := "dp-docker-job-" + run
	dpSubmit(t, base, map[string]any{
		"reference": map[string]any{"projectId": dpProject, "jobId": jobID},
		"placement": map[string]any{"clusterName": cluster},
		"pysparkJob": map[string]any{
			"mainPythonFileUri": bucketURI + "/main.py",
		},
	})

	final := dpPollJob(t, base, jobID, 5*time.Minute)
	if state := dpStr(final, "status", "state"); state != "DONE" {
		t.Fatalf("job state = %q (details %q), want DONE; driver output:\n%s",
			state, dpStr(final, "status", "details"), dpDriverOutput(t, final, jobID))
	}

	// The script really read the gs:// input (2 lines) and wrote a gs:// output
	// (the wired GCS connector).
	out := dpDriverOutput(t, final, jobID)
	if !strings.Contains(string(out), "LINES=2") {
		t.Fatalf("driver output does not contain LINES=2:\n%s", out)
	}
	if got := dpListObjects(t, bucket, "out/"); len(got) == 0 {
		t.Fatalf("no output objects under %s/out/ — the job did not write through the GCS connector", bucketURI)
	}

	// A labelled driver container must have run on the local daemon.
	if len(dpDataprocContainers(t)) == 0 {
		t.Fatalf("no jaiscloud.io/service=dataproc container found on the local daemon")
	}

	t.Run("cancel reaps the driver", func(t *testing.T) {
		longID := "dp-docker-long-" + run
		dpSubmit(t, base, map[string]any{
			"reference": map[string]any{"projectId": dpProject, "jobId": longID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": bucketURI + "/sleep.py",
			},
		})
		// Wait until the driver is running before cancelling.
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			code, resp := dpRequest(t, http.MethodGet, base+"/jobs/"+longID, nil, "")
			if code == http.StatusOK && dpStr(dpJSON(t, resp), "status", "state") == "RUNNING" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if code, resp := dpRequest(t, http.MethodPost, base+"/jobs/"+longID+":cancel", nil, ""); code != http.StatusOK {
			t.Fatalf("cancel job: HTTP %d: %s", code, resp)
		}
		// Poll to CANCELLED (the mock-free executor writes it once the driver is
		// cancelled and reaped).
		cancelled := dpPollJob(t, base, longID, 2*time.Minute)
		if state := dpStr(cancelled, "status", "state"); state != "CANCELLED" {
			t.Fatalf("cancelled job state = %q, want CANCELLED", state)
		}
		// The executor's container for the cancelled job must be reaped.
		deadline = time.Now().Add(60 * time.Second)
		for time.Now().Before(deadline) {
			out, err := dockerCLI("ps", "-a", "--filter", "label=jaiscloud.io/job-id="+longID, "--format", "{{.ID}}")
			if err == nil && strings.TrimSpace(out) == "" {
				return
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("driver container for cancelled job %s survived", longID)
	})
}

// dpDriverOutput fetches the job's staged driver-output object.
func dpDriverOutput(t *testing.T, job map[string]any, jobID string) []byte {
	t.Helper()
	uri := dpStr(job, "driverOutputResourceUri")
	if !strings.HasPrefix(uri, "gs://") {
		t.Fatalf("job %s driverOutputResourceUri = %q, want a gs:// URI", jobID, uri)
	}
	rest := strings.TrimPrefix(uri, "gs://")
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		t.Fatalf("job %s driverOutputResourceUri = %q, want gs://bucket/object", jobID, uri)
	}
	bucket, object := rest[:slash], rest[slash+1:]
	return dpGetObject(t, bucket, object+".000000000")
}

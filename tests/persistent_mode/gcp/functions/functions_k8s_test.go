//go:build functions_e2e

// Package functions_test verifies the Cloud Functions source-execution path end
// to end under the Kubernetes executor. The code-fetch init container downloads
// the function's archive from the admin API (JAISCLOUD_LAMBDA_CODE_URL) and
// unpacks it into /var/task, so the deployed function runs the uploaded code
// rather than an empty task root (FD7). The pod's bundled RIE then serves the
// invocation (FD14): the shared executor must leave AWS_LAMBDA_RUNTIME_API unset
// so the base image entrypoint starts the RIE, otherwise the pod never becomes
// Ready.
//
// The test invokes the function and asserts the uploaded handler actually ran,
// then additionally checks the code-fetch init container's terminal status and
// unpacked contents (the FD7 contract).
//
// Requires a jaiscloud-gcp started with JAISCLOUD_EXECUTOR_MODE=k8s and a
// cluster-reachable JAISCLOUD_LAMBDA_CODE_URL (the Makefile target
// test-e2e-functions-k8s configures both), plus a running cluster and kubectl.
//
// Required env:
//
//	FUNCTIONS_E2E_K8S — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	JAISCLOUD_HOST — default http://localhost:8080
//	K8S_NAMESPACE  — default jaiscloud
package functions_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestFunctionSourceCodeMountK8s(t *testing.T) {
	if os.Getenv("FUNCTIONS_E2E_K8S") == "" {
		t.Skip("FUNCTIONS_E2E_K8S not set — skipping Cloud Functions K8s e2e test")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not available")
	}
	ns := os.Getenv("K8S_NAMESPACE")
	if ns == "" {
		ns = "jaiscloud"
	}

	reset(t)
	// A unique function id per run guarantees a fresh pod: the K8s executor
	// caches warm pods, so reusing an id would reuse a pod left by an earlier
	// run instead of exercising a new code-fetch + RIE start.
	id := fmt.Sprintf("mount-%d", time.Now().UnixNano())
	deploySourceFunction(t, id)

	// Invoke synchronously. This requires the pod's RIE to actually serve on
	// :8080, which in turn requires the base image entrypoint to start it — the
	// FD14 regression: setting AWS_LAMBDA_RUNTIME_API stops the entrypoint from
	// starting the RIE and the pod never becomes Ready.
	invokeUntilHandlerResult(t, id)

	// The invocation proves the code was mounted and the runtime served; still
	// assert the code-fetch init container itself succeeded and unpacked the
	// archive (the FD7 contract).
	pod, exit := waitForCodeFetch(t, ns, id, 90*time.Second)
	logs := kubectl(t, ns, "logs", pod, "-c", "code-fetch")
	if exit != 0 {
		t.Fatalf("code-fetch init container in %s exited %d, want 0\nlogs:\n%s", pod, exit, logs)
	}
	if !strings.Contains(logs, "lambda_function.py") {
		t.Fatalf("code-fetch did not unpack lambda_function.py; logs:\n%s", logs)
	}
	t.Logf("function invoked and ran uploaded code (pod %s)", pod)
}

func kubectl(t *testing.T, ns string, args ...string) string {
	t.Helper()
	full := append([]string{"-n", ns}, args...)
	out, err := exec.Command("kubectl", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// waitForCodeFetch polls the function's pods until a code-fetch init container
// reports a terminal exit code, returning the pod name and that code.
func waitForCodeFetch(t *testing.T, ns, fn string, timeout time.Duration) (string, int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out, err := exec.Command("kubectl", "-n", ns, "get", "pods",
			"-l", "function="+fn, "-o", "json").Output()
		if err == nil {
			var list struct {
				Items []struct {
					Metadata struct {
						Name string `json:"name"`
					} `json:"metadata"`
					Status struct {
						InitContainerStatuses []struct {
							Name  string `json:"name"`
							State struct {
								Terminated *struct {
									ExitCode int `json:"exitCode"`
								} `json:"terminated"`
							} `json:"state"`
						} `json:"initContainerStatuses"`
					} `json:"status"`
				} `json:"items"`
			}
			if json.Unmarshal(out, &list) == nil {
				for _, p := range list.Items {
					for _, ic := range p.Status.InitContainerStatuses {
						if ic.Name == "code-fetch" && ic.State.Terminated != nil {
							return p.Metadata.Name, ic.State.Terminated.ExitCode
						}
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no completed code-fetch init container for %q within %s", fn, timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

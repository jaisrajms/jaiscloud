package lambda

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"jaiscloud/internal/k8stypes"
)

// fakeK8s is a minimal fake Kubernetes API server for unit tests.
type fakeK8s struct {
	mu      sync.Mutex
	pods    []string // pod names created
	svcs    []string // service names created
	deleted []string // names deleted (pods and services)
}

func newFakeK8sServer(f *fakeK8s) *httptest.Server {
	mux := http.NewServeMux()

	// POST pod
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/pods", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			meta, _ := body["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			f.mu.Lock()
			f.pods = append(f.pods, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(body)
			return
		}
		// GET list pods by label
		if r.Method == http.MethodGet {
			f.mu.Lock()
			items := make([]map[string]any, len(f.pods))
			for i, n := range f.pods {
				items[i] = map[string]any{"metadata": map[string]any{"name": n}}
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		}
	})

	// DELETE or GET individual pod
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/pods/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/jaiscloud/pods/")
		if r.Method == http.MethodDelete {
			f.mu.Lock()
			f.deleted = append(f.deleted, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		// GET — return Running+Ready pod status
		resp := map[string]any{
			"status": map[string]any{
				"phase": "Running",
				"conditions": []map[string]any{
					{"type": "Ready", "status": "True"},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	// POST service
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/services", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			meta, _ := body["metadata"].(map[string]any)
			name, _ := meta["name"].(string)
			f.mu.Lock()
			f.svcs = append(f.svcs, name)
			f.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(body)
			return
		}
		// GET list services by label
		if r.Method == http.MethodGet {
			f.mu.Lock()
			items := make([]map[string]any, len(f.svcs))
			for i, n := range f.svcs {
				items[i] = map[string]any{"metadata": map[string]any{"name": n}}
			}
			f.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"items": items})
		}
	})

	// DELETE individual service
	mux.HandleFunc("/api/v1/namespaces/jaiscloud/services/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			name := strings.TrimPrefix(r.URL.Path, "/api/v1/namespaces/jaiscloud/services/")
			f.mu.Lock()
			f.deleted = append(f.deleted, name)
			f.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	})

	return httptest.NewTLSServer(mux)
}

func newTestK8sExecutor(t *testing.T, srv *httptest.Server) *K8sExecutor {
	t.Helper()
	cfg := LambdaConfig{
		Namespace:     "jaiscloud",
		APIServer:     srv.URL,
		KeepaliveSecs: 300,
	}
	e := &K8sExecutor{
		cfg:    cfg,
		k8s:    srv.Client(),
		invoke: &http.Client{Timeout: 5 * time.Second},
		pods:   make(map[string]*warmPod),
		done:   make(chan struct{}),
	}
	return e
}

func TestK8sLambda_Close_DeletesAllWarmPods(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)

	// Manually insert warm pods.
	e.pods["fn-a"] = &warmPod{podName: "jc-lambda-fn-a-0001", svcName: "jc-lambda-fn-a", endpoint: "http://fake:8080"}
	e.pods["fn-b"] = &warmPod{podName: "jc-lambda-fn-b-0002", svcName: "jc-lambda-fn-b", endpoint: "http://fake:8080"}

	require.NoError(t, e.Close())

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-fn-a-0001")
	assert.Contains(t, f.deleted, "jc-lambda-fn-b-0002")
	assert.Contains(t, f.deleted, "jc-lambda-fn-a")
	assert.Contains(t, f.deleted, "jc-lambda-fn-b")
}

func TestK8sLambda_CleanupOrphans_DeletesOrphanedPodsAndServices(t *testing.T) {
	f := &fakeK8s{
		pods: []string{"jc-lambda-old-pod-aaaa"},
		svcs: []string{"jc-lambda-old-svc"},
	}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.cleanupOrphans()

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-old-pod-aaaa")
	assert.Contains(t, f.deleted, "jc-lambda-old-svc")
}

func TestK8sLambda_CleanupOrphans_NoOrphans_NoOp(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.cleanupOrphans()

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Empty(t, f.deleted)
}

func TestK8sLambda_DeleteFunction_RemovesPodAndService(t *testing.T) {
	f := &fakeK8s{}
	srv := newFakeK8sServer(f)
	defer srv.Close()

	e := newTestK8sExecutor(t, srv)
	e.pods["my-fn"] = &warmPod{
		podName:  "jc-lambda-my-fn-1234",
		svcName:  "jc-lambda-my-fn",
		endpoint: "http://fake:8080",
	}

	e.DeleteFunction(context.Background(), "my-fn")

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Contains(t, f.deleted, "jc-lambda-my-fn-1234")
	assert.Contains(t, f.deleted, "jc-lambda-my-fn")

	e.mu.Lock()
	_, exists := e.pods["my-fn"]
	e.mu.Unlock()
	assert.False(t, exists, "pod entry must be removed from map")
}

// fakeCodeLoader satisfies CodeLoader so applyCodeMount treats the loader as
// present. The bytes are never read by the pod-spec builder.
type fakeCodeLoader struct{}

func (fakeCodeLoader) LoadCode(context.Context, string, string, string) ([]byte, error) {
	return []byte("zip"), nil
}

// TestApplyCodeMount_InjectsInitContainer is the FD7 regression: with a
// CodeLoader and a CodeURL configured, the pod spec carries a code-fetch init
// container that downloads the function archive from the admin API into a
// shared emptyDir mounted read-only at /var/task.
func TestApplyCodeMount_InjectsInitContainer(t *testing.T) {
	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "lambda"}}}
	cfg := LambdaConfig{CodeURL: "http://jaiscloud:8080/_jaiscloud", InitImage: "alpine:latest"}
	req := InvokeRequest{FunctionName: "fn", AccountID: "proj", CodeKey: "us-central1.hello"}

	applyCodeMount(&spec, cfg, req, fakeCodeLoader{})

	require.Len(t, spec.InitContainers, 1)
	ic := spec.InitContainers[0]
	assert.Equal(t, "code-fetch", ic.Name)
	assert.Equal(t, "alpine:latest", ic.Image)
	require.Len(t, ic.VolumeMounts, 1)
	assert.Equal(t, "code", ic.VolumeMounts[0].Name)
	assert.Equal(t, "/var/task", ic.VolumeMounts[0].MountPath)
	assert.False(t, ic.VolumeMounts[0].ReadOnly)

	wantURL := "http://jaiscloud:8080/_jaiscloud/lambda/code/proj/us-central1.hello/$LATEST"
	// The command must reference the URL through the environment (quoted), not
	// embed it literally: a literal "$LATEST" would be expanded to empty by the
	// shell and 404.
	assert.Contains(t, ic.Args[0], "wget -qO /tmp/code.zip")
	assert.Contains(t, ic.Args[0], `"$`+codeArchiveEnvName+`"`)
	assert.NotContains(t, ic.Args[0], wantURL)
	require.Len(t, ic.Env, 1)
	assert.Equal(t, codeArchiveEnvName, ic.Env[0].Name)
	assert.Equal(t, wantURL, ic.Env[0].Value)

	require.Len(t, spec.Volumes, 1)
	assert.Equal(t, "code", spec.Volumes[0].Name)
	assert.NotNil(t, spec.Volumes[0].EmptyDir)

	require.Len(t, spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, "code", spec.Containers[0].VolumeMounts[0].Name)
	assert.Equal(t, "/var/task", spec.Containers[0].VolumeMounts[0].MountPath)
	assert.True(t, spec.Containers[0].VolumeMounts[0].ReadOnly)
}

// TestApplyCodeMount_CodeKeyFallsBackToFunctionName asserts the archive key used
// in the URL falls back to FunctionName when no explicit CodeKey is set (the AWS
// Lambda behavior), and that a missing loader or empty CodeURL disables the
// mount entirely.
func TestApplyCodeMount_CodeKeyFallsBackToFunctionName(t *testing.T) {
	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "lambda"}}}
	applyCodeMount(&spec, LambdaConfig{CodeURL: "http://x/_jaiscloud"}, InvokeRequest{AccountID: "acct", FunctionName: "my-fn"}, fakeCodeLoader{})

	require.Len(t, spec.InitContainers, 1)
	assert.Contains(t, spec.InitContainers[0].Env[0].Value, "/lambda/code/acct/my-fn/$LATEST")
}

func TestApplyCodeMount_DisabledWithoutLoaderOrURL(t *testing.T) {
	base := func() k8stypes.PodSpec {
		return k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "lambda"}}}
	}

	spec := base()
	applyCodeMount(&spec, LambdaConfig{CodeURL: "http://x/_jaiscloud"}, InvokeRequest{}, nil)
	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	assert.Empty(t, spec.Containers[0].VolumeMounts)

	spec = base()
	applyCodeMount(&spec, LambdaConfig{}, InvokeRequest{}, fakeCodeLoader{})
	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	assert.Empty(t, spec.Containers[0].VolumeMounts)
}

// TestApplyCodeMount_PreservesExistingInitContainers guards the platform layer:
// TLS (or other) init containers added before the code mount must survive.
func TestApplyCodeMount_PreservesExistingInitContainers(t *testing.T) {
	spec := k8stypes.PodSpec{
		Containers:     []k8stypes.Container{{Name: "lambda"}},
		InitContainers: []k8stypes.Container{{Name: "tls-materialize"}},
	}
	applyCodeMount(&spec, LambdaConfig{CodeURL: "http://x/_jaiscloud"}, InvokeRequest{AccountID: "p", FunctionName: "fn"}, fakeCodeLoader{})

	require.Len(t, spec.InitContainers, 2)
	assert.Equal(t, "tls-materialize", spec.InitContainers[0].Name)
	assert.Equal(t, "code-fetch", spec.InitContainers[1].Name)
}

// writeExec writes an executable shell script to path.
func writeExec(t *testing.T, path, script string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestApplyCodeMount_CommandKeepsLatestQualifier runs the generated init
// command under a real shell with stub wget/unzip on PATH, proving the literal
// "$LATEST" qualifier reaches wget intact. Before the fix the shell expanded it
// to empty and wget requested the wrong (404) URL.
func TestApplyCodeMount_CommandKeepsLatestQualifier(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	dir := t.TempDir()
	record := filepath.Join(dir, "url")
	writeExec(t, filepath.Join(dir, "wget"), "#!/bin/sh\nfor a in \"$@\"; do last=\"$a\"; done\nprintf '%s' \"$last\" > "+record+"\n")
	writeExec(t, filepath.Join(dir, "unzip"), "#!/bin/sh\nexit 0\n")

	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "lambda"}}}
	applyCodeMount(&spec, LambdaConfig{CodeURL: "http://x/_jaiscloud"}, InvokeRequest{AccountID: "p", CodeKey: "us-central1.hello"}, fakeCodeLoader{})
	require.Len(t, spec.InitContainers, 1)
	ic := spec.InitContainers[0]

	cmd := exec.Command("sh", append([]string{"-c"}, ic.Args...)...)
	cmd.Env = append(os.Environ(),
		ic.Env[0].Name+"="+ic.Env[0].Value,
		"PATH="+dir+":"+os.Getenv("PATH"),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run init command: %v: %s", err, out)
	}

	got, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("read recorded URL: %v", err)
	}
	want := "http://x/_jaiscloud/lambda/code/p/us-central1.hello/$LATEST"
	if string(got) != want {
		t.Fatalf("wget URL = %q, want %q (a bare $LATEST would be expanded by the shell)", got, want)
	}
}

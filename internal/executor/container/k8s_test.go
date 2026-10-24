package container

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/k8stypes"
)

// testProfile is a minimal Profile for the executor mechanics tests. It mirrors
// the shape the AWS Lambda profile produces (port 8080, /var/task, handler arg,
// jc-lambda- workload names) without importing the Lambda package.
type testProfile struct{}

func (testProfile) Name() string { return "test" }

func (testProfile) InvocationPort() int { return 8080 }

func (testProfile) Invocation(req Request) Invocation {
	return Invocation{Method: http.MethodPost, Path: "/invoke", Header: map[string]string{"Content-Type": "application/json"}, Body: req.Payload}
}

func (testProfile) DecodeResponse(_ int, body []byte) ([]byte, error) { return body, nil }

func (testProfile) RuntimeEnv(_ Config, req Request) []EnvVar {
	return []EnvVar{{Name: "HANDLER", Value: req.Handler}, {Name: "AWS_LAMBDA_FUNCTION_NAME", Value: req.FunctionName}}
}

func (testProfile) CodeMountDir() string  { return "/var/task" }
func (testProfile) LayerMountDir() string { return "/opt" }

func (testProfile) WorkloadLabel() string { return "jaiscloud-lambda" }

func (testProfile) WorkloadNamePrefix(instanceID string) string {
	if len(instanceID) >= 8 {
		return "jc-lambda-" + instanceID[:8] + "-"
	}
	if instanceID != "" {
		return "jc-lambda-" + instanceID + "-"
	}
	return "jc-lambda-"
}

func (testProfile) ContainerArgs(req Request) []string {
	if req.Handler == "" {
		return nil
	}
	return []string{req.Handler}
}

func (testProfile) ImageForRuntime(cfg Config, req Request) string {
	if req.Image != "" {
		return req.Image
	}
	if cfg.DefaultImage != "" {
		return cfg.DefaultImage
	}
	if req.Runtime == "python3.12" {
		return "public.ecr.aws/lambda/python:3.12"
	}
	return "public.ecr.aws/lambda/provided:al2"
}

// newTestK8sExecutor builds a K8sExecutor backed by a kubernetes/fake client.
// The workload probe always succeeds — the fake cluster has no live endpoints.
func newTestK8sExecutor(t *testing.T, client *fake.Clientset) *K8sExecutor {
	t.Helper()
	return &K8sExecutor{
		cfg: Config{
			Namespace:     "jaiscloud",
			InstanceID:    "testinst",
			KeepaliveSecs: 300,
		},
		profile: testProfile{},
		client:  client,
		invoke:  &http.Client{Timeout: 5 * time.Second},
		probe:   func(string) bool { return true },
		pods:    make(map[string]*warmPod),
		done:    make(chan struct{}),
	}
}

func ownedLabels() map[string]string {
	return map[string]string{"app": "jaiscloud-lambda", "jaiscloud.io/instance-id": "testinst"}
}

func TestK8sLambda_Close_DeletesAllWarmPods(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-fn-a-0001", Namespace: "jaiscloud"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-fn-a-0001", Namespace: "jaiscloud"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-fn-b-0002", Namespace: "jaiscloud"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-fn-b-0002", Namespace: "jaiscloud"}},
	)
	e := newTestK8sExecutor(t, client)

	// Manually insert warm pods.
	e.pods["fn-a"] = &warmPod{name: "jc-lambda-fn-a-0001", endpoint: "http://fake:8080"}
	e.pods["fn-b"] = &warmPod{name: "jc-lambda-fn-b-0002", endpoint: "http://fake:8080"}

	require.NoError(t, e.Close())

	ctx := context.Background()
	for _, name := range []string{"jc-lambda-fn-a-0001", "jc-lambda-fn-b-0002"} {
		_, podErr := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{})
		assert.Error(t, podErr, "pod %s must be deleted", name)
		_, svcErr := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{})
		assert.Error(t, svcErr, "service %s must be deleted", name)
	}
}

func TestK8sLambda_CleanupOrphans_DeletesOrphanedPodsAndServices(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-old-pod-aaaa", Namespace: "jaiscloud", Labels: ownedLabels()}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-old-svc", Namespace: "jaiscloud", Labels: ownedLabels()}},
	)
	e := newTestK8sExecutor(t, client)
	e.cleanupOrphans()

	ctx := context.Background()
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "jc-lambda-old-pod-aaaa", metav1.GetOptions{}); err == nil {
		t.Error("owned orphan pod survived cleanup")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "jc-lambda-old-svc", metav1.GetOptions{}); err == nil {
		t.Error("owned orphan service survived cleanup")
	}
}

func TestK8sLambda_CleanupOrphans_LeavesOtherInstancesAndUnlabeled(t *testing.T) {
	other := map[string]string{"app": "jaiscloud-lambda", "jaiscloud.io/instance-id": "someone-else"}
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other-instance", Namespace: "jaiscloud", Labels: other}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unlabeled", Namespace: "jaiscloud"}},
	)
	e := newTestK8sExecutor(t, client)
	e.cleanupOrphans()

	ctx := context.Background()
	for _, name := range []string{"other-instance", "unlabeled"} {
		if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("foreign pod %s was swept: %v", name, err)
		}
	}
}

func TestK8sLambda_DeleteFunction_RemovesPodAndService(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-my-fn-1234", Namespace: "jaiscloud", Labels: ownedLabels()}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "jc-lambda-my-fn-1234", Namespace: "jaiscloud", Labels: ownedLabels()}},
	)
	e := newTestK8sExecutor(t, client)
	e.pods["my-fn"] = &warmPod{name: "jc-lambda-my-fn-1234", endpoint: "http://fake:8080"}

	e.DeleteFunction(context.Background(), "my-fn")

	ctx := context.Background()
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "jc-lambda-my-fn-1234", metav1.GetOptions{}); err == nil {
		t.Error("pod survived DeleteFunction")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "jc-lambda-my-fn-1234", metav1.GetOptions{}); err == nil {
		t.Error("service survived DeleteFunction")
	}

	e.mu.Lock()
	_, exists := e.pods["my-fn"]
	e.mu.Unlock()
	assert.False(t, exists, "pod entry must be removed from map")
}

// TestK8sLambda_CreatePod_EnsuresWorkload drives createPod through the shared
// k8shelpers lifecycle against kubernetes/fake and asserts the Pod + ClusterIP
// Service shape (image, args, resources, labels/selector, port, endpoint).
func TestK8sLambda_CreatePod_EnsuresWorkload(t *testing.T) {
	client := fake.NewSimpleClientset()
	e := newTestK8sExecutor(t, client)
	e.cfg.ServiceAccount = "executor-sa"

	pod, err := e.createPod(context.Background(), Request{
		FunctionName: "MyFn",
		Runtime:      "python3.12",
		Handler:      "app.handler",
		MemoryMB:     256,
		AccountID:    "acct",
	})
	require.NoError(t, err)
	require.NotEmpty(t, pod.name)

	ctx := context.Background()
	list, err := client.CoreV1().Pods("jaiscloud").List(ctx, metav1.ListOptions{LabelSelector: "function=myfn"})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	got := list.Items[0]

	assert.Equal(t, "public.ecr.aws/lambda/python:3.12", got.Spec.Containers[0].Image)
	assert.Equal(t, []string{"app.handler"}, got.Spec.Containers[0].Args)
	assert.Equal(t, corev1.RestartPolicyNever, got.Spec.RestartPolicy)
	assert.Equal(t, "executor-sa", got.Spec.ServiceAccountName)
	assert.Equal(t, "256Mi", got.Spec.Containers[0].Resources.Limits.Memory().String())
	assert.Equal(t, "testinst", got.Labels["jaiscloud.io/instance-id"])

	svc, err := client.CoreV1().Services("jaiscloud").Get(ctx, pod.name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	assert.Equal(t, "myfn", svc.Spec.Selector["function"])
	assert.Equal(t, "testinst", svc.Spec.Selector["jaiscloud.io/instance-id"])

	assert.Equal(t, "http://"+pod.name+".jaiscloud.svc.cluster.local:8080", pod.endpoint)
}

// TestNewK8sExecutor_InjectsClientAndProbe guards the option wiring: the
// constructor must use an injected client/probe rather than building its own.
func TestNewK8sExecutor_InjectsClientAndProbe(t *testing.T) {
	client := fake.NewSimpleClientset()
	probe := func(string) bool { return true }
	e := NewK8sExecutor(Config{Namespace: "jaiscloud", InstanceID: "inst"}, testProfile{}, nil,
		WithK8sClient(client), WithWorkloadProbe(probe))
	defer e.Close()

	assert.Equal(t, client, e.client)
	assert.NotNil(t, e.probe)
}

// TestK8sLambda_CreatePod_BoundsWorkloadName is the CR9 regression for the
// 63-char DNS-1123 label limit: the Pod and its ClusterIP Service now share one
// name, so a long function name (plus a 16-char instance id) must still yield a
// valid Service name. kubernetes/fake does not validate names, so assert here.
func TestK8sLambda_CreatePod_BoundsWorkloadName(t *testing.T) {
	client := fake.NewSimpleClientset()
	e := newTestK8sExecutor(t, client)
	e.cfg.InstanceID = "0123456789abcdef"

	pod, err := e.createPod(context.Background(), Request{
		FunctionName: strings.Repeat("LongFunctionName", 5),
		Runtime:      "python3.12",
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(pod.name), 63, "workload name must be a DNS-1123 label")
	assert.Regexp(t, `^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`, pod.name)
	if _, err := client.CoreV1().Services("jaiscloud").Get(context.Background(), pod.name, metav1.GetOptions{}); err != nil {
		t.Errorf("service with bounded name not created: %v", err)
	}
}

// TestK8sLambda_CreatePod_NotReady_Reaps asserts a readiness failure propagates
// and leaves no Pod/Service behind (EnsureWorkload's own reap).
func TestK8sLambda_CreatePod_NotReady_Reaps(t *testing.T) {
	client := fake.NewSimpleClientset()
	e := newTestK8sExecutor(t, client)
	e.probe = func(string) bool { return false }

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := e.createPod(ctx, Request{FunctionName: "Fn", Runtime: "python3.12"})
	require.Error(t, err)

	list, err := client.CoreV1().Pods("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, list.Items, "a never-ready pod must be reaped")
	svcs, err := client.CoreV1().Services("jaiscloud").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, svcs.Items, "a never-ready service must be reaped")
}

// TestCoreV1PodSpec_ConvertsPlatformFields guards the k8stypes -> corev1
// conversion for the field groups the platform layer and code mount add:
// env valueFrom, volumes, probes and resource quantities.
func TestCoreV1PodSpec_ConvertsPlatformFields(t *testing.T) {
	spec := k8stypes.PodSpec{
		RestartPolicy:      "Never",
		ServiceAccountName: "executor-sa",
		InitContainers:     []k8stypes.Container{{Name: "code-fetch", Image: "alpine:latest"}},
		Containers: []k8stypes.Container{{
			Name:  "function",
			Image: "img:1",
			Env: []k8stypes.EnvVar{
				{Name: "PLAIN", Value: "v"},
				{Name: "FROM_SECRET", ValueFrom: &k8stypes.EnvVarSource{
					SecretKeyRef: &k8stypes.SecretKeySelector{Name: "s", Key: "k"},
				}},
			},
			Resources:      &k8stypes.Resources{Limits: map[string]string{"memory": "256Mi"}},
			ReadinessProbe: &k8stypes.Probe{TCPSocket: &k8stypes.TCPSocketAction{Port: 8080}},
		}},
		Volumes: []k8stypes.Volume{
			{Name: "code", EmptyDir: &k8stypes.EmptyDirVol{}},
			{Name: "tls", Secret: &k8stypes.SecretVol{SecretName: "tls-secret"}},
		},
	}

	out, err := coreV1PodSpec(spec)
	require.NoError(t, err)

	assert.Equal(t, corev1.RestartPolicyNever, out.RestartPolicy)
	assert.Equal(t, "executor-sa", out.ServiceAccountName)
	require.Len(t, out.InitContainers, 1)
	assert.Equal(t, "code-fetch", out.InitContainers[0].Name)
	require.Len(t, out.Containers, 1)
	require.Len(t, out.Containers[0].Env, 2)
	assert.Equal(t, "v", out.Containers[0].Env[0].Value)
	require.NotNil(t, out.Containers[0].Env[1].ValueFrom)
	require.NotNil(t, out.Containers[0].Env[1].ValueFrom.SecretKeyRef)
	assert.Equal(t, "s", out.Containers[0].Env[1].ValueFrom.SecretKeyRef.Name)
	require.NotNil(t, out.Containers[0].ReadinessProbe)
	require.NotNil(t, out.Containers[0].ReadinessProbe.TCPSocket)
	assert.Equal(t, int32(8080), out.Containers[0].ReadinessProbe.TCPSocket.Port.IntVal)
	assert.Equal(t, "256Mi", out.Containers[0].Resources.Limits.Memory().String())
	require.Len(t, out.Volumes, 2)
	assert.NotNil(t, out.Volumes[0].EmptyDir)
	require.NotNil(t, out.Volumes[1].Secret)
	assert.Equal(t, "tls-secret", out.Volumes[1].Secret.SecretName)
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
// shared emptyDir mounted read-only at the profile's code mount dir.
func TestApplyCodeMount_InjectsInitContainer(t *testing.T) {
	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "function"}}}
	cfg := Config{CodeURL: "http://jaiscloud:8080/_jaiscloud", InitImage: "alpine:latest"}
	req := Request{FunctionName: "fn", AccountID: "proj", CodeKey: "us-central1.hello"}

	applyCodeMount(&spec, cfg, req, fakeCodeLoader{}, "/var/task")

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
// in the URL falls back to FunctionName when no explicit CodeKey is set, and that
// a missing loader or empty CodeURL disables the mount entirely.
func TestApplyCodeMount_CodeKeyFallsBackToFunctionName(t *testing.T) {
	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "function"}}}
	applyCodeMount(&spec, Config{CodeURL: "http://x/_jaiscloud"}, Request{AccountID: "acct", FunctionName: "my-fn"}, fakeCodeLoader{}, "/var/task")

	require.Len(t, spec.InitContainers, 1)
	assert.Contains(t, spec.InitContainers[0].Env[0].Value, "/lambda/code/acct/my-fn/$LATEST")
}

func TestApplyCodeMount_DisabledWithoutLoaderOrURL(t *testing.T) {
	base := func() k8stypes.PodSpec {
		return k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "function"}}}
	}

	spec := base()
	applyCodeMount(&spec, Config{CodeURL: "http://x/_jaiscloud"}, Request{}, nil, "/var/task")
	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	assert.Empty(t, spec.Containers[0].VolumeMounts)

	spec = base()
	applyCodeMount(&spec, Config{}, Request{}, fakeCodeLoader{}, "/var/task")
	assert.Empty(t, spec.InitContainers)
	assert.Empty(t, spec.Volumes)
	assert.Empty(t, spec.Containers[0].VolumeMounts)
}

// TestApplyCodeMount_PreservesExistingInitContainers guards the platform layer:
// TLS (or other) init containers added before the code mount must survive.
func TestApplyCodeMount_PreservesExistingInitContainers(t *testing.T) {
	spec := k8stypes.PodSpec{
		Containers:     []k8stypes.Container{{Name: "function"}},
		InitContainers: []k8stypes.Container{{Name: "tls-materialize"}},
	}
	applyCodeMount(&spec, Config{CodeURL: "http://x/_jaiscloud"}, Request{AccountID: "p", FunctionName: "fn"}, fakeCodeLoader{}, "/var/task")

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
// "$LATEST" qualifier reaches wget intact.
func TestApplyCodeMount_CommandKeepsLatestQualifier(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh not available")
	}

	dir := t.TempDir()
	record := filepath.Join(dir, "url")
	writeExec(t, filepath.Join(dir, "wget"), "#!/bin/sh\nfor a in \"$@\"; do last=\"$a\"; done\nprintf '%s' \"$last\" > "+record+"\n")
	writeExec(t, filepath.Join(dir, "unzip"), "#!/bin/sh\nexit 0\n")

	spec := k8stypes.PodSpec{Containers: []k8stypes.Container{{Name: "function"}}}
	applyCodeMount(&spec, Config{CodeURL: "http://x/_jaiscloud"}, Request{AccountID: "p", CodeKey: "us-central1.hello"}, fakeCodeLoader{}, "/var/task")
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
